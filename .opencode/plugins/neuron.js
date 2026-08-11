// NeurOn plugin - auto-loads from ~/.config/opencode/plugins/
// Manages reservation lifecycle: cold-start detection → reservation → warmup wait → healthy
// Config via env: NEURON_API_BASE_URL, NEURON_API_KEY, NEURON_ALLOWED_PROVIDERS (optional provider filter)

const DEFAULT_POLL_S = 5;
const DEFAULT_DURATION_MINUTES = 2;
const DEFAULT_WAIT_TIMEOUT_S = 600;

let _statusCache = null;
let _statusCacheTime = 0;
let _statusCacheId = 0;
const STATUS_CACHE_TTL = 10000;

const state = {
  reservations: new Map(),
  inflight: new Map(),
  inflightTarget: new Map()
};

class NeurOnClient {
  constructor(config) {
    this.config = config;
  }

  async getStatus() {
    const [status, models] = await Promise.all([
      this.request("/api/status"),
      this.request("/api/models")
    ]);
    return { ...status, models: models.models ?? [] };
  }

  async createReservation(match) {
    return this.request("/api/reservations", {
      method: "POST",
      body: JSON.stringify({
        modelIds: match.modelIds,
        targetIds: match.targetIds,
        durationMinutes: this.config.durationMinutes,
        keepaliveMinutes: this.config.keepaliveMinutes
      })
    });
  }

  async refreshReservation(reservationId) {
    return this.request(`/api/reservations/${encodeURIComponent(reservationId)}/extend`, {
      method: "POST",
      body: JSON.stringify({
        durationMinutes: this.config.durationMinutes,
        fromNow: true
      })
    });
  }

  async waitForHealthy(reservationId) {
    const deadline = Date.now() + this.config.waitTimeoutMs;
    let lastReservation;
    while (Date.now() <= deadline) {
      lastReservation = await this.request(
        `/api/reservations/${encodeURIComponent(reservationId)}/status`
      );
      if (lastReservation.targets?.every((t) => t.observed === "healthy"))
        return lastReservation;
      const failed = lastReservation.targets?.find((t) => t.observed === "failed");
      if (failed)
        throw new Error(`NeurOn target ${failed.id} failed: ${failed.message}`);
      await sleep(this.config.pollMs);
    }
    const states = (lastReservation?.targets ?? [])
      .map((t) => `${t.id}:${t.observed}`)
      .join(", ");
    throw new Error(
      `Timed out waiting for NeurOn reservation ${reservationId} to become healthy${states ? ` (${states})` : ""}`
    );
  }

  async request(path, options = {}) {
    if (!this.config.apiKey)
      throw new Error("NEURON_API_KEY is required for the NeurOn OpenCode plugin");
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.config.requestTimeoutMs);
    try {
      const response = await fetch(`${this.config.apiBaseUrl}${path}`, {
        ...options,
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${this.config.apiKey}`,
          ...(options.headers ?? {})
        },
        signal: controller.signal
      });
      if (!response.ok) {
        const body = await response.text();
        throw new NeurOnApiError(response.status, path, body, response.statusText);
      }
      const raw = await response.text();
      try {
        return JSON.parse(raw);
      } catch (parseErr) {
        throw new NeurOnApiError(0, path, `Failed to parse response: ${raw}`, 'invalid_json');
      }
    } catch (error) {
      if (error?.name === 'AbortError') {
        throw new NeurOnApiError(0, path, 'Request timed out', 'timeout');
      }
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }
}

class NeurOnApiError extends Error {
  constructor(status, path, body, statusText) {
    super(`NeurOn API ${status} for ${path}: ${body || statusText}`);
    this.status = status;
  }
}

// ── Config ────────────────────────────────────────────────

function loadConfig() {
  const raw = process.env.NEURON_ALLOWED_PROVIDERS;
  const allowedProviders = raw
    ? raw.split(",").map((p) => p.trim()).filter(Boolean)
    : [];
  const baseUrl = process.env.NEURON_API_BASE_URL || "http://localhost:8090";
  if (!baseUrl.startsWith("http://") && !baseUrl.startsWith("https://"))
    throw new Error("NEURON_API_BASE_URL must be a valid http:// or https:// URL");
  return {
    apiBaseUrl: trimSlash(baseUrl),
    apiKey: process.env.NEURON_API_KEY,
    durationMinutes: positiveNumber(
      process.env.NEURON_RESERVATION_DURATION_MINUTES,
      DEFAULT_DURATION_MINUTES
    ),
    keepaliveMinutes: positiveNumber(
      process.env.NEURON_RESERVATION_KEEPALIVE_MINUTES,
      DEFAULT_DURATION_MINUTES
    ),
    waitForHealthy: boolEnv(process.env.NEURON_WAIT_FOR_HEALTHY, true),
    waitTimeoutMs: positiveNumber(process.env.NEURON_WAIT_TIMEOUT_SECONDS, DEFAULT_WAIT_TIMEOUT_S) * 1000,
    pollMs: positiveNumber(process.env.NEURON_WAIT_POLL_SECONDS, DEFAULT_POLL_S) * 1000,
    requestTimeoutMs: positiveNumber(process.env.NEURON_REQUEST_TIMEOUT_MS, 5000),
    allowedProviders
  };
}

// ── Model / provider helpers ──────────────────────────────

function splitProvider(modelId) {
  const slash = modelId.indexOf("/");
  if (slash > 0 && slash < modelId.length - 1) {
    return { provider: modelId.slice(0, slash), bareModelId: modelId.slice(slash + 1) };
  }
  return { provider: undefined, bareModelId: modelId };
}

function matchesAllowedProvider(providerId, modelId, allowedProviders) {
  if (!allowedProviders.length) return true;
  if (providerId) {
    for (const p of allowedProviders)
      if (providerId.toLowerCase() === p.toLowerCase()) return true;
    return false;
  }
  for (const p of allowedProviders)
    if (modelId.startsWith(p + "/")) return true;
  return false;
}

// ── Model → target matching ───────────────────────────────

function matchLiteLlmModel(targets, models, bareModelId) {
  const modelByLookup = buildModelLookup(models);

  // Try bare model ID first (e.g. "qwen-3.6-27b") against model lookup
  const model = modelByLookup.get(bareModelId);
  if (model && model.targetIds?.length) {
    for (const target of targets) {
      if (model.targetIds.includes(target.id)) {
        return { modelIds: [model.id], targetIds: [target.id] };
      }
    }
  }

  // Fallback: match bareModelId directly against any target's modelIds
  for (const target of targets) {
    if (target.modelIds?.includes(bareModelId)) {
      return { modelIds: [bareModelId], targetIds: [target.id] };
    }
  }

  return undefined;
}

function buildModelLookup(models) {
  const lookup = new Map();
  for (const model of models) {
    for (const id of [
      model.id,
      ...(model.aliases ?? []),
      ...(model.backendModelIds ?? []),
      ...(model.runtimeModelIds ?? [])
    ]) {
      if (id) lookup.set(id, model);
    }
  }
  return lookup;
}

function findTargetStatus(targets, targetId) {
  for (const t of targets)
    if (t.id === targetId) return t;
  return undefined;
}

async function getCachedStatus(client) {
  if (_statusCache && Date.now() - _statusCacheTime < STATUS_CACHE_TTL) {
    return _statusCache;
  }
  _statusCacheTime = Date.now();
  const myId = ++_statusCacheId;
  _statusCache = client.getStatus().finally(() => {
    if (_statusCacheId === myId) _statusCache = null;
  });
  return _statusCache;
}

// ── Reservation flow (keyed by model ID + target ID) ────────

function ensureReservation(client, modelId) {
  const existing = state.inflight.get(modelId);
  if (existing) {
    return existing;
  }

  const promise = resolveTargetForModel(client, modelId)
    .then(async ({ targetId, match }) => {
      // Secondary dedup at target level for models sharing targets
      const targetInflight = state.inflightTarget.get(targetId);
      if (targetInflight) return targetInflight;

      const p = reserveOrRefreshTarget(client, targetId, match).finally(() => {
        state.inflightTarget.delete(targetId);
      });
      state.inflightTarget.set(targetId, p);
      return p;
    })
    .finally(() => { state.inflight.delete(modelId); });

  state.inflight.set(modelId, promise);
  return promise;
}

async function resolveTargetForModel(client, modelId) {
  const status = await getCachedStatus(client);
  const splitResult = splitProvider(modelId);
  const { bareModelId } = splitResult;
  const match = matchLiteLlmModel(
    status.capacityTargets ?? [],
    status.models ?? [],
    bareModelId
  );
  if (!match)
    throw new Error(
      `NeurOn could not map OpenCode model "${modelId}" to a capacity target`
    );
  const targetId = match.targetIds[0];
  const targetInfo = findTargetStatus(status.capacityTargets ?? [], targetId);
  const targetHealthy = targetInfo?.observed === "healthy";

  // If target is healthy and we have no local reservation, adopt the server-side one
  if (targetHealthy && !state.reservations.has(targetId)) {
    try {
      adoptExistingReservation(targetId, status);
    } catch (e) {
      /* ignore — we'll create a new reservation if needed */
    }
  }

  return { targetId, match, targetHealthy };
}

async function reserveOrRefreshTarget(client, targetId, match) {
  const existingEntry = state.reservations.get(targetId);
  if (existingEntry) {
    if (existingEntry.expiresAt < Date.now()) {
      state.reservations.delete(targetId);
    } else {
      try {
        const refreshed = await client.refreshReservation(existingEntry.reservation.reservationId);
        return saveReservation(targetId, refreshed);
      } catch (error) {
        state.reservations.delete(targetId);
      }
    }
  }

  // Fall through to create new reservation
  const reservation = await client.createReservation(match);
  saveReservation(targetId, reservation);
  try {
    if (client.config.waitForHealthy) {
      await client.waitForHealthy(reservation.reservationId);
    }
  } catch (e) {
    state.reservations.delete(targetId);
    throw e;
  }
  return reservation;
}

function saveReservation(targetId, reservation) {
  const entry = {
    reservation,
    expiresAt: Date.now() + (reservation.durationMinutes ?? DEFAULT_DURATION_MINUTES) * 60 * 1000
  };
  state.reservations.set(targetId, entry);
  return reservation;
}

function adoptExistingReservation(targetId, status) {
  const active = status.activeReservations ?? status.reservations ?? [];
  for (const res of active) {
    const targets = res.targets ?? [];
    for (const t of targets) {
      if (t.id === targetId && res.status === "active") {
        saveReservation(targetId, res);
        return res;
      }
    }
  }
  return null;
}

async function refreshExistingReservation(client, modelId) {
  try {
    const { targetId } = await resolveTargetForModel(client, modelId);
    const existingEntry = state.reservations.get(targetId);
    if (!existingEntry) return undefined;
    const refreshed = await client.refreshReservation(existingEntry.reservation.reservationId);
    saveReservation(targetId, refreshed);
    return refreshed;
  } catch (e) {
    return undefined;
  }
}

// ── Utilities ─────────────────────────────────────────────

function positiveNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function boolEnv(value, fallback) {
  if (value === undefined) return fallback;
  return ["1", "true", "yes", "on"].includes(value.toLowerCase());
}

function trimSlash(value) {
  return value.replace(/\/+$/, "");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ── Background reservation (non-blocking) ─────────────────

async function checkTargetHealthy(client, modelId) {
  try {
    const status = await getCachedStatus(client);
    const { bareModelId } = splitProvider(modelId);
    const match = matchLiteLlmModel(
      status.capacityTargets ?? [],
      status.models ?? [],
      bareModelId
    );
    if (!match) return "cold";
    const targetInfo = findTargetStatus(
      status.capacityTargets ?? [],
      match.targetIds[0]
    );
    return targetInfo?.observed ?? "cold";
  } catch (e) {
    return "cold";
  }
}

function backgroundReserve(client, modelId, sessionID, sessionModels, ctx) {
  (async () => {
    try {
      await ensureReservation(client, modelId);
      const info = sessionModels.get(sessionID);
      if (info && ctx.client?.tui?.showToast) {
        info.warmupNotified = false;
        info.errorNotified = false;
        ctx.client.tui.showToast({
          body: {
            message: `NeurOn: model ready`,
            variant: "success"
          }
        });
      }
    } catch (e) {
      // Notify user only once per session
      const info = sessionModels.get(sessionID);
      if (info && !info.warmupNotified) {
        info.warmupNotified = true;
        if (ctx.client?.tui?.showToast) {
          ctx.client.tui.showToast({
            body: {
              message: `NeurOn: target cold, warming up… please retry in 2-3 min`,
              variant: "warning"
            }
          });
        }
      }
    }
  })();
}

// ── OpenCode plugin entry ─────────────────────────────────

export const NeurOnPlugin = async function NeurOnPlugin(ctx) {
  let client;
  let allowedProviders;
  try {
    client = new NeurOnClient(loadConfig());
    allowedProviders = client.config.allowedProviders;
  } catch (e) {
    return { event: () => {}, "tool.execute.before": () => {} };
  }

  // Track session -> model mapping from session.created events
  const sessionModels = new Map();

  return {
    event: async ({ event }) => {
      const type = event.type;
      const props = event?.properties || {};
      const sessionID = props.sessionID;

      if (type === "plugin.added" || type === "message.part.delta") return;

      // CAPTURE model from session.created
      if (type === "session.created" && props.info?.model) {
        const m = props.info.model;
        sessionModels.set(sessionID, {
          id: m.id,
          provider: m.providerID
        });
        return;
      }

      // Prefer model from current event (handles model switching within same session)
      const eventModel = props?.info?.model;
      const cachedModel = sessionModels.get(sessionID);
      const model = eventModel?.id ?? cachedModel?.id ?? event?.model;
      if (!model) return;

      const provider = eventModel?.providerID ?? cachedModel?.provider;
      const fullModel = provider
        ? `${provider}/${model}`
        : model;

      // Guard: prevent NeurOn from reserving its own traffic (recursive routing)
      if (provider) {
        const p = provider.toLowerCase();
        if (p === 'neuron' || p === 'neuron-bridge' || p === 'opencode-neuron') return;
      }

      if (eventModel?.id && eventModel.id !== cachedModel?.id) {
        try {
          const oldFullModel = cachedModel?.provider
            ? `${cachedModel.provider}/${cachedModel.id}`
            : cachedModel?.id;
          if (oldFullModel) {
            const oldTarget = await resolveTargetForModel(client, oldFullModel);
            state.reservations.delete(oldTarget.targetId);
          }
        } catch (e) { /* ignore cleanup errors */ }

        sessionModels.set(sessionID, { id: eventModel.id, provider: eventModel.providerID });
      }

      const role =
        event.role ?? event.properties?.info?.role ?? event.properties?.role;

      // Pre-request: check target health on user message
      if (type === "message.updated" && role === "user") {
        if (!matchesAllowedProvider(provider, fullModel, allowedProviders)) return;

        const targetState = await checkTargetHealthy(client, fullModel);

        if (targetState === "healthy") {
          // Target already running — refresh or create reservation
          try {
            const result = await resolveTargetForModel(client, fullModel);
            if (!state.reservations.has(result.targetId)) {
              await ensureReservation(client, fullModel);
            } else {
              await refreshExistingReservation(client, fullModel);
            }
          } catch (e) {
            /* ignore */
          }
          const info = sessionModels.get(sessionID);
          if (info) info.stoppingNotified = false;
          return;
        }

        if (targetState === "stopping") {
          // Target is shutting down — clear stale reservation, notify user
          try {
            const { targetId } = await resolveTargetForModel(client, fullModel);
            state.reservations.delete(targetId);
          } catch (e) {
            /* ignore */
          }
          const info = sessionModels.get(sessionID);
          if (info && !info.stoppingNotified) {
            info.stoppingNotified = true;
            if (ctx.client?.tui?.showToast) {
              ctx.client.tui.showToast({
                body: { message: "NeurOn: target stopping, restarting… please retry in 2-3 min", variant: "warning" }
              });
            }
          }
          backgroundReserve(client, fullModel, sessionID, sessionModels, ctx);
          return;
        }

        // Target is cold/stopped — fire reservation background, don't block
        const info = sessionModels.get(sessionID);
        if (info && !info.warmupNotified) {
          info.warmupNotified = true;
          if (ctx.client?.tui?.showToast) {
            ctx.client.tui.showToast({
              body: { message: "NeurOn: warming up… please retry in 2-3 min", variant: "warning" }
            });
          }
        }
        backgroundReserve(client, fullModel, sessionID, sessionModels, ctx);
        return;
      }

      if (type === "session.error") {
        if (!matchesAllowedProvider(provider, fullModel, allowedProviders)) return;
        try {
          await ensureReservation(client, fullModel);
        } catch (e) {
          const info = sessionModels.get(sessionID);
          if (info && !info.errorNotified && ctx.client?.tui?.showToast) {
            info.errorNotified = true;
            let msg = `NeurOn: reservation failed`;
            if (e instanceof NeurOnApiError) {
              if (e.status === 0) msg += ' (API timeout — check connectivity)';
              else if (e.status === 429) msg += ' (rate limited — wait and retry)';
              else if (e.status >= 500) msg += ' (server error — retrying automatically)';
              else msg += ` (HTTP ${e.status})`;
            }
            ctx.client.tui.showToast({
              body: { message: msg, variant: "error" }
            });
          }
        }
      }

      // Refresh reservation on session idle (keepalive)
      if (type === "session.idle") {
        if (!matchesAllowedProvider(provider, fullModel, allowedProviders)) return;
        refreshExistingReservation(client, fullModel).catch(() => {});
      }
    },

    "tool.execute.before": async ({ event }) => {
      try {
        const props = event?.properties || {};
        const sessionID = props.sessionID;
        const cachedModel = sessionModels.get(sessionID);
        const model = cachedModel?.id;
        if (!model) return;

        const provider = cachedModel?.provider;
        const fullModel = provider ? `${provider}/${model}` : model;
        if (!matchesAllowedProvider(provider, fullModel, allowedProviders)) return;

        const targetState = await checkTargetHealthy(client, fullModel);
        if (targetState !== "healthy") {
          backgroundReserve(client, fullModel, sessionID, sessionModels, ctx);
          throw new Error(`NeurOn: target is ${targetState}, warming up — please retry in 2-3 min`);
        }
      } catch (e) {
        if (e.message?.includes("NeurOn:")) throw e;
        // API unreachable — fail open to avoid blocking tool execution
      }
    }
  };
};
