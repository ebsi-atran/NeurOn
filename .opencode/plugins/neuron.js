// NeurOn plugin - auto-loads from ~/.config/opencode/plugins/
// Manages reservation lifecycle: cold-start detection → reservation → warmup wait → healthy
// Config via env: NEURON_API_BASE_URL, NEURON_API_KEY, NEURON_ALLOWED_PROVIDERS (optional provider filter)


const DEFAULT_POLL_S = 5;
const DEFAULT_DURATION_MINUTES = 2;
const DEFAULT_WAIT_TIMEOUT_S = 600;

let _statusCache = null;
let _statusCacheTime = 0;
let _statusInflight = null;
const STATUS_CACHE_TTL = 10000;

const state = {
  reservations: new Map(),
  inflight: new Map(),
  inflightTarget: new Map(),
  retryState: new Map()
};
let _lastTransportFailure = 0;

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

  async request(path, options = {}, requestTimeoutMs) {
    if (!this.config.apiKey)
      throw new Error("NEURON_API_KEY is required for the NeurOn OpenCode plugin");
    const controller = new AbortController();
    const timeout = requestTimeoutMs ?? this.config.requestTimeoutMs;
    const timer = setTimeout(() => controller.abort(), timeout);
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
    preflightTimeoutMs: positiveNumber(process.env.NEURON_PREFLIGHT_TIMEOUT_MS, 2000),
    cooldownPeriodMs: positiveNumber(process.env.NEURON_COOLDOWN_PERIOD_MS, 30000),
    retryMaxAttempts: positiveNumber(process.env.NEURON_RETRY_MAX_ATTEMPTS, 3),
    retryBaseMs: positiveNumber(process.env.NEURON_RETRY_BASE_MS, 1000),
    retryMaxMs: positiveNumber(process.env.NEURON_RETRY_MAX_MS, 8000),
    blockOnColdMessage: boolEnv(process.env.NEURON_BLOCK_ON_COLD_MESSAGE, true),
    strictProviderMatch: boolEnv(process.env.NEURON_STRICT_PROVIDER_MATCH, true),
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

function canonicalizeModel(provider, modelId) {
  const split = splitProvider(modelId ?? "");
  const finalProvider = provider ?? split.provider;
  const bareModelId = split.bareModelId;
  const fullModel = finalProvider ? `${finalProvider}/${bareModelId}` : bareModelId;
  return { provider: finalProvider, bareModelId, fullModel };
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

function matchLiteLlmModel(targets, models, bareModelId, provider, strictProviderMatch = true) {
  const modelByLookup = buildModelLookup(models);

  // ── Pass 1: Model lookup with provider preference ────────────────
  const model = modelByLookup.get(bareModelId);
  if (model && model.targetIds?.length) {
    if (provider) {
      const pLower = provider.toLowerCase();
      const providerFallbackTargets = [];
      for (const target of targets) {
        if (model.targetIds.includes(target.id) &&
            target.provider?.toLowerCase() === pLower) {
          return { modelIds: [model.id], targetIds: [target.id] };
        }
        if (model.targetIds.includes(target.id)) {
          providerFallbackTargets.push(target);
        }
      }
      if (!strictProviderMatch && providerFallbackTargets.length === 1) {
        return { modelIds: [model.id], targetIds: [providerFallbackTargets[0].id] };
      }
      if (!strictProviderMatch && providerFallbackTargets.length > 1) {
        const providers = [...new Set(providerFallbackTargets.map((t) => (t.provider || "unknown").toLowerCase()))];
        return { error: `provider_mapping_error`, detail: `Model "${bareModelId}" is on multiple NeurOn providers (${providers.join(", ")}). Configure provider mapping or use strict provider labels.` };
      }
      // Provider specified but no matching target — return error (no fallback)
      return { error: `provider_mapping_error`, detail: `Model "${bareModelId}" not found on provider "${provider}".` };
    }
    // No provider — collect providers hosting this model for ambiguity check
    const pass1Providers = new Set();
    let pass1Primary = null;
    for (const target of targets) {
      if (model.targetIds.includes(target.id)) {
        if (!pass1Primary) pass1Primary = target;
        if (target.provider) pass1Providers.add(target.provider.toLowerCase());
      }
    }
    if (pass1Providers.size > 1) {
      return { error: `ambiguous_model_mapping`, detail: `Model "${bareModelId}" is available on providers: ${[...pass1Providers].join(", ")}. Specify provider explicitly.` };
    }
    if (pass1Primary) {
      return { modelIds: [model.id], targetIds: [pass1Primary.id] };
    }
  }

  // ── Pass 2: Direct target modelIds match ─────────────────────────
  let primaryMatch = null;
  const providerFallbackMatches = [];
  let altProviders = new Set();

  for (const target of targets) {
    if (!target.modelIds?.includes(bareModelId)) continue;

    if (provider) {
      const tProv = target.provider?.toLowerCase();
      if (tProv === provider.toLowerCase()) {
        return { modelIds: [bareModelId], targetIds: [target.id] };
      }
      providerFallbackMatches.push(target);
      if (tProv) altProviders.add(tProv);
    } else {
      if (!primaryMatch) primaryMatch = target;
      if (target.provider) {
        altProviders.add(target.provider.toLowerCase());
      }
    }
  }

  // Provider was given but no exact match — return error deterministically
  if (provider && altProviders.size > 0) {
    if (!strictProviderMatch && providerFallbackMatches.length === 1) {
      return { modelIds: [bareModelId], targetIds: [providerFallbackMatches[0].id] };
    }
    if (!strictProviderMatch && providerFallbackMatches.length > 1) {
      return { error: `provider_mapping_error`, detail: `Model "${bareModelId}" is on multiple NeurOn providers (${[...altProviders].join(", ")}). Configure provider mapping or use strict provider labels.` };
    }
    return { error: `provider_mapping_error`, detail: `Model "${bareModelId}" not found on provider "${provider}". Available providers: ${[...altProviders].join(", ")}.` };
  }

  // No provider specified but multiple providers host this model — ambiguous
  if (!provider && altProviders.size > 1) {
    return { error: `ambiguous_model_mapping`, detail: `Model "${bareModelId}" is available on providers: ${[...altProviders].join(", ")}. Specify provider explicitly.` };
  }

  if (primaryMatch) {
    return { modelIds: [bareModelId], targetIds: [primaryMatch.id] };
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
  // 1. Return cached resolved status while TTL valid
  if (_statusCache && Date.now() - _statusCacheTime < STATUS_CACHE_TTL) {
    return _statusCache;
  }
  // 2. If stale and fetch in-flight, await the in-flight request
  if (_statusInflight) {
    return _statusInflight;
  }
  // 3. Fetch new, set inflight, save resolved value+time, clear inflight
  _statusInflight = client.getStatus().then((result) => {
    _statusCache = result;
    _statusCacheTime = Date.now();
    _statusInflight = null;
    return result;
  }).catch((e) => {
    _statusInflight = null;
    _lastTransportFailure = Date.now();
    throw e;
  });
  return _statusInflight;
}

// ── Reservation flow (keyed by model ID + target ID) ────────

function ensureReservation(client, modelId, sessionID) {
  const inflightKey = `${sessionID}::${modelId}`;
  const existing = state.inflight.get(inflightKey);
  if (existing) {
    return existing;
  }

  const promise = resolveTargetForModel(client, modelId, sessionID)
    .then(async ({ targetId, match }) => {
      // Secondary dedup at target level for models sharing targets
      const targetInflightKey = `${sessionID}::${targetId}`;
      const targetInflight = state.inflightTarget.get(targetInflightKey);
      if (targetInflight) return targetInflight;

      const p = reserveOrRefreshTarget(client, targetId, match, sessionID).finally(() => {
        state.inflightTarget.delete(targetInflightKey);
      });
      state.inflightTarget.set(targetInflightKey, p);
      return p;
    })
    .finally(() => { state.inflight.delete(inflightKey); });

  state.inflight.set(inflightKey, promise);
  return promise;
}

async function resolveTargetForModel(client, modelId, sessionID) {
  const status = await getCachedStatus(client);
  const splitResult = splitProvider(modelId);
  const match = matchLiteLlmModel(
    status.capacityTargets ?? [],
    status.models ?? [],
    splitResult.bareModelId,
    splitResult.provider,
    client.config.strictProviderMatch
  );
  if (!match)
    throw new Error(
      `NeurOn could not map OpenCode model "${modelId}" to a capacity target`
    );
  if (match.error)
    throw new Error(`NeurOn ${match.error}: ${match.detail}`);
  const targetId = match.targetIds[0];
  const targetInfo = findTargetStatus(status.capacityTargets ?? [], targetId);
  const targetHealthy = targetInfo?.observed === "healthy";
  const resKey = `${sessionID}::${targetId}`;

  // If target is healthy and this session has no local reservation, adopt the server-side one
  if (targetHealthy && !state.reservations.has(resKey)) {
    try {
      adoptExistingReservation(targetId, status, sessionID);
    } catch (e) {
      /* ignore — we'll create a new reservation if needed */
    }
  }

  return { targetId, match, targetHealthy, resKey };
}

// Determine if an error is transient and worth retrying.
// Transient: timeout (status 0), rate-limited (429), server errors (5xx).
// Permanent (no retry): 4xx client errors, mapping/config errors.
function isTransientError(err) {
  if (err instanceof NeurOnApiError) {
    // status 0 = timeout/network failure → transient
    if (err.status === 0) return true;
    // 429 rate limited → transient
    if (err.status === 429) return true;
    // 5xx server errors → transient
    if (err.status >= 500 && err.status < 600) return true;
    // All other 4xx are permanent — do not retry
    return false;
  }
  // Network/transport errors (non-NeurOnApiError) → assume transient
  return true;
}

// Bounded exponential backoff with jitter for reservation retries.
async function retryWithBackoff(key, fn, maxAttempts, baseMs, maxMs) {
  const rs = state.retryState.get(key) ?? { attempts: 0, nextDelay: baseMs };
  let lastErr;
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      const result = await fn();
      // Success — reset retry state immediately
      state.retryState.delete(key);
      return result;
    } catch (err) {
      lastErr = err;
      // Permanent errors — fail fast without further retries
      if (!isTransientError(err)) throw err;
      if (attempt < maxAttempts - 1) {
        const jitter = Math.random() * rs.nextDelay;
        const delay = rs.nextDelay + jitter;
        await sleep(Math.min(delay, maxMs));
        rs.nextDelay = Math.min(rs.nextDelay * 2, maxMs);
        rs.attempts = attempt + 1;
      }
    }
  }
  state.retryState.delete(key);
  throw lastErr;
}

async function reserveOrRefreshTarget(client, targetId, match, sessionID) {
  const resKey = `${sessionID}::${targetId}`;
  const existingEntry = state.reservations.get(resKey);
  if (existingEntry) {
    if (existingEntry.expiresAt < Date.now()) {
      state.reservations.delete(resKey);
    } else {
      try {
        const refreshed = await client.refreshReservation(existingEntry.reservation.reservationId);
        return saveReservation(targetId, refreshed, sessionID);
      } catch (error) {
        state.reservations.delete(resKey);
      }
    }
  }

  // Fall through to create new reservation (with retry backoff + jitter)
  const retryKey = `${sessionID}::${targetId}::reserve`;
  const reservation = await retryWithBackoff(
    retryKey,
    () => client.createReservation(match),
    client.config.retryMaxAttempts,
    client.config.retryBaseMs,
    client.config.retryMaxMs
  );
  saveReservation(targetId, reservation, sessionID);
  try {
    if (client.config.waitForHealthy) {
      await client.waitForHealthy(reservation.reservationId);
    }
  } catch (e) {
    state.reservations.delete(resKey);
    throw e;
  }
  return reservation;
}

function saveReservation(targetId, reservation, sessionID) {
  const entry = {
    reservation,
    expiresAt: Date.now() + (reservation.durationMinutes ?? DEFAULT_DURATION_MINUTES) * 60 * 1000
  };
  state.reservations.set(`${sessionID}::${targetId}`, entry);
  return reservation;
}

function adoptExistingReservation(targetId, status, sessionID) {
  const active = status.activeReservations ?? status.reservations ?? [];
  for (const res of active) {
    const targets = res.targets ?? [];
    for (const t of targets) {
      if (t.id === targetId && res.status === "active") {
        saveReservation(targetId, res, sessionID);
        return res;
      }
    }
  }
  return null;
}

async function refreshExistingReservation(client, modelId, sessionID) {
  try {
    const { targetId } = await resolveTargetForModel(client, modelId, sessionID);
    const key = `${sessionID}::${targetId}`;
    const existingEntry = state.reservations.get(key);
    if (!existingEntry) return undefined;
    try {
      const refreshed = await client.refreshReservation(existingEntry.reservation.reservationId);
      saveReservation(targetId, refreshed, sessionID);
      return refreshed;
    } catch (e) {
      // Refresh failed — drop the stale entry so it does not linger until expiry.
      state.reservations.delete(key);
      return undefined;
    }
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

// Extract the OpenCode session identifier from any event or hook input.
// Handles both the camelCase `sessionID` (primary; confirmed against OpenCode's
// Event schema and hook input types) and camelCase-lower `sessionId` fallbacks.
function extractSessionID(event) {
  return (
    event?.sessionID ??
    event?.sessionId ??
    event?.properties?.sessionID ??
    event?.properties?.sessionId ??
    undefined
  );
}

// Release every piece of per-session state for a given sessionID. Prevents the
// sessionModels map (and any stale reservations/inflight entries) from leaking
// across the lifetime of a long-running TUI that opens and closes many sessions.
function scrubSession(sessionID, sessionModels) {
  if (!sessionID) return;
  const prefix = `${sessionID}::`;
  for (const key of [...state.reservations.keys()])
    if (key.startsWith(prefix)) state.reservations.delete(key);
  for (const key of [...state.inflight.keys()])
    if (key.startsWith(prefix)) state.inflight.delete(key);
  for (const key of [...state.inflightTarget.keys()])
    if (key.startsWith(prefix)) state.inflightTarget.delete(key);
  for (const key of [...state.retryState.keys()])
    if (key.startsWith(prefix)) state.retryState.delete(key);
  sessionModels?.delete(sessionID);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ── Background reservation (non-blocking) ─────────────────

async function checkTargetHealthy(client, modelId) {
  try {
    const status = await getCachedStatus(client);
    const splitResult = splitProvider(modelId);
    const match = matchLiteLlmModel(
      status.capacityTargets ?? [],
      status.models ?? [],
      splitResult.bareModelId,
      splitResult.provider,
      client.config.strictProviderMatch
    );
    if (!match) return "cold";
    if (match.error) return "unreachable";
    const targetInfo = findTargetStatus(
      status.capacityTargets ?? [],
      match.targetIds[0]
    );
    return targetInfo?.observed ?? "cold";
  } catch (e) {
    // API unreachable/unknown — distinguish from true cold so callers can fail open.
    return "unreachable";
  }
}

// Preflight variant with a hard timeout budget — used by tool.execute.before.
async function checkTargetHealthyWithTimeout(client, modelId, timeoutMs) {
  try {
    const result = await Promise.race([
      checkTargetHealthy(client, modelId),
      new Promise((resolve) => setTimeout(() => resolve("unreachable"), timeoutMs))
    ]);
    return result;
  } catch (e) {
    return "unreachable";
  }
}

function backgroundReserve(client, modelId, sessionID, sessionModels, ctx) {
  (async () => {
    try {
      await ensureReservation(client, modelId, sessionID);
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
      // Notify user only once per session — classify the failure type
      const info = sessionModels.get(sessionID);
      if (!info || !ctx.client?.tui?.showToast) return;

      if (e.message?.includes("ambiguous")) {
        if (!info.errorNotified) {
          info.errorNotified = true;
          ctx.client.tui.showToast({
            body: { message: e.message, variant: "error" }
          });
        }
        return;
      }

      if (e instanceof NeurOnApiError) {
        if (e.status === 0) {
          // Timeout/unreachable — fail open silently
          return;
        }
        if (!info.errorNotified) {
          info.errorNotified = true;
          let msg = `NeurOn: reservation failed`;
          if (e.status === 401 || e.status === 403) msg += ' (authentication error)';
          else if (e.status === 429) msg += ' (rate limited — wait and retry)';
          else if (e.status >= 500) msg += ' (server error)';
          else msg += ` (HTTP ${e.status})`;
          ctx.client.tui.showToast({
            body: { message: msg, variant: "error" }
          });
        }
        return;
      }

      // Generic cold/stopped warmup notification — only for truly cold states
      if (!info.warmupNotified) {
        info.warmupNotified = true;
        ctx.client.tui.showToast({
          body: {
            message: `NeurOn: target cold, warming up… please retry in 2-3 min`,
            variant: "warning"
          }
        });
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
      // Prefer camelCase `sessionID`; fall back to `sessionId` defensively.
      const sessionID = extractSessionID(event);
      if (!sessionID) return;

      if (type === "plugin.added" || type === "message.part.delta") return;

      // Release all per-session state only on true terminal events.
      // session.compacted keeps the session alive (model/reservation state remains valid).
      if (type === "session.deleted") {
        scrubSession(sessionID, sessionModels);
        return;
      }

      // CAPTURE model from session.created
      if (type === "session.created" && props.info?.model) {
        const m = props.info.model;
        const normalized = canonicalizeModel(m.providerID, m.id);
        sessionModels.set(sessionID, {
          id: normalized.bareModelId,
          provider: normalized.provider
        });
        return;
      }

      // Prefer model from current event (handles model switching within same session)
      const eventModel = props?.info?.model;
      const cachedModel = sessionModels.get(sessionID);
      const rawModel = eventModel?.id ?? cachedModel?.id ?? event?.model;
      if (!rawModel) return;

      const normalizedCurrent = canonicalizeModel(
        eventModel?.providerID ?? cachedModel?.provider,
        rawModel
      );
      const model = normalizedCurrent.bareModelId;
      const provider = normalizedCurrent.provider;
      const fullModel = normalizedCurrent.fullModel;

      // Guard: prevent NeurOn from reserving its own traffic (recursive routing)
      if (provider) {
        const p = provider.toLowerCase();
        if (p === 'neuron' || p === 'neuron-bridge' || p === 'opencode-neuron') return;
      }

      if (eventModel?.id) {
        const normalizedEvent = canonicalizeModel(eventModel.providerID, eventModel.id);
        if (normalizedEvent.bareModelId !== cachedModel?.id || normalizedEvent.provider !== cachedModel?.provider) {
        try {
          const oldFullModel = cachedModel?.provider
            ? `${cachedModel.provider}/${cachedModel.id}`
            : cachedModel?.id;
          if (oldFullModel) {
            const oldTarget = await resolveTargetForModel(client, oldFullModel, sessionID);
            state.reservations.delete(oldTarget.resKey);
          }
        } catch (e) { /* ignore cleanup errors */ }

          sessionModels.set(sessionID, {
            id: normalizedEvent.bareModelId,
            provider: normalizedEvent.provider
          });
        }
      }

      const role =
        event.role ?? event.properties?.info?.role ?? event.properties?.role;

      // Pre-request: check target health on user message
      if (type === "message.updated" && role === "user") {
        if (!matchesAllowedProvider(provider, fullModel, allowedProviders)) return;

        const targetState = await checkTargetHealthyWithTimeout(
          client, fullModel, client.config.preflightTimeoutMs
        );

        if (targetState === "healthy") {
          // Target already running — background any reserve/refresh work entirely.
          // Do NOT block the message path for network I/O.
          const info = sessionModels.get(sessionID);
          if (info) info.stoppingNotified = false;
          (async () => {
            try {
              const result = await resolveTargetForModel(client, fullModel, sessionID);
              if (!state.reservations.has(result.resKey)) {
                await ensureReservation(client, fullModel, sessionID);
              } else {
                await refreshExistingReservation(client, fullModel, sessionID);
              }
            } catch (e) { /* ignore */ }
          })();
          return;
        }

        if (targetState === "stopping") {
          // Target is shutting down — clear stale reservation, notify user
          try {
            const result = await resolveTargetForModel(client, fullModel, sessionID);
            state.reservations.delete(result.resKey);
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
          if (client.config.blockOnColdMessage) {
            throw new Error("NeurOn: target is stopping, warming up - please retry in 2-3 min");
          }
          return;
        }

        // NeurOn API unreachable — fail open, no toast, no warmup trigger
        if (targetState === "unreachable") {
          // Still attempt a silent background reserve; if transport is truly down,
          // backgroundReserve will fail open quietly for timeout/unreachable errors.
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
        if (client.config.blockOnColdMessage) {
          throw new Error("NeurOn: target is cold, warming up - please retry in 2-3 min");
        }
        return;
      }

      if (type === "session.error") {
        if (!matchesAllowedProvider(provider, fullModel, allowedProviders)) return;
        // Background the reservation — do not block on potentially long
        // waitForHealthy. Error toasts are handled inside backgroundReserve.
        backgroundReserve(client, fullModel, sessionID, sessionModels, ctx);
      }

      // Refresh reservation on session idle (keepalive)
      if (type === "session.idle") {
        if (!matchesAllowedProvider(provider, fullModel, allowedProviders)) return;
        refreshExistingReservation(client, fullModel, sessionID).catch(() => {});
      }
    },

    "tool.execute.before": async ({ event }) => {
      try {
        const props = event?.properties || {};
        // sessionID arrives at the top level of the hook input in OpenCode's
        // documented signature; fall back to properties defensively.
        const sessionID = extractSessionID(event);
        if (!sessionID) return;

        // A tool may execute before the session.created event is processed, so
        // hydrate from the event's own model info when the cache is empty.
        let cachedModel = sessionModels.get(sessionID);
        if (!cachedModel && props?.info?.model) {
          const normalized = canonicalizeModel(props.info.model.providerID, props.info.model.id);
          cachedModel = {
            id: normalized.bareModelId,
            provider: normalized.provider
          };
          sessionModels.set(sessionID, cachedModel);
        }
        const model = cachedModel?.id;
        if (!model) return;

        const provider = cachedModel?.provider;
        const fullModel = provider ? `${provider}/${model}` : model;
        if (!matchesAllowedProvider(provider, fullModel, allowedProviders)) return;

        // Fail-open cooldown: skip health check if recent transport failure.
        if (Date.now() - _lastTransportFailure < client.config.cooldownPeriodMs) {
          return;
        }

        // Use fast preflight timeout for health check to avoid blocking tool execution.
        const targetState = await checkTargetHealthyWithTimeout(
          client, fullModel, client.config.preflightTimeoutMs
        );
        if (targetState === "unreachable") {
          // API unreachable — set transport-failure timestamp so cooldown activates
          _lastTransportFailure = Date.now();
          return;
        }
        if (targetState !== "healthy") {
          backgroundReserve(client, fullModel, sessionID, sessionModels, ctx);
          throw new Error(`NeurOn: target is ${targetState}, warming up — please retry in 2-3 min`);
        }
      } catch (e) {
        if (e.message?.includes("NeurOn:")) throw e;
        // API unreachable — fail open to avoid blocking tool execution
      }
    },

    // Release all per-session state on plugin shutdown so nothing lingers
    // between sessions in a long-running process.
    dispose: async () => {
      state.reservations.clear();
      state.inflight.clear();
      state.inflightTarget.clear();
      state.retryState.clear();
      sessionModels.clear();
      _statusCache = null;
      _statusCacheTime = 0;
      _statusInflight = null;
      _lastTransportFailure = 0;
    }
  };
};
