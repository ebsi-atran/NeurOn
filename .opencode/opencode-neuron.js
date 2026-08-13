// NeurOn plugin - auto-loads from ~/.config/opencode/plugins/
// Manages reservation lifecycle: cold-start detection → reservation → warmup wait → healthy
// Config via env: NEURON_API_BASE_URL, NEURON_API_KEY, NEURON_ALLOWED_PROVIDERS (optional provider filter)

import { appendFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const DEFAULT_LOG_FILE = join(dirname(fileURLToPath(import.meta.url)), "opencode-neuron.log");
const SENSITIVE_LOG_KEY = /(?:api[_-]?key|authorization|proxy-authorization|token|secret|password|credential)/i;
const SENSITIVE_ISH_LOG_KEY = /(?:error|message|reason)/i;

function redactLogValue(value, key, depth = 0) {
  if (SENSITIVE_LOG_KEY.test(key ?? "")) return "[REDACTED]";
  if (SENSITIVE_ISH_LOG_KEY.test(key ?? ""))
    return terminalSafe(value instanceof Error ? value.message : String(value), 500);
  if (typeof value === "string") return terminalSafe(value, 500);
  if (value == null || typeof value === "number" || typeof value === "boolean") return value;
  if (depth >= 3) return "[TRUNCATED]";
  if (Array.isArray(value)) return value.slice(0, 20).map((item) => redactLogValue(item, undefined, depth + 1));
  if (typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).slice(0, 30).map(([entryKey, entryValue]) => [
        entryKey,
        redactLogValue(entryValue, entryKey, depth + 1)
      ])
    );
  }
  return terminalSafe(String(value), 500);
}

// Diagnostics must never interfere with OpenCode or reservation behavior.
function diagnosticLog(level, event, context = {}) {
  try {
    const line = `${new Date().toISOString()} ${level} ${event} ${JSON.stringify(redactLogValue(context))}\n`;
    appendFileSync(process.env.OPENCODE_NEURON_LOG_FILE || DEFAULT_LOG_FILE, line, "utf8");
  } catch {
    // Intentionally ignore unavailable/unwritable diagnostic log destinations.
  }
}


const DEFAULT_POLL_S = 5;
const DEFAULT_DURATION_MINUTES = 2;
const DEFAULT_WAIT_TIMEOUT_S = 600;
const DEFAULT_DISPOSE_RELEASE_TIMEOUT_MS = 5000;

let _statusCache = null;
let _statusCacheTime = 0;
let _statusInflight = null;
const STATUS_CACHE_TTL = 10000;

// Plugin-lifecycle cancellation: monotonic generation counter so that resetting
// _disposed back to false (e.g., for tests) does NOT re-enable cancelled work.
// Each dispose() increments _disposeGen; each async task captures the current gen
// at start and checks equality after every await.
let _disposed = false;
let _disposeGen = 0;

const state = {
  reservations: new Map(),
  inflight: new Map(),
  inflightTarget: new Map(),
  retryState: new Map()
};
// Scoped transport failure cooldown — keyed by "baseUrl::model" to avoid
// one failed request suppressing health checks for all sessions/models.
const transportCooldown = new Map();

// Per-session lease refresh intervals — maintained during active generations so
// long streams (>2 min) do not lose capacity. Cancelled on session.idle/deleted/dispose.
const leaseRefreshIntervals = new Map();
const leaseRefreshInflight = new Map();
const sessionAbortControllers = new Map();
const releaseInflight = new Map();
const reservationCreateInflight = new Set();
// Reservation IDs created by this process. This survives the creator's local
// entry while borrowers remain, so the final local reference can release it.
const locallyOwnedReservationIds = new Set();
// Owned reservations whose final DELETE failed. Keep ownership until a later
// lifecycle cleanup can retry; otherwise clearing it would silently orphan them.
const pendingOwnedReleases = new Map();
const pendingOwnedReleaseTimers = new Map();

function terminalSafe(value, limit = 500) {
  return String(value ?? "")
    .replace(/\\(["'])/g, "$1")
    .replace(/((?:Basic|Bearer)\s+)[^\s,]+/gi, "$1[REDACTED]")
    .replace(/((?:["']?(?:api[_-]?key|access[_-]?token|refresh[_-]?token|id[_-]?token|token|password|passwd|secret|authorization|proxy-authorization|credential|credentials)["']?\s*[:=]\s*))(?!\s*(?:Basic|Bearer)\s+\[REDACTED\])(?:(?:["'])(?:\\.|[^"'\\])*["']|(?:Basic|Bearer)\s+[^\s,}\]]+|[^\s,}\]]+)/gi, "$1[REDACTED]")
    .replace(/\b((?:api[_-]?key|access[_-]?token|refresh[_-]?token|id[_-]?token|token|password|passwd|secret|authorization|proxy-authorization|credential|credentials)\s+)(?!\[REDACTED\])(?:(?:Basic|Bearer)\s+[^\s,}\]]+|[^\s,}\]]+)/gi, "$1[REDACTED]")
    .replace(/([?&](?:api[_-]?key|token|authorization)=)[^&\s]+/gi, "$1[REDACTED]")
    .replace(/([a-z][a-z0-9+.-]*:\/\/)[^@/\s]+@/gi, "$1[REDACTED]@")
    .replace(/[\r\n\t\0]/g, " ")
    .slice(0, limit);
}

export class NeurOnClient {
  constructor(config) {
    this.config = config;
  }

  async getStatus(signal) {
    const [status, models] = await Promise.all([
      this.request("/api/status", {}, undefined, signal),
      this.request("/api/models", {}, undefined, signal)
    ]);
    return { ...status, models: models.models ?? [] };
  }

  async createReservation(match, signal) {
    return this.request("/api/reservations", {
      method: "POST",
      body: JSON.stringify({
        modelIds: match.modelIds,
        targetIds: match.targetIds,
        durationMinutes: this.config.durationMinutes,
        keepaliveMinutes: this.config.keepaliveMinutes
      })
    }, undefined, signal);
  }

  async refreshReservation(reservationId, signal) {
    return this.request(`/api/reservations/${encodeURIComponent(reservationId)}/extend`, {
      method: "POST",
      body: JSON.stringify({
        durationMinutes: this.config.durationMinutes,
        fromNow: true
      })
    }, undefined, signal);
  }

  async releaseReservation(reservationId, requestTimeoutMs) {
    return this.request(`/api/reservations/${encodeURIComponent(reservationId)}`, {
      method: "DELETE"
    }, requestTimeoutMs);
  }

  async waitForHealthy(reservationId, signal) {
    const deadline = Date.now() + this.config.waitTimeoutMs;
    let lastReservation;
    while (Date.now() <= deadline) {
      if (signal?.aborted) throw Object.assign(new Error('Aborted'), { name: 'AbortError' });
      lastReservation = await this.request(
        `/api/reservations/${encodeURIComponent(reservationId)}/status`, {}, undefined, signal
      );
      const targets = lastReservation.targets ?? [];
      if (targets.length > 0 && targets.every((t) => t.observed === "healthy"))
        return lastReservation;
      const failed = lastReservation.targets?.find((t) => t.observed === "failed");
      if (failed)
        throw new Error(`NeurOn target ${terminalSafe(failed.id, 100)} failed: ${terminalSafe(failed.message)}`);
      // Abortable sleep — resolves on timeout or when signal fires
      await new Promise((resolve, reject) => {
        if (signal?.aborted) { reject(Object.assign(new Error('Aborted'), { name: 'AbortError' })); return; }
        let done = false;
        const id = setTimeout(() => {
          if (done) return;
          done = true;
          if (signal) signal.removeEventListener('abort', onAbort);
          resolve();
        }, this.config.pollMs);
        const onAbort = () => {
          if (done) return;
          done = true;
          clearTimeout(id);
          reject(Object.assign(new Error('Aborted'), { name: 'AbortError' }));
        };
        if (signal) signal.addEventListener('abort', onAbort);
      });
    }
    const states = (lastReservation?.targets ?? [])
      .map((t) => `${t.id}:${t.observed}`)
      .join(", ");
    throw new Error(
      `Timed out waiting for NeurOn reservation ${reservationId} to become healthy${states ? ` (${states})` : ""}`
    );
  }

  async request(path, options = {}, requestTimeoutMs, externalSignal) {
    const method = options.method ?? "GET";
    const startedAt = Date.now();
    const timeoutMs = requestTimeoutMs ?? this.config.requestTimeoutMs;
    if (!this.config.apiKey)
      throw new Error("NEURON_API_KEY is required for the NeurOn OpenCode plugin");
    if (externalSignal?.aborted) {
      diagnosticLog("WARN", "traffic.request.aborted", { method, path, reason: "external_signal_preaborted" });
      throw new NeurOnApiError(0, path, 'Request aborted', 'aborted');
    }
    const controller = new AbortController();

    // Declare both timers in function scope so finally can always clear them safely.
    let timer = null;
    let externalTimer = null;
    let abortReason = null;

    // Combine internal timeout with optional external abort signal (e.g. preflight timeout).
    // If the external signal fires, we also abort our own controller to clean up the fetch.
    let externalListenerCleanup = null;
    if (externalSignal && !externalSignal.aborted) {
      let externalFired = false;
      const onExternalAbort = () => {
        if (externalFired) return;
        externalFired = true;
        abortReason = "external_signal";
        controller.abort();
        clearTimeout(externalTimer);
      };
      externalSignal.addEventListener('abort', onExternalAbort);
      externalListenerCleanup = () => externalSignal.removeEventListener('abort', onExternalAbort);
      // Set a longer internal timeout so external abort wins first
      externalTimer = setTimeout(() => { abortReason = "request_timeout"; controller.abort(); }, timeoutMs);
    } else {
      timer = setTimeout(() => { abortReason = "request_timeout"; controller.abort(); }, timeoutMs);
    }

    try {
      diagnosticLog("DEBUG", "traffic.request.start", { method, path, timeoutMs, hasExternalSignal: Boolean(externalSignal) });
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
        diagnosticLog("WARN", "traffic.response.error", { method, path, status: response.status, durationMs: Date.now() - startedAt });
        throw new NeurOnApiError(response.status, path, body, response.statusText);
      }
      const raw = await response.text();
      try {
        const result = JSON.parse(raw);
        diagnosticLog("INFO", "traffic.response.success", { method, path, status: response.status, durationMs: Date.now() - startedAt });
        return result;
      } catch (parseErr) {
        diagnosticLog("WARN", "traffic.response.invalid_json", { method, path, status: response.status, durationMs: Date.now() - startedAt });
        throw new NeurOnApiError(0, path, `Failed to parse response: ${raw}`, 'invalid_json');
      }
    } catch (error) {
      if (error?.name === 'AbortError') {
        diagnosticLog("WARN", "traffic.request.aborted", { method, path, durationMs: Date.now() - startedAt, reason: abortReason ?? "fetch_abort" });
        throw new NeurOnApiError(0, path, 'Request timed out', 'timeout');
      }
      if (!(error instanceof NeurOnApiError)) diagnosticLog("ERROR", "traffic.request.failed", { method, path, durationMs: Date.now() - startedAt, error: error?.message });
      throw error;
    } finally {
      // Clear both internal timeout and external abort listener cleanup
      clearTimeout(timer);
      clearTimeout(externalTimer);
      if (externalListenerCleanup) externalListenerCleanup();
    }
  }
}

class NeurOnApiError extends Error {
  constructor(status, path, body, statusText) {
    super(`NeurOn API ${status} for ${terminalSafe(path, 200)}: ${terminalSafe(body || statusText)}`);
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
  let parsedUrl;
  try { parsedUrl = new URL(baseUrl); } catch { throw new Error("NEURON_API_BASE_URL must be a valid http:// or https:// URL"); }
  if (!['http:', 'https:'].includes(parsedUrl.protocol))
    throw new Error("NEURON_API_BASE_URL must be a valid http:// or https:// URL");
  if (parsedUrl.username || parsedUrl.password)
    throw new Error("NEURON_API_BASE_URL must not contain credentials");
  const host = parsedUrl.hostname.toLowerCase();
  if (parsedUrl.protocol === 'http:' && host !== 'localhost' && host !== '::1' && host !== '[::1]' && !/^127(?:\.\d{1,3}){3}$/.test(host))
    throw new Error("NEURON_API_BASE_URL may use http:// only for loopback endpoints");
  const config = {
    apiBaseUrl: trimSlash(baseUrl),
    apiOrigin: parsedUrl.origin,
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
    disposeReleaseTimeoutMs: positiveNumber(
      process.env.NEURON_DISPOSE_RELEASE_TIMEOUT_MS,
      DEFAULT_DISPOSE_RELEASE_TIMEOUT_MS
    ),
    blockOnColdMessage: boolEnv(process.env.NEURON_BLOCK_ON_COLD_MESSAGE, true),
    allowedProviders
  };
  diagnosticLog("INFO", "config.loaded", {
    apiOrigin: config.apiOrigin,
    hasApiKey: Boolean(config.apiKey),
    allowedProviderCount: allowedProviders.length,
    waitForHealthy: config.waitForHealthy,
    requestTimeoutMs: config.requestTimeoutMs
  });
  return config;
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

function matchLiteLlmModel(targets, models, bareModelId) {
  const modelByLookup = buildModelLookup(models);

  // ── Pass 1: Model lookup ─────────────────────────────────────────
  const model = modelByLookup.get(bareModelId);
  if (model && model.targetIds?.length) {
    for (const target of targets) {
      if (model.targetIds.includes(target.id)) {
        return { modelIds: [model.id], targetIds: [target.id] };
      }
    }
  }

  // ── Pass 2: Direct target modelIds match ─────────────────────────
  for (const target of targets) {
    if (!target.modelIds?.includes(bareModelId)) continue;
    return { modelIds: [bareModelId], targetIds: [target.id] };
  }

  return undefined;
}

function buildModelLookup(models) {
  const lookup = new Map();
  const duplicates = [];
  for (const model of models) {
    for (const id of [
      model.id,
      ...(model.aliases ?? []),
      ...(model.backendModelIds ?? []),
      ...(model.runtimeModelIds ?? [])
    ]) {
      if (id) {
        if (lookup.has(id)) {
          duplicates.push({ id, existing: lookup.get(id)?.id, incoming: model.id });
        } else {
          lookup.set(id, model);
        }
      }
    }
  }
  if (duplicates.length > 0) {
    console.warn(`[NeurOn] Duplicate model aliases detected:`, JSON.stringify(duplicates));
  }
  return lookup;
}

function findTargetStatus(targets, targetId) {
  for (const t of targets)
    if (t.id === targetId) return t;
  return undefined;
}

async function getCachedStatus(client, _signal) {
  // NOTE: external AbortSignal is intentionally ignored here. The shared
  // _statusInflight is used by multiple callers — one caller's preflight
  // timeout must NOT abort the module-global fetch that unrelated callers
  // depend on. Per-caller timeouts are handled upstream via Promise.race
  // in checkTargetHealthyWithTimeout.
  // 1. Return cached resolved status while TTL valid
  if (_statusCache && Date.now() - _statusCacheTime < STATUS_CACHE_TTL) {
    diagnosticLog("DEBUG", "health.status.cache_hit", { ageMs: Date.now() - _statusCacheTime });
    return _statusCache;
  }
  // 2. If stale and fetch in-flight, await the in-flight request
  if (_statusInflight) {
    diagnosticLog("DEBUG", "health.status.join_inflight");
    return _statusInflight;
  }
  // 3. Fetch new (no caller signal), set inflight, save resolved value+time, clear inflight
  const currentDisposeGen = _disposeGen;
  diagnosticLog("DEBUG", "health.status.fetch_start", { apiOrigin: client.config.apiOrigin });
  const fetchPromise = client.getStatus();
  _statusInflight = fetchPromise.then((result) => {
    // Gap 1: Do not repopulate cache after dispose
    if (!_disposed && _disposeGen === currentDisposeGen) {
      _statusCache = result;
      _statusCacheTime = Date.now();
    }
    diagnosticLog("INFO", "health.status.fetch_success", { targetCount: result.capacityTargets?.length ?? 0, modelCount: result.models?.length ?? 0 });
    _statusInflight = null;
    return result;
  }).catch((e) => {
    _statusInflight = null;
    // Invalidate stale cache so next call re-fetches instead of adopting
    // a cached reservation from before the transport failure.
    _statusCache = null;
    _statusCacheTime = 0;
    // Gap 1: Only record cooldown if not disposed
    if (!_disposed && _disposeGen === currentDisposeGen) {
      transportCooldown.set(client.config.apiBaseUrl, Date.now());
    }
    console.debug(`[NeurOn] Status fetch failed: ${terminalSafe(e.message)}`);
    diagnosticLog("WARN", "health.status.fetch_failed", { error: e.message });
    throw e;
  });
  return _statusInflight;
}

// ── Reservation flow (keyed by model ID + target ID) ────────

function ensureReservation(client, modelId, sessionID, signal) {
  signal ??= getSessionSignal(sessionID);
  if (_disposed || signal?.aborted) {
    throw Object.assign(new Error('cancelled'), { name: 'AbortError' });
  }
  const inflightKey = `${sessionID}::${modelId}`;
  const existing = state.inflight.get(inflightKey);
  if (existing) {
    return existing;
  }

  const genAtStart = getSessionGen(sessionID);
  let promise;
  promise = resolveTargetForModel(client, modelId, sessionID, signal, genAtStart)
    .then(async ({ targetId, match, resKey }) => {
      if (!isSessionWorkCurrent(sessionID, genAtStart, signal)) {
        throw Object.assign(new Error('cancelled'), { name: 'AbortError' });
      }
      // Secondary dedup at target level for models sharing targets
      const targetInflightKey = `${sessionID}::${targetId}`;
      const targetInflight = state.inflightTarget.get(targetInflightKey);
      if (targetInflight) return targetInflight;

      let p;
      p = reserveOrRefreshTarget(client, targetId, match, sessionID, signal, genAtStart).finally(() => {
        if (state.inflightTarget.get(targetInflightKey) === p)
          state.inflightTarget.delete(targetInflightKey);
      });
      if (!_disposed) state.inflightTarget.set(targetInflightKey, p);
      return p;
    })
    .finally(() => {
      if (state.inflight.get(inflightKey) === promise)
        state.inflight.delete(inflightKey);
    });

  if (!_disposed) state.inflight.set(inflightKey, promise);
  return promise;
}

async function resolveTargetForModel(client, modelId, sessionID, signal, expectedGen = getSessionGen(sessionID), allowAdoption = true) {
  if (!isSessionWorkCurrent(sessionID, expectedGen, signal)) throw Object.assign(new Error('cancelled'), { name: 'AbortError' });
  const status = await getCachedStatus(client);
  if (!isSessionWorkCurrent(sessionID, expectedGen, signal)) throw Object.assign(new Error('cancelled'), { name: 'AbortError' });
  const splitResult = splitProvider(modelId);
  const match = matchLiteLlmModel(
    status.capacityTargets ?? [],
    status.models ?? [],
    splitResult.bareModelId
  );
  if (!match) {
    diagnosticLog("WARN", "model.mapping.unmapped", { modelId, sessionID });
    throw new Error(
      `NeurOn could not map OpenCode model "${modelId}" to a capacity target`
    );
  }
  if (match.error)
    throw new Error(`NeurOn ${match.error}: ${match.detail}`);
  const targetId = match.targetIds[0];
  const targetInfo = findTargetStatus(status.capacityTargets ?? [], targetId);
  const targetHealthy = targetInfo?.observed === "healthy";
  const resKey = `${sessionID}::${targetId}`;
  diagnosticLog("INFO", "model.mapping.selected", { modelId, sessionID, targetId, targetState: targetInfo?.observed ?? "unknown", allowAdoption });

  // If target is healthy and this session has no local reservation, adopt the server-side one
  if (allowAdoption && targetHealthy && !state.reservations.has(resKey)) {
    try {
      if (!isSessionWorkCurrent(sessionID, expectedGen, signal)) throw Object.assign(new Error('cancelled'), { name: 'AbortError' });
      adoptExistingReservation(targetId, status, sessionID, undefined, expectedGen, signal);
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
async function retryWithBackoff(key, fn, maxAttempts, baseMs, maxMs, signal) {
  const rs = state.retryState.get(key) ?? { attempts: 0, nextDelay: baseMs };
  let lastErr;
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    if (signal?.aborted) throw Object.assign(new Error('cancelled'), { name: 'AbortError' });
    try {
      const result = await fn();
      // Success — reset retry state immediately
      state.retryState.delete(key);
      return result;
    } catch (err) {
      lastErr = err;
      diagnosticLog("WARN", "reservation.retry.failed", { key, attempt: attempt + 1, maxAttempts, transient: isTransientError(err), error: err?.message });
      // Permanent errors — fail fast without further retries
      if (!isTransientError(err)) throw err;
      if (attempt < maxAttempts - 1) {
        const jitter = Math.random() * rs.nextDelay;
        const delay = rs.nextDelay + jitter;
        diagnosticLog("INFO", "reservation.retry.scheduled", { key, nextAttempt: attempt + 2, delayMs: Math.min(delay, maxMs) });
        await sleep(Math.min(delay, maxMs), signal);
        rs.nextDelay = Math.min(rs.nextDelay * 2, maxMs);
        rs.attempts = attempt + 1;
      }
    }
  }
  state.retryState.delete(key);
  throw lastErr;
}

async function reserveOrRefreshTarget(client, targetId, match, sessionID, signal, expectedGen = getSessionGen(sessionID)) {
  if (_disposed || signal?.aborted) {
    throw Object.assign(new Error('cancelled'), { name: 'AbortError' });
  }
  // Capture generation at entry so saveReservation can detect stale async results
  const genAtEntry = expectedGen;
  const resKey = `${sessionID}::${targetId}`;
  const existingEntry = state.reservations.get(resKey);
  let failedReservationId = null;
  if (existingEntry) {
    if (existingEntry.expiresAt < Date.now()) {
      console.debug(`[NeurOn] Reservation expired for ${targetId}, discarding`);
      state.reservations.delete(resKey);
    } else {
      try {
        diagnosticLog("INFO", "reservation.refresh.start", { sessionID, targetId, reservationId: existingEntry.reservation.reservationId, owned: existingEntry.owned });
        const refreshed = await client.refreshReservation(existingEntry.reservation.reservationId, signal);
        if (_disposed || signal?.aborted) {
          throw Object.assign(new Error('cancelled'), { name: 'AbortError' });
        }
        if (getSessionGen(sessionID) !== genAtEntry) return refreshed;
        console.debug(`[NeurOn] Refreshed reservation for ${targetId}`);
        diagnosticLog("INFO", "reservation.refresh.success", { sessionID, targetId, reservationId: refreshed.reservationId ?? existingEntry.reservation.reservationId });
        return saveReservation(targetId, refreshed, sessionID, genAtEntry, existingEntry.owned);
      } catch (error) {
        // Gap 3: Invalidate status cache before adoptExistingReservation so we
        // fetch current server state rather than stale data from before the failure.
        _statusCache = null;
        _statusCacheTime = 0;
        // Remember failed reservation ID so adoptExistingReservation does not re-adopt it.
        failedReservationId = existingEntry.reservation.reservationId;
        console.debug(`[NeurOn] Refresh failed for ${targetId}: ${error.message}`);
        diagnosticLog("WARN", "reservation.refresh.failed", { sessionID, targetId, reservationId: existingEntry.reservation.reservationId, error: error.message });
        // A newer attempt may have replaced this entry while its refresh was
        // in flight. It owns the reused key now, so this stale attempt must
        // neither delete it nor create another reservation.
        if (!isReservationCurrent(resKey, existingEntry.reservation, sessionID, genAtEntry)) {
          return state.reservations.get(resKey)?.reservation;
        }
      }
    }
  }

  // Idempotency: before creating a new reservation, check if there's an active
  // server-side reservation for this target that we can adopt. This prevents
  // duplicate reservations when the refresh response was lost but succeeded server-side.
  try {
    const status = await getCachedStatus(client);
    if (!isSessionWorkCurrent(sessionID, genAtEntry, signal)) throw Object.assign(new Error('cancelled'), { name: 'AbortError' });
    const confirmed = findActiveReservation(targetId, status, failedReservationId);
    if (confirmed) {
      console.debug(`[NeurOn] Confirmed refreshed server-side reservation for ${targetId}`);
      saveReservation(targetId, confirmed, sessionID, genAtEntry, existingEntry?.owned);
      diagnosticLog("INFO", "reservation.ownership.confirmed", { sessionID, targetId, reservationId: confirmed.reservationId });
      if (client.config.waitForHealthy) await client.waitForHealthy(confirmed.reservationId, signal);
      return confirmed;
    }
    const adopted = adoptExistingReservation(targetId, status, sessionID, failedReservationId, genAtEntry, signal);
    if (adopted) {
      console.debug(`[NeurOn] Adopted existing server-side reservation for ${targetId}`);
      if (client.config.waitForHealthy) await client.waitForHealthy(adopted.reservationId, signal);
      return adopted;
    }
    // The authoritative lookup completed and found no active reservation to
    // adopt, so it is safe to replace the failed one. Do not let a stale
    // attempt remove a newer reservation saved under the same session key.
    if (existingEntry && failedReservationId) {
      deleteReservationIfCurrent(resKey, existingEntry.reservation, sessionID, genAtEntry);
    }
  } catch (e) {
    console.debug(`[NeurOn] Could not check for existing reservation: ${e.message}`);
    diagnosticLog("WARN", "reservation.ownership.check_failed", { sessionID, targetId, error: e.message });
    // A refresh may have succeeded server-side even though its response was
    // lost. Without authoritative status, creating here could duplicate it.
    if (existingEntry && failedReservationId) {
      throw new Error('NeurOn: refresh outcome ambiguous; preserving reservation until authoritative status lookup succeeds');
    }
    // No prior refresh ambiguity, so a fresh create remains safe.
  }

  // Gap 4: Cooldown guard — prevent starting fresh retry batches during outage
  const globalCooldownKey = client.config.apiBaseUrl;
  const modelCooldownKey = cooldownKey(client, match, targetId);
  const lastFailure = Math.max(
    transportCooldown.get(globalCooldownKey) ?? 0,
    transportCooldown.get(modelCooldownKey) ?? 0
  );
  if (Date.now() - lastFailure < client.config.cooldownPeriodMs) {
    throw new Error('NeurOn: cooldown active, deferring reservation');
  }

  // POST creation has no documented idempotency key in this API contract. Never
  // blindly retry it: a transport failure may have created a server reservation.
  let reservation;
  try {
    diagnosticLog("INFO", "reservation.create.start", { sessionID, targetId, modelIds: match.modelIds });
    reservation = await trackReservationCreate((async () => {
      const created = await client.createReservation(match, signal);
      if (!isSessionWorkCurrent(sessionID, genAtEntry, signal)) {
        if (created?.reservationId) {
          locallyOwnedReservationIds.add(created.reservationId);
          const cleanup = releaseOwnedReservation(client, created.reservationId, targetId);
          if (_disposed) await cleanup;
        }
        throw Object.assign(new Error('cancelled'), { name: 'AbortError' });
      }
      return created;
    })());
  } catch (e) {
    if (e?.name === 'AbortError') throw e;
    // Reconcile before another create: only authoritative status can distinguish
    // a lost response from a POST that never reached the service.
    _statusCache = null;
    _statusCacheTime = 0;
    try {
      const status = await getCachedStatus(client);
      const reconciled = adoptExistingReservation(targetId, status, sessionID, undefined, genAtEntry, signal);
      if (reconciled) {
        if (client.config.waitForHealthy) await client.waitForHealthy(reconciled.reservationId, signal);
        return reconciled;
      }
    } catch (statusError) {
      console.debug(`[NeurOn] Create reconciliation failed for ${targetId}: ${terminalSafe(statusError.message)}`);
    }
    transportCooldown.set(cooldownKey(client, match, targetId), Date.now());
    diagnosticLog("WARN", "reservation.create.failed", { sessionID, targetId, error: e.message });
    throw e;
  }
  console.debug(`[NeurOn] Created new reservation for ${targetId}`);
  diagnosticLog("INFO", "reservation.create.success", { sessionID, targetId, reservationId: reservation.reservationId });
  saveReservation(targetId, reservation, sessionID, genAtEntry, true);
  try {
    if (client.config.waitForHealthy) {
      await client.waitForHealthy(reservation.reservationId, signal);
    }
  } catch (e) {
    console.debug(`[NeurOn] Wait-for-healthy failed for ${targetId}: ${e.message}`);
    diagnosticLog("WARN", "reservation.health_wait.failed", { sessionID, targetId, reservationId: reservation.reservationId, error: e.message });
    await releaseReservationIfOwned(client, resKey, reservation, sessionID, genAtEntry);
    throw e;
  }
  return reservation;
}

function saveReservation(targetId, reservation, sessionID, genOverride, owned = false) {
  // Gap 1: Do not repopulate reservations after dispose
  if (_disposed) return reservation;
  // Defend against stale async work: if the session was deleted/idled/model-switched
  // after this reservation was created/refreshed, discard the result.
  if (sessionID) {
    if (genOverride != null) {
      // Async path: caller captured generation at entry — verify it still matches
      if (getSessionGen(sessionID) !== genOverride) return reservation;
    } else {
      // Synchronous path (e.g., adoptExistingReservation): initialize if needed
      if (getSessionGen(sessionID) === 0) incrementSessionGen(sessionID);
    }
  }
  // Prefer server-provided expiration timestamp; fall back to local calculation.
  const serverExpires = reservation.expiresAtMs ?? reservation.expiresAt;
  const expiresAt = serverExpires
    ? Number(serverExpires)
    : Date.now() + (reservation.durationMinutes ?? DEFAULT_DURATION_MINUTES) * 60 * 1000;

  const entry = {
    reservation,
    owned: Boolean(owned),
    generation: sessionID ? (genOverride ?? getSessionGen(sessionID)) : undefined,
    expiresAt: Number.isFinite(expiresAt) ? expiresAt : Date.now() + DEFAULT_DURATION_MINUTES * 60 * 1000
  };
  if (entry.owned && reservation.reservationId) locallyOwnedReservationIds.add(reservation.reservationId);
  state.reservations.set(`${sessionID}::${targetId}`, entry);
  diagnosticLog("INFO", "reservation.saved", { sessionID, targetId, reservationId: reservation.reservationId, owned: entry.owned, expiresAt: entry.expiresAt });
  return reservation;
}

function isReservationCurrent(resKey, expectedReservation, sessionID, expectedGen) {
  const entry = state.reservations.get(resKey);
  return Boolean(
    entry?.reservation === expectedReservation &&
    (entry.generation == null || entry.generation === expectedGen) &&
    getSessionGen(sessionID) === expectedGen
  );
}

function deleteReservationIfCurrent(resKey, expectedReservation, sessionID, expectedGen) {
  if (isReservationCurrent(resKey, expectedReservation, sessionID, expectedGen)) {
    state.reservations.delete(resKey);
  }
}

async function releaseReservationIfOwned(client, resKey, reservation, sessionID, expectedGen) {
  const entry = state.reservations.get(resKey);
  if (!entry?.owned || entry.reservation !== reservation || !isReservationCurrent(resKey, reservation, sessionID, expectedGen)) return;
  const releaseKey = reservation.reservationId;
  if (hasOtherReservationReference(releaseKey, resKey)) return;
  if (!releaseKey || releaseInflight.has(releaseKey)) return releaseInflight.get(releaseKey);
  return releaseOwnedReservation(client, releaseKey, resKey)
    .then((released) => { if (released) deleteReservationIfCurrent(resKey, reservation, sessionID, expectedGen); });
}

function releaseOwnedReservation(client, reservationId, targetId) {
  if (!reservationId || releaseInflight.has(reservationId)) return releaseInflight.get(reservationId);
  diagnosticLog("INFO", "reservation.release.start", { targetId, reservationId });
  const release = client.releaseReservation(reservationId, client.config.requestTimeoutMs)
    .then(() => {
      locallyOwnedReservationIds.delete(reservationId);
      pendingOwnedReleases.delete(reservationId);
      clearPendingOwnedReleaseTimer(reservationId);
      diagnosticLog("INFO", "reservation.release.success", { targetId, reservationId });
      return true;
    })
    .catch((error) => {
      const previous = pendingOwnedReleases.get(reservationId);
      const attempts = (previous?.attempts ?? 0) + 1;
      const delay = Math.min(
        client.config.retryBaseMs * (2 ** Math.max(0, attempts - 1)),
        client.config.retryMaxMs
      );
      pendingOwnedReleases.set(reservationId, {
        targetId,
        attempts,
        nextAttemptAt: Date.now() + delay
      });
      schedulePendingOwnedRelease(client, reservationId);
      console.debug(`[NeurOn] Failed to release reservation for ${targetId}: ${terminalSafe(error.message)}`);
      diagnosticLog("WARN", "reservation.release.failed", { targetId, reservationId, attempts, nextRetryMs: delay, error: error.message });
      return false;
    })
    .finally(() => releaseInflight.delete(reservationId));
  releaseInflight.set(reservationId, release);
  return release;
}

function clearPendingOwnedReleaseTimer(reservationId) {
  const timer = pendingOwnedReleaseTimers.get(reservationId);
  if (timer != null) clearTimeout(timer);
  pendingOwnedReleaseTimers.delete(reservationId);
}

function schedulePendingOwnedRelease(client, reservationId) {
  const pending = pendingOwnedReleases.get(reservationId);
  if (!pending) {
    clearPendingOwnedReleaseTimer(reservationId);
    return;
  }
  clearPendingOwnedReleaseTimer(reservationId);
  const timer = setTimeout(() => {
    pendingOwnedReleaseTimers.delete(reservationId);
    const current = pendingOwnedReleases.get(reservationId);
    if (!current || hasOtherReservationReference(reservationId)) return;
    if (Date.now() < current.nextAttemptAt) {
      schedulePendingOwnedRelease(client, reservationId);
      return;
    }
    // Deliberately remains active after dispose: a create that resolves after
    // its bounded shutdown drain still needs a best-effort release path.
    releaseOwnedReservation(client, reservationId, current.targetId);
  }, Math.max(0, pending.nextAttemptAt - Date.now()));
  // Pending cleanup must not keep OpenCode alive once all normal work is done.
  timer.unref?.();
  pendingOwnedReleaseTimers.set(reservationId, timer);
}

function trackReservationCreate(create) {
  reservationCreateInflight.add(create);
  create.then(
    () => reservationCreateInflight.delete(create),
    () => reservationCreateInflight.delete(create)
  );
  return create;
}

function retryPendingOwnedReleases(client, force = false) {
  const releases = [];
  for (const [reservationId, pending] of pendingOwnedReleases) {
    if (!hasOtherReservationReference(reservationId)) {
      if (force || Date.now() >= pending.nextAttemptAt) {
        releases.push(releaseOwnedReservation(client, reservationId, pending.targetId));
      }
    }
  }
  return releases;
}

function findActiveReservation(targetId, status, reservationId) {
  if (!reservationId) return null;
  const active = status.activeReservations ?? status.reservations ?? [];
  return active.find((res) =>
    res.reservationId === reservationId &&
    res.status === "active" &&
    (res.targets ?? []).some((target) => target.id === targetId)
  ) ?? null;
}

function adoptExistingReservation(targetId, status, sessionID, skipReservationId, expectedGen = getSessionGen(sessionID), signal) {
  const active = status.activeReservations ?? status.reservations ?? [];
  for (const res of active) {
    // Gap 3: Skip the reservation that just failed refresh — do not re-adopt it.
    if (res.reservationId === skipReservationId) continue;
    const targets = res.targets ?? [];
    for (const t of targets) {
      if (t.id === targetId && res.status === "active") {
        if (!isSessionWorkCurrent(sessionID, expectedGen, signal)) return null;
        saveReservation(targetId, res, sessionID, expectedGen, false);
        diagnosticLog("INFO", "reservation.adopted", { sessionID, targetId, reservationId: res.reservationId });
        return res;
      }
    }
  }
  return null;
}

async function refreshExistingReservation(client, modelId, sessionID, signal, expectedGen = getSessionGen(sessionID)) {
  signal ??= getSessionSignal(sessionID);
  if (_disposed || signal?.aborted) return undefined;
  // Capture generation before async work so stale results are discarded
  const genAtEntry = expectedGen;
  const { targetId } = await resolveTargetForModel(client, modelId, sessionID, signal, genAtEntry, false);
  if (_disposed || signal?.aborted) return undefined;
  const key = `${sessionID}::${targetId}`;
  const existingEntry = state.reservations.get(key);
  if (!existingEntry) return undefined;

  try {
    const refreshed = await client.refreshReservation(existingEntry.reservation.reservationId, signal);
    if (!isSessionWorkCurrent(sessionID, genAtEntry, signal)) return undefined;
    // Guard: discard if session was deleted/idled/model-switched during refresh
    if (getSessionGen(sessionID) !== genAtEntry) return refreshed;
    saveReservation(targetId, refreshed, sessionID, genAtEntry, existingEntry.owned);
    return refreshed;
  } catch (error) {
    // Invalidate status cache so next getCachedStatus re-fetches instead of
    // returning stale data from before the transport failure.
    _statusCache = null;
    _statusCacheTime = 0;
    if (!isReservationCurrent(key, existingEntry.reservation, sessionID, genAtEntry)) {
      return state.reservations.get(key)?.reservation;
    }
    try {
      // A timeout may mean the extend succeeded server-side. Only discard the
      // local reservation after an authoritative status lookup proves there is
      // no other active reservation to adopt.
      const status = await getCachedStatus(client);
      if (!isSessionWorkCurrent(sessionID, genAtEntry, signal)) return undefined;
      const confirmed = findActiveReservation(targetId, status, existingEntry.reservation.reservationId);
      if (confirmed) return saveReservation(targetId, confirmed, sessionID, genAtEntry);
      const adopted = adoptExistingReservation(targetId, status, sessionID, existingEntry.reservation.reservationId, genAtEntry, signal);
      if (adopted) return adopted;
      deleteReservationIfCurrent(key, existingEntry.reservation, sessionID, genAtEntry);
    } catch (statusError) {
      if (!isReservationCurrent(key, existingEntry.reservation, sessionID, genAtEntry)) {
        return state.reservations.get(key)?.reservation;
      }
      throw new Error('NeurOn: refresh outcome ambiguous; preserving reservation until authoritative status lookup succeeds');
    }
    // Rethrow so caller's .catch (e.g., session.idle handler) observes failure.
    throw error;
  }
}

// ── Utilities ─────────────────────────────────────────────

function positiveNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function boolEnv(value, fallback) {
  if (value === undefined) return fallback;
  const lower = value.toLowerCase();
  if (["1", "true", "yes", "on"].includes(lower)) return true;
  if (["0", "false", "no", "off"].includes(lower)) return false;
  // Invalid value — warn and use fallback
  console.warn(`[NeurOn] Invalid boolean env value "${value}", using default`);
  return fallback;
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

// Per-session generation counter — incremented on session deletion so that async
// work started before deletion can detect staleness and discard its results.
const sessionGeneration = new Map();

function incrementSessionGen(sessionID) {
  const current = sessionGeneration.get(sessionID) ?? 0;
  sessionGeneration.set(sessionID, current + 1);
  return current + 1;
}

function getSessionGen(sessionID) {
  return sessionGeneration.get(sessionID) ?? 0;
}

function cooldownKey(client, match, targetId) {
  // Target IDs are the authoritative capacity unit; use this format everywhere
  // after resolution. Model-scoped keys are retained only for pre-resolution
  // fail-open checks, where a target cannot yet be known without network I/O.
  return `${client.config.apiBaseUrl}::${targetId ?? match?.targetIds?.[0]}`;
}

function getSessionSignal(sessionID) {
  if (!sessionID) return undefined;
  let controller = sessionAbortControllers.get(sessionID);
  if (!controller || controller.signal.aborted) {
    controller = new AbortController();
    sessionAbortControllers.set(sessionID, controller);
  }
  return controller.signal;
}

function abortSessionWork(sessionID) {
  const controller = sessionAbortControllers.get(sessionID);
  if (controller) controller.abort();
  sessionAbortControllers.delete(sessionID);
}

function isSessionWorkCurrent(sessionID, expectedGen, signal) {
  return !_disposed && !signal?.aborted && getSessionGen(sessionID) === expectedGen;
}

function hasOtherReservationReference(reservationId, excludedKey) {
  if (!reservationId) return false;
  for (const [key, entry] of state.reservations) {
    if (key !== excludedKey && entry.reservation?.reservationId === reservationId) return true;
  }
  return false;
}

function releaseUnreferencedOwnedReservations(client, reservations) {
  const releases = retryPendingOwnedReleases(client);
  const candidates = new Map();
  for (const { reservationId, targetId, owned } of reservations) {
    if (!reservationId) continue;
    candidates.set(reservationId, {
      targetId,
      owned: Boolean(owned) || locallyOwnedReservationIds.has(reservationId)
    });
    if (owned) locallyOwnedReservationIds.add(reservationId);
  }
  for (const [reservationId, { targetId, owned }] of candidates) {
    if (!owned || hasOtherReservationReference(reservationId)) continue;
    releases.push(releaseOwnedReservation(client, reservationId, targetId));
  }
  return releases;
}

// Release every piece of per-session state for a given sessionID. Prevents the
// sessionModels map (and any stale reservations/inflight entries) from leaking
// across the lifetime of a long-running TUI that opens and closes many sessions.
// Also increments the generation counter so in-flight async work can detect that
// it has become stale and discard its results.
function releaseActiveReservations(client, reservations) {
  return releaseUnreferencedOwnedReservations(client, reservations);
}

async function drainReleaseAttempts(releases, timeoutMs) {
  if (releases.length === 0) return;
  let timer;
  try {
    await Promise.race([
      Promise.allSettled(releases),
      new Promise((resolve) => { timer = setTimeout(resolve, timeoutMs); })
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function scrubSession(client, sessionID, sessionModels) {
  if (!sessionID) return;
  abortSessionWork(sessionID);
  // Cancel lease refresh interval for this session
  const intervalId = leaseRefreshIntervals.get(sessionID);
  if (intervalId != null) {
    clearInterval(intervalId);
    leaseRefreshIntervals.delete(sessionID);
  }
  const prefix = `${sessionID}::`;
  const reservations = [];
  for (const [key, entry] of state.reservations) {
    if (key.startsWith(prefix)) {
      reservations.push({
        reservationId: entry.reservation?.reservationId,
        targetId: key.slice(prefix.length),
        owned: entry.owned
      });
    }
  }
  for (const key of [...state.reservations.keys()])
    if (key.startsWith(prefix)) state.reservations.delete(key);
  releaseActiveReservations(client, reservations);
  for (const key of [...state.inflight.keys()])
    if (key.startsWith(prefix)) state.inflight.delete(key);
  for (const key of [...state.inflightTarget.keys()])
    if (key.startsWith(prefix)) state.inflightTarget.delete(key);
  for (const key of [...state.retryState.keys()])
    if (key.startsWith(prefix)) state.retryState.delete(key);
  sessionModels?.delete(sessionID);
  incrementSessionGen(sessionID);
}

function releaseSessionReservations(client, sessionID) {
  abortSessionWork(sessionID);
  cancelLeaseRefresh(sessionID);
  const prefix = `${sessionID}::`;
  const reservations = [];
  for (const [key, entry] of state.reservations) {
    if (key.startsWith(prefix)) {
      reservations.push({
        reservationId: entry.reservation?.reservationId,
        targetId: key.slice(prefix.length),
        owned: entry.owned
      });
    }
  }
  for (const key of [...state.reservations.keys()])
    if (key.startsWith(prefix)) state.reservations.delete(key);
  releaseActiveReservations(client, reservations);
  for (const key of [...state.inflight.keys()])
    if (key.startsWith(prefix)) state.inflight.delete(key);
  for (const key of [...state.inflightTarget.keys()])
    if (key.startsWith(prefix)) state.inflightTarget.delete(key);
  for (const key of [...state.retryState.keys()])
    if (key.startsWith(prefix)) state.retryState.delete(key);
  incrementSessionGen(sessionID);
}

// ── Lease refresh loop (Gap 5) ─────────────────────────────
// Starts a per-session interval that proactively refreshes all reservations
// for a session before they expire. Cancelled on session.idle/deleted/dispose.
// Recovers from transient extend failures by reacquiring reservation on next tick.

function startLeaseRefresh(sessionID, client, sessionModels) {
  // Cancel existing interval if any (e.g., model switch within same session)
  cancelLeaseRefresh(sessionID);

  const intervalMs = Math.max(10000, (client.config.durationMinutes * 60 * 1000) / 4);
  const disposeGenAtStart = _disposeGen;
  let consecutiveFailures = 0;
  const maxConsecutiveFailures = 5;
  const intervalId = setInterval(async () => {
    if (leaseRefreshInflight.has(sessionID)) return;
    const tick = (async () => {
    if (_disposed || _disposeGen !== disposeGenAtStart) { cancelLeaseRefresh(sessionID); return; }
    const info = sessionModels.get(sessionID);
    if (!info) { cancelLeaseRefresh(sessionID); return; }
    const fullModel = info.provider ? `${info.provider}/${info.id}` : info.id;

    // If local reservation was deleted (transient failure), reacquire it
    const resKey = `${sessionID}::`;
    const hasActiveReservation = [...state.reservations.keys()].some(k => k.startsWith(resKey));
    if (!hasActiveReservation) {
      console.debug(`[NeurOn] Lease loop reacquiring reservation for ${fullModel}${consecutiveFailures > 0 ? ` after ${consecutiveFailures} failures` : ''}`);
      ensureReservation(client, fullModel, sessionID).catch((e) => {
        if (_disposed || _disposeGen !== disposeGenAtStart) return;
        consecutiveFailures++;
        console.debug(`[NeurOn] Lease reacquisition failed for ${fullModel}: ${e.message} (${consecutiveFailures}/${maxConsecutiveFailures})`);
        if (consecutiveFailures >= maxConsecutiveFailures) {
          console.debug(`[NeurOn] Cancelling lease refresh after ${consecutiveFailures} consecutive failures for ${sessionID}`);
          cancelLeaseRefresh(sessionID);
        }
      });
      return;
    }

    return refreshExistingReservation(client, fullModel, sessionID)
      .then((result) => {
        if (result) consecutiveFailures = 0;
      })
      .catch((e) => {
        if (_disposed || _disposeGen !== disposeGenAtStart) return;
        consecutiveFailures++;
        console.debug(`[NeurOn] Lease refresh failed for ${fullModel}: ${e.message} (${consecutiveFailures}/${maxConsecutiveFailures})`);
        if (consecutiveFailures >= maxConsecutiveFailures) {
          // Too many failures — cancel lease loop to prevent thundering herd
          console.debug(`[NeurOn] Cancelling lease refresh after ${consecutiveFailures} consecutive failures for ${sessionID}`);
          cancelLeaseRefresh(sessionID);
        }
      });
    })();
    leaseRefreshInflight.set(sessionID, tick);
    try { await tick; } finally {
      if (leaseRefreshInflight.get(sessionID) === tick) leaseRefreshInflight.delete(sessionID);
    }
  }, intervalMs);
  leaseRefreshIntervals.set(sessionID, intervalId);
}

function cancelLeaseRefresh(sessionID) {
  const intervalId = leaseRefreshIntervals.get(sessionID);
  if (intervalId != null) {
    clearInterval(intervalId);
    leaseRefreshIntervals.delete(sessionID);
  }
}

// Check whether the session is still alive (not deleted/scrubbed).
function isSessionActive(sessionID, sessionModels) {
  return sessionModels?.has(sessionID) ?? sessionID === undefined;
}

function sleep(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) { reject(Object.assign(new Error('cancelled'), { name: 'AbortError' })); return; }
    const id = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(id);
      reject(Object.assign(new Error('cancelled'), { name: 'AbortError' }));
    };
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

// Safe toast wrapper — catches synchronous errors and suppresses rejected
// promises so a failing showToast never becomes an unhandled rejection.
function safeToast(tui, body) {
  try {
    tui?.showToast?.({ body })?.catch(() => {});
  } catch (e) {
    // Swallow — toast must never crash event processing
  }
}

// ── Background reservation (non-blocking) ─────────────────

async function checkTargetHealthy(client, modelId, _signal) {
  try {
    // Signal is ignored — shared status fetch must not be aborted by caller timeouts.
    const status = await getCachedStatus(client);
    const splitResult = splitProvider(modelId);
    const match = matchLiteLlmModel(
      status.capacityTargets ?? [],
      status.models ?? [],
      splitResult.bareModelId
    );
    if (!match || match.error) {
      diagnosticLog("WARN", "health.check.unmapped", { modelId });
      return "unmapped";
    }
    const targetInfo = findTargetStatus(
      status.capacityTargets ?? [],
      match.targetIds[0]
    );
    const outcome = targetInfo?.observed ?? "cold";
    diagnosticLog("INFO", "health.check.outcome", { modelId, targetId: match.targetIds[0], outcome });
    return outcome;
  } catch (e) {
    // Propagate abort so the caller can handle it
    if (e?.name === 'AbortError') throw e;
    // API unreachable/unknown — distinguish from true cold so callers can fail open.
    console.debug(`[NeurOn] Health check failed for ${modelId}: ${e.message}`);
    diagnosticLog("WARN", "health.check.failed", { modelId, error: e.message });
    return "unreachable";
  }
}

// Preflight variant with a hard timeout budget — used by tool.execute.before.
// Uses Promise.race so that a caller's timeout stops waiting without aborting
// the module-global _statusInflight (which may be shared by unrelated callers).
async function checkTargetHealthyWithTimeout(client, modelId, timeoutMs) {
  let timer;
  const timedOut = new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('preflight_timeout')), timeoutMs); });
  try {
    // checkTargetHealthy no longer receives a per-caller signal;
    // its internal status fetch is isolated from this caller's timeout.
    return await Promise.race([
      checkTargetHealthy(client, modelId),
      timedOut
    ]);
  } catch (e) {
    if (e?.message === 'preflight_timeout') {
      diagnosticLog("WARN", "health.check.timeout", { modelId, timeoutMs });
      return "unreachable";
    }
    if (e?.name === 'AbortError') {
      diagnosticLog("WARN", "health.check.aborted", { modelId });
      return "unreachable";
    }
    diagnosticLog("WARN", "health.check.failed", { modelId, error: e?.message });
    return "unreachable";
  } finally {
    clearTimeout(timer);
  }
}

// Internal health-check — signal parameter accepted for compatibility but ignored.
async function checkTargetHealthyWithSignal(client, modelId, _signal) {
  try {
    const status = await getCachedStatus(client);
    const splitResult = splitProvider(modelId);
    const match = matchLiteLlmModel(
      status.capacityTargets ?? [],
      status.models ?? [],
      splitResult.bareModelId
    );
    if (!match || match.error) return "unmapped";
    const targetInfo = findTargetStatus(
      status.capacityTargets ?? [],
      match.targetIds[0]
    );
    return targetInfo?.observed ?? "cold";
  } catch (e) {
    // Propagate abort so the caller can handle it
    if (e?.name === 'AbortError') throw e;
    // API unreachable/unknown — distinguish from true cold so callers can fail open.
    return "unreachable";
  }
}

function backgroundReserve(client, modelId, sessionID, sessionModels, ctx) {
  // Capture (or initialize) generation counter — scrubSession increments on
  // delete/idle/model-switch so our equality check detects staleness.
  let genAtStart = getSessionGen(sessionID);
  if (genAtStart === 0) genAtStart = incrementSessionGen(sessionID);
  const disposeGenAtStart = _disposeGen;
  (async () => {
    // Early exit if disposed before async work begins
    if (_disposed || _disposeGen !== disposeGenAtStart) return;
    try {
      await ensureReservation(client, modelId, sessionID);
      // Guard: session may have been deleted while we were creating the reservation
      if (_disposed || _disposeGen !== disposeGenAtStart || getSessionGen(sessionID) !== genAtStart) return;
      // Gap 5: Start per-session lease refresh loop to maintain reservation through active generation
      startLeaseRefresh(sessionID, client, sessionModels);

      const info = sessionModels.get(sessionID);
      if (info) {
        info.warmupNotified = false;
        info.errorNotified = false;
        safeToast(ctx.client?.tui, {
          message: `NeurOn: model ready`,
          variant: "success"
        });
      }
    } catch (e) {
      // Session deleted or plugin disposed — discard notification
      if (_disposed || _disposeGen !== disposeGenAtStart || getSessionGen(sessionID) !== genAtStart) return;

      // Cooldown/deferral errors from reserveOrRefreshTarget — silent fail
      if (e.message?.includes('cooldown active')) {
        console.debug(`[NeurOn] Reservation deferred due to cooldown for ${modelId}`);
        return;
      }

      // Notify user only once per session — classify the failure type
      const info = sessionModels.get(sessionID);
      if (!info) return;

      if (e.message?.includes("ambiguous")) {
        if (!info.errorNotified) {
          info.errorNotified = true;
          safeToast(ctx.client?.tui, { message: e.message, variant: "error" });
        }
        return;
      }

      if (e instanceof NeurOnApiError) {
        if (e.status === 0) {
          // Timeout/unreachable — fail open silently
          console.debug(`[NeurOn] Reservation request timed out for ${modelId}`);
          return;
        }
        if (!info.errorNotified) {
          info.errorNotified = true;
          let msg = `NeurOn: reservation failed`;
          if (e.status === 401 || e.status === 403) msg += ' (authentication error)';
          else if (e.status === 429) msg += ' (rate limited — wait and retry)';
          else if (e.status >= 500) msg += ' (server error)';
          else msg += ` (HTTP ${e.status})`;
          console.debug(`[NeurOn] ${msg} for ${modelId}`);
          safeToast(ctx.client?.tui, { message: msg, variant: "error" });
        }
        return;
      }

      // Generic cold/stopped warmup notification — only for truly cold states
      if (!info.warmupNotified) {
        info.warmupNotified = true;
        console.debug(`[NeurOn] Target cold for ${modelId}, warming up`);
        safeToast(ctx.client?.tui, {
          message: `NeurOn: target cold, warming up… please retry in 2-3 min`,
          variant: "warning"
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
    console.log(`[NeurOn] Plugin initialized (API: ${client.config.apiOrigin})`);
    diagnosticLog("INFO", "plugin.initialized", { apiOrigin: client.config.apiOrigin, allowedProviderCount: allowedProviders.length });
  } catch (e) {
    // Log the configuration error so the user knows the plugin is disabled.
    console.error(`[NeurOn] Failed to initialize: ${e.message}`);
    diagnosticLog("ERROR", "plugin.initialization_failed", { error: e.message });
    safeToast(ctx.client?.tui, { message: `NeurOn: ${e.message}`, variant: "error" });
    return { event: () => {}, "tool.execute.before": () => {} };
  }

  // Track session -> model mapping from session.created events
  const sessionModels = new Map();

  return {
    event: async ({ event }) => {
      if (_disposed) return;
      const type = event.type;
      const props = event?.properties || {};
      // Prefer camelCase `sessionID`; fall back to `sessionId` defensively.
      const sessionID = extractSessionID(event);
      if (!sessionID) return;

      if (type === "plugin.added" || type === "message.part.delta") return;

      // Release all per-session state only on true terminal events.
      // session.compacted keeps the session alive (model/reservation state remains valid).
      if (type === "session.deleted") {
        diagnosticLog("INFO", "session.deleted", { sessionID });
        scrubSession(client, sessionID, sessionModels);
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
        diagnosticLog("INFO", "model.session_captured", { sessionID, modelId: normalized.fullModel });
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

      if (eventModel?.id) {
        const normalizedEvent = canonicalizeModel(eventModel.providerID, eventModel.id);
        if (normalizedEvent.bareModelId !== cachedModel?.id || normalizedEvent.provider !== cachedModel?.provider) {
          // Model switched — reset notification flags so user gets fresh warnings
          const info = sessionModels.get(sessionID);
          if (info) {
            info.warmupNotified = false;
            info.errorNotified = false;
            info.stoppingNotified = false;
          }

          const oldFullModel = cachedModel?.provider
            ? `${cachedModel.provider}/${cachedModel.id}`
            : cachedModel?.id;
          if (oldFullModel) {
            // Clear the old generation before any new-model lookup can block.
            releaseSessionReservations(client, sessionID);
          } else {
            // Preserve the existing no-prior-model behavior without releasing
            // an unassociated current reservation.
            abortSessionWork(sessionID);
            incrementSessionGen(sessionID);
            cancelLeaseRefresh(sessionID);
          }
          if (oldFullModel) {
            console.debug(`[NeurOn] Model switched from ${oldFullModel} to ${normalizedEvent.fullModel}`);
            diagnosticLog("INFO", "model.session_switched", { sessionID, from: oldFullModel, to: normalizedEvent.fullModel });
          }

          sessionModels.set(sessionID, {
            id: normalizedEvent.bareModelId,
            provider: normalizedEvent.provider
          });
        }
      }

      // Recursive traffic must never reserve, but perform model-switch cleanup first.
      if (provider) {
        const p = provider.toLowerCase();
        if (p === 'neuron' || p === 'neuron-bridge' || p === 'opencode-neuron') return;
      }

      const role =
        event.role ?? event.properties?.info?.role ?? event.properties?.role;

      // Pre-request: check target health on user message
      if (type === "message.updated" && role === "user") {
        const genAtEventStart = getSessionGen(sessionID);
        console.debug(`[NeurOn] message.updated: model=${fullModel}, provider=${provider}`);
        if (!matchesAllowedProvider(provider, fullModel, allowedProviders)) {
          console.debug(`[NeurOn] message.updated: blocked by provider filter`);
          return;
        }

        // Cooldown gate — prevent any network work during active transport failure window.
        // Catches reservation-only outages (target-scoped cooldown) that the healthy-path
        // would otherwise bypass since it skips all cooldown checks after status returns ok.
        const msgCooldownGlobal = transportCooldown.get(client.config.apiBaseUrl) ?? 0;
        const msgCooldownModel = transportCooldown.get(`${client.config.apiBaseUrl}::${fullModel}`) ?? 0;
        const msgLastFailure = Math.max(msgCooldownGlobal, msgCooldownModel);
        if (Date.now() - msgLastFailure < client.config.cooldownPeriodMs) {
          console.debug(`[NeurOn] message.updated skipped — cooldown active for ${fullModel}`);
          return;
        }

        const targetState = await checkTargetHealthyWithTimeout(
          client, fullModel, client.config.preflightTimeoutMs
        );
        diagnosticLog("INFO", "health.preflight.outcome", { sessionID, modelId: fullModel, outcome: targetState, source: "message.updated" });

        if (targetState === "healthy") {
          // Target already running — background any reserve/refresh work entirely.
          // Do NOT block the message path for network I/O.
          const info = sessionModels.get(sessionID);
          if (info) info.stoppingNotified = false;
          // Stale-gen guard — do not spawn background work after session.deleted
          if (getSessionGen(sessionID) !== genAtEventStart) {
            console.debug(`[NeurOn] Skipping healthy-path reserve — session deleted for ${sessionID}`);
            return;
          }
          const genAtStart = getSessionGen(sessionID);
          const disposeGenAtStart = _disposeGen;
          (async () => {
            // Early exit if disposed before async work begins
            if (_disposed || _disposeGen !== disposeGenAtStart) return;
            try {
              // Check staleness/disposed before and after async work
              if (_disposeGen !== disposeGenAtStart || getSessionGen(sessionID) !== genAtStart) return;
              const result = await resolveTargetForModel(client, fullModel, sessionID);
              if (_disposed || _disposeGen !== disposeGenAtStart || getSessionGen(sessionID) !== genAtStart) return;
              if (!state.reservations.has(result.resKey)) {
                await ensureReservation(client, fullModel, sessionID);
              } else {
                await refreshExistingReservation(client, fullModel, sessionID);
              }
              // Gap 5: Start lease refresh after successful reservation/refresh
              if (!_disposed && _disposeGen === disposeGenAtStart && getSessionGen(sessionID) === genAtStart) {
                startLeaseRefresh(sessionID, client, sessionModels);
              }
            } catch (e) {
              if (_disposed || _disposeGen !== disposeGenAtStart) return;
              console.debug(`[NeurOn] Background refresh failed for ${fullModel}: ${e.message}`);
            }
          })();
          return;
        }

        if (targetState === "stopping") {
          // Target is shutting down — clear stale reservation, notify user
          try {
            const result = await resolveTargetForModel(client, fullModel, sessionID);
            const entry = state.reservations.get(result.resKey);
            state.reservations.delete(result.resKey);
            if (entry) {
              releaseActiveReservations(client, [{
                reservationId: entry.reservation?.reservationId,
                targetId: result.targetId,
                owned: entry.owned
              }]);
            }
          } catch (e) {
            console.debug(`[NeurOn] Failed to clear stopping reservation: ${e.message}`);
          }
          // Stale-gen guard — do not spawn work after session.deleted
          if (getSessionGen(sessionID) !== genAtEventStart) {
            console.debug(`[NeurOn] Skipping stopping reserve — session deleted for ${sessionID}`);
            return;
          }
          const info = sessionModels.get(sessionID);
          if (info && !info.stoppingNotified) {
            info.stoppingNotified = true;
            safeToast(ctx.client?.tui, { message: "NeurOn: target stopping, restarting… please retry in 2-3 min", variant: "warning" });
          }
          backgroundReserve(client, fullModel, sessionID, sessionModels, ctx);
          // Generic event hook never blocks on stopping — tool.execute.before handles blocking.
          return;
        }

        // NeurOn API unreachable — fail open, no toast, no warmup trigger
        if (targetState === "unreachable") {
          console.debug(`[NeurOn] API unreachable for ${fullModel}, failing open`);
          // Stale-gen guard — do not spawn work after session.deleted
          if (getSessionGen(sessionID) !== genAtEventStart) {
            console.debug(`[NeurOn] Skipping unreachable reserve — session deleted for ${sessionID}`);
            return;
          }
          // Gap 4: Cooldown guard — prevent repeated reconnect events from starting fresh retries
          const lastFailure = Math.max(
            transportCooldown.get(client.config.apiBaseUrl) ?? 0,
            transportCooldown.get(`${client.config.apiBaseUrl}::${fullModel}`) ?? 0
          );
          if (Date.now() - lastFailure >= client.config.cooldownPeriodMs) {
            // Still attempt a silent background reserve; if transport is truly down,
            // backgroundReserve will fail open quietly for timeout/unreachable errors.
            backgroundReserve(client, fullModel, sessionID, sessionModels, ctx);
          } else {
            console.debug(`[NeurOn] Skipping background reserve — cooldown active (${Math.ceil((client.config.cooldownPeriodMs - (Date.now() - lastFailure)) / 1000)}s remaining)`);
          }
          return;
        }

        if (targetState === "unmapped") {
          console.debug(`[NeurOn] No capacity mapping for ${fullModel}`);
          return;
        }

        // Target is cold/stopped — start warmup and notify, then return.
        // Generic event hook never blocks on cold — tool.execute.before handles blocking.
        console.debug(`[NeurOn] Target cold/stopped for ${fullModel} (${targetState})`);
        const info = sessionModels.get(sessionID);
        if (info && !info.warmupNotified) {
          info.warmupNotified = true;
          safeToast(ctx.client?.tui, { message: "NeurOn: warming up… please retry in 2-3 min", variant: "warning" });
        }
        // Gap 4: Cooldown guard + stale-gen guard — prevent reconnect storms and
        // prevent spawning work after session.deleted incremented generation counter
        if (getSessionGen(sessionID) !== genAtEventStart) {
          console.debug(`[NeurOn] Skipping background reserve (cold) — session deleted for ${sessionID}`);
          return;
        }
        const lastFailureCold = Math.max(
          transportCooldown.get(client.config.apiBaseUrl) ?? 0,
          transportCooldown.get(`${client.config.apiBaseUrl}::${fullModel}`) ?? 0
        );
        if (Date.now() - lastFailureCold >= client.config.cooldownPeriodMs) {
          backgroundReserve(client, fullModel, sessionID, sessionModels, ctx);
        } else {
          console.debug(`[NeurOn] Skipping background reserve (cold) — cooldown active for ${fullModel}`);
        }
        return;
      }

      if (type === "session.error") {
        if (!matchesAllowedProvider(provider, fullModel, allowedProviders)) return;
        // Gap 4: Cooldown guard — prevent repeated error events from starting fresh retries during outage
        const lastFailure = Math.max(
          transportCooldown.get(client.config.apiBaseUrl) ?? 0,
          transportCooldown.get(`${client.config.apiBaseUrl}::${fullModel}`) ?? 0
        );
        if (Date.now() - lastFailure < client.config.cooldownPeriodMs) {
          console.debug(`[NeurOn] Session error skipped — cooldown active for ${fullModel}`);
          return;
        }
        const targetState = await checkTargetHealthyWithTimeout(
          client, fullModel, client.config.preflightTimeoutMs
        );
        diagnosticLog("INFO", "health.preflight.outcome", { sessionID, modelId: fullModel, outcome: targetState, source: "session.error" });
        if (targetState === "unmapped") {
          console.debug(`[NeurOn] No capacity mapping for ${fullModel}`);
          return;
        }
        console.debug(`[NeurOn] Session error for ${fullModel}, attempting background reserve`);
        // Background the reservation — do not block on potentially long
        // waitForHealthy. Error toasts are handled inside backgroundReserve.
        backgroundReserve(client, fullModel, sessionID, sessionModels, ctx);
      }

      // Refresh reservation on session idle (keepalive)
      if (type === "session.idle") {
        if (!matchesAllowedProvider(provider, fullModel, allowedProviders)) return;
        // Invalidate pending async work so stale results cannot restore state after idle
        abortSessionWork(sessionID);
        const idleGen = incrementSessionGen(sessionID);
        // Gap 5: Cancel lease refresh on idle — session is no longer actively generating
        cancelLeaseRefresh(sessionID);
        // Catch observes failures rethrown by refreshExistingReservation without
        // rejecting the event promise — session.idle must never throw to caller.
        refreshExistingReservation(client, fullModel, sessionID, undefined, idleGen).catch((e) => {
          if (_disposed) return;
          console.debug(`[NeurOn] Idle refresh failed for ${fullModel}: ${e.message}`);
        });
      }
    },

    "tool.execute.before": async ({ tool, sessionID, callID }, _output) => {
      if (_disposed) return;
      try {
        if (!sessionID) return;

        // Model lookup from session state — populated by session.created / message.updated.
        let cachedModel = sessionModels.get(sessionID);

        // Hydrate from tool input when session cache is empty
        // (tool may execute before session.created event is processed).
        if (!cachedModel && tool?.input) {
          const info = tool.input.info ?? tool.input;
          const modelData = info?.model ?? info;
          if (modelData?.id) {
            const normalized = canonicalizeModel(modelData.providerID, modelData.id);
            cachedModel = {
              id: normalized.bareModelId,
              provider: normalized.provider
            };
            sessionModels.set(sessionID, cachedModel);
          }
        }
        const model = cachedModel?.id;
        if (!model) return;

        const provider = cachedModel?.provider;
        const fullModel = provider ? `${provider}/${model}` : model;
        const normalizedProvider = provider?.toLowerCase();
        if (normalizedProvider === 'neuron' || normalizedProvider === 'neuron-bridge' || normalizedProvider === 'opencode-neuron') return;
        if (!matchesAllowedProvider(provider, fullModel, allowedProviders)) return;

        // Fail-open cooldown: skip health check if recent transport failure.
        // Two-tier check: global (set by getCachedStatus on any status-fetch failure)
        // and model-scoped (set here on per-model unreachable results).
        const globalCooldownKey = client.config.apiBaseUrl;
        const modelCooldownKey = `${client.config.apiBaseUrl}::${fullModel}`;
        const globalFailure = transportCooldown.get(globalCooldownKey) ?? 0;
        const modelFailure = transportCooldown.get(modelCooldownKey) ?? 0;
        const lastFailure = Math.max(globalFailure, modelFailure);
        if (Date.now() - lastFailure < client.config.cooldownPeriodMs) {
          return;
        }

        // Use fast preflight timeout for health check to avoid blocking tool execution.
        const targetState = await checkTargetHealthyWithTimeout(
          client, fullModel, client.config.preflightTimeoutMs
        );
        if (targetState === "unreachable") {
          // API unreachable — set model-scoped transport-failure timestamp
          transportCooldown.set(modelCooldownKey, Date.now());
          return;
        }
        if (targetState === "unmapped") return;
        if (targetState !== "healthy") {
          diagnosticLog("WARN", "reservation.blocked_cold_target", { sessionID, modelId: fullModel, outcome: targetState, tool: tool?.id ?? tool?.name });
          backgroundReserve(client, fullModel, sessionID, sessionModels, ctx);
          throw new Error(`NeurOn: target is ${targetState}, warming up — please retry in 2-3 min`);
        }
      } catch (e) {
        if (e.message?.includes("NeurOn:")) throw e;
        // API unreachable — fail open to avoid blocking tool execution
        console.debug(`[NeurOn] Tool preflight failed, failing open: ${e.message}`);
      }
    },

    // Release all per-session state on plugin shutdown so nothing lingers
    // between sessions in a long-running process. Setting _disposed first
    // prevents fire-and-forget background work from mutating state or showing toasts.
    dispose: async () => {
      diagnosticLog("INFO", "plugin.dispose.start", { reservationCount: state.reservations.size });
      _disposed = true;
      _disposeGen++; // Monotonic — old async work will detect gen mismatch
      for (const [, controller] of sessionAbortControllers) controller.abort();
      sessionAbortControllers.clear();
      // Cancel all lease refresh intervals before clearing state
      for (const [sid, intervalId] of leaseRefreshIntervals) {
        clearInterval(intervalId);
      }
      leaseRefreshIntervals.clear();
      leaseRefreshInflight.clear();
      const reservations = [];
      for (const [key, entry] of state.reservations) {
        reservations.push({
          reservationId: entry.reservation?.reservationId,
          targetId: key.slice(key.indexOf('::') + 2),
          owned: entry.owned
        });
      }
      state.reservations.clear();
      const releases = releaseActiveReservations(client, reservations);
      // Shutdown is the final chance to return capacity. Force one retry for
      // retained owned releases, but bound the wait so teardown never hangs.
      releases.push(...retryPendingOwnedReleases(client, true));
      releases.push(...releaseInflight.values());
      const deadline = Date.now() + client.config.disposeReleaseTimeoutMs;
      releases.push(...reservationCreateInflight);
      await drainReleaseAttempts(releases, Math.max(0, deadline - Date.now()));
      const finalReleases = [
        ...retryPendingOwnedReleases(client, true),
        ...releaseInflight.values()
      ];
      await drainReleaseAttempts(finalReleases, Math.max(0, deadline - Date.now()));
      state.inflight.clear();
      state.inflightTarget.clear();
      state.retryState.clear();
      sessionModels.clear();
      sessionGeneration.clear();
      transportCooldown.clear();
      _statusCache = null;
      _statusCacheTime = 0;
      _statusInflight = null;
      diagnosticLog("INFO", "plugin.dispose.complete");
    }
  };
};

// ── Test-only reset (not part of public API) ────────────────
// Resets all module-level state so tests run in isolation.
export function __testReset() {
  // Increment dispose gen so old background work from previous tests detects staleness
  _disposeGen++;
  _disposed = false;
  _statusCache = null;
  _statusCacheTime = 0;
  _statusInflight = null;
  state.reservations.clear();
  state.inflight.clear();
  state.inflightTarget.clear();
  state.retryState.clear();
  transportCooldown.clear();
  sessionGeneration.clear();
  for (const [, controller] of sessionAbortControllers) controller.abort();
  sessionAbortControllers.clear();
  // Cancel and clear all lease refresh intervals
  for (const [, intervalId] of leaseRefreshIntervals) {
    clearInterval(intervalId);
  }
  leaseRefreshIntervals.clear();
  leaseRefreshInflight.clear();
  releaseInflight.clear();
  reservationCreateInflight.clear();
  locallyOwnedReservationIds.clear();
  pendingOwnedReleases.clear();
  for (const timer of pendingOwnedReleaseTimers.values()) clearTimeout(timer);
  pendingOwnedReleaseTimers.clear();
}

// Expose internals for testing
export { state, transportCooldown, leaseRefreshIntervals };
