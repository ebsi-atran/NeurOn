import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { NeurOnPlugin } from "../plugins/neuron.js";

function makeCtx() {
  const toasts = [];
  return {
    toasts,
    ctx: {
      client: {
        tui: {
          showToast(payload) {
            toasts.push(payload.body ?? payload);
          }
        }
      }
    }
  };
}

function eventWithModel({ type, sessionID, role, modelId, providerID }) {
  const model = modelId ? { id: modelId, providerID } : undefined;
  return {
    type,
    sessionID,
    role,
    properties: {
      info: model ? { model } : {}
    }
  };
}

function userMessageUpdated(sessionID, modelId, providerID) {
  return eventWithModel({
    type: "message.updated",
    sessionID,
    role: "user",
    modelId,
    providerID
  });
}

function sessionCreated(sessionID, modelId, providerID) {
  return eventWithModel({
    type: "session.created",
    sessionID,
    modelId,
    providerID
  });
}

function jsonResponse(status, body) {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: status >= 200 && status < 300 ? "OK" : "ERR",
    async text() {
      return JSON.stringify(body);
    }
  };
}

function okJson(body) {
  return jsonResponse(200, body);
}

describe("NeurOn OpenCode plugin (public API)", () => {
  it("blocks cold message path and starts reservation warmup", async () => {
    const { ctx, toasts } = makeCtx();
    const calls = [];

    const originalEnv = {
      NEURON_API_KEY: process.env.NEURON_API_KEY,
      NEURON_API_BASE_URL: process.env.NEURON_API_BASE_URL,
      NEURON_BLOCK_ON_COLD_MESSAGE: process.env.NEURON_BLOCK_ON_COLD_MESSAGE,
      NEURON_ALLOWED_PROVIDERS: process.env.NEURON_ALLOWED_PROVIDERS
    };
    const originalFetch = globalThis.fetch;

    process.env.NEURON_API_KEY = "test-key";
    process.env.NEURON_API_BASE_URL = "http://neuron.test";
    process.env.NEURON_BLOCK_ON_COLD_MESSAGE = "true";
    process.env.NEURON_ALLOWED_PROVIDERS = "";

    globalThis.fetch = async (url, options = {}) => {
      const path = new URL(url).pathname;
      const method = options.method || "GET";
      calls.push({ path, method });

      if (path === "/api/status") {
        return okJson({
          capacityTargets: [
            { id: "t-1", provider: "aws-ec2", modelIds: ["gemma-4-26b-a4b"], observed: "cold" }
          ],
          activeReservations: []
        });
      }
      if (path === "/api/models") {
        return okJson({
          models: [
            {
              id: "gemma-4-26b-a4b",
              aliases: ["gemma-4"],
              backendModelIds: ["gemma-4-26b-a4b"],
              targetIds: ["t-1"]
            }
          ]
        });
      }
      if (path === "/api/reservations" && method === "POST") {
        return jsonResponse(201, {
          reservationId: "r-new",
          status: "active",
          durationMinutes: 2,
          targets: [{ id: "t-1", observed: "cold" }]
        });
      }
      if (path === "/api/reservations/r-new/status") {
        return okJson({
          reservationId: "r-new",
          status: "active",
          targets: [{ id: "t-1", observed: "healthy" }]
        });
      }
      throw new Error(`Unexpected fetch: ${method} ${path}`);
    };

    try {
      const plugin = await NeurOnPlugin(ctx);
      await plugin.event({ event: sessionCreated("s1", "gemma-4-26b-a4b", "aws-ec2") });

      await assert.rejects(
        () => plugin.event({ event: userMessageUpdated("s1", "gemma-4-26b-a4b", "aws-ec2") }),
        /target is cold, warming up/
      );

      await new Promise((resolve) => setTimeout(resolve, 25));
      assert.ok(toasts.some((t) => String(t.message).includes("warming up")));
      assert.ok(
        calls.some((c) => c.path === "/api/reservations" && c.method === "POST"),
        "expected warmup reservation create call"
      );

      await plugin.dispose();
    } finally {
      globalThis.fetch = originalFetch;
      process.env.NEURON_API_KEY = originalEnv.NEURON_API_KEY;
      process.env.NEURON_API_BASE_URL = originalEnv.NEURON_API_BASE_URL;
      process.env.NEURON_BLOCK_ON_COLD_MESSAGE = originalEnv.NEURON_BLOCK_ON_COLD_MESSAGE;
      process.env.NEURON_ALLOWED_PROVIDERS = originalEnv.NEURON_ALLOWED_PROVIDERS;
    }
  });

  it("fails open in tool preflight on unreachable API and cooldown suppresses immediate recheck", async () => {
    const { ctx } = makeCtx();
    const calls = [];

    const originalEnv = {
      NEURON_API_KEY: process.env.NEURON_API_KEY,
      NEURON_API_BASE_URL: process.env.NEURON_API_BASE_URL,
      NEURON_COOLDOWN_PERIOD_MS: process.env.NEURON_COOLDOWN_PERIOD_MS,
      NEURON_PREFLIGHT_TIMEOUT_MS: process.env.NEURON_PREFLIGHT_TIMEOUT_MS,
      NEURON_ALLOWED_PROVIDERS: process.env.NEURON_ALLOWED_PROVIDERS
    };
    const originalFetch = globalThis.fetch;

    process.env.NEURON_API_KEY = "test-key";
    process.env.NEURON_API_BASE_URL = "http://neuron.test";
    process.env.NEURON_COOLDOWN_PERIOD_MS = "60000";
    process.env.NEURON_PREFLIGHT_TIMEOUT_MS = "50";
    process.env.NEURON_ALLOWED_PROVIDERS = "";

    globalThis.fetch = async (url) => {
      const path = new URL(url).pathname;
      calls.push(path);
      if (path === "/api/status") {
        throw new Error("network down");
      }
      if (path === "/api/models") {
        return okJson({ models: [] });
      }
      throw new Error(`Unexpected fetch path: ${path}`);
    };

    try {
      const plugin = await NeurOnPlugin(ctx);
      await plugin.event({ event: sessionCreated("s2", "gemma-4-26b-a4b", "aws-ec2") });

      await assert.doesNotReject(() =>
        plugin["tool.execute.before"]({ event: { sessionID: "s2", properties: {} } })
      );
      const firstCount = calls.length;

      await assert.doesNotReject(() =>
        plugin["tool.execute.before"]({ event: { sessionID: "s2", properties: {} } })
      );
      const secondCount = calls.length;

      assert.ok(firstCount >= 1);
      assert.equal(secondCount, firstCount);

      await plugin.dispose();
    } finally {
      globalThis.fetch = originalFetch;
      process.env.NEURON_API_KEY = originalEnv.NEURON_API_KEY;
      process.env.NEURON_API_BASE_URL = originalEnv.NEURON_API_BASE_URL;
      process.env.NEURON_COOLDOWN_PERIOD_MS = originalEnv.NEURON_COOLDOWN_PERIOD_MS;
      process.env.NEURON_PREFLIGHT_TIMEOUT_MS = originalEnv.NEURON_PREFLIGHT_TIMEOUT_MS;
      process.env.NEURON_ALLOWED_PROVIDERS = originalEnv.NEURON_ALLOWED_PROVIDERS;
    }
  });

  it("uses strict provider matching by default and fails open on provider mismatch", async () => {
    const { ctx } = makeCtx();
    const calls = [];

    const originalEnv = {
      NEURON_API_KEY: process.env.NEURON_API_KEY,
      NEURON_API_BASE_URL: process.env.NEURON_API_BASE_URL,
      NEURON_BLOCK_ON_COLD_MESSAGE: process.env.NEURON_BLOCK_ON_COLD_MESSAGE,
      NEURON_ALLOWED_PROVIDERS: process.env.NEURON_ALLOWED_PROVIDERS
    };
    const originalFetch = globalThis.fetch;

    process.env.NEURON_API_KEY = "test-key";
    process.env.NEURON_API_BASE_URL = "http://neuron.test";
    process.env.NEURON_BLOCK_ON_COLD_MESSAGE = "false";
    process.env.NEURON_ALLOWED_PROVIDERS = "";

    globalThis.fetch = async (url, options = {}) => {
      const path = new URL(url).pathname;
      const method = options.method || "GET";
      calls.push({ path, method });

      if (path === "/api/status") {
        return okJson({
          capacityTargets: [
            { id: "t-aws", provider: "aws-ec2", modelIds: ["gemma-4-26b-a4b"], observed: "cold" }
          ],
          activeReservations: []
        });
      }
      if (path === "/api/models") {
        return okJson({
          models: [
            {
              id: "gemma-4-26b-a4b",
              aliases: [],
              backendModelIds: ["gemma-4-26b-a4b"],
              targetIds: ["t-aws"]
            }
          ]
        });
      }
      if (path === "/api/reservations") {
        return jsonResponse(201, {
          reservationId: "unexpected",
          status: "active",
          targets: [{ id: "t-aws", observed: "cold" }]
        });
      }
      throw new Error(`Unexpected fetch path: ${method} ${path}`);
    };

    try {
      const plugin = await NeurOnPlugin(ctx);
      await plugin.event({ event: sessionCreated("s3", "gemma-4-26b-a4b", "anthropic") });

      await assert.doesNotReject(() => plugin.event({ event: userMessageUpdated("s3", "gemma-4-26b-a4b", "anthropic") }));

      await new Promise((resolve) => setTimeout(resolve, 25));

      assert.ok(calls.some((c) => c.path === "/api/status"));
      assert.ok(calls.some((c) => c.path === "/api/models"));

      await plugin.dispose();
    } finally {
      globalThis.fetch = originalFetch;
      process.env.NEURON_API_KEY = originalEnv.NEURON_API_KEY;
      process.env.NEURON_API_BASE_URL = originalEnv.NEURON_API_BASE_URL;
      process.env.NEURON_BLOCK_ON_COLD_MESSAGE = originalEnv.NEURON_BLOCK_ON_COLD_MESSAGE;
      process.env.NEURON_ALLOWED_PROVIDERS = originalEnv.NEURON_ALLOWED_PROVIDERS;
    }
  });

  it("keeps healthy-session traffic non-blocking while performing reservation maintenance", async () => {
    const { ctx } = makeCtx();
    const calls = [];

    const originalEnv = {
      NEURON_API_KEY: process.env.NEURON_API_KEY,
      NEURON_API_BASE_URL: process.env.NEURON_API_BASE_URL,
      NEURON_BLOCK_ON_COLD_MESSAGE: process.env.NEURON_BLOCK_ON_COLD_MESSAGE,
      NEURON_ALLOWED_PROVIDERS: process.env.NEURON_ALLOWED_PROVIDERS
    };
    const originalFetch = globalThis.fetch;

    process.env.NEURON_API_KEY = "test-key";
    process.env.NEURON_API_BASE_URL = "http://neuron.test";
    process.env.NEURON_BLOCK_ON_COLD_MESSAGE = "false";
    process.env.NEURON_ALLOWED_PROVIDERS = "";

    globalThis.fetch = async (url, options = {}) => {
      const path = new URL(url).pathname;
      const method = options.method || "GET";
      calls.push({ path, method });

      if (path === "/api/status") {
        return okJson({
          capacityTargets: [
            { id: "t-shared", provider: "aws-ec2", modelIds: ["gemma-4-26b-a4b"], observed: "healthy" }
          ],
          activeReservations: [
            {
              reservationId: "r-existing",
              status: "active",
              durationMinutes: 2,
              targets: [{ id: "t-shared", observed: "healthy" }]
            }
          ]
        });
      }
      if (path === "/api/models") {
        return okJson({
          models: [
            {
              id: "gemma-4-26b-a4b",
              aliases: ["gemma-4"],
              backendModelIds: ["gemma-4-26b-a4b"],
              targetIds: ["t-shared"]
            }
          ]
        });
      }

      if (path === "/api/reservations/r-existing/extend" && method === "POST") {
        return okJson({
          reservationId: "r-existing",
          status: "active",
          durationMinutes: 2,
          targets: [{ id: "t-shared", observed: "healthy" }]
        });
      }

      if (path === "/api/reservations" && method === "POST") {
        return jsonResponse(201, {
          reservationId: "r-new-should-not-happen",
          status: "active",
          durationMinutes: 2,
          targets: [{ id: "t-shared", observed: "healthy" }]
        });
      }

      throw new Error(`Unexpected fetch path: ${method} ${path}`);
    };

    try {
      const plugin = await NeurOnPlugin(ctx);

      await plugin.event({ event: sessionCreated("s4", "gemma-4-26b-a4b", "aws-ec2") });
      await plugin.event({ event: userMessageUpdated("s4", "gemma-4-26b-a4b", "aws-ec2") });

      await plugin.event({ event: sessionCreated("s5", "gemma-4-26b-a4b", "aws-ec2") });
      await plugin.event({ event: userMessageUpdated("s5", "gemma-4-26b-a4b", "aws-ec2") });

      await new Promise((resolve) => setTimeout(resolve, 30));

      const maintenanceCalls = calls.filter(
        (c) =>
          (c.path === "/api/reservations" && c.method === "POST") ||
          (c.path === "/api/reservations/r-existing/extend" && c.method === "POST")
      );

      assert.ok(maintenanceCalls.length >= 1);

      await plugin.dispose();
    } finally {
      globalThis.fetch = originalFetch;
      process.env.NEURON_API_KEY = originalEnv.NEURON_API_KEY;
      process.env.NEURON_API_BASE_URL = originalEnv.NEURON_API_BASE_URL;
      process.env.NEURON_BLOCK_ON_COLD_MESSAGE = originalEnv.NEURON_BLOCK_ON_COLD_MESSAGE;
      process.env.NEURON_ALLOWED_PROVIDERS = originalEnv.NEURON_ALLOWED_PROVIDERS;
    }
  });
});
