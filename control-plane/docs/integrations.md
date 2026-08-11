---
type: Reference
title: Integrations
description: API keys, OpenAPI, Swagger UI, MCP, and plugin integration notes.
tags: [integrations, api, openapi, mcp, api-keys]
timestamp: 2026-06-28T00:00:00Z
---

# Integrations

NeurOn exposes integration surfaces for internal tools and plugins that need to
reserve capacity or inspect current capacity state.

## API Keys

Users can create API keys from:

```text
GET /api-keys
```

Generated keys use the `sk-neuron-...` format. The full key is shown only once
in the creation response/page. After that, NeurOn stores only a SHA-256 hash and
a display prefix.

Reservations created with an API key keep the real owner as the username, but
status pages show the API key name in parentheses, for example
`clint ( OpenCode )`.

Use keys with Bearer auth:

```http
Authorization: Bearer sk-neuron-...
```

API keys resolve to the username that created them. Their admin status follows
the same `ADMIN_USERS` rule as cookie and Basic auth. For safety, the MCP
`end_reservation` tool only ends reservations owned by the key's user, even if
that user is an admin.

API keys are stored through the configured storage driver:

- `memory`: process-local keys for throwaway development
- `sqlite`: keys in the SQLite `api_keys` table
- `postgres`: keys in the Postgres `api_keys` table

SQLite and Postgres keys survive NeurOn restarts. Keys created before durable
API-key storage existed were never written to disk and must be regenerated.

## OpenAPI And Swagger

The OpenAPI 3.0 document is available at:

```text
GET /openapi.json
```

Swagger UI is available at:

```text
GET /docs
```

The OpenAPI document includes Basic and Bearer authentication schemes and
schemas for the main plugin-facing endpoints, including models, reservations,
status, API keys, and MCP.

Reservation responses may include `costEstimate` after the reconciler has
allocated estimated target activation cost to that reservation. This is best-effort
chargeback metadata, not a provider invoice.

Good read-only smoke tests:

```bash
curl -H "Authorization: Bearer sk-neuron-..." http://localhost:8090/api/models
curl -H "Authorization: Bearer sk-neuron-..." http://localhost:8090/api/status
```

## OpenCode Plugin

This repository includes a project-local OpenCode plugin at
`.opencode/plugins/neuron.js`. It reads `NEURON_API_KEY` by default and creates
a short NeurOn reservation before a chat message is sent. Later messages for
the same model wait for health before the request is sent. Completion events
refresh that reservation to the configured duration from now without waiting for
health again or stacking more time onto the old expiration.

The plugin package is publishable as `opencode-neuron` from the `.opencode`
directory. The plugin build workflow checks syntax, unit tests, and `npm pack
--dry-run`.

Release process:

1. Update `.opencode/package.json` and `.opencode/package-lock.json` to the new
   version.
2. Merge the change after the plugin build workflow passes.
3. Run the `Publish OpenCode plugin` workflow manually with the same version.
4. Leave `dry_run=true` for a release rehearsal, then rerun with
   `dry_run=false` to publish to npm using the repository `NPM_TOKEN` secret.

For local registry testing, publish to Verdaccio with:

```bash
cd .opencode
npm publish --registry http://localhost:4873
```

Default behavior:

- `NEURON_API_BASE_URL=http://localhost:8090`
- `NEURON_RESERVATION_DURATION_MINUTES=2`
- `NEURON_RESERVATION_KEEPALIVE_MINUTES=2`
- `NEURON_WAIT_FOR_HEALTHY=true`
- `NEURON_STRICT_PROVIDER_MATCH=true`

When `NEURON_WAIT_FOR_HEALTHY` is enabled, the plugin blocks the chat message
until all reservation targets report `healthy`. NeurOn performs any configured
model warmup before reporting `healthy`, so the plugin only waits for NeurOn's
readiness signal.

The plugin maps OpenCode's LiteLLM-facing model name to NeurOn model metadata
using model IDs, aliases, backend IDs, runtime IDs, and each target's
`litellmDisplayPrefix`. If `litellmDisplayPrefix` is not configured, NeurOn
publishes the first `trafficModelPrefixes` value as the display prefix, or
`<target-id>/` when no prefixes are configured. Set the display prefix to `""`
in JSON config, or `__empty__` in env-expanded config, when LiteLLM aliases the
route prefix away for users.

`NEURON_STRICT_PROVIDER_MATCH=true` requires `provider/model` requests from
OpenCode to resolve to NeurOn targets with matching provider labels. Set
`NEURON_STRICT_PROVIDER_MATCH=false` when OpenCode provider labels and NeurOn
target provider labels intentionally differ; in that mode, the plugin allows an
alias-based fallback only when the target match is unambiguous.

The Admin target create and persisted-target edit forms expose
`trafficModelPrefixes` as **LiteLLM model route prefixes**, so a value such as
`clint-desktop/` links `clint-desktop/gemma-4-e2b` to the selected target
without editing JSON. Declarative targets can set the field in JSON/env config
or use **Copy to DB** before editing it in Admin.

With global LiteLLM connectivity configured, successful runtime discovery
upserts a `neuron/<target-id>` credential and publishes each primary runtime
model ID under the effective display prefix. The friendly target display name
is preserved in LiteLLM metadata as `neuron_target_display_name`; routing remains
based on the stable target ID. Credentials identify `openai` as their provider
and use `noapikey` when the target has no configured runtime key. Target stops
do not block or delete deployments, allowing LiteLLM requests to remain queued
while NeurOn starts capacity.

LiteLLM traffic monitoring remains useful for clients that cannot run a plugin.
The OpenCode plugin is a stronger signal when it is available because it can
reserve capacity before sending traffic, rather than reacting to logs after a
request has already reached LiteLLM.

## MCP

NeurOn exposes an authenticated JSON-RPC endpoint:

```text
POST /mcp
```

It supports:

- `initialize`
- `tools/list`
- `tools/call`

Current MCP tools:

- `list_models`: list configured and discovered models
- `list_targets`: list capacity targets and runtime state
- `get_status`: return active reservations and target status
- `create_reservation`: create a reservation for models or targets
- `end_reservation`: mark one of the key user's reservations done

Example:

```bash
curl -H "Authorization: Bearer sk-neuron-..." \
  -H "content-type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' \
  http://localhost:8090/mcp
```

Create a short target reservation:

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "tools/call",
  "params": {
    "name": "create_reservation",
    "arguments": {
      "targetIds": ["runpod"],
      "durationMinutes": 5,
      "keepaliveMinutes": 2
    }
  }
}
```

End that reservation:

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "tools/call",
  "params": {
    "name": "end_reservation",
    "arguments": {
      "reservationId": "<reservation-id>"
    }
  }
}
```

## Codex Stdio Bridge

Some MCP clients, including local Codex MCP configuration, launch command-based
stdio servers instead of connecting directly to HTTP JSON-RPC. NeurOn includes
a small bridge:

```text
scripts/neuron-mcp-stdio.js
```

The bridge reads stdio-framed MCP messages, forwards them to NeurOn's HTTP
`/mcp` endpoint, and writes stdio-framed responses.

Required environment variables:

```env
NEURON_MCP_URL=http://localhost:8090/mcp
NEURON_API_KEY=sk-neuron-...
```

Example Codex MCP config:

```toml
[mcp_servers.neuron]
command = 'C:\Path\To\node.exe'
args = ['C:\Users\Clint\source\repos\NeurOn\control-plane\scripts\neuron-mcp-stdio.js']
startup_timeout_sec = 30

[mcp_servers.neuron.env]
NEURON_MCP_URL = 'http://localhost:8090/mcp'
NEURON_API_KEY = 'sk-neuron-...'
```
