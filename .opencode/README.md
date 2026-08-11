# opencode-neuron

OpenCode plugin for [NeurOn](https://github.com/cvalusek/NeurOn), a lightweight
control plane for shared self-hosted LLM capacity.

The plugin reserves NeurOn capacity before OpenCode sends a chat message. It
waits until NeurOn reports the matching target healthy, then lets the request
continue. After completions, it refreshes the same reservation to keep capacity
warm without stacking long reservation tails.

## Install

Install the package wherever OpenCode loads npm plugins:

```bash
npm install opencode-neuron
```

For project-local development, this repository also keeps the plugin at:

```text
.opencode/plugins/neuron.js
```

## Configuration

Required:

```env
NEURON_API_KEY=sk-neuron-...
```

Optional:

```env
NEURON_API_BASE_URL=http://localhost:8090
NEURON_RESERVATION_DURATION_MINUTES=2
NEURON_RESERVATION_KEEPALIVE_MINUTES=2
NEURON_WAIT_FOR_HEALTHY=true
NEURON_WAIT_TIMEOUT_SECONDS=600
NEURON_WAIT_POLL_SECONDS=5
NEURON_STRICT_PROVIDER_MATCH=true
```

- `NEURON_STRICT_PROVIDER_MATCH=true` keeps provider matching strict (`provider/model`
  must map to a target with the same provider label).
- Set `NEURON_STRICT_PROVIDER_MATCH=false` when OpenCode provider labels (for example,
  `litellm`) differ from NeurOn target provider labels (for example, `aws-ec2`), and
  you still want alias-based model routing to work when the target match is
  unambiguous.

## Model Mapping

OpenCode model names are LiteLLM-facing names. NeurOn maps those names through
configured model IDs, aliases, backend IDs, runtime IDs, and target
`litellmDisplayPrefix` metadata.

If LiteLLM aliases a route prefix away, configure NeurOn with an empty display
prefix for that target.

## Runtime Warmup

Model warmup happens in NeurOn, not in this plugin. When configured, NeurOn keeps
a target in `provisioning` until the requested reservation models have been
warmed. The plugin simply waits for NeurOn's reservation status to become
healthy.

## License

AGPL-3.0-only.
