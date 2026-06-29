# Model providers for `ask`

`ask` talks to model providers through an adapter seam. Provider output is advisory Query Draft content; `kusto-cli` still performs independent safety validation and execution remains gated by `--execute`.

## Provider modes

| Provider | External model call | Use case |
|----------|---------------------|----------|
| `fake` | No | Deterministic offline Query Drafts for tests, demos, and automation wiring. This is the default. |
| `openai-compatible` | Yes | Any chat-completions endpoint compatible with the OpenAI request/response shape used by `kusto-cli`. |

## Fake provider default

No provider configuration is required for offline behavior:

```bash
kusto-cli \
  --service-uri https://help.kusto.windows.net \
  --database Samples \
  ask 'show recent storm events'
```

The fake provider returns a deterministic bounded draft shaped like:

```kql
search '<prompt>'
| take 10
```

It is intentionally simple. Use it to evaluate Query Draft plumbing, Target resolution, validation, disclosure reporting, examples/shots handling, and Execution Gate behavior without external model calls.

## OpenAI-compatible provider

Configure a real provider explicitly with flags, environment variables, or config keys. Keep secrets in environment variables; config stores only the variable name.

```bash
export OPENAI_API_KEY="..."

kusto-cli \
  --service-uri https://help.kusto.windows.net \
  --database Samples \
  --model-provider openai-compatible \
  --model-endpoint https://api.openai.com/v1/chat/completions \
  --model test-model \
  --model-api-key-env OPENAI_API_KEY \
  ask 'show recent storm events'
```

Equivalent config:

```bash
kusto-cli config set model-provider openai-compatible
kusto-cli config set model-endpoint https://api.openai.com/v1/chat/completions
kusto-cli config set model test-model
kusto-cli config set model-api-key-env OPENAI_API_KEY
```

Do **not** store an API key value in `model-api-key-env`; store the environment variable name only. `kusto-cli` reads the secret at runtime and redacts provider error snippets when possible.

## Environment variables

| Environment variable | Equivalent flag | Description |
|----------------------|-----------------|-------------|
| `KUSTO_MODEL_PROVIDER` | `--model-provider` | `fake` or `openai-compatible`. |
| `KUSTO_MODEL_ENDPOINT` | `--model-endpoint` | Chat completions endpoint URL. |
| `KUSTO_MODEL` | `--model` | Provider model/deployment name. |
| `KUSTO_MODEL_API_KEY_ENV` | `--model-api-key-env` | Name of the environment variable containing the API key. Defaults to `OPENAI_API_KEY`. |

## Structured provider output

The OpenAI-compatible adapter requests JSON with this provider-owned shape:

```json
{
  "query": "StormEvents | take 5",
  "clarification_required": false,
  "clarification_question": "",
  "assumptions": ["Using StormEvents from Schema Context."],
  "warnings": ["Review before execution."],
  "model_safety": {
    "classification": "safe",
    "reason": "Read-only query draft."
  }
}
```

`model_safety` is advisory. The final CLI response adds Target, Schema Context, Data Disclosure Policy, validation, execution, and optional Repair Pass metadata.

## Disclosure to providers

Provider requests may include:

- the selected Target URI and database name;
- compact Schema Context;
- docstrings when present;
- bundled public examples and safe configured shots;
- sample rows only when explicitly requested with `--include-samples` or `--include-sample-rows`.

Provider requests do not include Kusto query results by default, including after `ask --execute`.

## Troubleshooting

- `unknown model-provider`: use `fake` or `openai-compatible`.
- `model is required`: set `--model` or `KUSTO_MODEL` when using `openai-compatible`.
- `model API key environment variable ... is empty or not set`: set the named environment variable, or change `--model-api-key-env` to the variable that holds your key.
- Provider HTTP errors are surfaced as concise redacted snippets; secrets should not appear in normal output.
