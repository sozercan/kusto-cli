# Configuration

`kusto-cli` can persist default values in a JSON config file.

## Commands

```bash
kusto-cli config path
kusto-cli config show
kusto-cli config set service-uri https://help.kusto.windows.net
kusto-cli config set database Samples
kusto-cli config set known-services '[{"alias":"samples","service_uri":"https://help.kusto.windows.net","default_database":"Samples"}]'
kusto-cli config set target samples
kusto-cli config unset database
```

## Supported keys

| Key | Equivalent flag |
|-----|-----------------|
| `service-uri` | `--service-uri` |
| `database` | `--database` |
| `known-services` | `--known-services` |
| `target` | `--target` |
| `target-alias` | `--target-alias` |
| `tenant` | `--tenant` |
| `auth` | `--auth` |
| `output` | `--output` |
| `model-provider` | `--model-provider` |
| `model-endpoint` | `--model-endpoint` |
| `model` | `--model` |
| `model-api-key-env` | `--model-api-key-env` |

Flags and environment variables take precedence over config values.

## Target Catalog aliases for `ask`

`ask` requires exactly one Target, a single cluster/database pair. Configure a Target Catalog with stable aliases when you want to select a target without repeating the service URI and database:

```bash
kusto-cli --known-services '[{"alias":"samples","service_uri":"https://help.kusto.windows.net","default_database":"Samples"}]' \
  --target samples \
  ask 'show recent storm events'
```

A catalog entry may use `alias` or `name` as its stable selector. Existing `service_uri`, `service`, `default_database`, and `description` fields remain supported. If multiple targets are configured, `ask` will not infer a target from the prompt; select one with `--target` or provide both `--service-uri` and `--database`.

## Model provider configuration for `ask`

`ask` uses the deterministic fake model provider unless a real provider is explicitly configured. To use an OpenAI-compatible chat completions endpoint, configure the provider, endpoint, model name, and the name of an environment variable that contains the API key:

```bash
export OPENAI_API_KEY="..."
kusto-cli config set model-provider openai-compatible
kusto-cli config set model-endpoint https://api.openai.com/v1/chat/completions
kusto-cli config set model test-model
kusto-cli config set model-api-key-env OPENAI_API_KEY
```

The config stores only the environment variable name, not the API key value. API keys are sent as bearer credentials to the model provider and are not included in normal `ask` output. Model safety classifications in provider output are advisory; `kusto-cli` still applies its own Query Draft validation and execution remains gated.
