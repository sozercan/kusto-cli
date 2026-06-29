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

Flags and environment variables take precedence over config values.

## Target Catalog aliases for `ask`

`ask` requires exactly one Target, a single cluster/database pair. Configure a Target Catalog with stable aliases when you want to select a target without repeating the service URI and database:

```bash
kusto-cli --known-services '[{"alias":"samples","service_uri":"https://help.kusto.windows.net","default_database":"Samples"}]' \
  --target samples \
  ask 'show recent storm events'
```

A catalog entry may use `alias` or `name` as its stable selector. Existing `service_uri`, `service`, `default_database`, and `description` fields remain supported. If multiple targets are configured, `ask` will not infer a target from the prompt; select one with `--target` or provide both `--service-uri` and `--database`.
