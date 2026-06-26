# Configuration

`kusto-cli` can persist default values in a JSON config file.

## Commands

```bash
kusto-cli config path
kusto-cli config show
kusto-cli config set service-uri https://help.kusto.windows.net
kusto-cli config set database Samples
kusto-cli config unset database
```

## Supported keys

| Key | Equivalent flag |
|-----|-----------------|
| `service-uri` | `--service-uri` |
| `database` | `--database` |
| `known-services` | `--known-services` |
| `tenant` | `--tenant` |
| `auth` | `--auth` |
| `output` | `--output` |

Flags and environment variables take precedence over config values.
