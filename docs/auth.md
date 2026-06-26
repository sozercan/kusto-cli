# Authentication

`kusto-cli` supports non-interactive authentication for agent and CI use.

## Resolution order

With `--auth auto`, the CLI tries:

1. The token environment variable configured by `--token-env`.
2. Azure CLI token acquisition using `az account get-access-token`.

## Commands

Check auth availability:

```bash
kusto-cli auth status
```

Print a bearer token for debugging or integration plumbing:

```bash
kusto-cli auth token
```

Treat token output as secret material.

## Modes

| Mode | Behavior |
|------|----------|
| `auto` | Environment token, then Azure CLI |
| `env` | Environment token only |
| `azcli` | Azure CLI only |
| `none` | No token; useful for offline tool discovery |
