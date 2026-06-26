# kusto-cli

Standalone, agent-friendly Go CLI for Kusto workflows. It provides human-readable subcommands and also includes a advanced API explorer for advanced automation.

It runs as a self-contained Go binary with standard environment and Azure CLI authentication options.

## Install

### Homebrew

```bash
brew tap sozercan/repo
brew install kusto-cli
```

### Prebuilt binaries

Prebuilt archives for Linux, macOS, and Windows are published on the [GitHub Releases](https://github.com/sozercan/kusto-cli/releases) page. Each release includes `checksums.txt`.

### From source

```bash
go build -o bin/kusto-cli ./cmd/kusto-cli
```

## Quick start

Use the public sample endpoint for examples:

```text
Cluster URI: https://help.kusto.windows.net
Database:    Samples
```

Run a query:

```bash
kusto-cli --service-uri https://help.kusto.windows.net --database Samples query 'StormEvents | count'
```

List databases:

```bash
kusto-cli --service-uri https://help.kusto.windows.net --database Samples databases list
```

Describe a table:

```bash
kusto-cli --service-uri https://help.kusto.windows.net --database Samples tables describe StormEvents
```

Sample rows:

```bash
kusto-cli --service-uri https://help.kusto.windows.net --database Samples tables sample StormEvents 5
```

Run as an stdio service:

```bash
kusto-cli --service-uri https://help.kusto.windows.net --database Samples serve
```

## Authentication

`kusto-cli` is non-interactive by default. It resolves a bearer token in this order when `--auth auto` is used:

1. `KUSTO_ACCESS_TOKEN`
2. `az account get-access-token --resource https://kusto.kusto.windows.net`

Check auth:

```bash
kusto-cli auth status
```

## Common commands

| Command | Description |
|---------|-------------|
| `kusto-cli query '<kql>'` | Run a KQL query |
| `kusto-cli command '.show tables'` | Run a management command |
| `kusto-cli databases list` | List databases |
| `kusto-cli tables list` | List tables |
| `kusto-cli tables describe <table>` | Describe a table |
| `kusto-cli tables sample <table> [size]` | Sample rows from a table |
| `kusto-cli entities list <type>` | List entities by type |
| `kusto-cli services list` | List configured services |
| `kusto-cli deeplink '<kql>'` | Build a web explorer deeplink |
| `kusto-cli queryplan '<kql>'` | Show query plan |
| `kusto-cli diagnostics` | Run diagnostics |
| `kusto-cli api tools` | List API tools |
| `kusto-cli api schema <tool>` | Show API tool schema |
| `kusto-cli api call <tool> '<json>'` | Call a API tool |

## Output

Direct commands support:

```bash
-o json
-o table
-o tsv
```

Example:

```bash
kusto-cli --service-uri https://help.kusto.windows.net --database Samples -o table query 'StormEvents | count'
```

## Safety flags

| Flag | Description |
|------|-------------|
| `--allow-write` | Allow write-capable operations such as inline ingestion and non-`.show` management commands |
| `--dry-run` | Preview write-capable direct calls without executing |
| `--no-input` | Reserved for non-interactive consistency |
| `--force` | Reserved for confirmation consistency |

## Documentation

- [Agent guide](docs/agent-guide.md)
- [Authentication](docs/auth.md)
- [Configuration](docs/config.md)
- [Safety](docs/safety.md)
- [Release](docs/release.md)
- [Protocol behavior](docs/protocol.md)
- [Architecture](docs/architecture.md)

## Development

```bash
make test-short
make vet
make build-static
```

## License

[MIT](LICENSE)
