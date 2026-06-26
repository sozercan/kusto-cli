# kusto-cli

Standalone, agent-friendly Go CLI for Kusto MCP workflows. It can run as an MCP stdio server for agents or as a direct JSON command runner for scripts.

No Agency runtime, config, or binary is required.

## Install

```bash
go build -o bin/kusto-cli ./cmd/kusto-cli
```

## Quick start

Run as an MCP stdio server, which is the default mode used by agent clients:

```bash
bin/kusto-cli --service-uri https://help.kusto.windows.net --database Samples
```

Run a direct read-only query and print JSON:

```bash
bin/kusto-cli \
  --service-uri https://help.kusto.windows.net \
  --database Samples \
  query 'StormEvents | count'
```

List available MCP tools as JSON:

```bash
bin/kusto-cli tools
```

Call any MCP tool directly:

```bash
bin/kusto-cli call kusto_deeplink_from_query \
  '{"cluster_uri":"https://help.kusto.windows.net","database":"Samples","query":"StormEvents | count"}'
```

## Public sample endpoint

For documentation and smoke testing, use the public Kusto help cluster:

```text
Cluster URI: https://help.kusto.windows.net
Database:    Samples
```

The endpoint is public, but REST query execution still requires Entra authentication.

## Authentication

`kusto-cli` is non-interactive by default. It resolves a bearer token in this order when `--auth auto` is used:

1. `KUSTO_ACCESS_TOKEN`
2. `az account get-access-token --resource https://kusto.kusto.windows.net`

Auth modes:

| Mode | Behavior |
|------|----------|
| `--auth auto` | Environment token, then Azure CLI |
| `--auth env` | Environment token only |
| `--auth azcli` | Azure CLI only |
| `--auth none` | No query execution; useful only for command discovery and diagnostics |

## Configuration

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--service-uri` | `KUSTO_SERVICE_URI` | — | Default Kusto cluster URI |
| `--database` | `KUSTO_SERVICE_DEFAULT_DB` | `NetDefaultDB` | Default database |
| `--known-services` | `KUSTO_KNOWN_SERVICES` | — | JSON array of known services |
| `--token-env` | — | `KUSTO_ACCESS_TOKEN` | Environment variable containing a bearer token |
| `--auth` | — | `auto` | `auto`, `env`, `azcli`, or `none` |
| `--timeout` | — | `90s` | HTTP and TLS handshake timeout |
| `--debug` | — | `false` | Write diagnostic logs to stderr |

## Tools

The MCP server exposes 13 Kusto tools:

- `kusto_query`
- `kusto_command`
- `kusto_known_services`
- `kusto_list_entities`
- `kusto_describe_database`
- `kusto_describe_database_entity`
- `kusto_sample_entity`
- `kusto_graph_query`
- `kusto_ingest_inline_into_table`
- `kusto_get_shots`
- `kusto_deeplink_from_query`
- `kusto_show_queryplan`
- `kusto_diagnostics`

## Agent usage

Add this command as a stdio MCP server in your agent config:

```json
{
  "command": "/absolute/path/to/kusto-cli",
  "args": ["--service-uri", "https://help.kusto.windows.net", "--database", "Samples"]
}
```

## Documentation

- [Agent guide](docs/agent-guide.md)
- [MCP protocol behavior](docs/mcp.md)
- [Architecture](docs/architecture.md)

## Development

```bash
make test-short
make vet
make build-static
```

## License

[MIT](LICENSE)
