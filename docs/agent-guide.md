# Agent guide

`kusto-cli` is designed to be easy for agents to operate safely:

- Default mode is MCP over stdio.
- Direct commands return JSON.
- Read-only query tools set Kusto readonly request properties.
- User-supplied request properties cannot disable readonly flags.
- Credentials are read from the environment or Azure CLI; no prompts are emitted by the CLI itself.

## Recommended MCP server config

```json
{
  "command": "/absolute/path/to/kusto-cli",
  "args": ["--service-uri", "https://help.kusto.windows.net", "--database", "Samples"]
}
```

## Direct command mode

Run a query:

```bash
kusto-cli --service-uri https://help.kusto.windows.net --database Samples query 'StormEvents | count'
```

Run a management command:

```bash
kusto-cli --service-uri https://help.kusto.windows.net --database Samples command '.show tables'
```

Call an MCP tool directly:

```bash
kusto-cli call kusto_known_services '{}'
```

## Non-interactive auth

For CI or unattended agents, prefer:

```bash
export KUSTO_ACCESS_TOKEN="..."
kusto-cli --auth env --service-uri https://help.kusto.windows.net --database Samples query 'StormEvents | count'
```

Use `--auth azcli` only when `az login` has already completed outside the agent run.
