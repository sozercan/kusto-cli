# Agent guide

`kusto-cli` is designed for both humans and agents.

Use human-friendly commands first:

```bash
kusto-cli query 'StormEvents | count'
kusto-cli databases list
kusto-cli tables describe StormEvents
kusto-cli tables sample StormEvents 5
```

Use the raw MCP API explorer only when a high-level command does not exist:

```bash
kusto-cli api tools
kusto-cli api schema kusto_query
kusto-cli api call kusto_query '{"cluster_uri":"https://help.kusto.windows.net","database":"Samples","query":"StormEvents | count"}'
```

## MCP server mode

For agents that need a stdio MCP server:

```json
{
  "command": "/absolute/path/to/kusto-cli",
  "args": ["--service-uri", "https://help.kusto.windows.net", "--database", "Samples", "serve"]
}
```

## Non-interactive auth

For CI or unattended agents, prefer:

```bash
export KUSTO_ACCESS_TOKEN="..."
kusto-cli --auth env --service-uri https://help.kusto.windows.net --database Samples query 'StormEvents | count'
```

Use `--auth azcli` only when `az login` has already completed outside the agent run.
