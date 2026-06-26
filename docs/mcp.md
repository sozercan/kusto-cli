# MCP behavior

`kusto-cli` implements a local stdio MCP server.

## Startup sequence

1. The agent sends `initialize` over stdio.
2. `kusto-cli` returns server info, instructions, and tool capability metadata.
3. The agent sends `notifications/initialized`.
4. The agent can call `tools/list` and `tools/call`.

## Result format

Kusto query and command tools return text content containing JSON:

```json
{
  "data": {
    "columns": [{"ColumnName":"Count","ColumnType":"long"}],
    "rows": [[59066]]
  },
  "format": "kusto_response"
}
```

## Safety

- `kusto_query` rejects management commands starting with `.`.
- `kusto_command` requires management commands starting with `.`.
- Read-only operations set `request_readonly` and `request_readonly_hardline`.
- Attempts to override readonly flags through `client_request_properties` fail.
