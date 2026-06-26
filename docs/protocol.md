# Protocol behavior

`kusto-cli` can run in stdio service mode for agent integrations.

## Startup sequence

1. The client sends an initialization request over stdio.
2. `kusto-cli` returns server info, instructions, and tool metadata.
3. The client sends an initialized notification.
4. The client can list tools and call tools.

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
