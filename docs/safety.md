# Safety

`kusto-cli` separates read operations from write-capable operations.

## Read-only defaults

Read-oriented tools set Kusto readonly request properties:

- `request_readonly`
- `request_readonly_hardline`

User-supplied client request properties cannot override those flags.

## Write-capable operations

The following operations require `--allow-write`:

- inline ingestion
- management commands other than safe `.show` commands

Use `--dry-run` to preview write-capable direct calls without executing them.

## Agent guidance

- Prefer `kusto_query` for KQL queries.
- Use `kusto_command` only for management commands.
- Run `schema <tool>` before calling unfamiliar tools.
- Keep credentials in environment variables or an external credential provider.
