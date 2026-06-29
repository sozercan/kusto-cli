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

## Query Draft safety validation v1

`ask` is generate-first and does not execute generated KQL. The model provider's `model_safety` field is advisory only; the CLI-owned Query Draft validator is authoritative and records its result in `validation`.

The v1 static validator is conservative and intentionally not a full KQL parser. A Query Draft is considered execution-eligible only when validation status is `passed` and `safe_for_execution` is `true`:

- Generated output must be raw KQL, not Markdown code fences or prose.
- Management commands are blocked for `ask`, including read-only `.show` commands. Use explicit CLI commands such as `command`, `databases`, or `tables` for management/schema tasks.
- Obvious write-capable or destructive shapes are blocked, including ingestion, `into table`, create/alter/drop/delete/purge/rename/move/truncate forms, and set-or-append/set-or-replace forms.
- Multi-statement output may contain only `let` declarations or `declare query_parameters` statements before one final query expression. Multiple executable query statements and generated `set` request-option statements are blocked.
- Exploratory row-returning drafts must include an explicit result bound (`take`, `limit`, `top`, or `sample`) or a reducing aggregation such as `count`, `summarize`, or `make-series`. Unbounded drafts produce validation warnings and are not execution-eligible until corrected.
- If schema/prompt ambiguity blocks a safe table or function choice, a Query Draft may set `clarification_required` with a concise `clarification_question` instead of hallucinating a table choice. Non-blocking ambiguity should appear as explicit `assumptions`.

## Agent guidance

- Prefer `kusto_query` for KQL queries.
- Use `kusto_command` only for management commands.
- Run `schema <tool>` before calling unfamiliar tools.
- Keep credentials in environment variables or an external credential provider.
