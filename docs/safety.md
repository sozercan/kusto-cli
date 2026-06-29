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

`ask` is generate-first and does not execute generated KQL unless the user supplies the explicit Execution Gate with `ask --execute`. The model provider's `model_safety` field is advisory only; the CLI-owned Query Draft validator is authoritative and records its result in `validation`.

The v1 static validator is conservative and intentionally not a full KQL parser. A Query Draft is considered execution-eligible only when validation status is `passed` and `safe_for_execution` is `true`:

- Generated output must be raw KQL, not Markdown code fences or prose.
- Management commands are blocked for `ask`, including read-only `.show` commands. Use explicit CLI commands such as `command`, `databases`, or `tables` for management/schema tasks.
- Obvious write-capable or destructive shapes are blocked, including ingestion, `into table`, create/alter/drop/delete/purge/rename/move/truncate forms, and set-or-append/set-or-replace forms.
- Multi-statement output may contain only `let` declarations or `declare query_parameters` statements before one final query expression. Multiple executable query statements and generated `set` request-option statements are blocked.
- Exploratory row-returning drafts must include an explicit result bound (`take`, `limit`, `top`, or `sample`) or a reducing aggregation such as `count`, `summarize`, or `make-series`. Unbounded drafts produce validation warnings and are not execution-eligible until corrected.
- If schema/prompt ambiguity blocks a safe table or function choice, a Query Draft may set `clarification_required` with a concise `clarification_question` instead of hallucinating a table choice. Non-blocking ambiguity should appear as explicit `assumptions`.

When `ask --execute` is used, execution is attempted only after static validation passes. The execution request uses Kusto read-only request properties (`request_readonly` and `request_readonly_hardline`) plus a returned-record cap (`query_take_max_records`). The default cap is 100 rows and can be changed with `ask --execute --max-rows <N>` or `--execute-max-rows <N>`. Execution results or execution errors are reported under `execution` in the Query Draft response; query results are not sent back to the model provider by default, and `data_disclosure_policy.sent_to_model_provider.query_results` remains `false`.

## Query-plan validation and Repair Passes

`ask --validate-plan` requests Kusto-side query-plan validation with `.show queryplan <| <draft query>` after static safety validation passes and before any `--execute` query execution. The same behavior can be enabled for scripted use with `KUSTO_ASK_VALIDATE_PLAN=true`. If query-plan validation fails, the Query Draft is returned with `validation.query_plan.status="failed"`, the validation error is recorded, and execution is blocked.

`ask --repair` enables bounded Repair Passes, and `KUSTO_ASK_REPAIR=true` can enable the same behavior for scripted use. A Repair Pass sends only the original prompt, Target, Schema Context, Data Disclosure Policy, previous query, and validation error to the Query Draft Agent/provider seam. It does not send execution results back to the model provider and it does not run exploratory queries or sample-data probes; the only Kusto-side validation route used by this feature is the explicit query-plan validation path. `--repair` implies query-plan validation. The default maximum is one Repair Pass; use `--max-repair-attempts <N>` (maximum 5) or `KUSTO_ASK_MAX_REPAIR_ATTEMPTS` to set a strict bound. If repair fails or the maximum is exhausted, `ask` returns the last Query Draft, the last validation error, and `repair_history` without executing.

## Agent guidance

- Prefer `kusto_query` for KQL queries.
- Use `kusto_command` only for management commands.
- Run `schema <tool>` before calling unfamiliar tools.
- Keep credentials in environment variables or an external credential provider.
