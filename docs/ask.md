# `ask` Query Drafts

`ask` is the generate-first natural-language mode for `kusto-cli`. It turns a prompt into a **Query Draft**: proposed read-only KQL plus assumptions, warnings, Schema Context, Data Disclosure Policy, validation metadata, and optional execution metadata.

By default, `ask` **does not execute** the generated KQL. Execution requires the explicit **Execution Gate**: `--execute`.

All examples in this document use the public sample Target:

```text
Cluster URI: https://help.kusto.windows.net
Database:    Samples
```

## Generate-only mode

Run `ask` with a Target and a natural-language prompt:

```bash
kusto-cli \
  --service-uri https://help.kusto.windows.net \
  --database Samples \
  ask 'show recent storm events'
```

The response is JSON by default. In generate-only mode:

- `format` is `query_draft`.
- `query` contains proposed KQL.
- `execution.executed` is `false`.
- `execution.reason` explains that no Execution Gate was requested.
- `validation.safe_for_execution` indicates whether the draft is eligible for `--execute`.

The default model provider is `fake`, which is deterministic and offline from model providers. It is useful for tests, automation wiring, and demos. Configure a real provider only when you intentionally want model-provider calls; see [Model providers](providers.md).

## Select one Target

`ask` must resolve exactly one **Target** before schema discovery, generation, validation, or execution. A Target is one cluster/database pair.

### Direct Target selection

Provide both `--service-uri` and `--database`:

```bash
kusto-cli \
  --service-uri https://help.kusto.windows.net \
  --database Samples \
  ask 'count storm events by state'
```

Providing only a service URI is not enough unless the selected Target Catalog entry supplies exactly one database. Missing database selection fails before the Query Draft Agent is called.

### Target Catalog alias

Configure a Target Catalog and select an alias:

```bash
kusto-cli \
  --known-services '[{"alias":"samples","service_uri":"https://help.kusto.windows.net","default_database":"Samples"}]' \
  --target samples \
  ask 'count storm events by state'
```

Catalog entries may use `alias`, `name`, and `aliases` as stable selectors. `service_uri`, legacy `service`, `default_database`, and `description` are supported.

### Ambiguous or missing Targets fail safely

`ask` does not infer a Target from prompt text. If multiple targets are configured and none is selected, it fails with the available Target list, for example:

```text
ask requires exactly one Target; multiple targets are configured, select one with --target:
  - samples: https://help.kusto.windows.net / Samples
  - samples-alt: https://help.kusto.windows.net:443 / Samples
```

If an alias is missing or does not resolve a database, `ask` fails before schema discovery or model-provider calls.

## Execution Gate: `--execute`

Use `--execute` only when the user has intentionally requested execution:

```bash
kusto-cli \
  --service-uri https://help.kusto.windows.net \
  --database Samples \
  ask --execute --max-rows 25 'show recent storm events'
```

Execution is attempted only when the Query Draft passes CLI-owned safety validation:

- `validation.status` must be `passed`.
- `validation.safe_for_execution` must be `true`.
- The query must be read-only and result-bounded.

Execution uses read-only Kusto request properties and caps returned records:

- `request_readonly=true`
- `request_readonly_hardline=true`
- `query_take_max_records=<N>` where `N` defaults to `100` and can be set with `--max-rows` or `--execute-max-rows`.

If validation blocks execution, `execution.executed` remains `false`, `execution.status` is `blocked`, and `execution.reason` includes the validation warning or error.

## Validation behavior

The v1 validator is intentionally conservative. It rejects or blocks execution for unsafe or ambiguous shapes, including:

- Markdown/prose instead of raw KQL.
- Management commands such as `.show tables`.
- Write-capable or destructive forms such as ingestion, `into table`, create/alter/drop/delete/purge/rename/move/truncate, and set-or-append/set-or-replace forms.
- Multiple executable statements.
- Unbounded row-returning queries that do not include `take`, `limit`, `top`, `sample`, or a reducing aggregation such as `count`, `summarize`, or `make-series`.

Non-blocking uncertainty should appear in `assumptions`. Blocking ambiguity should set `clarification_required=true` and include one concise `clarification_question`.

## Data Disclosure Policy

By default, `ask` uses `schema-only` disclosure:

- Schema Context may be sent to the model provider.
- Table/function/column docstrings may be sent when available.
- Bundled public read-only examples may be sent as Query Draft shape guidance.
- Raw sample rows are not sent unless `--include-samples` or `--include-sample-rows` is explicitly set.
- Query results are not sent back to the model provider by default, including after `--execute`.

Opt in to sample rows only when appropriate for the selected Target:

```bash
kusto-cli \
  --service-uri https://help.kusto.windows.net \
  --database Samples \
  ask --include-samples 'show recent storm events'
```

The response reports disclosure under `data_disclosure_policy.sent_to_model_provider`.

## Examples and shots

`ask` always includes a small bundled set of public read-only KQL examples using sample-style identifiers such as `StormEvents`. They are shape guidance only; providers should adapt table and column names to the selected Schema Context.

If you maintain a Query Draft examples table in the selected Target, configure it explicitly:

```bash
kusto-cli \
  --service-uri https://help.kusto.windows.net \
  --database Samples \
  ask --shots-table QueryDraftExamples --shots-limit 5 'count storm events by state'
```

Configured shots are retrieved with a read-only query, filtered to safe bounded Query Draft examples, and then reported in `examples`. Unsafe configured rows are skipped with warnings. Missing shots configuration does not block `ask`; retrieval failures become warnings and bundled examples are still used.

## Query-plan validation and Repair Passes

`--validate-plan` requests Kusto-side query-plan validation after static validation and before execution:

```bash
kusto-cli \
  --service-uri https://help.kusto.windows.net \
  --database Samples \
  ask --validate-plan 'show recent storm events'
```

`--repair` enables bounded Repair Passes and implies query-plan validation:

```bash
kusto-cli \
  --service-uri https://help.kusto.windows.net \
  --database Samples \
  ask --repair --max-repair-attempts 1 'show recent storm events'
```

A Repair Pass receives only the prompt, Target, Schema Context, examples, Data Disclosure Policy, previous query, and validation error. It does not receive query results and does not perform exploratory Kusto calls.
