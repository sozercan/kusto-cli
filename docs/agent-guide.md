# Agent guide

`kusto-cli` is designed for both humans and agents. Prefer high-level commands before dropping to the advanced API explorer.

```bash
kusto-cli ask 'show recent storm events'
kusto-cli query 'StormEvents | count'
kusto-cli databases list
kusto-cli tables describe StormEvents
kusto-cli tables sample StormEvents 5
```

Use the advanced API explorer only when a high-level command does not exist:

```bash
kusto-cli api tools
kusto-cli api schema kusto_query
kusto-cli api call kusto_query '{"cluster_uri":"https://help.kusto.windows.net","database":"Samples","query":"StormEvents | count"}'
```

## Stdio service mode

For agents that need a stdio service:

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

## Target selection for `ask`

`ask` must resolve one Target before it generates a Query Draft. Provide both `--service-uri` and `--database`, or configure a Target Catalog alias and select it with `--target`:

```bash
kusto-cli --known-services '[{"alias":"samples","service_uri":"https://help.kusto.windows.net","default_database":"Samples"}]' \
  --target samples \
  ask 'show recent storm events'
```

If multiple targets are configured and none is selected, `ask` fails instead of choosing from prompt text. If a database is missing, `ask` fails before schema discovery or model-provider calls.

## Model provider mode for `ask`

By default, `ask` stays offline from model providers and deterministic by using the fake model provider. For a real model path, use an OpenAI-compatible chat completions endpoint and keep the API key in an environment variable:

```bash
export OPENAI_API_KEY="..."
kusto-cli \
  --service-uri https://help.kusto.windows.net \
  --database Samples \
  --model-provider openai-compatible \
  --model-endpoint https://api.openai.com/v1/chat/completions \
  --model test-model \
  --model-api-key-env OPENAI_API_KEY \
  ask 'show recent storm events'
```

Provider output is structured JSON that becomes a Query Draft. Treat any model safety classification as advisory only: validation and execution gating remain independent CLI responsibilities. See [Model providers](providers.md).

## Query Draft response shape

Agents should parse `ask` output as JSON and require `format == "query_draft"` before consuming it. The stable top-level shape is:

```json
{
  "format": "query_draft",
  "target": {
    "cluster_uri": "https://help.kusto.windows.net",
    "database": "Samples"
  },
  "prompt": "show recent storm events",
  "query": "StormEvents | take 5",
  "clarification_required": false,
  "clarification_question": "",
  "assumptions": ["Using StormEvents from Schema Context."],
  "warnings": ["Review before execution."],
  "examples": [
    {
      "source": "bundled",
      "name": "recent_rows",
      "intent": "Return recent rows with an explicit result bound.",
      "query": "StormEvents\n| where StartTime > ago(1d)\n| project StartTime, State, EventType\n| take 100"
    }
  ],
  "schema_context": [
    {
      "source": "schema-query",
      "entities": ["StormEvents"],
      "tables": [
        {
          "name": "StormEvents",
          "docstring": "Public sample storm event records.",
          "columns": [
            {"name": "StartTime", "type": "datetime"},
            {"name": "State", "type": "string"},
            {"name": "EventType", "type": "string"}
          ]
        }
      ]
    }
  ],
  "data_disclosure_policy": {
    "mode": "schema-only",
    "sent_to_model_provider": {
      "schema": true,
      "docstrings": true,
      "shots": true,
      "sample_rows": false,
      "query_results": false
    }
  },
  "validation": {
    "status": "passed",
    "read_only": true,
    "bounded": true,
    "safe_for_execution": true,
    "checks": [],
    "warnings": [],
    "errors": []
  },
  "execution": {
    "executed": false,
    "reason": "generate-only; execution requires an explicit execution gate"
  },
  "model_safety": {
    "classification": "safe",
    "reason": "Read-only query draft.",
    "advisory": true
  }
}
```

Fields with no content may be omitted when tagged `omitempty`, such as `clarification_required`, `clarification_question`, `repair_history`, and `model_safety`.

## How agents should consume Query Drafts

1. **Do not execute by default.** Treat `ask` as generate-only unless the user explicitly requested execution. If execution is requested, prefer invoking `ask --execute` so the CLI applies the Execution Gate, read-only request properties, and row cap.
2. **Respect Target resolution.** Use the returned `target` as the resolved Target. Do not substitute another cluster or database based on prompt text.
3. **Handle clarification first.** If `clarification_required` is `true`, show `clarification_question` to the user and do not execute or rewrite the query silently.
4. **Check CLI validation, not provider confidence.** Execute only when `validation.status == "passed"` and `validation.safe_for_execution == true`. `model_safety` is advisory.
5. **Surface assumptions and warnings.** Show `assumptions`, `warnings`, and validation messages with the draft so the user can judge whether the query matches intent.
6. **Preserve disclosure defaults.** Do not send `execution.result` or raw Kusto rows back to a model provider unless the user has explicitly opted into that separate disclosure path. `data_disclosure_policy.sent_to_model_provider.query_results` is `false` by default.
7. **Treat examples as shape guidance.** `examples` may include bundled public examples and safe configured shots. They are not proof that identifiers exist in the selected Target.
8. **Avoid bypassing safety.** Do not copy `query` into the direct `query` command to get around an Execution Gate. If a human approves a generated query outside `ask`, still use read-only paths and bounded results.

## Query-plan validation and Repair Passes

`ask --validate-plan` requests Kusto-side query-plan validation with `.show queryplan <| <draft query>` after static safety validation passes and before any `--execute` query execution. The same behavior can be enabled for scripted use with `KUSTO_ASK_VALIDATE_PLAN=true`. If query-plan validation fails, the Query Draft is returned with `validation.query_plan.status="failed"`, the validation error is recorded, and execution is blocked.

`ask --repair` enables bounded Repair Passes, and `KUSTO_ASK_REPAIR=true` can enable the same behavior for scripted use. A Repair Pass sends only the original prompt, Target, Schema Context, Data Disclosure Policy, examples, previous query, and validation error to the Query Draft Agent/provider seam. It does not send execution results back to the model provider and it does not run exploratory queries or sample-data probes; the only Kusto-side validation route used by this feature is the explicit query-plan validation path. `--repair` implies query-plan validation. The default maximum is one Repair Pass; use `--max-repair-attempts <N>` (maximum 5) or `KUSTO_ASK_MAX_REPAIR_ATTEMPTS` to set a strict bound. If repair fails or the maximum is exhausted, `ask` returns the last Query Draft, the last validation error, and `repair_history` without executing.

## General agent guidance

- Prefer `ask` for natural-language drafting and `kusto_query` for exact KQL queries.
- Use `kusto_command` only for management commands.
- Run `schema <tool>` before calling unfamiliar tools.
- Keep credentials in environment variables or an external credential provider.
