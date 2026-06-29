# Agent guide

`kusto-cli` is designed for both humans and agents.

Use human-friendly commands first:

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

## Target selection for ask

`ask` must resolve one Target before it generates a Query Draft. Provide both `--service-uri` and `--database`, or configure a Target Catalog alias and select it with `--target`:

```bash
kusto-cli --known-services '[{"alias":"samples","service_uri":"https://help.kusto.windows.net","default_database":"Samples"}]' \
  --target samples \
  ask 'show recent storm events'
```

If multiple targets are configured and none is selected, `ask` fails instead of choosing from prompt text.

## Model provider mode for ask

By default, `ask` stays offline and deterministic by using the fake model provider. For a real model path, use an OpenAI-compatible chat completions endpoint and keep the API key in an environment variable:

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

Provider output is structured JSON that becomes a Query Draft. Treat any model safety classification as advisory only: validation and execution gating remain independent CLI responsibilities.
