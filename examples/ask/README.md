# Public `ask` examples

These examples use only the public sample Target documented by this repository:

```text
Cluster URI: https://help.kusto.windows.net
Database:    Samples
```

## Generate a Query Draft without execution

```bash
kusto-cli \
  --service-uri https://help.kusto.windows.net \
  --database Samples \
  --model-provider fake \
  ask 'show recent storm events'
```

Expected behavior: JSON `format` is `query_draft`, `execution.executed` is `false`, and the draft is not sent to Kusto for execution.

## Select a Target Catalog alias

```bash
kusto-cli \
  --known-services '[{"alias":"samples","service_uri":"https://help.kusto.windows.net","default_database":"Samples"}]' \
  --target samples \
  ask 'count storm events by state'
```

## See safe failure for ambiguous Targets

```bash
kusto-cli \
  --known-services '[{"alias":"samples","service_uri":"https://help.kusto.windows.net","default_database":"Samples"},{"alias":"samples-alt","service_uri":"https://help.kusto.windows.net:443","default_database":"Samples"}]' \
  ask 'use the samples target'
```

Expected behavior: the command fails and lists available Targets instead of inferring one from the prompt.

## Execute only through the Execution Gate

```bash
kusto-cli \
  --service-uri https://help.kusto.windows.net \
  --database Samples \
  ask --execute --max-rows 25 'show recent storm events'
```

Expected behavior: execution is attempted only after static validation passes. Kusto request properties are read-only and returned records are capped.

## Configure a real provider without storing secrets

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

The command references the environment variable name, not the secret value.
