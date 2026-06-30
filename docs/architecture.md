# Architecture

```text
Agent or script
  └─ stdio JSON-RPC / direct command
      └─ kusto-cli
          ├─ token provider: env or Azure CLI
          ├─ tool registry
          ├─ Kusto REST client
          └─ Kusto cluster
```

## Packages

The current implementation is intentionally small and standard-library only:

- `cmd/kusto-cli/main.go` contains CLI parsing, stdio serving, direct command mode, auth, Kusto REST calls, and result formatting.
- `cmd/kusto-cli/main_test.go` covers validation, result parsing, request-property safety, and deeplink generation.

## Design choices

- Self-contained Go implementation.
- Small JSON config for defaults and Target Catalog aliases.
- No interactive prompts.
- Bearer tokens are never printed.
- Direct `query`, `command`, `tools`, and `call` modes make validation and agent scripting straightforward.

## Query Draft Agent architecture

`ask` follows a generate-first pipeline:

```text
Target resolution
  └─ Schema Context discovery
      └─ examples/shots selection
          └─ model-provider adapter (fake by default)
              └─ Query Draft normalization
                  └─ CLI-owned validation
                      └─ optional query-plan validation / Repair Passes
                          └─ optional Execution Gate (`--execute`)
```

Target resolution happens before schema discovery or model-provider calls. The model-provider adapter keeps provider-specific request and response formats out of command handling and allows offline evals to use fake providers. Execution remains separate from generation and uses the same read-only Kusto path as direct read queries.
