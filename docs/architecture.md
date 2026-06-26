# Architecture

```text
Agent or script
  └─ stdio JSON-RPC / direct command
      └─ kusto-cli
          ├─ token provider: env or Azure CLI
          ├─ MCP tool registry
          ├─ Kusto REST client
          └─ Kusto cluster
```

## Packages

The current implementation is intentionally small and standard-library only:

- `cmd/kusto-cli/main.go` contains CLI parsing, MCP stdio serving, direct command mode, auth, Kusto REST calls, and result formatting.
- `cmd/kusto-cli/main_test.go` covers validation, result parsing, request-property safety, and deeplink generation.

## Design choices

- No Agency dependency.
- No persistent local config.
- No interactive prompts.
- Bearer tokens are never printed.
- Direct `query`, `command`, `tools`, and `call` modes make validation and agent scripting straightforward.
