# Offline Query Draft evals

`query_draft.jsonl` contains deterministic offline-first eval cases for `kusto-cli ask` behavior. The Go test harness in `cmd/kusto-cli/ask_evals_test.go` loads these fixtures under `go test ./...`.

The harness uses fake Query Draft providers, a fake Schema Context, and stubbed Kusto execution hooks. It does not require private clusters, external model calls, or live Kusto authentication.

Coverage includes:

- representative natural-language prompts;
- Target resolution and safe failures for missing/ambiguous Targets;
- Data Disclosure Policy defaults and sample-row opt-in;
- static validation of management commands and unbounded row-returning drafts;
- Execution Gate behavior, read-only execution, and result caps;
- bundled examples and configured shots filtering.

All public Target values in fixtures use `https://help.kusto.windows.net` and `Samples`.
