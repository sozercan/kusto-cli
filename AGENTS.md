# Agent instructions for kusto-cli

- This repository builds a standalone Go CLI.
- Prefer non-interactive commands and JSON output when validating behavior.
- Before handing off changes, run:
  - `go test ./...`
  - `go vet ./...`
  - `CGO_ENABLED=0 go build -trimpath -o bin/kusto-cli ./cmd/kusto-cli`
- Do not add private cluster names, private database names, customer data, incident data, or internal service examples to public docs.
- Public docs may use only generic sample endpoints documented in this repository.
