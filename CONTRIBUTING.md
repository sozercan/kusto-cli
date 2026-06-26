# Contributing

## Development loop

```bash
go test ./...
go vet ./...
CGO_ENABLED=0 go build -trimpath -o bin/kusto-cli ./cmd/kusto-cli
```

## Agent-friendly expectations

- Keep commands non-interactive by default.
- Prefer JSON output for direct command modes.
- Keep credentials out of logs and test fixtures.
- Keep live CI checks limited to public unauthenticated endpoints unless a workflow explicitly requires secrets.

## Documentation hygiene

Do not include private resource names, private database names, customer data, incident contents, or internal service-specific examples in public documentation.
