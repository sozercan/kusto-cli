GO ?= go
BINARY := kusto-cli
CMD := ./cmd/kusto-cli

.PHONY: build build-static test test-short vet fmt lint clean help

## build: Build the CLI
build:
	$(GO) build -o bin/$(BINARY) $(CMD)

## build-static: Build a pure-Go binary
build-static:
	CGO_ENABLED=0 $(GO) build -trimpath -o bin/$(BINARY) $(CMD)

## test: Run all tests
test:
	$(GO) test ./... -v

## test-short: Run tests without verbose output
test-short:
	$(GO) test ./...

## vet: Run go vet
vet:
	$(GO) vet ./...

## fmt: Format Go source files
fmt:
	$(GO) fmt ./...

## lint: Run fmt and vet
lint: fmt vet

## clean: Remove build artifacts
clean:
	rm -rf bin

## help: Show this help
help:
	@grep -E '^##' $(MAKEFILE_LIST) | sed 's/## //' | column -t -s ':'
