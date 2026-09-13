.PHONY: build test test-race lint test-e2e test-topology up tui down status

GO ?= go
GOLANGCI_LINT_VERSION ?= v2.13.2
GOLANGCI_LINT ?= $(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
ifeq ($(shell command -v $(GO) 2>/dev/null),)
ifneq ($(wildcard /usr/local/go/bin/go),)
GO := /usr/local/go/bin/go
endif
endif

build:
	mkdir -p bin
	$(GO) build -o bin/flink-tui ./cmd/flink-tui

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

lint:
	PATH="$(dir $(GO)):$$PATH" $(GOLANGCI_LINT) run

test-e2e:
	GO="$(GO)" ./scripts/test-e2e

test-topology:
	FLINK_TUI_INTEGRATION_ENDPOINT=http://localhost:8081 $(GO) test ./internal/ui/coordinator -run TestTopologyLabRenderingIntegration -v

up:
	docker compose up --build -d

tui:
	$(GO) run ./cmd/flink-tui

down:
	docker compose down

status:
	docker compose ps -a
