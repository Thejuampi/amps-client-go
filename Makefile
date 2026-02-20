GO ?= go
PKG ?= ./...
GOFLAGS ?=
VERSION ?= $(strip $(file < VERSION))

.PHONY: help build test test-race integration-test install fmt vet tidy clean release

help:
	@echo Available targets:
	@echo   make build            Build all packages
	@echo   make test             Run unit tests
	@echo   make test-race        Run tests with race detector
	@echo   make integration-test Run integration tests only (-run Integration)
	@echo   make install          Install packages with go install
	@echo   make fmt              Format Go source files
	@echo   make vet              Run go vet
	@echo   make tidy             Run go mod tidy
	@echo   make clean            Clean Go build/test caches
	@echo   make release          Run release verification pipeline

build:
	$(GO) build $(GOFLAGS) $(PKG)

test:
	$(GO) test $(GOFLAGS) $(PKG)

test-race:
	$(GO) test -race $(GOFLAGS) $(PKG)

integration-test:
	$(GO) test $(GOFLAGS) $(PKG) -run Integration

install:
	$(GO) install $(GOFLAGS) $(PKG)

fmt:
	$(GO) fmt $(PKG)

vet:
	$(GO) vet $(PKG)

tidy:
	$(GO) mod tidy

clean:
	$(GO) clean -cache -testcache
	$(GO) clean $(PKG)

release: vet test build
	@echo Release checks passed for $(VERSION).
