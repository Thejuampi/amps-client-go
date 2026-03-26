GO ?= go
PKG ?= ./...
GOFLAGS ?=
VERSION ?= $(strip $(file < VERSION))
STATICCHECK_VERSION ?= v0.7.0
INEFFASSIGN_VERSION ?= v0.2.0
ERRCHECK_VERSION ?= v1.10.0
GOVULNCHECK_VERSION ?= v1.1.4
COVERPROFILE ?= $(abspath coverage.out)

.PHONY: help build test test-race integration-test integration-fakeamps install fmt vet static-scan vuln-scan tidy clean parity-check coverage-check perf-check release

help:
	@echo Available targets:
	@echo   make build            Build all packages
	@echo   make test             Run unit tests
	@echo   make test-race        Run tests with race detector
	@echo   make integration-test Run integration tests only (-run Integration)
	@echo   make integration-fakeamps Run strict integration gates against ephemeral fakeamps endpoints
	@echo   make install          Install packages with go install
	@echo   make fmt              Format Go source files
	@echo   make vet              Run go vet
	@echo   make static-scan      Run blocking static analysis (vet, staticcheck, ineffassign, errcheck)
	@echo   make vuln-scan        Run advisory vulnerability scan
	@echo   make tidy             Run go mod tidy
	@echo   make clean            Clean Go build/test caches
	@echo   make parity-check     Validate C++->Go parity manifest mappings
	@echo   make coverage-check   Run ./amps/... coverage gate checks
	@echo   make perf-check       Run hot-path benchmark regression gate
	@echo   make release          Run release verification pipeline

build:
	$(GO) build $(GOFLAGS) $(PKG)

test:
	$(GO) test $(GOFLAGS) $(PKG) -skip Integration

test-race:
	$(GO) test -race $(GOFLAGS) $(PKG) -skip Integration

integration-test:
	$(GO) test $(GOFLAGS) $(PKG) -run Integration

integration-fakeamps:
	$(GO) run ./tools/fakeampsgate

install:
	$(GO) install $(GOFLAGS) $(PKG)

fmt:
	$(GO) fmt $(PKG)

vet:
	$(GO) vet $(PKG)

static-scan:
	$(GO) vet $(PKG)
	$(GO) run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) -checks=SA* $(PKG)
	$(GO) run github.com/gordonklaus/ineffassign@$(INEFFASSIGN_VERSION) $(PKG)
	$(GO) run github.com/kisielk/errcheck@$(ERRCHECK_VERSION) -ignoretests $(PKG)

vuln-scan:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) $(PKG)

tidy:
	$(GO) mod tidy

clean:
	$(GO) clean -cache -testcache
	$(GO) clean $(PKG)

parity-check:
	$(GO) run ./tools/paritycheck -manifest tools/parity_manifest.json -behavior-manifest tools/parity_behavior_manifest.json

coverage-check:
	$(GO) test -count=1 ./amps/... -coverprofile=$(COVERPROFILE)
	$(GO) run ./tools/coveragegate -profile $(COVERPROFILE)

perf-check:
	$(GO) run ./tools/perfgate -baseline tools/perf_baseline.json

release: static-scan test test-race build integration-fakeamps parity-check coverage-check perf-check
	@echo Release checks passed for $(VERSION).
