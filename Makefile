GO ?= go
PKG ?= ./...
GOFLAGS ?=
VERSION ?= $(strip $(file < VERSION))
STATICCHECK_VERSION ?= v0.7.0
INEFFASSIGN_VERSION ?= v0.2.0
ERRCHECK_VERSION ?= v1.10.0
GOVULNCHECK_VERSION ?= v1.1.4
COVERPROFILE ?= $(abspath coverage.out)
RELEASE_VERSION ?=
RELEASE_FLAGS ?=

ifeq ($(OS),Windows_NT)
POWERSHELL ?= powershell
PARITY_CHECK_IF_AVAILABLE = @if exist ..\amps-c++-client-5.3.5.1-Windows ( $(MAKE) parity-check ) else ( echo Skipping parity check: ../amps-c++-client-5.3.5.1-Windows not found. )
else
POWERSHELL ?= pwsh
PARITY_CHECK_IF_AVAILABLE = @if [ -d ../amps-c++-client-5.3.5.1-Windows ]; then \
	$(MAKE) parity-check; \
else \
	echo "Skipping parity check: ../amps-c++-client-5.3.5.1-Windows not found."; \
fi
endif

.PHONY: help build test test-race integration-test integration-fakeamps install fmt vet static-scan vuln-scan tidy clean parity-check parity-check-if-available coverage-check perf-check release release-hosted release-local release-dry-run

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
	@echo   make release          Run release verification pipeline only
	@echo   make release-local    Run the local scripted release flow (optional RELEASE_VERSION/RELEASE_FLAGS)
	@echo   make release-dry-run  Run scripted release validation and restore version files (requires RELEASE_VERSION=X.Y.Z)

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

parity-check-if-available:
	$(PARITY_CHECK_IF_AVAILABLE)

coverage-check:
	$(GO) test -count=1 ./amps/... -coverprofile=$(COVERPROFILE)
	$(GO) run ./tools/coveragegate -profile $(COVERPROFILE)

perf-check:
	$(GO) run ./tools/perfgate -baseline tools/perf_baseline.json

release: static-scan perf-check test test-race build integration-fakeamps parity-check coverage-check
	@echo Release checks passed for $(VERSION).

release-hosted: static-scan test test-race build integration-fakeamps parity-check-if-available coverage-check
	@echo Hosted release checks passed for $(VERSION).

release-local:
	$(POWERSHELL) -ExecutionPolicy Bypass -File .\release.local.ps1 $(if $(RELEASE_VERSION),-Version $(RELEASE_VERSION),) $(RELEASE_FLAGS)

release-dry-run:
	$(if $(strip $(RELEASE_VERSION)),,$(error RELEASE_VERSION is required, e.g. make release-dry-run RELEASE_VERSION=0.8.10))
	$(POWERSHELL) -ExecutionPolicy Bypass -File .\release.local.ps1 -Version $(RELEASE_VERSION) -Yes -DryRun
