GO ?= go
PKG ?= ./...
GOFLAGS ?=
VERSION ?= $(strip $(file < VERSION))
STATICCHECK_VERSION ?= v0.7.0
INEFFASSIGN_VERSION ?= v0.2.0
ERRCHECK_VERSION ?= v1.10.0
GOLANGCI_LINT_VERSION ?= v1.64.8
GOVULNCHECK_VERSION ?= v1.1.4
GITLEAKS_VERSION ?= v8.30.1
GOSEC_VERSION ?= v2.22.4
BENCHSTAT_VERSION ?= latest
MIN_GO_TOOLCHAIN ?= go1.25.13+auto
CANDIDATE_GO_TOOLCHAIN ?= go1.26.3+auto
PERF_GO_TOOLCHAIN ?= $(MIN_GO_TOOLCHAIN)
VULN_GO_TOOLCHAIN ?= go1.25.13+auto
COVERPROFILE ?= $(abspath coverage.out)
FUZZTIME ?= 5s
STRESS_COUNT ?= 20
STRESS_SHUFFLE ?= on
FUZZ_TMPDIR ?= $(abspath .tmp/go-tmp)
GO_CACHE_DIR ?= $(abspath .tmp/go-cache)
PERF_COMPARE_DIR ?= $(abspath .tmp/perf)
PERF_COMPARE_OLD_TOOLCHAIN ?= $(MIN_GO_TOOLCHAIN)
PERF_COMPARE_NEW_TOOLCHAIN ?= $(CANDIDATE_GO_TOOLCHAIN)
PERF_COMPARE_OLD_LABEL ?= go1.25.13
PERF_COMPARE_NEW_LABEL ?= go1.26.3
PERF_COMPARE_BENCHTIME ?= 1s
PERF_COMPARE_SAMPLES ?= 10
PERF_COMPARE_MAX_REGRESSION ?= 1000000
PERF_COMPARE_OLD_OUTPUT ?= $(PERF_COMPARE_DIR)/$(PERF_COMPARE_OLD_LABEL).bench.txt
PERF_COMPARE_NEW_OUTPUT ?= $(PERF_COMPARE_DIR)/$(PERF_COMPARE_NEW_LABEL).bench.txt
PERF_COMPARE_REPORT ?= $(PERF_COMPARE_DIR)/benchstat.txt
STRESS_PKG ?= ./amps/... ./cmd/gofer ./internal/... ./tools/coveragegate ./tools/patterncheck ./tools/perfgate ./tools/perfreport ./tools/publicationcheck ./tools/withtoolchain
MARKDOWNLINT ?= npx --yes markdownlint-cli2
MARKDOWNLINT_REPORT ?= $(abspath markdownlint-report.txt)
GITLEAKS_REPORT ?= $(abspath gitleaks-report.sarif)
GOSEC_REPORT ?= $(abspath gosec-report.sarif)

ifeq ($(OS),Windows_NT)
PARITY_CHECK_IF_AVAILABLE = @if exist ..\amps-c++-client-5.3.5.1-Windows ( $(MAKE) parity-check ) else ( echo Skipping parity check: ../amps-c++-client-5.3.5.1-Windows not found. )
ENSURE_FUZZ_TMPDIR = powershell -NoProfile -Command "New-Item -ItemType Directory -Force -Path '$(FUZZ_TMPDIR)','$(GO_CACHE_DIR)' | Out-Null"
ENSURE_PERF_COMPARE_DIR = powershell -NoProfile -Command "New-Item -ItemType Directory -Force -Path '$(PERF_COMPARE_DIR)' | Out-Null"
FUZZ_ENV = set "TMP=$(FUZZ_TMPDIR)" && set "TEMP=$(FUZZ_TMPDIR)" && set "TMPDIR=$(FUZZ_TMPDIR)" && set "GOTMPDIR=$(FUZZ_TMPDIR)" && set "GOCACHE=$(GO_CACHE_DIR)" &&
else
PARITY_CHECK_IF_AVAILABLE = @if [ -d ../amps-c++-client-5.3.5.1-Windows ]; then \
	$(MAKE) parity-check; \
else \
	echo "Skipping parity check: ../amps-c++-client-5.3.5.1-Windows not found."; \
fi
ENSURE_FUZZ_TMPDIR = mkdir -p "$(FUZZ_TMPDIR)" "$(GO_CACHE_DIR)"
ENSURE_PERF_COMPARE_DIR = mkdir -p "$(PERF_COMPARE_DIR)"
FUZZ_ENV = TMP=$(FUZZ_TMPDIR) TEMP=$(FUZZ_TMPDIR) TMPDIR=$(FUZZ_TMPDIR) GOTMPDIR=$(FUZZ_TMPDIR) GOCACHE=$(GO_CACHE_DIR)
endif

.PHONY: help build test test-race compat-check integration-test integration-fakeamps integration-live-smoke install fmt vet static-scan golangci-scan pattern-scan publication-scan leak-check fuzz-smoke stress-check preprod-check preprod-check-hosted security-scan gosec-scan gosec-report secret-scan secret-report scan markdown-scan markdown-report markdown-fix vuln-scan tidy clean parity-check parity-check-if-available coverage-check perf-check perf-compare-toolchains release release-hosted

help:
	@echo Available targets:
	@echo   make build            Build all packages
	@echo   make test             Run unit tests
	@echo   make test-race        Run tests with race detector
	@echo   make compat-check     Run unit tests and build with the minimum supported Go toolchain
	@echo   make integration-test Run integration tests only (-run Integration)
	@echo   make integration-fakeamps Run strict integration gates against ephemeral fakeamps endpoints
	@echo   make install          Install packages with go install
	@echo   make fmt              Format Go source files
	@echo   make vet              Run go vet
	@echo   make static-scan      Run blocking static analysis (vet, staticcheck, ineffassign, errcheck, patterncheck, expanded golangci-lint)
	@echo   make golangci-scan    Run expanded golangci-lint bug detectors from .golangci.yml
	@echo   make pattern-scan     Run the repo-specific bug-pattern analyzer
	@echo   make publication-scan Prevent publication of unauthorized external-client benchmark results
	@echo   make leak-check       Run goroutine leak detection on key packages
	@echo   make fuzz-smoke       Run short fuzzing smoke tests for parser-heavy code
	@echo   make stress-check     Run repeated shuffled race tests before pre-prod
	@echo   make integration-live-smoke Run live-broker pre-prod smoke tests when AMPS_TEST_* env vars are set
	@echo   make preprod-check    Run the expanded pre-production analysis gates
	@echo   make security-scan    Run blocking security scans (gosec, govulncheck, gitleaks)
	@echo   make gosec-scan       Run blocking gosec security analysis
	@echo   make gosec-report     Write gosec SARIF output to $(GOSEC_REPORT)
	@echo   make secret-scan      Run secret scanning with gitleaks
	@echo   make secret-report    Write gitleaks SARIF output to $(GITLEAKS_REPORT)
	@echo   make scan             Run the full blocking code scanning suite
	@echo   make markdown-scan    Run markdownlint using .markdownlint-cli2.jsonc
	@echo   make markdown-report  Write markdownlint output to $(MARKDOWNLINT_REPORT)
	@echo   make markdown-fix     Auto-fix markdownlint issues where possible
	@echo   make vuln-scan        Run blocking vulnerability scan on the supported Go release line
	@echo   make tidy             Run go mod tidy
	@echo   make clean            Clean Go build/test caches
	@echo   make parity-check     Validate C++->Go parity manifest mappings
	@echo   make coverage-check   Run ./amps/... coverage gate checks
	@echo   make perf-check       Run hot-path benchmark regression gate
	@echo   make perf-compare-toolchains Compare hot-path benchmarks across minimum and preferred Go toolchains
	@echo   make release          Run release verification pipeline

build:
	$(GO) build $(GOFLAGS) $(PKG)

test:
	$(GO) test $(GOFLAGS) $(PKG) -skip Integration

test-race:
	$(GO) test -race $(GOFLAGS) $(PKG) -skip Integration

compat-check:
	$(GO) run ./tools/withtoolchain -toolchain $(MIN_GO_TOOLCHAIN) -- test $(GOFLAGS) $(PKG) -skip Integration
	$(GO) run ./tools/withtoolchain -toolchain $(MIN_GO_TOOLCHAIN) -- build $(GOFLAGS) $(PKG)

integration-test:
	$(GO) test $(GOFLAGS) $(PKG) -run Integration

integration-fakeamps:
	$(GO) run ./tools/fakeampsgate

integration-live-smoke:
	$(GO) test -count=1 ./amps -run "^(TestIntegrationConnectLogonPublishSubscribe|TestIntegrationSOWAndSowAndSubscribeLifecycle|TestIntegrationQueueAutoAckBatching|TestIntegrationBookmarkResumeAcrossReconnect|TestIntegrationHAConnectAndLogonWithFailoverChooser)$$"

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
	$(MAKE) pattern-scan PKG=$(PKG)
	$(MAKE) publication-scan
	$(MAKE) golangci-scan PKG=$(PKG)

golangci-scan:
	$(GO) run github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run --config .golangci.yml $(PKG)

pattern-scan:
	$(GO) run ./tools/patterncheck $(PKG)

publication-scan:
	$(GO) run ./tools/publicationcheck -root .

leak-check:
	@$(ENSURE_FUZZ_TMPDIR)
	$(FUZZ_ENV) $(GO) test -count=1 ./amps
	$(FUZZ_ENV) $(GO) test -count=1 ./tools/fakeamps -run "^(TestStartLeaseWatcher|TestStopLeaseWatcherStopsBackgroundLoop)$$"

fuzz-smoke:
	@$(ENSURE_FUZZ_TMPDIR)
	$(FUZZ_ENV) $(GO) test ./amps -run=^$$ -fuzz=FuzzParseHeader -fuzztime=$(FUZZTIME)
	$(FUZZ_ENV) $(GO) test ./amps -run=^$$ -fuzz=FuzzParseBookmarkToken -fuzztime=$(FUZZTIME)
	$(FUZZ_ENV) $(GO) test ./amps -run=^$$ -fuzz=FuzzParseSocketOptions -fuzztime=$(FUZZTIME)
	$(FUZZ_ENV) $(GO) test ./amps -run=^$$ -fuzz=FuzzCompositeMessageParser -fuzztime=$(FUZZTIME)

stress-check:
	@$(ENSURE_FUZZ_TMPDIR)
	$(FUZZ_ENV) $(GO) test -race -shuffle=$(STRESS_SHUFFLE) -count=$(STRESS_COUNT) $(STRESS_PKG) -skip Integration

preprod-check: scan leak-check fuzz-smoke stress-check coverage-check perf-check
	@echo Pre-production checks passed for $(VERSION).

preprod-check-hosted: scan leak-check fuzz-smoke stress-check coverage-check perf-check parity-check-if-available
	@echo Hosted pre-production checks passed for $(VERSION).

security-scan: gosec-scan vuln-scan secret-scan

gosec-scan:
	$(GO) run github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION) ./...

gosec-report:
	$(GO) run github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION) -fmt sarif -out $(GOSEC_REPORT) ./...

secret-scan:
	$(GO) run github.com/zricethezav/gitleaks/v8@$(GITLEAKS_VERSION) dir . --no-banner --redact --exit-code 1

secret-report:
	$(GO) run github.com/zricethezav/gitleaks/v8@$(GITLEAKS_VERSION) dir . --no-banner --redact --report-format sarif --report-path $(GITLEAKS_REPORT)

scan: static-scan security-scan

markdown-scan:
	$(MARKDOWNLINT)

markdown-report:
	$(MARKDOWNLINT) > $(MARKDOWNLINT_REPORT) 2>&1

markdown-fix:
	$(MARKDOWNLINT) --fix

vuln-scan:
	$(GO) run ./tools/withtoolchain -toolchain $(VULN_GO_TOOLCHAIN) -- run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) $(PKG)

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
	@$(ENSURE_FUZZ_TMPDIR)
	$(FUZZ_ENV) $(GO) test -count=1 ./amps/... -coverprofile=$(COVERPROFILE)
	$(GO) run ./tools/coveragegate -profile $(COVERPROFILE)

perf-check:
	@$(ENSURE_FUZZ_TMPDIR)
	$(FUZZ_ENV) $(GO) run ./tools/withtoolchain -toolchain $(PERF_GO_TOOLCHAIN) -- run ./tools/perfgate -baseline tools/perf_baseline.json

perf-compare-toolchains:
	@$(ENSURE_FUZZ_TMPDIR)
	@$(ENSURE_PERF_COMPARE_DIR)
	$(FUZZ_ENV) $(GO) run ./tools/withtoolchain -toolchain $(PERF_COMPARE_OLD_TOOLCHAIN) -- run ./tools/perfgate -baseline tools/perf_baseline.json -benchtime $(PERF_COMPARE_BENCHTIME) -samples $(PERF_COMPARE_SAMPLES) -retry-count 0 -max-regression $(PERF_COMPARE_MAX_REGRESSION) -report-only > "$(PERF_COMPARE_OLD_OUTPUT)"
	$(FUZZ_ENV) $(GO) run ./tools/withtoolchain -toolchain $(PERF_COMPARE_NEW_TOOLCHAIN) -- run ./tools/perfgate -baseline tools/perf_baseline.json -benchtime $(PERF_COMPARE_BENCHTIME) -samples $(PERF_COMPARE_SAMPLES) -retry-count 0 -max-regression $(PERF_COMPARE_MAX_REGRESSION) -report-only > "$(PERF_COMPARE_NEW_OUTPUT)"
	$(GO) run golang.org/x/perf/cmd/benchstat@$(BENCHSTAT_VERSION) "$(PERF_COMPARE_OLD_OUTPUT)" "$(PERF_COMPARE_NEW_OUTPUT)" > "$(PERF_COMPARE_REPORT)"
	@echo Perf comparison written to $(PERF_COMPARE_REPORT)

release: preprod-check test test-race build integration-fakeamps parity-check
	@echo Release checks passed for $(VERSION).

release-hosted: preprod-check-hosted test test-race build integration-fakeamps
	@echo Hosted release checks passed for $(VERSION).
