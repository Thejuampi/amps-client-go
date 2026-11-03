# Testing and Validation

## Test Layers

Unit tests:

- Deterministic behavior checks for parsing, routing, ack batching, store replay, HA recovery sequencing, and API parity aliases.

Integration tests:

- Environment-gated endpoint validation for connect/logon, pub/sub, SOW flows, queue ack behavior, bookmark resume, timer commands, and HA failover setup.

## Commands

```bash
make static-scan
make leak-check
make fuzz-smoke
make test-race
make test
make integration-test
make integration-live-smoke
make stress-check
make parity-check
make coverage-check
make perf-check
make perf-compare-toolchains
make compat-check
make vuln-scan
make preprod-check
make release
```

Equivalent direct commands:

```bash
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 -checks=SA* ./...
go run github.com/gordonklaus/ineffassign@v0.2.0 ./...
go run github.com/kisielk/errcheck@v1.10.0 -ignoretests ./...
go run ./tools/patterncheck ./...
go run github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.8 run --config .golangci.yml ./...
go test -count=1 ./amps
go test -count=1 ./tools/fakeamps -run '^(TestStartLeaseWatcher|TestStopLeaseWatcherStopsBackgroundLoop)$$'
go test ./amps -run=^$$ -fuzz=FuzzParseHeader -fuzztime=5s
go test ./amps -run=^$$ -fuzz=FuzzParseBookmarkToken -fuzztime=5s
go test ./amps -run=^$$ -fuzz=FuzzParseSocketOptions -fuzztime=5s
go test ./amps -run=^$$ -fuzz=FuzzCompositeMessageParser -fuzztime=5s
go test -race ./... -skip Integration
go test ./... -skip Integration
go test ./... -run Integration
go test -count=1 ./amps -run '^(TestIntegrationConnectLogonPublishSubscribe|TestIntegrationSOWAndSowAndSubscribeLifecycle|TestIntegrationQueueAutoAckBatching|TestIntegrationBookmarkResumeAcrossReconnect|TestIntegrationHAConnectAndLogonWithFailoverChooser)$$'
go test -race -shuffle=on -count=20 ./... -skip Integration
go run ./tools/fakeampsgate -race -count=20 -shuffle=on
go run ./tools/paritycheck -manifest tools/parity_manifest.json -behavior-manifest tools/parity_behavior_manifest.json
go test -count=1 ./amps/... -coverprofile=coverage.out
go run ./tools/coveragegate -profile coverage.out
go run ./tools/withtoolchain -toolchain go1.25.13 -- test ./... -skip Integration
go run ./tools/withtoolchain -toolchain go1.25.13 -- build ./...
go run ./tools/withtoolchain -toolchain go1.26.10 -- run ./tools/perfgate -baseline tools/perf_baseline.json
go run ./tools/withtoolchain -toolchain go1.26.10 -- run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...
```

Windows pre-commit or pre-release parity with the CI static-analysis host:

```powershell
.\tools\static-scan-linux.ps1
```

## Static Analysis Gate

Blocking static-analysis policy:

- `go vet ./...`
- `staticcheck` correctness checks only (`SA*`)
- `ineffassign` for ineffectual assignments
- `errcheck` on non-test packages
- `patterncheck` for repo-specific transport and goroutine bug patterns
- `golangci-lint` with an expanded bug-focused ruleset from `.golangci.yml`

This gate is exposed locally via `make static-scan` and is required in CI and release validation.

`make static-scan` follows the host OS build tags. On Windows, run `.\tools\static-scan-linux.ps1` before commit or release prep when you need parity with the Ubuntu CI job, especially after touching files guarded by `//go:build !windows` or other non-Windows paths.

Race coverage is enforced separately with `make test-race` in CI and release validation.

The expanded `golangci-lint` lane currently focuses on:

- aliasing and unicode hazards (`asasalint`, `bidichk`)
- resource leaks and close discipline (`bodyclose`, `rowserrcheck`, `sqlclosecheck`)
- loop-variable and zero-length allocation traps (`copyloopvar`, `makezero`)
- unsafe or unchecked JSON encoding paths (`errchkjson`)
- wrapped-error handling mistakes (`errorlint`)
- compiler directive mistakes (`gocheckcompilerdirectives`)
- `govet` analyzers, including `shadow` and `unusedwrite`
- nil/error contract mistakes (`nilerr`, `nilnesserr`)
- suspicious assignments (`wastedassign`)
- duration arithmetic mistakes (`durationcheck`)

`errchkjson` stays enabled with narrow path exclusions for a small set of helper-heavy files where the current upstream analyzer reports false positives. The rest of the repository remains covered by the check.

## Pre-Production Gates

Pre-production validation adds heavier checks that are useful before a cut or deploy:

- `make leak-check` for goroutine leak detection with `go.uber.org/goleak`
- `make fuzz-smoke` for short parser fuzzing runs
- `make stress-check` for repeated shuffled race execution of all packages and both fakeamps-backed integration suites
- `make preprod-check` to run scan + leak + fuzz + stress + coverage + perf together
- `make integration-live-smoke` to run the live-broker smoke subset when `AMPS_TEST_*` is configured

`make release` now flows through the pre-production gate before the existing unit, race, build, fake broker, and parity steps.

`stress-check` defaults to `STRESS_COUNT=20`, `STRESS_SHUFFLE=on`,
`STRESS_WORKERS=2`, and `STRESS_PKG=./...`, including fakeamps and its gate.
It first repeats unit tests within each test process, then repeats the client
and broker integration suites with the race detector and shuffle. Each worker
has its own pair of ephemeral fakeamps endpoints, which the client tests
receive automatically without `AMPS_TEST_URI` supplied by the caller. Each
iteration starts new test processes with `-count=1`. A failure stops new
iterations; already-running iterations finish before their brokers are
cleaned up. These are repetitions, not retries of failed tests. Internal
deadlines and Go's default per-process test timeout are unchanged.

For a reproducible ordering, set `STRESS_SHUFFLE` to the reported numeric seed.
`STRESS_PKG` customizes the unit package selection; both integration suites
always run. Set `STRESS_WORKERS=1` for sequential integration iterations. The
existing CI, nightly and release repetition counts remain 2, 5 and 3
respectively.

Toolchain compatibility, security, and performance are separate gates. The module
keeps its Go 1.25 language floor and prefers Go 1.26.10. Make exports the exact
Go 1.26.10 selection to builds, scanners, tests, and their nested Go commands.
`make compat-check` selects Go 1.25.13 exactly; it must not use `+auto`, which
would silently upgrade to the module's preferred compiler. The CI compatibility
matrix uses `GOTOOLCHAIN=local` for the same reason. Historical compatibility
testing does not make the older compiler suitable for release. Downstream
applications must select a patched Go compiler themselves: a dependency's
`toolchain` directive does not select the application's compiler.

`make perf-compare-toolchains` compares the floor with the Go 1.27.3 candidate,
saves both raw outputs under `.tmp/perf/`, and writes `.tmp/perf/benchstat.txt`.
To reproduce the security upgrade comparison, use:

```bash
make perf-compare-toolchains PERF_COMPARE_NEW_TOOLCHAIN=go1.26.10 PERF_COMPARE_NEW_LABEL=go1.26.10
```

That comparison is diagnostic; `make perf-check` remains the blocking gate with
the committed baseline and unchanged limits. A security upgrade must pass that
gate. Performance-only promotions additionally require a measured client
speedup without unexplained allocation regressions; neutral or noisy results
do not justify a performance release or baseline refresh.

## CI and Release Automation

- `ci.yml` runs Ubuntu analysis, a cross-platform Go test/build matrix, a blocking paired performance job for PRs and pushes, and a main-branch pre-prod smoke job.
- `nightly-analysis.yml` runs the heavier `make preprod-check` lane on a schedule and optionally exercises a live AMPS broker.
- `release.yml` runs `make release-hosted` with the expanded pre-production gate and an optional live-broker smoke before publishing.

The repository also runs a separate GitHub CodeQL workflow with `security-and-quality` queries for deeper code scanning on pull requests, pushes to `main`, and a weekly schedule.

The CI workflow publishes a required `validate` status only after the blocking analysis and performance jobs and every cross-platform test-matrix entry succeed. Branch protection should require this aggregate status.

`make vuln-scan` runs `govulncheck` with the pinned patched Go toolchain and is part of the blocking `make scan` merge gate. Keep that toolchain current so standard-library findings reflect the release runtime rather than a stale compiler patch.

## Coverage Gate (`./amps/...`)

Coverage gate policy:

- Aggregate coverage: `>=90.0%`
- Pure-functional files: `100.0%`
- I/O/stateful files: `>=80.0%`

Coverage gate classification and file allowlists are source-of-truth in `tools/coveragegate/main.go`.

Coverage gate scope is `./amps/...` and is separate from integration tests.

PowerShell note: quote the coverprofile flag if needed, for example `go test -count=1 ./amps/... '-coverprofile=coverage.out'`.

## Parity and Gap Gate

Parity gate policy:

- Symbol parity: `MISSING_HEADER_SYMBOLS=0` and `MISSING_GO_SYMBOLS=0`
- Behavior parity: `OPEN_GAPS=0`

Tracked files:

- `tools/parity_manifest.json`
- `tools/parity_behavior_manifest.json`
- `docs/gap_register.md`

## Performance Gate

Performance gate policy:

- `tools/perf_baseline.json` and the reference in `tools/perf_reference.json` are fixed inputs.
- The candidate and reference run in six alternating pairs on the same runner with the same exact Go 1.26.10 toolchain.
- Each candidate `ns/op` is normalized using its paired reference result and the committed baseline. For each benchmark, all pairs within the existing limit pass, all outside fail, and a mixture blocks as inconclusive. No result is rescued by a retry or median.
- The existing `10%` default and `25%` group override, 0.1 ns comparison slack, and allocation budgets remain unchanged. Any allocation-budget violation fails the gate.
- Raw samples, provenance and `report.json` are retained under `.tmp/perf/gate/<runid>/` and uploaded by CI, nightly and release workflows even when the gate fails.

Required output:

- A successful `PASS` result; `FAIL` or `INCONCLUSIVE` blocks verification.

The method, reference provenance, historical host variance, and troubleshooting
are documented in [Performance Gate](performance_gate.md).

## Integration Environment Contract

- `AMPS_TEST_URI`
- `AMPS_TEST_FAILOVER_URIS`
- `AMPS_TEST_PROTOCOL`
- `AMPS_TEST_USER`
- `AMPS_TEST_PASSWORD`

`AMPS_TEST_FAILOVER_URIS` accepts comma-separated URIs.

## Validation Checklist for Parity-Sensitive Changes

1. Confirm no exported API signature regressions.
2. Run `make static-scan`. On Windows, also run `.\tools\static-scan-linux.ps1` before commit when you need CI-equivalent Linux static analysis.
3. Run `make leak-check`.
4. Run `make fuzz-smoke` for parser- or protocol-heavy changes.
5. Run `make test-race`.
6. Run full unit suite.
7. Run parity check and verify `OPEN_GAPS=0`.
8. Run coverage gate for `./amps/...`.
9. Run performance gate against locked baseline.
10. Run `make stress-check` for concurrency, reconnect, queueing, or HA-sensitive changes.
11. Run integration suite with target endpoint if available.
12. Run `make integration-live-smoke` when a real AMPS environment is configured.
13. Validate handler order expectations.
14. Validate retry and replay behaviors under disconnect.
15. Validate queue auto-ack batching and timeout behavior.
16. Update parity matrix and relevant workflow docs.
17. Confirm support matrix statements still match observed behavior.

## Link and Documentation Integrity

- Ensure every new API behavior has at least one workflow or reference entry.
- Ensure all relative links in `README.md` and `docs/*.md` resolve.
- Ensure each major workflow page contains a runnable-style snippet.
- Ensure README can reach quickstart, HA path, parity matrix, and support matrix in <=2 clicks.

## Related

- [Parity Acceptance](parity_acceptance.md)
- [Gap Register](gap_register.md)
- [C++ to Go Parity Matrix](cpp_to_go_parity_matrix.md)
- [Supported Scope and Constraints](supported_scope.md)
- [Reference: Client](reference_client.md)
- [Reference: HAClient](reference_ha_client.md)
