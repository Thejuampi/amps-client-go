# Testing and Validation

## Test Layers

Unit tests:

- Deterministic behavior checks for parsing, routing, ack batching, store replay, HA recovery sequencing, and API parity aliases.

Integration tests:

- Environment-gated endpoint validation for connect/logon, pub/sub, SOW flows, queue ack behavior, bookmark resume, timer commands, and HA failover setup.

## Commands

```bash
make static-scan
make test-race
make test
make integration-test
make parity-check
make coverage-check
make perf-check
make vuln-scan
make release
```

Equivalent direct commands:

```bash
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 -checks=SA* ./...
go run github.com/gordonklaus/ineffassign@v0.2.0 ./...
go run github.com/kisielk/errcheck@v1.10.0 -ignoretests ./...
go test -race ./... -skip Integration
go test ./... -skip Integration
go test ./... -run Integration
go run ./tools/paritycheck -manifest tools/parity_manifest.json -behavior-manifest tools/parity_behavior_manifest.json
go test -count=1 ./amps/... -coverprofile=coverage.out
go run ./tools/coveragegate -profile coverage.out
go run ./tools/perfgate -baseline tools/perf_baseline.json
go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...
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

This gate is exposed locally via `make static-scan` and is required in CI and release validation.

`make static-scan` follows the host OS build tags. On Windows, run `.\tools\static-scan-linux.ps1` before commit or release prep when you need parity with the Ubuntu CI job, especially after touching files guarded by `//go:build !windows` or other non-Windows paths.

Race coverage is enforced separately with `make test-race` in CI and release validation.

The repository also runs a separate GitHub CodeQL workflow with `security-and-quality` queries for deeper code scanning on pull requests, pushes to `main`, and a weekly schedule.

`make vuln-scan` runs `govulncheck` as an advisory scan. Standard-library findings are toolchain-sensitive, so the workflow records them without turning them into a required merge gate.

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

- Regression is measured against `tools/perf_baseline.json`
- `tools/perfgate` collects repeated samples and compares the median result for each benchmark
- if a benchmark initially fails, `tools/perfgate` reruns only that failing subset with a longer benchtime before declaring the gate red
- Allowed regression threshold is `10%` for both `ns/op` and `allocs/op`, unless a benchmark is assigned to a baseline group with an explicit override

Required output:

- `perf gate: PASS`

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
3. Run `make test-race`.
4. Run full unit suite.
5. Run parity check and verify `OPEN_GAPS=0`.
6. Run coverage gate for `./amps/...`.
7. Run performance gate against locked baseline.
8. Run integration suite with target endpoint if available.
9. Validate handler order expectations.
10. Validate retry and replay behaviors under disconnect.
11. Validate queue auto-ack batching and timeout behavior.
12. Update parity matrix and relevant workflow docs.
13. Confirm support matrix statements still match observed behavior.

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
