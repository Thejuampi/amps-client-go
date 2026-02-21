# Testing and Validation

## Test Layers

Unit tests:

- Deterministic behavior checks for parsing, routing, ack batching, store replay, HA recovery sequencing, and API parity aliases.

Integration tests:

- Environment-gated endpoint validation for connect/logon, pub/sub, SOW flows, queue ack behavior, bookmark resume, timer commands, and HA failover setup.

## Commands

```bash
make test
make integration-test
make parity-check
make coverage-check
make perf-check
make release
```

Equivalent direct commands:

```bash
go test ./...
go test ./... -run Integration
go run ./tools/paritycheck -manifest tools/parity_manifest.json -behavior-manifest tools/parity_behavior_manifest.json
go test -count=1 ./amps/... -coverprofile=coverage.out
go run ./tools/coveragegate -profile coverage.out
go run ./tools/perfgate -baseline tools/perf_baseline.json
```

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
- Allowed regression threshold is `10%` for both `ns/op` and `allocs/op`

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
2. Run full unit suite.
3. Run parity check and verify `OPEN_GAPS=0`.
4. Run coverage gate for `./amps/...`.
5. Run performance gate against locked baseline.
6. Run integration suite with target endpoint if available.
7. Validate handler order expectations.
8. Validate retry and replay behaviors under disconnect.
9. Validate queue auto-ack batching and timeout behavior.
10. Update parity matrix and relevant workflow docs.
11. Confirm support matrix statements still match observed behavior.

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
