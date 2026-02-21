# Parity Acceptance

This runbook defines the offline acceptance commands for enterprise parity closure.

All commands are local/offline. No live AMPS endpoint is required for these gates.

## Required Commands

1. Unit and package tests:

```bash
go test -count=1 ./amps/...
```

2. Coverage profile and gate:

```bash
go test -count=1 ./amps/... -coverprofile=coverage.out
go run ./tools/coveragegate -profile coverage.out
```

3. Symbol + behavior parity gate:

```bash
go run ./tools/paritycheck -manifest tools/parity_manifest.json -behavior-manifest tools/parity_behavior_manifest.json
```

4. Performance regression gate:

```bash
go run ./tools/perfgate -baseline tools/perf_baseline.json
```

## Required Outputs

- Parity gate output includes:
  - `MISSING_HEADER_SYMBOLS=0`
  - `MISSING_GO_SYMBOLS=0`
  - `OPEN_GAPS=0`
- Coverage gate output includes:
  - `coverage gate: PASS`
- Performance gate output includes:
  - `perf gate: PASS`

## Acceptance Result

Parity closure is accepted only when all required commands pass and `OPEN_GAPS=0`.

## References

- [Gap Register](gap_register.md)
- [Testing and Validation](testing_and_validation.md)
- [Supported Scope and Constraints](supported_scope.md)
