# Gap Register

This register is the source of truth for behavior parity closure in this repository.

Gate contract:

- `go run ./tools/paritycheck -manifest tools/parity_manifest.json -behavior-manifest tools/parity_behavior_manifest.json`
- required: `OPEN_GAPS=0`

Current status:

- `OPEN_GAPS=0`
- unresolved items: `0`

## Domains

| ID | Domain | Initial State | Current State | Pass Criterion |
|---|---|---|---|---|
| `contract-parity-gating` | Parity contract | Open | Closed | `paritycheck` reports `OPEN_GAPS=0` |
| `correctness-parser-builder-hardening` | Correctness | Open | Closed | Parser/builder tests pass in `./amps/...` |
| `logon-timeout-enforcement` | Session | Open | Closed | `LogonParams.Timeout` behavior is covered by tests |
| `wal-checkpoint-durability` | Durability | Open | Closed | File stores recover with WAL/checkpoint replay |
| `ha-recovery-ordering` | High availability | Open | Closed | Recovery order remains deterministic and test-covered |
| `kerberos-pluggable-authenticator` | Authentication | Open | Closed | `amps/auth/kerberos` tests pass with capability reporting |
| `capi-ssl-stateful-emulation` | C API SSL | Open | Closed | SSL lifecycle/read/write/pending behaviors are test-covered |
| `perf-regression-gate` | Performance | Open | Closed | `perfgate` passes against locked baseline |
| `documentation-alignment` | Documentation | Open | Closed | Scope, persistence, CAPI, cppcompat, validation docs updated |

## References

- `tools/parity_behavior_manifest.json`
- [Parity Acceptance](parity_acceptance.md)
- [Supported Scope and Constraints](supported_scope.md)
