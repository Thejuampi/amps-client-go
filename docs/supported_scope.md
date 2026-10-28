# Supported Scope and Constraints

## Scope Baseline

The 2026-10-24 [behavior audit](behavior_parity_audit.md) compares this implementation
with **C++ 5.3.5.14**. Its 23 finding groups are [corrected](behavior_parity_fixes.md),
with 32 original probes in the ordinary suite and zero open registered gaps.
The scope below still distinguishes compatibility limits from verified contracts;
this does not certify complete behavioral equivalence against a production server.

This document defines the supported behavior scope for `github.com/Thejuampi/amps-client-go/amps`.

Parity target:

- Original C++ client port baseline: `5.3.5.1`
- Current behavior review reference: `5.3.5.14`
- Scope focus: full public C++ client families, including compatibility layers in `amps/capi` and `amps/cppcompat`

Symbol gate baseline:

- manifest file: `tools/parity_manifest.json`
- current mapped symbols: `261`
- required gate: `MISSING_HEADER_SYMBOLS=0` and `MISSING_GO_SYMBOLS=0`

Behavior gate baseline:

- manifest file: `tools/parity_behavior_manifest.json`
- required gate: `OPEN_GAPS=0`

## Quality Gates

Parity symbol gate:

- command: `go run ./tools/paritycheck -manifest tools/parity_manifest.json`
- required result: `MISSING_HEADER_SYMBOLS=0` and `MISSING_GO_SYMBOLS=0`

Parity behavior gate:

- command: `go run ./tools/paritycheck -manifest tools/parity_manifest.json -behavior-manifest tools/parity_behavior_manifest.json`
- required result: `OPEN_GAPS=0`

Coverage gate:

- scope: `./amps/...` (not full `./...`)
- aggregate threshold: `>=90.0%`
- pure-functional threshold: `100.0%`
- I/O/stateful threshold: `>=80.0%`
- commands:
  - `go test -count=1 ./amps/... -coverprofile=coverage.out`
  - `go run ./tools/coveragegate -profile coverage.out`
- source of truth for thresholds and file classification: `tools/coveragegate/main.go`

Performance regression gate:

- baseline file: `tools/perf_baseline.json`
- command: `go run ./tools/perfgate -baseline tools/perf_baseline.json`
- required result: `perf gate: PASS`

## Supported Workflows

| Workflow | Status | Primary APIs |
|---|---|---|
| Connection and session lifecycle | Corrected; R08, R09, R20 | `NewClient`, `Connect`, `Logon`, `Disconnect`, `Close` |
| Publish and delta publish | Corrected; R06, R11 | `Publish`, `PublishBytes`, `DeltaPublish`, `DeltaPublishBytes` |
| Streaming subscribe flows | Corrected; R02, R03, R07 | `Subscribe*`, `DeltaSubscribe*`, `Unsubscribe` |
| SOW query flows | Corrected; R02, R21 | `Sow*`, `SowAndSubscribe*`, `SowAndDeltaSubscribe*`, `SOWHistoricalQueryAndSubscribe`, `SOWPaginatedQueryAndSubscribe`, `SowDelete*` |
| Queue acknowledgement controls | Corrected; R01, R03 | `Ack`, `AckMessage`, `SetAutoAck`, `SetAckBatchSize`, `SetAckTimeout`, `FlushAcks` |
| Queue backlog requests | Supported | `SetMaxBacklog`, `SubscribeWithMaxBacklog`, `SubscribeAsyncWithMaxBacklog` |
| Bookmark resume and duplicate detection | Corrected; R04, R05 | `BookmarkSubscribe*`, bookmark constants/helpers, `BookmarkStore` implementations |
| Bookmark replay controls | Supported | `SetBookmarkNotFound*`, `SetFullyDurable`, `FullyDurableSubscribe` |
| Publish replay and persistence tracking | Corrected; R06, R16, R23 | `PublishStore` implementations, `PublishFlush` |
| Retry and reconnect recovery | Corrected R07, R08, R20; C04 limits remain | `SetRetryOnDisconnect`, `HAClient.ConnectAndLogon`, chooser/strategy controls |
| Transport compression on TCP/TLS | Supported | `SetCompression`, `compression=zlib` on `tcp`/`tcps` URIs |
| Kerberos compatibility auth (best effort, pure Go) | Supported with limits | `amps/auth/kerberos.NewAuthenticator`, additive capability/challenge interfaces |
| Transport and routing hooks | Supported | transport filter, receive-start callback, global handlers, listener APIs |
| C compatibility layer | Corrected R18; partial/emulated C01, C03 | `amps/capi` client/message handle APIs, TLS/zlib compatibility entrypoints |
| C++ utility compatibility | Corrected R13, R22, R23; partial C02 | `amps/cppcompat` utility/store/recovery/FIX families |

Rxx and Cxx references identify findings in the [behavior audit](behavior_parity_audit.md).

## Compatibility Guarantees

- Existing public method signatures in package `amps` remain available.
- C++-style alias methods are retained where implemented (`SetName`/`Name`, correlation aliases, store and hook aliases).
- Protocol-facing semantics target C++ behavior; the audit records current divergences.
- C and C++ compatibility entrypoints are additive and do not break existing Go-first API usage.

## Constraints and Operational Assumptions

- Endpoint compatibility depends on AMPS server capability and entitlement.
- Delta and bookmark semantics require server-side support.
- Connection compression is zlib-only and supported on `tcp` and `tcps`. It is intentionally unsupported on `ws`, `wss`, and `unix`.
- File-backed publish and bookmark stores use Go-native checkpoint/WAL persistence; binary compatibility with non-Go client store files is not required.
- HTTP preflight headers apply only to HTTP upgrade transports.
- Callbacks may run from receive or recovery paths and should be thread-safe.
- Kerberos integration is pure-Go best-effort and intentionally avoids CGO SSPI/GSSAPI bindings.

## Known Unsupported or Intentional Differences

- `HAClient.SetDisconnectHandler(...)` intentionally returns a usage error; HA reconnect logic owns disconnect handling.
- Persistence is behaviorally compatible but not binary-file compatible with C++ store file formats.
- C-compatible thread counters are lifecycle-compatible counters, but callback `threadID` values are Go-generated identifiers rather than OS thread IDs.
- Kerberos authenticators report capabilities and limitations at runtime; OS-native ticket acquisition must be supplied by caller callbacks, environment, or a static token.
- Generic option strings remain supported, but first-class helpers now exist for `send_keys`, `fully_durable`, `max_backlog`, and `bookmark_not_found`.

## Validation References

- [Testing and Validation](testing_and_validation.md)
- [Parity Acceptance](parity_acceptance.md)
- [Gap Register](gap_register.md)
- [Operational Playbook](operational_playbook.md)
- [C++ to Go Parity Matrix](cpp_to_go_parity_matrix.md)
- [C API Compatibility Reference](capi_reference.md)
- [C++ Compatibility Reference](cppcompat_reference.md)

## Behavior correction migration notes

`cppcompat.CRC.Compute` now returns uint64 and matches the portable C++ CRC.
`Client.Ack` treats its optional third argument as options; `SetTopN(0)` is an
explicit zero limit. See the [fix record](behavior_parity_fixes.md) for details.
