# Supported Scope and Constraints

## Scope Baseline

This document defines the supported behavior scope for `github.com/Thejuampi/amps-client-go/amps`.

Parity target:

- C++ AMPS client behavior baseline: `5.3.5.1`
- Scope focus: full public C++ client families, including compatibility layers in `amps/capi` and `amps/cppcompat`

Symbol gate baseline:

- manifest file: `tools/parity_manifest.json`
- current mapped symbols: `253`
- required gate: `MISSING_HEADER_SYMBOLS=0` and `MISSING_GO_SYMBOLS=0`

## Supported Workflows

| Workflow | Status | Primary APIs |
|---|---|---|
| Connection and session lifecycle | Supported | `NewClient`, `Connect`, `Logon`, `Disconnect`, `Close` |
| Publish and delta publish | Supported | `Publish`, `PublishBytes`, `DeltaPublish`, `DeltaPublishBytes` |
| Streaming subscribe flows | Supported | `Subscribe*`, `DeltaSubscribe*`, `Unsubscribe` |
| SOW query flows | Supported | `Sow*`, `SowAndSubscribe*`, `SowAndDeltaSubscribe*`, `SowDelete*` |
| Queue acknowledgement controls | Supported | `Ack`, `AckMessage`, `SetAutoAck`, `SetAckBatchSize`, `SetAckTimeout`, `FlushAcks` |
| Bookmark resume and duplicate detection | Supported | `BookmarkSubscribe*`, bookmark constants/helpers, `BookmarkStore` implementations |
| Publish replay and persistence tracking | Supported | `PublishStore` implementations, `PublishFlush` |
| Retry and reconnect recovery | Supported | `SetRetryOnDisconnect`, `HAClient.ConnectAndLogon`, chooser/strategy controls |
| Transport and routing hooks | Supported | transport filter, receive-start callback, global handlers, listener APIs |
| C compatibility layer | Supported | `amps/capi` client/message handle APIs, TLS/zlib compatibility entrypoints |
| C++ utility compatibility | Supported | `amps/cppcompat` utility/store/recovery/FIX families |

## Compatibility Guarantees

- Existing public method signatures in package `amps` remain available.
- C++-style alias methods are retained where implemented (`SetName`/`Name`, correlation aliases, store and hook aliases).
- Protocol-facing command and topic semantics are preserved.
- C and C++ compatibility entrypoints are additive and do not break existing Go-first API usage.

## Constraints and Operational Assumptions

- Endpoint compatibility depends on AMPS server capability and entitlement.
- Delta and bookmark semantics require server-side support.
- File-backed publish and bookmark stores are Go-native formats; binary compatibility with non-Go client store files is not required.
- HTTP preflight headers apply only to HTTP upgrade transports.
- Callbacks may run from receive/recovery paths and should be thread-safe.

## Known Unsupported or Intentional Differences

- `HAClient.SetDisconnectHandler(...)` intentionally returns a usage error; HA reconnect logic owns disconnect handling.
- Persistence is behaviorally compatible but not binary-file compatible with C++ store file formats.
- C-compatible thread/socket reporting reflects Go runtime and transport capabilities.

## Validation References

- [Testing and Validation](testing_and_validation.md)
- [Operational Playbook](operational_playbook.md)
- [C++ to Go Parity Matrix](cpp_to_go_parity_matrix.md)
- [C API Compatibility Reference](capi_reference.md)
- [C++ Compatibility Reference](cppcompat_reference.md)
