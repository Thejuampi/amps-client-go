# Supported Scope and Constraints

## Scope Baseline

This document defines the supported behavior scope for `github.com/Thejuampi/amps-client-go/amps`.

Parity target:

- C++ AMPS client behavior baseline: `5.3.5.1`
- Scope focus: `Client` and `HAClient` APIs and required supporting types

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

## Compatibility Guarantees

- Existing public method signatures in package `amps` remain available.
- C++-style alias methods are retained where implemented (`SetName`/`Name`, correlation aliases, store and hook aliases).
- Protocol-facing command and topic semantics are preserved.

## Constraints and Operational Assumptions

- Endpoint compatibility depends on AMPS server capability and entitlement.
- Delta and bookmark semantics require server-side support.
- File-backed publish and bookmark stores are Go-native formats; binary compatibility with non-Go client store files is not required.
- HTTP preflight headers apply only to HTTP upgrade transports.
- Callbacks may run from receive/recovery paths and should be thread-safe.

## Known Unsupported or Intentional Differences

- `HAClient.SetDisconnectHandler(...)` intentionally returns a usage error; HA reconnect logic owns disconnect handling.
- C++ low-level handle semantics are replaced by Go-native hooks (`RawConnection`, `SetTransportFilter`, `SetReceiveRoutineStartedCallback`).
- This repository does not guarantee compatibility with unrelated legacy helper APIs outside scoped `Client`/`HAClient` parity surface.

## Validation References

- [Testing and Validation](testing_and_validation.md)
- [Operational Playbook](operational_playbook.md)
- [C++ to Go Parity Matrix](cpp_to_go_parity_matrix.md)
