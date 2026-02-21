# AMPS Go Client Implementation Guide

This repository is a new Go implementation of an AMPS client.

Parity target:

- C++ client behavior target: `5.3.5.1`

## Import Path

Use:

- `import "github.com/Thejuampi/amps-client-go/amps"`

## Adoption Orientation

This guide describes how to align existing AMPS usage with this Go API surface.

## Compatibility Aliases

- `SetClientName` and `SetName`
- `ClientName` and `Name`
- `SetLogonCorrelationID` and `SetLogonCorrelationData`
- `LogonCorrelationID` and `LogonCorrelationData`

## Behavior Controls to Validate During Adoption

- Retry: `SetRetryOnDisconnect`
- Queue ack policy: `SetAutoAck`, `SetAckBatchSize`, `SetAckTimeout`, `FlushAcks`
- Replay and dedupe: `SetPublishStore`, `SetBookmarkStore`
- HA reconnection: `NewHAClient`, chooser and delay strategy controls

## Recommended Adoption Sequence

1. Validate connect/logon/publish/subscribe baseline.
2. Enable and test queue ack policy.
3. Enable publish/bookmark stores for replay-sensitive workloads.
4. Validate failover behavior with HA client and server chooser.
5. Run integration suite against target endpoint.

## Validation Commands

Use these commands as the default contributor validation path:

- `make test`
- `make parity-check`
- `make coverage-check`

Integration tests remain environment-gated and separate from the coverage gate workflow:

- `make integration-test`
- `go test ./... -run Integration`

## Related

- [Documentation Index](index.md)
- [Supported Scope and Constraints](supported_scope.md)
- [Testing and Validation](testing_and_validation.md)
- [C++ to Go Parity Matrix](cpp_to_go_parity_matrix.md)
