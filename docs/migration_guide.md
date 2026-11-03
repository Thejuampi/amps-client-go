# AMPS Go Client Implementation Guide

This repository is a new Go implementation of an AMPS client.

Parity target:

- C++ client behavior review: `5.3.5.14`

## Import Path

Use:

- `import "github.com/Thejuampi/amps-client-go/amps"`

## Upgrading from 0.8.x to 1.0.0

The module path stays the same. Update checksum consumers and review the
corrected behavior before deploying:

- `cppcompat.CRC.Compute` returns `uint64` and uses the portable C++ checksum
  algorithm. Update typed variables, interfaces, and stored checksum fields;
  truncating the result does not preserve the former CRC32 algorithm.
- The optional third argument of `Client.Ack` contains acknowledgement options,
  such as `cancel`, rather than a subscription id.
- `Command.SetTopN(0)` requests zero results. Use `uint(^uint32(0))` to clear
  an explicitly configured limit.
- Queue streams auto-ack the previously delivered message when the consumer
  advances. Bounded streams apply backpressure, so consumers must keep advancing
  or close the stream to release a blocked receive loop.
- `RingSize` is a history-retention target; active, unacknowledged publishes
  remain retained even when their count exceeds it.
- Fresh publish stores use a time-based initial sequence. Recovered stores
  preserve their persisted sequence history.

Existing WAL/checkpoint formats remain readable, including legacy
`discard_upto` records. The corrections cannot reconstruct entries already
removed by earlier discard or retention defects.

Build release binaries with Go 1.26.10 or another supported, patched compiler.
Go 1.25 remains the source-language compatibility floor; a dependency's
toolchain preference does not upgrade the consuming application's compiler.

See the [changelog](../CHANGELOG.md), [behavior corrections](behavior_parity_fixes.md),
and [supported scope](supported_scope.md) for details and compatibility limits.

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
