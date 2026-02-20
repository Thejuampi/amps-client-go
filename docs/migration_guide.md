# AMPS Go Client Implementation Guide

This repository is a completely new Go implementation of an AMPS client.
The behavior and API parity targets are based on the AMPS C++ client (5.3.5.1) as a reference.

## Import Path

Use:

- `import "github.com/Thejuampi/amps-client-go/amps"`

## API Design Goals

- Provide a Go-native AMPS client with broad C++ parity coverage.
- Keep the public `Client`, `Command`, `Message`, and `MessageStream` APIs stable and practical for Go usage.
- Expose explicit alias methods where they improve C++ parity discoverability.

## Parity Aliases

- `SetClientName` and alias `SetName`
- `ClientName` and alias `Name`
- `SetLogonCorrelationID` and alias `SetLogonCorrelationData`
- `LogonCorrelationID` and alias `LogonCorrelationData`

## Core Behavior Hooks

- Publish persistence and replay:
  - `SetPublishStore`, `PublishStore`, `PublishFlush`
- Bookmark tracking and dedupe:
  - `SetBookmarkStore`, `BookmarkStore`
- Queue ack batching:
  - `SetAutoAck`, `SetAckBatchSize`, `SetAckTimeout`, `Ack`, `AckMessage`, `FlushAcks`
- Reconnect replay controls:
  - `SetRetryOnDisconnect`, `ExecuteAsyncNoResubscribe`

## HAClient Notes

- Use `NewHAClient(...)` for managed reconnect behavior.
- Configure:
  - `SetServerChooser(...)`
  - `SetReconnectDelayStrategy(...)` or `SetReconnectDelay(...)`
  - `SetLogonOptions(...)`
- `HAClient.SetDisconnectHandler(...)` intentionally returns a usage error because HA manages reconnect handling internally.

## Testing

- Unit tests cover parity helpers and core behavior adapters.
- Integration tests are optional and environment-gated via:
  - `AMPS_TEST_URI`
  - `AMPS_TEST_FAILOVER_URIS`
  - `AMPS_TEST_PROTOCOL`
  - `AMPS_TEST_USER`
  - `AMPS_TEST_PASSWORD`
