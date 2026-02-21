# Reference: Types and Handlers

## Core Types

| Type | Purpose |
|---|---|
| `ConnectionInfo` | String map describing current connection metadata.
| `ConnectionState` | Connection lifecycle state enum.
| `LogonParams` | Optional logon parameters (`Timeout`, `Authenticator`, `CorrelationID`).

## ConnectionState Values

| Value | Meaning |
|---|---|
| `ConnectionStateDisconnected` | Transport disconnected.
| `ConnectionStateShutdown` | Explicit shutdown initiated.
| `ConnectionStateConnected` | Transport connected.
| `ConnectionStateLoggedOn` | Logon completed.
| `ConnectionStatePublishReplayed` | Publish replay completed.
| `ConnectionStateHeartbeatInitiated` | Heartbeat command acknowledged.
| `ConnectionStateResubscribed` | Resubscribe pass completed.
| `ConnectionStateUnknown` | Unclassified state.

## Handler and Listener Interfaces

| Interface / Func Adapter | Purpose |
|---|---|
| `ConnectionStateListener` / `ConnectionStateListenerFunc` | Observe connection-state transitions.
| `ExceptionListener` / `ExceptionListenerFunc` | Receive internal exception signals.
| `FailedWriteHandler` / `FailedWriteHandlerFunc` | Receive failed publish write events.
| `FailedResubscribeHandler` / `FailedResubscribeHandlerFunc` | Control behavior on resubscribe failures.

## Strategy and Selection Interfaces

| Interface | Purpose |
|---|---|
| `ServerChooser` | URI selection and failure/success reporting.
| `ReconnectDelayStrategy` | Reconnect delay policy abstraction.
| `SubscriptionManager` | Subscription tracking and resubscribe behavior.

Concrete strategy/chooser types:

- `DefaultServerChooser`
- `FixedDelayStrategy`
- `ExponentialDelayStrategy`

## Store Interfaces and Implementations

| Interface | Implementations |
|---|---|
| `PublishStore` | `MemoryPublishStore`, `FilePublishStore` |
| `BookmarkStore` | `MemoryBookmarkStore`, `FileBookmarkStore`, `MMapBookmarkStore`, `RingBookmarkStore` |

## Bookmark and Version Helpers

Bookmark constants/functions:

- `BookmarksEPOCH`, `BookmarksRECENT`, `BookmarksNOW`
- `BOOKMARK_EPOCH()`, `BOOKMARK_RECENT()`, `BOOKMARK_NOW()`
- `EPOCH()`, `RECENT()`, `NOW()`, `MOST_RECENT()`

Version helper:

- `ConvertVersionToNumber(string) uint64`

## Transport Hook Types

| Type | Purpose |
|---|---|
| `TransportFilterDirection` | Indicates inbound or outbound frame flow.
| `TransportFilter` | Frame transformation hook signature.

## Related

- [Transport and Hooks](transport_and_hooks.md)
- [Stores and Persistence](stores_and_persistence.md)
- [Reference: Client](reference_client.md)
- [Reference: HAClient](reference_ha_client.md)
