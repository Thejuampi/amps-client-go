# Pub/Sub and SOW

## Scope

This document covers stream and query entrypoints:

- `Subscribe*`
- `DeltaSubscribe*`
- `Sow*`
- `SowAndSubscribe*`
- `SowAndDeltaSubscribe*`
- `Unsubscribe`
- `SowDelete*`

## Preconditions

- Connected and logged on.
- Topic exists and caller has entitlement.
- Handler callbacks are safe for concurrent invocation.

## Pub/Sub Commands

| Method | Pattern | Notes |
|---|---|---|
| `Subscribe(...)` / `SubscribeAsync(...)` | Continuous stream | Optional content filter.
| `DeltaSubscribe(...)` / `DeltaSubscribeAsync(...)` | Delta stream | Endpoint SOW/delta behavior applies.
| `Unsubscribe(subID ...string)` | Stream termination | `all` when omitted.

## SOW Commands

| Method | Pattern | Termination Condition |
|---|---|---|
| `Sow(...)` / `SowAsync(...)` | Snapshot query | Group end/completed ack.
| `SowAndSubscribe(...)` / `SowAndSubscribeAsync(...)` | Snapshot + stream | Remains active until unsubscribe.
| `SowAndDeltaSubscribe(...)` / `SowAndDeltaSubscribeAsync(...)` | Snapshot + delta stream | Remains active until unsubscribe.
| `SowDelete(...)` / `SowDeleteByData(...)` / `SowDeleteByKeys(...)` | Mutation + stats ack | Stats ack consumed by helper.

## Ack and Handler Order

Route handling order for inbound messages:

1. Route-specific handler.
2. Global command-type handler.
3. Duplicate handler.
4. Unhandled handler.
5. Last-chance handler.

For queue-specific ack semantics, see [Queue Ack Semantics](queue_ack_semantics.md).

## Command Flow and Termination Semantics

`Subscribe*`:

- Continuous stream until `Unsubscribe`.
- Initial command ack indicates route setup success/failure.

`Sow*`:

- Finite query stream.
- Terminates on group-end/completed ack.

`SowAndSubscribe*` / `SowAndDeltaSubscribe*`:

- SOW snapshot first, then live stream continuation.
- Requires explicit unsubscribe for termination.

## Stream Management Controls

Synchronous variants return `MessageStream` with these controls:

- `SetTimeout(...)` for blocking reads.
- `SetMaxDepth(...)` for queue depth bound.
- `Conflate()` for queue compaction.
- `Close()` to terminate local stream consumption.

## Failure Modes

- Entitlement errors -> command failure ack / returned error.
- Invalid topic/filter -> command failure ack / returned error.
- Connection loss -> disconnect flow; resubscribe depends on manager and retry settings.

Recovery path:

1. Validate session state with `GetConnectionInfo()`.
2. Reconnect/logon as needed.
3. Re-execute subscription or query.
4. Re-verify route handler registration.

## Example: SOW-and-Subscribe Lifecycle

```go
stream, err := client.SowAndSubscribe("orders", "/status = 'open'")
if err != nil {
	panic(err)
}
defer stream.Close()

for stream.HasNext() {
	msg := stream.Next()
	if msg == nil {
		break
	}
	// process snapshot or live update
}

if err := client.Unsubscribe(); err != nil {
	panic(err)
}
```

## Related

- [Queue Ack Semantics](queue_ack_semantics.md)
- [Bookmarks and Replay](bookmarks_and_replay.md)
- [Reference: Command and MessageStream](reference_command_message_stream.md)
