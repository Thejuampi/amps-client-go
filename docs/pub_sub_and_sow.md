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

## Ack and Handler Behavior

Route handling order for inbound messages:

1. Route-specific handler.
2. Global command-type handler.
3. Duplicate handler.
4. Unhandled handler.
5. Last-chance handler.

For queue-specific ack semantics, see [Queue Ack Semantics](queue_ack_semantics.md).

## Stream Management

Synchronous calls return `MessageStream`.

Typical controls:

- `SetTimeout(...)`
- `SetMaxDepth(...)`
- `Conflate()`
- `Close()`

## Failure Modes

- Entitlement errors -> command failure ack / returned error.
- Invalid topic/filter -> command failure ack / returned error.
- Connection loss -> disconnect flow; resubscribe depends on manager and retry settings.

## Related

- [Queue Ack Semantics](queue_ack_semantics.md)
- [Bookmarks and Replay](bookmarks_and_replay.md)
- [Reference: Command and MessageStream](reference_command_message_stream.md)
