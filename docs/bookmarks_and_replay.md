# Bookmarks and Replay

## Scope

Covers bookmark resume, duplicate handling, and publish replay behavior.

## Bookmark Subscription APIs

| Method | Purpose |
|---|---|
| `BookmarkSubscribe(...)` | Sync bookmark-based subscribe.
| `BookmarkSubscribeAsync(...)` | Async bookmark-based subscribe.
| `SetBookmarkStore(...)` / `BookmarkStore()` | Attach bookmark persistence and dedupe policy.

Bookmark constants:

- `BOOKMARK_EPOCH()` / `EPOCH()`
- `BOOKMARK_RECENT()` / `RECENT()` / `MOST_RECENT()`
- `BOOKMARK_NOW()` / `NOW()`

## Duplicate Detection Path

When bookmark store is attached:

1. Incoming bookmark-bearing message is logged.
2. Duplicate/discard decision computed by store.
3. If duplicate, duplicate handler is invoked.

Related hooks:

- `SetDuplicateMessageHandler(...)`
- `SetUnhandledMessageHandler(...)`
- `SetLastChanceMessageHandler(...)`

## Publish Replay Path

Replay components:

- `SetPublishStore(...)` / `PublishStore()`
- `PublishFlush(...)`

Behavior:

- Publish/delta-publish commands may be persisted with sequence IDs.
- Persisted ack (`AckTypePersisted`, success) discards up-to sequence.
- Post-logon recovery replays unpersisted publish commands.

## Retry Interaction and Recovery

`SetRetryOnDisconnect(true)` enables queueing eligible commands for retry after reconnect.

`ExecuteAsyncNoResubscribe(...)` excludes routes from resubscribe tracking.

Recovery sequence after reconnect/logon:

1. Publish replay runs from configured publish store.
2. Subscription manager replays tracked subscriptions.
3. Bookmark-based consumers resume from configured bookmark policy.
4. Duplicate path drops replayed messages already marked discarded.

## Failure Behaviors

- Store write failures are surfaced as command errors.
- Failed outbound publish writes trigger `FailedWriteHandler` when configured.
- Bookmark store and publish store errors are routed to exception listener where available.

## Example: Bookmark Resume with Durable Store

```go
client.SetBookmarkStore(amps.NewFileBookmarkStore("state/bookmarks.json"))

stream, err := client.BookmarkSubscribe("orders", amps.BOOKMARK_RECENT())
if err != nil {
	panic(err)
}
defer stream.Close()

for stream.HasNext() {
	msg := stream.Next()
	if msg == nil {
		break
	}
	// process message and optional explicit discard policy
}
```

## Related

- [Stores and Persistence](stores_and_persistence.md)
- [HA Failover](ha_failover.md)
- [Reference: Types and Handlers](reference_types_and_handlers.md)
