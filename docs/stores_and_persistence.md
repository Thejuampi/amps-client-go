# Stores and Persistence

## Scope

Covers bookmark and publish storage abstractions and concrete implementations.

## Publish Store

Interface: `PublishStore`

Implementations:

- `MemoryPublishStore`
- `FilePublishStore`

Key behaviors:

- `Store(command)` assigns/records sequence.
- `DiscardUpTo(sequence)` removes persisted commands.
- `Replay(replayer)` emits unpersisted commands in sequence order.
- `Flush(timeout)` waits until unpersisted set is empty.

Failure behavior:

- Store/write errors are returned to caller.
- Replay callback errors halt replay with propagated error.

## Bookmark Store

Interface: `BookmarkStore`

Implementations:

- `MemoryBookmarkStore`
- `FileBookmarkStore`
- `MMapBookmarkStore` (compatibility wrapper)
- `RingBookmarkStore` (compatibility wrapper)

Key behaviors:

- `Log(message)` records bookmark sequencing.
- `IsDiscarded(message)` duplicate/discard detection.
- `GetMostRecent(subID)` resume point lookup.
- `Persisted(subID, bookmark)` mark persisted checkpoint.

Failure behavior:

- Invalid bookmark/message state may no-op or return default values based on implementation.
- File-backed persistence failures can affect restart continuity.

## File-Backed Format

File stores are Go-native persisted formats. Binary compatibility with non-Go client stores is not required.

## Selection Guidance

- Memory stores: simplest runtime setup, non-durable.
- File stores: durable replay/resume across process restarts.

## Example: Configure Durable Stores

```go
client.SetPublishStore(amps.NewFilePublishStore("state/publish.json"))
client.SetBookmarkStore(amps.NewFileBookmarkStore("state/bookmarks.json"))

if err := client.Publish("orders", `{"id":42}`); err != nil {
	panic(err)
}
if err := client.PublishFlush(2 * time.Second); err != nil {
	panic(err)
}
```

## Related

- [Bookmarks and Replay](bookmarks_and_replay.md)
- [Operational Playbook](operational_playbook.md)
- [Reference: Types and Handlers](reference_types_and_handlers.md)
