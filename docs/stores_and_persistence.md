# Stores and Persistence

## Scope

Covers publish/bookmark store abstractions, durability behavior, and restart recovery semantics.

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
- File-backed mode persists mutations through append-only WAL records and periodic atomic checkpoints.

Failure behavior:

- Store/write errors are returned to caller.
- Replay callback errors halt replay with propagated error.
- WAL replay errors prevent full restart recovery and are surfaced from store construction/load paths.

## Bookmark Store

Interface: `BookmarkStore`

Implementations:

- `MemoryBookmarkStore`
- `FileBookmarkStore`
- `MMapBookmarkStore` (file-backed store with mmap-enabled checkpoint I/O)
- `RingBookmarkStore` (compatibility wrapper)

Key behaviors:

- `Log(message)` records bookmark sequencing.
- `IsDiscarded(message)` duplicate/discard detection.
- `GetMostRecent(subID)` resume point lookup.
- `Persisted(subID, bookmark)` mark persisted checkpoint.
- File-backed mode records upsert/discard/purge/version mutations in WAL and replays on restart.

Failure behavior:

- Invalid bookmark/message state may no-op or return default values based on implementation.
- File-backed persistence failures can affect restart continuity.

## File-Backed Durability Model

- Behavior target is crash-safe recovery and deterministic replay order.
- Store files are Go-native checkpoint + WAL artifacts.
- Binary compatibility with C++/non-Go on-disk store formats is not required.

## File Store Options

Additive constructors:

- `NewFilePublishStoreWithOptions(path string, opts FileStoreOptions)`
- `NewFileBookmarkStoreWithOptions(path string, opts FileStoreOptions)`

Options:

- `UseWAL`: enable append-only mutation log for restart continuity.
- `SyncOnWrite`: request fsync-style durability on WAL append.
- `CheckpointInterval`: number of mutations between checkpoint rewrites.
- `MMap.Enabled`: enable mmap-style checkpoint read/write path.
- `MMap.InitialSize`: initial mapped length hint for checkpoint files.

## Selection Guidance

- Memory stores: simplest runtime setup, non-durable.
- File stores: durable replay/resume across process restarts with WAL + checkpoint recovery.
- `MMapBookmarkStore`: same behavior as file store with mmap-enabled checkpoint path.

## Example: Configure Durable Stores

```go
opts := amps.FileStoreOptions{
	UseWAL:             true,
	SyncOnWrite:        true,
	CheckpointInterval: 10,
	MMap: amps.MMapOptions{
		Enabled:     true,
		InitialSize: 64 * 1024,
	},
}
client.SetPublishStore(amps.NewFilePublishStoreWithOptions("state/publish.json", opts))
client.SetBookmarkStore(amps.NewFileBookmarkStoreWithOptions("state/bookmarks.json", opts))

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
