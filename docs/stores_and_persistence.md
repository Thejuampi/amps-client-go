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

## File-Backed Format

File stores are Go-native persisted formats. Binary compatibility with non-Go client stores is not required.

## Selection Guidance

- Memory stores: simplest runtime setup, non-durable.
- File stores: durable replay/resume across process restarts.

## Related

- [Bookmarks and Replay](bookmarks_and_replay.md)
- [Operational Playbook](operational_playbook.md)
- [Reference: Types and Handlers](reference_types_and_handlers.md)
