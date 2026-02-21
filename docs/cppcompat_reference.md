# Reference: C++ Compatibility (`amps/cppcompat`)

## Purpose

`amps/cppcompat` provides Go-native equivalents for C++ utility and extension families used with AMPS client workflows.

Compatibility target:

- C++ utility model parity for application code migration
- store/recovery abstractions with behavioral compatibility
- FIX container and iterator-style access in Go

## Utility and Model Types

- `VersionInfo`, `ParseVersionInfo`
- `URI`, `ParseURI`
- `Reason`
- `Field`
- `BookmarkRange`
- `CRC`

## Store and Buffer Families

Buffer abstractions:

- `Buffer`
- `MemoryStoreBuffer`
- `MMapStoreBuffer`

Store abstractions:

- `BlockStore`
- `Store`
- `StoreReplayer`
- `BlockPublishStore`
- `HybridPublishStore`
- `LoggedBookmarkStore`

Subscription compatibility:

- `MemorySubscriptionManager`

## Recovery Families

- `RecoveryPoint`
- `FixedRecoveryPoint`
- `DynamicRecoveryPoint`
- `RecoveryPointAdapter`
- `ConflatingRecoveryPointAdapter`
- `SOWRecoveryPointAdapter`

## FIX Family

- `FIX`
- iterators:
  - `FIX.Begin()`
  - `FIX.End()`
  - `FIXIterator.Next()`

## Behavioral Notes

- These APIs preserve behavioral intent from C++ families while using Go memory and concurrency rules.
- Store persistence parity is behavioral and does not require binary compatibility with C++ on-disk formats.
- Recovery adapters expose explicit composition semantics (`fixed`, `dynamic`, `conflating`, `sow`) and are deterministic under single-threaded access.

## Validation

Run parity gates:

```bash
go run ./tools/paritycheck -manifest tools/parity_manifest.json -behavior-manifest tools/parity_behavior_manifest.json
```

Required gate output includes:

- `MISSING_HEADER_SYMBOLS=0`
- `MISSING_GO_SYMBOLS=0`
- `OPEN_GAPS=0`
