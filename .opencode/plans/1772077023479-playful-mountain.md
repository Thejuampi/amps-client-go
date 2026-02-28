# Code Review Plan: Concurrency & Optimization Analysis

## Overview
Split the codebase across 5 sub-agents to scan for concurrency issues and optimization opportunities, with emphasis on SIMD/AVX potential.

## Codebase Summary
This is a **Go client library for AMPS** (Advanced Message Processing System) - a high-performance messaging system. The library provides:
- TCP/TLS connections, message publishing, subscriptions
- High-availability with automatic reconnection
- Bookmark/publish sequence tracking for guaranteed delivery
- C++ API parity layer

---

## Agent Review Results Summary

### Agent 1: Client Core (`client.go`)
**Key Findings:**
- **CRITICAL**: Single goroutine for all network I/O + message handling (lines 230-386)
- **HIGH**: Synchronous `onMessage` handler blocks entire read pipeline (lines 361-375, 388-495)
- **HIGH**: SOW batch processing is sequential (lines 335-367)
- **MEDIUM**: Redundant lock in ack path (line 1470)
- **MEDIUM**: Lock re-acquisition pattern risk (lines 1476-1481)
- **MEDIUM**: `ackProcessingLock` used for simple ops - could use atomic.Pointer

### Agent 2: Message Streams (`message_stream.go`)
**Key Findings:**
- **CRITICAL**: Race condition in `notifyNotEmptyLocked` - lost wakeups (lines 409-412)
- **CRITICAL**: TOCTOU race in `waitDequeue` and `waitDequeueTimeout` (lines 490-543)
- **HIGH**: Unnecessary lock in `length()` - could use atomic.Uint64 (lines 414-419)
- **HIGH**: Inefficient `tryDequeue` with defer overhead (lines 479-488)
- **MEDIUM**: Timer allocation in `waitDequeueTimeout` (lines 508-543)
- **MEDIUM**: Channel recreation pattern causes allocation pressure (lines 409-412)

### Agent 3: HA Client (`ha_client.go`)
**Key Findings:**
- **CRITICAL**: `connectAndLogonLock` held during blocking I/O (lines 127-128)
- **HIGH**: Race between `stopped.Load()` and `reconnecting.CompareAndSwap()` (lines 47-52)
- **HIGH**: Nested lock acquisition during replay - deadlock risk (client_parity_methods.go:467-469)
- **MEDIUM**: Reconnection errors silently discarded (line 67)
- **MEDIUM**: No callback for reconnection failure notification

### Agent 4: Stores (Bookmark, Publish, Subscription)
**Key Findings:**
- **CRITICAL**: Lock held during file I/O in `saveCheckpoint` (bookmark_store.go:378-442, publish_store.go:298-346)
- **CRITICAL**: Lost checkpoint race in `bumpMutationAndMaybeCheckpoint` (bookmark_store.go:454-470, publish_store.go:280-296)
- **CRITICAL**: Busy-wait loop in `Flush` with lock acquisition (publish_store.go:169-193)
- **HIGH**: TOCTOU in `loadCheckpoint` (bookmark_store.go:319-376)
- **HIGH**: WAL append without lock - data inconsistency (bookmark_store.go:544-564)
- **HIGH**: Full map copy under lock in `Resubscribe` (subscription_manager.go:112-123)
- **MEDIUM**: Single mutex for all stores - should use RWMutex

### Agent 5: Supporting Components & SIMD Potential
**Key Findings:**
- **CRITICAL**: `parseHeader` in message.go is main hot path (lines 55-143) - HIGH SIMD potential
- **HIGH**: `parseUintBytes` in header.go - digit-by-digit loop (lines 70-85) - HIGH SIMD potential
- **MEDIUM**: `ToMap` in fix_shredder.go - delimiter scanning (lines 24-57) - MEDIUM SIMD potential
- **MEDIUM**: CAPI allocations - `append([]byte(nil), ...)` should use `bytes.Clone()` (lines 370,399,732,817)
- **MEDIUM**: Composite message parser - pre-allocate slice capacity (line 14)

---

## SIMD/AVX Feasibility Assessment

| Code Path | Current Implementation | SIMD Benefit | Priority |
|-----------|----------------------|--------------|----------|
| `parseHeader` (message.go) | Byte-by-byte JSON parsing | **HIGH** - Find `"`, `\`, `{`, `}` delimiters | CRITICAL |
| `parseUintBytes` (header.go) | Digit-by-digit validation | **HIGH** - Validate all digits at once | HIGH |
| `ToMap` (fix_shredder.go) | Sequential scan | **MEDIUM** - Find `=` and `\x01` positions | MEDIUM |
| `bytesEqualString` (command.go) | 5-10 byte compare | LOW - Too short for SIMD | LOW |

**Implementation Options for SIMD in Go:**
1. Pure Go using lookup tables (lowest overhead, portable)
2. `github.com/minio/asm2plan9s` for AVX intrinsics
3. Assembly for critical paths (increases maintenance burden)

---

## Implementation Status

### Completed ✅

#### Critical Priority
1. ✅ **message_stream.go** - Fixed race condition in wait/dequeue, used atomic.Uint64 for length()
2. ⚠️ **client.go** - Single-threaded receive pipeline (SKIPPED - requires significant architectural change)
3. ✅ **bookmark_store.go** - Fixed lock during file I/O, fixed lost checkpoint race
4. ✅ **publish_store.go** - Fixed lock during file I/O, fixed lost checkpoint race
5. ✅ **publish_store.go** - Busy-wait loop improved

#### High Priority
6. ⚠️ **ha_client.go** - Lock held during blocking I/O (SKIPPED - architectural change needed)
7. ✅ **ha_client.go** - Fixed race between stopped check and CAS
8. ✅ **subscription_manager.go** - Fixed full map copy under lock, converted to RWMutex
9. ✅ **message_stream.go** - Used atomic.Uint64 for length()
10. ⚠️ **client_parity_methods.go** - Nested lock during replay (SKIPPED - requires careful analysis)

#### Medium Priority
11. ✅ **MemoryBookmarkStore, MemoryPublishStore** - Converted to RWMutex
12. ✅ **message_stream.go** - Used atomic.Uint64 for _length
13. ✅ **capi/capi.go** - Replaced `append([]byte(nil), ...)` with `bytes.Clone()`
14. ✅ **composite_message_parser.go** - Pre-allocate slice capacity
15. ⚠️ HAClient callbacks (SKIPPED - low impact)

#### SIMD Opportunities (Future)
- `message.go:55-143` - SIMD delimiter detection for JSON parsing
- `header.go:70-85` - SIMD digit validation
- `fix_shredder.go:24-57` - SIMD delimiter scanning

---

## Files to Modify

| Priority | File | Changes Needed |
|----------|------|----------------|
| CRITICAL | `amps/message_stream.go` | Fix race condition, use atomic for length |
| CRITICAL | `amps/client.go` | Add worker pool for message dispatch |
| CRITICAL | `amps/bookmark_store.go` | Fix lock during file I/O, checkpoint race |
| CRITICAL | `amps/publish_store.go` | Fix lock during file I/O, checkpoint race, Flush loop |
| HIGH | `amps/ha_client.go` | Fix lock ordering, add try-lock pattern |
| HIGH | `amps/subscription_manager.go` | Use RWMutex, reduce lock hold time |
| MEDIUM | `amps/capi/capi.go` | Use bytes.Clone() |
| MEDIUM | `amps/composite_message_parser.go` | Pre-allocate slice capacity |
| FUTURE | `amps/message.go` | SIMD JSON parsing |
| FUTURE | `amps/header.go` | SIMD digit validation |

---

## Testing Recommendations

1. **Concurrency tests**: Add race detector tests (`go test -race`) for all concurrent code paths
2. **Load testing**: Test high-throughput scenarios to verify worker pool benefits
3. **Benchmark**: Run existing benchmarks before/after changes
4. **Integration tests**: Verify HA client reconnection works under various failure scenarios
