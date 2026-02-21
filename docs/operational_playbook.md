# Operational Playbook

## Scope

Production-oriented guidance for reliability and debugging.

## Baseline Runtime Profile

1. Use explicit client name.
2. Configure retry policy intentionally.
3. For queue workloads, enable auto-ack with tuned batch and timeout.
4. For replay-sensitive workloads, configure publish and bookmark stores.
5. Register connection-state and exception listeners.

## Reconnect Strategy Selection

Fixed delay (`NewFixedDelayStrategy`):

- Stable, predictable reconnect cadence.
- Suitable for controlled environments.

Exponential delay (`NewExponentialDelayStrategy`):

- Backoff under repeated failures.
- Better for endpoint instability or failover storms.

## Queue Auto-Ack Tuning

Controls:

- `SetAutoAck(...)`
- `SetAckBatchSize(...)`
- `SetAckTimeout(...)`

Tuning heuristics:

- Lower batch size for lower redelivery window.
- Higher timeout only when throughput benefits outweigh replay risk.
- Always validate queue messages include bookmark and lease metadata.

## Bookmark Replay Policy

- For cold start from beginning: `BOOKMARK_EPOCH()`.
- For near-head catch-up: `BOOKMARK_RECENT()`.
- For start-at-current behavior: `BOOKMARK_NOW()`.

Store policy:

- Memory stores for stateless workers.
- File stores for restart continuity.

## Publish Durability Tradeoffs

- Use `PublishStore` for replay after disconnect.
- Use `PublishFlush(...)` where publish persistence completion must be enforced.
- Configure `FailedWriteHandler` for explicit outbound failure telemetry.

## Failure Triage Checklist

1. Confirm transport connectivity and URI correctness.
2. Inspect `GetConnectionInfo()` and `ServerVersion()`.
3. Check auth/logon path and command failure reasons.
4. Validate chooser and reconnect strategy parameters.
5. Inspect store state (`UnpersistedCount`, most recent bookmark).
6. Verify listener and handler registrations.

## Recovery Checklist

1. Re-establish connect/logon.
2. Verify post-logon replay completed.
3. Verify resubscription completed.
4. Validate queue/bookmark positions after reconnect.

## Related

- [HA Failover](ha_failover.md)
- [Queue Ack Semantics](queue_ack_semantics.md)
- [Bookmarks and Replay](bookmarks_and_replay.md)
