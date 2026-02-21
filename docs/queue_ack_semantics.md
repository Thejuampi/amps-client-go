# Queue Ack Semantics

## Scope

This document defines queue consume ack behavior and API controls.

## Queue Message Requirements

Auto-ack batching requires queue-like publish messages containing:

- `topic`
- `bookmark`
- lease metadata (`LeasePeriod()` present)

Without these fields, auto-ack does not emit `ack` commands.

## Explicit Ack Methods

| Method | Input | Behavior |
|---|---|---|
| `Ack(topic, bookmark, subID...)` | Topic + bookmark | Sends `ack` command immediately.
| `AckMessage(message)` | Message containing topic/bookmark | Extracts and sends ack.
| `FlushAcks()` | None | Flushes buffered auto-acks by topic/subID batch key.

## Auto-Ack Controls

| Method | Purpose | Default |
|---|---|---|
| `SetAutoAck(bool)` / `AutoAck()` | Enable or disable auto-ack engine | Disabled.
| `SetAckBatchSize(uint)` / `AckBatchSize()` | Batch threshold before flush | `1`.
| `SetAckTimeout(time.Duration)` / `AckTimeout()` | Timer-based flush | `1s`.

## Batching Model

Batches are keyed by `(topic, subID)`.

Flush triggers:

- Batch-size threshold reached.
- Timeout elapsed.
- Explicit `FlushAcks()` call.

Flush payload uses comma-separated bookmark values.

## Failure and Recovery

- Failed ack send returns error from `FlushAcks()`/`Ack(...)`.
- On disconnect, pending ack batches are cleared.
- Auto-ack should be combined with retry/failover strategy where queue processing guarantees are required.

## Operational Tuning

- Lower batch size: lower latency, higher ack traffic.
- Higher batch size: lower traffic, increased replay window risk.
- Timeout bounds worst-case ack delay.

## Related

- [Bookmarks and Replay](bookmarks_and_replay.md)
- [Operational Playbook](operational_playbook.md)
- [Reference: Client](reference_client.md)
