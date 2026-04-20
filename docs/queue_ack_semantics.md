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
| `Ack(topic, bookmark, subID...)` | Topic + bookmark | Sends `ack` command immediately. |
| `AckMessage(message)` | Message containing topic/bookmark | Extracts and sends ack. |
| `FlushAcks()` | None | Flushes buffered auto-acks by topic/subID batch key. |

## Queue Backlog Controls

Queue consumers that want more than one in-flight message should request a backlog explicitly.

| API | Purpose |
|---|---|
| `SetMaxBacklog(n)` | Set `max_backlog=n` on a command. |
| `QueueSetMaxBacklog(topic, n)` | Existing helper that sends a queue subscribe with backlog. |
| `SubscribeWithMaxBacklog(...)` / `SubscribeAsyncWithMaxBacklog(...)` | Preferred convenience APIs for backlog-aware queue subscriptions. |

Notes:

- `SetMaxBacklog(0)` preserves an explicit `max_backlog=0` request.
- The server may grant a smaller effective backlog than requested.
- `max_backlog` is meaningful for queue-like subscriptions, not ordinary pub/sub topics.

## Auto-Ack Controls

| Method | Purpose | Default |
|---|---|---|
| `SetAutoAck(bool)` / `AutoAck()` | Enable or disable auto-ack engine | Disabled. |
| `SetAckBatchSize(uint)` / `AckBatchSize()` | Batch threshold before flush | `1`. |
| `SetAckTimeout(time.Duration)` / `AckTimeout()` | Timer-based flush | `1s`. |

## Batching Model

Batches are keyed by `(topic, subID)`.

Flush triggers:

- Batch-size threshold reached.
- Timeout elapsed.
- Explicit `FlushAcks()` call.

Flush payload uses comma-separated bookmark values.

## Expected Ack/Event Sequence

1. Queue message arrives with bookmark metadata.
2. Auto-ack logic buffers bookmark by `(topic, subID)` if enabled.
3. Flush trigger sends `ack` command with batched bookmarks.
4. Ack command result is surfaced through normal command-ack handling.

## Failure and Recovery

- Failed ack send returns error from `FlushAcks()` or `Ack(...)`.
- On disconnect, pending ack batches are cleared.
- Auto-ack should be combined with retry or failover strategy where queue processing guarantees are required.

Recovery path:

1. Reconnect and logon.
2. Re-establish queue subscription.
3. Confirm auto-ack and backlog controls are still configured.
4. Flush pending acknowledgements explicitly where required by runbook.

## Operational Tuning

- Lower batch size: lower latency, higher ack traffic.
- Higher batch size: lower traffic, increased replay window risk.
- Timeout bounds worst-case ack delay.

## Example: Auto-Ack by Batch Size and Timeout

```go
client.SetAutoAck(true).
	SetAckBatchSize(50).
	SetAckTimeout(500 * time.Millisecond)

_, err := client.SubscribeAsyncWithMaxBacklog(func(msg *amps.Message) error {
	// process queue item; ack is deferred to auto-ack policy
	return nil
}, "queue://orders", 8)
if err != nil {
	panic(err)
}

if err := client.FlushAcks(); err != nil {
	panic(err)
}
```

## Related

- [Bookmarks and Replay](bookmarks_and_replay.md)
- [Operational Playbook](operational_playbook.md)
- [Reference: Client](reference_client.md)
