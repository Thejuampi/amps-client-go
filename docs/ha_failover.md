# HA Failover

## Scope

Covers `HAClient` behavior for reconnect, logon retry loop, chooser strategy, and recovery.

## Construction

| API | Purpose |
|---|---|
| `NewHAClient(...)` | Create HA wrapper around `Client`.
| `CreateMemoryBackedHAClient(...)` | HA with memory publish/bookmark stores.
| `CreateFileBackedHAClient(...)` | HA with file-backed publish/bookmark stores.

## Core Lifecycle

| Method | Behavior |
|---|---|
| `ConnectAndLogon()` | Repeatedly selects URI, connects, logs on, applies delay strategy between failures.
| `Disconnect()` | Stops reconnect loop and disconnects wrapped client.
| `Disconnected()` | Reports wrapped client connection state.

## Reconnect Controls

| Method | Notes |
|---|---|
| `SetTimeout(...)` / `Timeout()` | Global deadline for connect/logon retry loop.
| `SetReconnectDelay(...)` / `ReconnectDelay()` | Fixed-delay policy.
| `SetReconnectDelayStrategy(...)` / `ReconnectDelayStrategy()` | Strategy-based delay policy.
| `SetServerChooser(...)` / `ServerChooser()` | URI selection policy and failure reporting.
| `SetLogonOptions(...)` / `LogonOptions()` | Logon params reused for retries.

## Recovery Model

After reconnect and logon:

- Publish replay executes when publish store exists.
- Resubscribe executes via subscription manager.
- Connection-state listeners receive state events.

## Failure Behavior

- URI selection failure returns a `ConnectionError`.
- Retry loop termination obeys `SetTimeout` when configured.
- Delay strategy errors abort reconnect loop immediately.
- Per-attempt failures are reported to `ServerChooser.ReportFailure(...)`.

## Constraints

`HAClient.SetDisconnectHandler(...)` intentionally returns usage error. Disconnect behavior is HA-managed.

## Failure Triage

Inspect:

- chooser error (`ServerChooser().Error()`)
- `GetConnectionInfo()` output
- reconnect strategy parameters
- store state and replay depth

## Example: HA Setup with Delay Strategy

```go
ha := amps.NewHAClient("ha-example")
ha.SetServerChooser(
	amps.NewDefaultServerChooser(
		"tcp://amps-a:9000/amps/json",
		"tcp://amps-b:9000/amps/json",
	),
).SetReconnectDelayStrategy(
	amps.NewExponentialDelayStrategy(200*time.Millisecond, 5*time.Second, 2.0),
).SetTimeout(30 * time.Second)

if err := ha.ConnectAndLogon(); err != nil {
	panic(err)
}
defer ha.Disconnect()
```

## Related

- [Operational Playbook](operational_playbook.md)
- [Testing and Validation](testing_and_validation.md)
- [Reference: HAClient](reference_ha_client.md)
