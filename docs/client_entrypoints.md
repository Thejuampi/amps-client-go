# Client Entrypoints

## Scope

This document covers the connection/session control surface on `Client` and the command execution entrypoints used by higher-level workflows.

## State Model and Preconditions

Lifecycle states:

- Constructed: `NewClient(...)` completed.
- Connected: `Connect(...)` completed.
- Logged on: `Logon(...)` completed.
- Disconnected: `Disconnect()`/`Close()` called or transport failed.

Required state assumptions:

- `Connect` requires a constructed client with a valid URI.
- `Logon` requires connected state.
- Publish/query/subscribe APIs generally require logged-on state.
- `Disconnect`/`Close` are valid from any state.

## Core Entrypoint Methods

| Method | Required State | Purpose | Notes |
|---|---|---|---|
| `NewClient(clientName ...string)` | None | Construct client instance | Initializes routing and parity state.
| `Connect(uri string)` | Constructed | Open transport connection | Persists URI for connection info and reconnect.
| `Logon(optionalParams ...LogonParams)` | Connected | Authenticate session | Supports timeout, auth, and correlation options.
| `Disconnect()` / `Close()` | Any | Stop transport and message routes | Emits disconnect path and closes socket resources.
| `ClientName()` / `SetClientName(...)` | Any | Manage client identity | C++ parity aliases: `Name` / `SetName`.
| `SetLogonCorrelationID(...)` | Before `Logon` | Set logon correlation metadata | Alias parity: `SetLogonCorrelationData`.
| `SetHeartbeat(interval, timeout)` | Connected/logged on | Configure heartbeat policy | Effective only when endpoint heartbeat behavior is enabled.

## Command Execution Entrypoints

| Method | Required State | Purpose | Return |
|---|---|---|---|
| `Execute(command)` | Connected | Sync command execution | `*MessageStream`
| `ExecuteAsync(command, handler)` | Connected | Async command with callback routing | route ID
| `ExecuteAsyncNoResubscribe(...)` | Connected | Async command excluded from resubscribe tracking | route ID

Execution semantics:

- Route registration occurs before send for APIs that expect acknowledgements.
- Command failure acks are surfaced as returned errors for sync flows.
- Async flows dispatch callback errors through configured error/exception paths.

## Expected Acks and Events

- Session setup: `Connect` + `Logon` yields connection and logon acks.
- Sync command APIs use processed/completed ack gates when required by command type.
- Disconnect path broadcasts connection state transitions to registered listeners.

## Failure and Recovery

Common error kinds:

- `ConnectionError`
- `DisconnectedError`
- `AuthenticationError`
- `CommandError`
- `TimedOutError`

Recovery checklist:

1. Confirm endpoint URI and protocol path.
2. Reconnect and logon.
3. Validate `GetConnectionInfo()` output and server version.
4. Re-run the failed command path with explicit timeout and handler instrumentation.

## Example: Connect, Logon, and Execute

```go
client := amps.NewClient("entrypoint-example")
if err := client.Connect("tcp://localhost:9000/amps/json"); err != nil {
	panic(err)
}
defer client.Close()

if err := client.Logon(); err != nil {
	panic(err)
}

stream, err := client.Execute(amps.NewCommand("flush"))
if err != nil {
	panic(err)
}
defer stream.Close()
```

## Observability and Introspection

- `ServerVersion()`
- `URI()`
- `GetConnectionInfo()` / `GatherConnectionInfo()`
- `String()`

## Related

- [Pub/Sub and SOW](pub_sub_and_sow.md)
- [Transport and Hooks](transport_and_hooks.md)
- [Reference: Client](reference_client.md)
