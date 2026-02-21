# Client Entrypoints

## Client State Model

Relevant states for API usage:

- Constructed: `NewClient(...)` called.
- Connected: `Connect(...)` succeeded.
- Logged on: `Logon(...)` succeeded.
- Disconnected: `Disconnect()` or connection failure.

State assumptions:

- Most command execution paths require connected state.
- Logon is required before normal data flows.

## Core Entrypoint Methods

| Method | Required State | Purpose | Notes |
|---|---|---|---|
| `NewClient(clientName ...string)` | None | Construct client | Assigns client identity.
| `Connect(uri string)` | Constructed | Open transport connection | URI persisted for connection info.
| `Logon(optionalParams ...LogonParams)` | Connected | Authenticate/log on | Supports timeout/authenticator/correlation.
| `Disconnect()` / `Close()` | Any | Stop transport and routes | Broadcasts disconnect states.
| `ClientName()` / `SetClientName(...)` | Any | Identity management | Alias parity available (`SetName`, `Name`).
| `SetLogonCorrelationID(...)` | Before `Logon` | Correlation metadata | Alias parity available.
| `SetHeartbeat(interval, timeout)` | Connected/logged on | Heartbeat setup | Endpoint support required.

## Command Entrypoints

| Method | Required State | Purpose | Return |
|---|---|---|---|
| `Execute(command)` | Connected | Sync command execution | `*MessageStream`
| `ExecuteAsync(command, handler)` | Connected | Async command execution | route ID
| `ExecuteAsyncNoResubscribe(...)` | Connected | Async command excluding resubscribe tracking | route ID

## Failure Model

Common error families:

- `ConnectionError`
- `DisconnectedError`
- `AuthenticationError`
- `CommandError`
- `TimedOutError`

Synchronous `Execute(...)` uses route-managed ack processing for operations that require processed/completed termination.

## Observability and State Introspection

- `ServerVersion()`
- `URI()`
- `GetConnectionInfo()` / `GatherConnectionInfo()`
- `String()`

## Related

- [Pub/Sub and SOW](pub_sub_and_sow.md)
- [Transport and Hooks](transport_and_hooks.md)
- [Reference: Client](reference_client.md)
