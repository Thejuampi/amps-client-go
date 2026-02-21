# Reference: HAClient

## Constructors

| Symbol | Purpose | Required State | Notes |
|---|---|---|---|
| `NewHAClient(clientName ...string)` | Create HA wrapper | None | Enables retry-on-disconnect on wrapped client.
| `CreateMemoryBackedHAClient(clientName ...string)` | HA with in-memory stores | None | Configures publish and bookmark memory stores.
| `CreateFileBackedHAClient(args ...string)` | HA with file-backed stores | None | Args: publish-store path, bookmark-store path, client name.

## Lifecycle and Connectivity

| Symbol | Purpose | Required State | Side Effects / Errors |
|---|---|---|---|
| `ConnectAndLogon()` | Connect/logon loop with chooser and delay strategy | Constructed/disconnected | Returns terminal error on timeout/stop conditions.
| `Disconnect()` | Stop reconnect loop and disconnect client | Any | Stops HA reconnect behavior.
| `Disconnected()` | Report disconnected status | Any | True if wrapped client not connected.
| `Client()` | Access wrapped client | Any | Use for advanced hooks and client methods.

## Reconnect and Selection Controls

| Symbol | Purpose | Notes |
|---|---|---|
| `SetTimeout(timeout)` / `Timeout()` | Configure connect/logon deadline | `0` means no deadline.
| `SetReconnectDelay(delay)` / `ReconnectDelay()` | Configure fixed delay | Also sets fixed delay strategy.
| `SetReconnectDelayStrategy(strategy)` / `ReconnectDelayStrategy()` | Configure strategy-driven delay | Strategy reset on successful connect/logon.
| `SetServerChooser(chooser)` / `ServerChooser()` | Configure URI chooser policy | `nil` chooser replaced by default chooser.
| `SetLogonOptions(options)` / `LogonOptions()` | Configure logon options for retries | Authenticator from chooser can override per-attempt.

## Connection Metadata

| Symbol | Purpose |
|---|---|
| `GetConnectionInfo()` | Read wrapped client connection metadata.
| `GatherConnectionInfo()` | Alias for `GetConnectionInfo()`.

## Constraints

| Symbol | Behavior |
|---|---|
| `SetDisconnectHandler(handler)` | Always returns usage error by design; HA controls reconnect handling internally.

## Related

- [HA Failover](ha_failover.md)
- [Reference: Types and Handlers](reference_types_and_handlers.md)
