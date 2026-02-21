# Reference: Client

## Constructors and Identity

| Symbol | Purpose | Required State | Side Effects / Errors |
|---|---|---|---|
| `NewClient(clientName ...string)` | Construct client instance | None | Initializes routing and parity state.
| `ClientName()` | Get client name | Any | None.
| `SetClientName(name)` | Set client name | Any | Mutates identity field.
| `SetName(name)` / `Name()` | C++ parity alias pair | Any | Same behavior as client-name APIs.

## Connection and Session

| Symbol | Purpose | Required State | Side Effects / Errors |
|---|---|---|---|
| `Connect(uri)` | Open connection | Constructed/disconnected | Returns connection/URI errors.
| `Logon(optionalParams...)` | Start authenticated session | Connected | Returns auth or timeout errors.
| `Disconnect()` | Disconnect and stop routes | Any | Resets connection state; may return `DisconnectedError` when already disconnected.
| `Close()` | Alias to disconnect | Any | Same as `Disconnect`.
| `SetTLSConfig(config)` | Configure TLS parameters | Before `Connect` | Misconfiguration fails on connect.
| `SetHeartbeat(interval, timeout...)` | Configure heartbeat behavior | Connected/logged on | Depends on endpoint heartbeat support.
| `ServerVersion()` | Get server version from logon ack | Logged on recommended | Empty if unavailable.
| `URI()` | Get configured URI | Any | None.
| `GetConnectionInfo()` / `GatherConnectionInfo()` | Best-effort connection metadata | Any | Values depend on current transport state.
| `String()` | Debug summary | Any | Derived from connection info.

## Command Execution

| Symbol | Purpose | Required State | Side Effects / Errors |
|---|---|---|---|
| `Execute(command)` | Synchronous execution | Connected | Returns `MessageStream` and route/ack failures.
| `ExecuteAsync(command, handler)` | Async execution | Connected (or retry-enabled for eligible commands) | Returns route ID or command error.
| `ExecuteAsyncNoResubscribe(command, handler)` | Async execution excluded from resubscribe tracking | Connected | Route excluded from subscription manager replay.
| `Flush()` | Send flush command | Connected | Requires processed ack path.
| `PublishFlush(timeout...)` | Wait for publish-store drain or issue flush | Connected | Timeout returns `TimedOutError` when store does not drain.

## Publish and Mutations

| Symbol | Purpose | Required State | Side Effects / Errors |
|---|---|---|---|
| `Publish(topic, data, expiration...)` | Publish string payload | Connected/logged on | Failures can trigger `FailedWriteHandler`.
| `PublishBytes(topic, data, expiration...)` | Publish byte payload | Connected/logged on | Same as publish.
| `DeltaPublish(topic, data, expiration...)` | Delta publish string payload | Connected/logged on | Endpoint must support delta behavior.
| `DeltaPublishBytes(topic, data, expiration...)` | Delta publish byte payload | Connected/logged on | Same delta constraints.
| `SowDelete(topic, filter)` | Delete by filter with stats | Connected/logged on | Returns stats ack or mapped error.
| `SowDeleteByData(topic, data)` | Delete by payload key match | Connected/logged on | Stats ack behavior.
| `SowDeleteByKeys(topic, keys)` | Delete by SOW keys | Connected/logged on | Stats ack behavior.

## Subscriptions and Queries

| Symbol | Purpose | Required State | Side Effects / Errors |
|---|---|---|---|
| `Subscribe(topic, filter...)` / `SubscribeAsync(handler, topic, filter...)` | Stream subscribe | Connected/logged on | Returns sub ID and route.
| `DeltaSubscribe(...)` / `DeltaSubscribeAsync(...)` | Delta stream subscribe | Connected/logged on | Endpoint delta support required.
| `Sow(topic, filter...)` / `SowAsync(...)` | Snapshot query | Connected/logged on | Terminates on query completion.
| `SowAndSubscribe(...)` / `SowAndSubscribeAsync(...)` | Snapshot + live subscribe | Connected/logged on | Requires explicit unsubscribe.
| `SowAndDeltaSubscribe(...)` / `SowAndDeltaSubscribeAsync(...)` | Snapshot + live delta subscribe | Connected/logged on | Delta behavior depends on endpoint.
| `BookmarkSubscribe(topic, bookmark, filter...)` / `BookmarkSubscribeAsync(...)` | Bookmark-based subscribe | Connected/logged on | Bookmark semantics require endpoint support.
| `Unsubscribe(subID...)` | Unsubscribe by ID or all | Connected | `all` when omitted.

## Queue Ack Controls

| Symbol | Purpose | Required State | Side Effects / Errors |
|---|---|---|---|
| `Ack(topic, bookmark, subID...)` | Send explicit ack | Connected | Command error if topic/bookmark missing.
| `AckMessage(message)` | Ack from message fields | Connected | Requires bookmark-bearing message.
| `SetAutoAck(enabled)` / `AutoAck()` | Enable auto-ack | Any | Auto-ack uses queue message metadata.
| `SetAckBatchSize(size)` / `AckBatchSize()` | Batch threshold | Any | Size 0 coerced to 1.
| `SetAckTimeout(timeout)` / `AckTimeout()` | Timeout flush trigger | Any | Timer reset on each eligible message.
| `FlushAcks()` | Force flush pending ack batches | Connected | Returns first ack-send error.

## Retry, Replay, and Stores

| Symbol | Purpose | Required State | Side Effects / Errors |
|---|---|---|---|
| `SetRetryOnDisconnect(enabled)` / `RetryOnDisconnect()` | Retry eligible commands on reconnect | Any | Affects command queueing policy.
| `SetPublishStore(store)` / `PublishStore()` | Configure publish replay persistence | Any | Store errors surface during send/replay.
| `SetBookmarkStore(store)` / `BookmarkStore()` | Configure bookmark dedupe/resume | Any | Influences duplicate routing.
| `SetSubscriptionManager(manager)` / `SubscriptionManager()` | Configure resubscribe manager | Any | Used during post-logon recovery.
| `SetDefaultMaxDepth(depth)` / `DefaultMaxDepth()` | Default stream queue depth | Any | Applied to newly created streams.

## Routing, Error, and Transport Hooks

| Symbol | Purpose | Required State | Side Effects / Errors |
|---|---|---|---|
| `ErrorHandler()` / `SetErrorHandler(handler)` | General error hook | Any | Handles connection/protocol/message errors.
| `DisconnectHandler()` / `SetDisconnectHandler(handler)` | External disconnect callback | Any | Called on unintentional disconnect path.
| `SetDuplicateMessageHandler(handler)` / `DuplicateMessageHandler()` | Duplicate-message hook | Any | Invoked when bookmark store marks duplicate.
| `SetUnhandledMessageHandler(handler)` | Unhandled-message hook | Any | Fallback layer before last chance.
| `SetLastChanceMessageHandler(handler)` | Final fallback hook | Any | Invoked after unhandled hook.
| `SetGlobalCommandTypeMessageHandler(commandType, handler)` | Global per-command hook | Any | Runs after route handler.
| `SetFailedWriteHandler(handler)` / `FailedWriteHandler()` | Outbound publish failure hook | Any | Triggered on failed publish writes and negative acks.
| `SetExceptionListener(listener)` / `ExceptionListener()` | Internal exception hook | Any | Receives panics/errors in listener/filter callbacks.
| `AddConnectionStateListener(listener)` | Register state listener | Any | Listener keyed by identity.
| `RemoveConnectionStateListener(listener)` | Remove state listener | Any | No-op if missing.
| `ClearConnectionStateListeners()` | Remove all listeners | Any | Clears listener map.
| `AddHTTPPreflightHeader(header)` | Append preflight header | Any | Used by HTTP upgrade transports.
| `SetHTTPPreflightHeaders(headers)` | Replace preflight headers | Any | Empty/blank headers filtered out.
| `ClearHTTPPreflightHeaders()` | Clear preflight headers | Any | No-op when empty.
| `RawConnection()` | Access underlying `net.Conn` | Connected recommended | Diagnostic/advanced use.
| `SetTransportFilter(filter)` | Register frame filter | Any | Must preserve valid framing.
| `SetReceiveRoutineStartedCallback(callback)` | Receive-loop startup callback | Any | Exceptions routed to exception listener.

## Related

- [Client Entrypoints](client_entrypoints.md)
- [Pub/Sub and SOW](pub_sub_and_sow.md)
- [Queue Ack Semantics](queue_ack_semantics.md)
- [Transport and Hooks](transport_and_hooks.md)
