# Transport and Hooks

## Scope

Covers low-level hooks for transport, message routing fallback, and connection-state observation.

## Transport Hooks

| API | Purpose |
|---|---|
| `RawConnection()` | Access underlying `net.Conn`.
| `SetTransportFilter(filter)` | Intercept inbound/outbound frames.
| `SetReceiveRoutineStartedCallback(callback)` | Callback when receive loop starts.

Transport filter contract:

- Direction is `TransportFilterInbound` or `TransportFilterOutbound`.
- Filter may return modified payload.
- Invalid payload can break framing; keep frame header semantics intact.

## Routing Hooks

| API | Purpose |
|---|---|
| `SetGlobalCommandTypeMessageHandler(cmd, handler)` | Global post-route handler per command type.
| `SetDuplicateMessageHandler(handler)` | Called for bookmark-identified duplicates.
| `SetUnhandledMessageHandler(handler)` | Called when no previous layer handled message.
| `SetLastChanceMessageHandler(handler)` | Final fallback handler.

## Error and Exception Hooks

| API | Purpose |
|---|---|
| `SetErrorHandler(handler)` | General error path.
| `SetExceptionListener(listener)` | Internal exception surfacing.
| `SetFailedWriteHandler(handler)` | Failed publish write reporting.

## Connection-State Observability

| API | Purpose |
|---|---|
| `AddConnectionStateListener(listener)` | Register state listener.
| `RemoveConnectionStateListener(listener)` | Remove listener.
| `ClearConnectionStateListeners()` | Remove all listeners.

State enum is defined in [Reference: Types and Handlers](reference_types_and_handlers.md).

## HTTP Preflight Headers

| API | Purpose |
|---|---|
| `AddHTTPPreflightHeader(header)` | Append preflight header.
| `SetHTTPPreflightHeaders(headers)` | Replace preflight header set.
| `ClearHTTPPreflightHeaders()` | Clear preflight headers.

## Related

- [Client Entrypoints](client_entrypoints.md)
- [Reference: Client](reference_client.md)
