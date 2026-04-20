# Transport and Hooks

## Scope

Covers low-level hooks for transport, message routing fallback, and connection-state observation.

## Transport Hooks

| API | Purpose |
|---|---|
| `RawConnection()` | Access underlying `net.Conn`. |
| `SetCompression(enabled)` | Configure default zlib transport compression for `tcp`/`tcps`. |
| `SetTransportFilter(filter)` | Intercept inbound or outbound frames. |
| `SetReceiveRoutineStartedCallback(callback)` | Callback when receive loop starts. |

Compression contract:

- Supported value: `compression=zlib`.
- Supported schemes: `tcp://` and `tcps://`.
- Unsupported schemes: `ws://`, `wss://`, `unix://`.
- `SetCompression(true)` is a client default. `?compression=zlib` on the URI takes precedence.
- Transport filters operate on normal AMPS frames after decompression on inbound and before compression on outbound.

Transport filter contract:

- Direction is `TransportFilterInbound` or `TransportFilterOutbound`.
- Filter may return modified payload.
- Invalid payload can break framing; keep frame header semantics intact.
- Callback exceptions are routed through `ExceptionListener` when configured.

## Routing Hooks

| API | Purpose |
|---|---|
| `SetGlobalCommandTypeMessageHandler(cmd, handler)` | Global post-route handler per command type. |
| `SetDuplicateMessageHandler(handler)` | Called for bookmark-identified duplicates. |
| `SetUnhandledMessageHandler(handler)` | Called when no previous layer handled message. |
| `SetLastChanceMessageHandler(handler)` | Final fallback handler. |

Dispatch sequence:

1. Route-specific handler.
2. Global command-type handler.
3. Duplicate handler.
4. Unhandled handler.
5. Last-chance handler.

## Error and Exception Hooks

| API | Purpose |
|---|---|
| `SetErrorHandler(handler)` | General error path. |
| `SetExceptionListener(listener)` | Internal exception surfacing. |
| `SetFailedWriteHandler(handler)` | Failed publish write reporting. |

## Connection-State Observability

| API | Purpose |
|---|---|
| `AddConnectionStateListener(listener)` | Register state listener. |
| `RemoveConnectionStateListener(listener)` | Remove listener. |
| `ClearConnectionStateListeners()` | Remove all listeners. |

State enum is defined in [Reference: Types and Handlers](reference_types_and_handlers.md).

## HTTP Preflight Headers

| API | Purpose |
|---|---|
| `AddHTTPPreflightHeader(header)` | Append preflight header. |
| `SetHTTPPreflightHeaders(headers)` | Replace preflight header set. |
| `ClearHTTPPreflightHeaders()` | Clear preflight headers. |

## Example: Compression, Filter, and State Listener

```go
client.SetCompression(true).
	SetTransportFilter(func(direction amps.TransportFilterDirection, payload []byte) []byte {
		// preserve framing; optionally inspect bytes
		return payload
	}).
	SetReceiveRoutineStartedCallback(func() {
		// record receive loop startup
	})

client.AddConnectionStateListener(amps.ConnectionStateListenerFunc(func(state amps.ConnectionState) {
	// emit metrics or logs
	_ = state
}))
```

## Related

- [Client Entrypoints](client_entrypoints.md)
- [Reference: Client](reference_client.md)
