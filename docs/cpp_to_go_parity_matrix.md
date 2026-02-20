# C++ 5.3.5.1 -> Go Parity Matrix (`Client` / `HAClient`)

Verification markers:

- `Verified (Unit)` = covered by deterministic unit tests.
- `Verified (Integration)` = covered by AMPS endpoint integration tests (env-gated).

## Client

| C++ Concept | Go Equivalent | Verification |
|---|---|---|
| `SetName` / `Name` | `Client.SetName` / `Client.Name` | Verified (Unit) |
| `SetLogonCorrelationData` / getter | `Client.SetLogonCorrelationData` / `Client.LogonCorrelationData` | Verified (Unit) |
| `URI` | `Client.URI` | Verified (Unit) |
| `GetConnectionInfo` / `GatherConnectionInfo` | `Client.GetConnectionInfo` / `Client.GatherConnectionInfo` | Verified (Unit) |
| Bookmark subscribe | `Client.BookmarkSubscribe`, `Client.BookmarkSubscribeAsync` | Verified (Integration) |
| Queue ack APIs | `Client.Ack`, `Client.AckMessage`, `Client.FlushAcks`, `Client.SetAutoAck`, `Client.SetAckBatchSize`, `Client.SetAckTimeout` | Verified (Unit + Integration) |
| Retry on disconnect | `Client.SetRetryOnDisconnect`, `Client.RetryOnDisconnect` | Verified (Unit) |
| Default stream depth | `Client.SetDefaultMaxDepth`, `Client.DefaultMaxDepth` | Verified (Unit) |
| Publish flush | `Client.PublishFlush` | Verified (Unit) |
| Timer commands | `Client.StartTimer`, `Client.StopTimer` | Verified (Unit + Integration) |
| Async no-resubscribe | `Client.ExecuteAsyncNoResubscribe` | Verified (Unit) |
| Bookmark store hooks | `Client.SetBookmarkStore`, `Client.BookmarkStore` | Verified (Unit) |
| Publish store hooks | `Client.SetPublishStore`, `Client.PublishStore` | Verified (Unit) |
| Subscription manager hooks | `Client.SetSubscriptionManager`, `Client.SubscriptionManager` | Verified (Unit) |
| Duplicate handler | `Client.SetDuplicateMessageHandler`, `Client.DuplicateMessageHandler` | Verified (Unit) |
| Failed write handler | `Client.SetFailedWriteHandler`, `Client.FailedWriteHandler` | Verified (Unit) |
| Exception listener | `Client.SetExceptionListener`, `Client.ExceptionListener` | Verified (Unit) |
| Unhandled / last chance handlers | `Client.SetUnhandledMessageHandler`, `Client.SetLastChanceMessageHandler` | Verified (Unit) |
| Global command handlers | `Client.SetGlobalCommandTypeMessageHandler` | Verified (Unit) |
| Connection state listeners | `AddConnectionStateListener`, `RemoveConnectionStateListener`, `ClearConnectionStateListeners` | Verified (Unit + Integration) |
| HTTP preflight headers | `AddHTTPPreflightHeader`, `SetHTTPPreflightHeaders`, `ClearHTTPPreflightHeaders` | Verified (Unit) |
| Raw connection handle | `Client.RawConnection` | Verified (Unit) |
| Transport filter | `Client.SetTransportFilter` | Verified (Unit + Integration) |
| Receive-thread-start callback | `Client.SetReceiveRoutineStartedCallback` | Verified (Unit) |
| Handler ordering (route -> global -> duplicate -> fallback chain) | `Client.onMessage` routing behavior | Verified (Unit) |

## HAClient

| C++ Concept | Go Equivalent | Verification |
|---|---|---|
| Constructor | `NewHAClient` | Verified (Unit) |
| Connect + logon loop | `HAClient.ConnectAndLogon` | Verified (Integration) |
| Disconnected state | `HAClient.Disconnected` | Verified (Unit + Integration) |
| Timeout | `HAClient.SetTimeout`, `HAClient.Timeout` | Verified (Unit) |
| Reconnect delay | `HAClient.SetReconnectDelay`, `HAClient.ReconnectDelay` | Verified (Unit) |
| Reconnect strategy | `HAClient.SetReconnectDelayStrategy`, `HAClient.ReconnectDelayStrategy` | Verified (Unit) |
| Logon options | `HAClient.SetLogonOptions`, `HAClient.LogonOptions` | Verified (Unit) |
| Server chooser | `HAClient.SetServerChooser`, `HAClient.ServerChooser` | Verified (Unit + Integration) |
| Connection info | `HAClient.GetConnectionInfo`, `HAClient.GatherConnectionInfo` | Verified (Unit + Integration) |
| Memory-backed constructor | `CreateMemoryBackedHAClient` | Verified (Unit) |
| File-backed constructor | `CreateFileBackedHAClient` | Verified (Unit) |
| Disconnect handler override | `HAClient.SetDisconnectHandler` returns usage error by design | Verified (Unit) |

## Go-Native Replacements for C++ Low-Level Handles

- `Client.RawConnection() net.Conn`
- `Client.SetTransportFilter(...)`
- `Client.SetReceiveRoutineStartedCallback(...)`

## Integration Coverage (Env-Gated)

- `TestIntegrationConnectLogonPublishSubscribe`
- `TestIntegrationSOWAndSowAndSubscribeLifecycle`
- `TestIntegrationQueueAutoAckBatching`
- `TestIntegrationBookmarkResumeAcrossReconnect`
- `TestIntegrationHAConnectAndLogonWithFailoverChooser`
- `TestIntegrationStartStopTimerCommandPath`
- `TestIntegrationReconnectDuringInFlightOperation`
