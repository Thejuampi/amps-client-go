# C++ 5.3.5.1 -> Go Parity Matrix (`Client` / `HAClient`)

## Client

| C++ Concept | Go Equivalent |
|---|---|
| `SetName` / `Name` | `Client.SetName` / `Client.Name` |
| `SetLogonCorrelationData` / getter | `Client.SetLogonCorrelationData` / `Client.LogonCorrelationData` |
| `URI` | `Client.URI` |
| `GetConnectionInfo` / `GatherConnectionInfo` | `Client.GetConnectionInfo` / `Client.GatherConnectionInfo` |
| Bookmark subscribe | `Client.BookmarkSubscribe`, `Client.BookmarkSubscribeAsync` |
| Queue ack APIs | `Client.Ack`, `Client.AckMessage`, `Client.FlushAcks`, `Client.SetAutoAck`, `Client.SetAckBatchSize`, `Client.SetAckTimeout` |
| Retry on disconnect | `Client.SetRetryOnDisconnect`, `Client.RetryOnDisconnect` |
| Default stream depth | `Client.SetDefaultMaxDepth`, `Client.DefaultMaxDepth` |
| Publish flush | `Client.PublishFlush` |
| Timer commands | `Client.StartTimer`, `Client.StopTimer` |
| Async no-resubscribe | `Client.ExecuteAsyncNoResubscribe` |
| Bookmark store hooks | `Client.SetBookmarkStore`, `Client.BookmarkStore` |
| Publish store hooks | `Client.SetPublishStore`, `Client.PublishStore` |
| Subscription manager hooks | `Client.SetSubscriptionManager`, `Client.SubscriptionManager` |
| Duplicate handler | `Client.SetDuplicateMessageHandler`, `Client.DuplicateMessageHandler` |
| Failed write handler | `Client.SetFailedWriteHandler`, `Client.FailedWriteHandler` |
| Exception listener | `Client.SetExceptionListener`, `Client.ExceptionListener` |
| Unhandled / last chance handlers | `Client.SetUnhandledMessageHandler`, `Client.SetLastChanceMessageHandler` |
| Global command handlers | `Client.SetGlobalCommandTypeMessageHandler` |
| Connection state listeners | `AddConnectionStateListener`, `RemoveConnectionStateListener`, `ClearConnectionStateListeners` |
| HTTP preflight headers | `AddHTTPPreflightHeader`, `SetHTTPPreflightHeaders`, `ClearHTTPPreflightHeaders` |
| Raw connection handle | `Client.RawConnection` |
| Transport filter | `Client.SetTransportFilter` |
| Receive-thread-start callback | `Client.SetReceiveRoutineStartedCallback` |

## HAClient

| C++ Concept | Go Equivalent |
|---|---|
| Constructor | `NewHAClient` |
| Connect + logon loop | `HAClient.ConnectAndLogon` |
| Disconnected state | `HAClient.Disconnected` |
| Timeout | `HAClient.SetTimeout`, `HAClient.Timeout` |
| Reconnect delay | `HAClient.SetReconnectDelay`, `HAClient.ReconnectDelay` |
| Reconnect strategy | `HAClient.SetReconnectDelayStrategy`, `HAClient.ReconnectDelayStrategy` |
| Logon options | `HAClient.SetLogonOptions`, `HAClient.LogonOptions` |
| Server chooser | `HAClient.SetServerChooser`, `HAClient.ServerChooser` |
| Connection info | `HAClient.GetConnectionInfo`, `HAClient.GatherConnectionInfo` |
| Memory-backed constructor | `CreateMemoryBackedHAClient` |
| File-backed constructor | `CreateFileBackedHAClient` |
| Disconnect handler override | `HAClient.SetDisconnectHandler` returns usage error by design |

## Go-Native Replacements for C++ Low-Level Handles

- `Client.RawConnection() net.Conn`
- `Client.SetTransportFilter(...)`
- `Client.SetReceiveRoutineStartedCallback(...)`
