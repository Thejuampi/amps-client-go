# C++ 5.3.5.1 to Go Parity Matrix (`Client` / `HAClient`)

Verification markers:

- `Verified (Unit)` means deterministic unit-test coverage.
- `Verified (Integration)` means endpoint-backed integration coverage.

## Client

| C++ Concept | Go Equivalent | Verification | Details |
|---|---|---|---|
| Naming and identity | `SetName`, `Name`, `SetClientName`, `ClientName` | Verified (Unit) | [Client Entrypoints](client_entrypoints.md) |
| Logon correlation | `SetLogonCorrelationData`, `LogonCorrelationData` | Verified (Unit) | [Client Entrypoints](client_entrypoints.md) |
| URI and connection info | `URI`, `GetConnectionInfo`, `GatherConnectionInfo` | Verified (Unit) | [Client Entrypoints](client_entrypoints.md) |
| Bookmark subscribe | `BookmarkSubscribe`, `BookmarkSubscribeAsync` | Verified (Integration) | [Bookmarks and Replay](bookmarks_and_replay.md) |
| Queue ack APIs | `Ack`, `AckMessage`, `FlushAcks`, auto-ack controls | Verified (Unit + Integration) | [Queue Ack Semantics](queue_ack_semantics.md) |
| Retry on disconnect | `SetRetryOnDisconnect`, `RetryOnDisconnect` | Verified (Unit) | [Bookmarks and Replay](bookmarks_and_replay.md) |
| Stream depth defaults | `SetDefaultMaxDepth`, `DefaultMaxDepth` | Verified (Unit) | [Pub/Sub and SOW](pub_sub_and_sow.md) |
| Publish flush | `PublishFlush` | Verified (Unit) | [Stores and Persistence](stores_and_persistence.md) |
| Timer commands | `StartTimer`, `StopTimer` | Verified (Unit + Integration) | [Transport and Hooks](transport_and_hooks.md) |
| Async no-resubscribe | `ExecuteAsyncNoResubscribe` | Verified (Unit) | [Bookmarks and Replay](bookmarks_and_replay.md) |
| Bookmark and publish stores | `SetBookmarkStore`, `SetPublishStore` | Verified (Unit) | [Stores and Persistence](stores_and_persistence.md) |
| Subscription manager | `SetSubscriptionManager`, `SubscriptionManager` | Verified (Unit) | [HA Failover](ha_failover.md) |
| Routing hooks | duplicate/unhandled/last-chance/global handlers | Verified (Unit) | [Transport and Hooks](transport_and_hooks.md) |
| Connection state listeners | add/remove/clear listener APIs | Verified (Unit + Integration) | [Transport and Hooks](transport_and_hooks.md) |
| Transport-level hooks | raw connection, transport filter, receive callback | Verified (Unit + Integration) | [Transport and Hooks](transport_and_hooks.md) |

## HAClient

| C++ Concept | Go Equivalent | Verification | Details |
|---|---|---|---|
| Constructor | `NewHAClient` | Verified (Unit) | [HA Failover](ha_failover.md) |
| Connect and logon loop | `ConnectAndLogon` | Verified (Integration) | [HA Failover](ha_failover.md) |
| Disconnected state | `Disconnected` | Verified (Unit + Integration) | [HA Failover](ha_failover.md) |
| Timeout and reconnect delay | timeout and delay setters/getters | Verified (Unit) | [HA Failover](ha_failover.md) |
| Delay strategy | `SetReconnectDelayStrategy`, getter | Verified (Unit) | [HA Failover](ha_failover.md) |
| Logon options | `SetLogonOptions`, getter | Verified (Unit) | [HA Failover](ha_failover.md) |
| Server chooser | `SetServerChooser`, getter | Verified (Unit + Integration) | [HA Failover](ha_failover.md) |
| Connection info | `GetConnectionInfo`, `GatherConnectionInfo` | Verified (Unit + Integration) | [HA Failover](ha_failover.md) |
| Store-backed constructors | `CreateMemoryBackedHAClient`, `CreateFileBackedHAClient` | Verified (Unit) | [Stores and Persistence](stores_and_persistence.md) |
| Disconnect handler override restriction | `SetDisconnectHandler` usage error by design | Verified (Unit) | [HA Failover](ha_failover.md) |

## Related Reference

- [Supported Scope and Constraints](supported_scope.md)
- [Documentation Index](index.md)
- [Reference: Client](reference_client.md)
- [Reference: HAClient](reference_ha_client.md)
- [Reference: Types and Handlers](reference_types_and_handlers.md)
