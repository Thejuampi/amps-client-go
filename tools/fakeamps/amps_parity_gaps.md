# fakeamps vs. 60east AMPS Parity Gap Analysis & Exhaustive List

Based on the recent review and enhancements (which added advanced filtering, OOF on mismatch, pause/resume, having clauses, and an admin API), this document provides an **exact and exhaustive list** of the remaining features required to achieve *full, 100% feature parity* with a production 60east AMPS message broker.

If the goal is to fully emulate AMPS for all possible client use cases, the following systems and protocol features must be implemented:

## 1. Storage & Persistence Engine
*   **Disk-Backed Transaction Log**: Real AMPS persists the message journal to disk (e.g., `journal.dat`) to survive restarts, allow point-in-time recovery, and support long-term bookmark replay. `fakeamps` only uses a memory ring buffer.
*   **Persistent SOW Topics**: AMPS supports SOW topics backed by disk (SOW files). `fakeamps` SOW is strictly RAM-only and lost on restart.
*   **Transaction Log Archives & Compression**: AMPS automatically rotates, archives, and compresses transaction logs.

## 2. Advanced High Availability (Replication)
*   **Sync & Catch-up Phases**: When a replica joins, AMPS performs a state transfer (sync) of missing transaction logs before going live. `fakeamps` only forwards new, live publishes.
*   **Synchronous Acknowledgment**: Support for [a:sync](file:///g:/dev/repos/amps-client-go/amps/client.go#1558-1566) or acknowledgment routing where a publish is only acked to the client after it is replicated to peer nodes.
*   **Replication Filters**: AMPS can replicate only specific topics or filtered subsets between distinct nodes.
*   **Distributed Loop Detection**: Complex mesh topology loop avoidance beyond a simple instance ID check.
*   **Message Expiration Sync**: SOW expirations synced perfectly across replicas.

## 3. Full AMPS Filter Language (XPath/SQL-like)
The current `fakeamps` engine supports common operators, but the full AMPS engine includes a highly optimized parser with:
*   **Math Operations**: `+`, `-`, `*`, `/`, `%`, `NaN`, `INF` handling.
*   **String Functions**: `BEGINS WITH`, `ENDS WITH`, `CONTAINS`, `INSTR()`, `SUBSTR()`, `UPPER()`, `LOWER()`, `LEN()`.
*   **Regex Variations**: Case insensitive regex mappings.
*   **Array Handling**: Evaluation over arrays within JSON documents (e.g., `[ANY] /items/price > 10`).

## 4. Complex Views & Joins
*   **Inner, Left, and Right Joins**: Real AMPS can join two or more SOW topics together into a virtual view topic. When either source topic updates, the join view is dynamically recomputed and OOFs/publishes are generated.
*   **Multi-Level Aggregations**: Complex grouped aggregations spanning multiple joined datasets.

## 5. Message Types & Parsers
`fakeamps` relies almost entirely on JSON or raw byte pass-through. Real AMPS deeply parses and understands multiple formats, allowing filtering and delta-merges on all of them natively:
*   `nvfix` (Non-Validating FIX)
*   `fix` (Strict FIX engine)
*   `xml` (XPath filtering support)
*   `bflat` (Binary Flat)
*   `protobuf` (Requires loading `.proto` schemas into the server)
*   `composite` (Messages with multiple distinct parts, e.g., JSON header + Binary payload)

## 6. Advanced Message Queues
*   **Message Acknowledgment (sow_delete for queues)**: Full tracking of delivered vs. unacknowledged messages.
*   **Message Leases and Timeouts**: Re-queuing messages if a client drops or fails to ack within the `lease_period`. `fakeamps` accepts queue subscriptions but doesn't strictly enforce lease timeouts and redelivery.
*   **Queue Push vs. Pull**: Supporting the `sow_and_subscribe` pull semantics for queues to get a specific batch of messages.

## 7. Advanced Conflation
*   **Sequence-based Conflation**: Skipping conflation for messages marked with specific flags or sequence intervals.
*   **Adaptive Conflation**: Dynamic rate matching based on network backpressure.

## 8. Pluggable Authentication & Entitlements
*   **PAM/LDAP/Kerberos Integration**: Dynamic auth via modules.
*   **Granular Entitlements**: AMPS allows permissions per-topic, per-command, AND per-filter (e.g., "User A can only see records where `/owner = 'UserA'`).
*   **Dynamic Re-authentication**: Dropping clients if their entitlements change dynamically during a session.

## 9. Historical Queries
*   **Time-Travel Queries**: Asking SOW to return the state of the world *as it existed* at a specific historical timestamp (supported by deep transaction log integration).

## 10. Protocol Nuances & Commands
*   **Compression**: AMPS protocol supports zlib continuous streaming compression (`c` flag) within the wire format.
*   **Chunking & Batching**: Support for chunked/batched publishing from clients.
*   **Record Level Options**: specific routing or TTL configurations injected directly into the message by the publisher rather than simply configured on the topic structure.
*   **Publish Store Integration**: Full support for client-side publish stores by echoing the correct sequence `s` and providing exactly-once semantics ([CommandID](file:///g:/dev/repos/amps-client-go/amps/command.go#268-272) deduping on reconnects).
*   **Command Boundaries**: `CommandGroupBegin` and `CommandGroupEnd` to execute multiple commands atomically.
*   **Heartbeat Management**: The `heartbeat` command for connection liveness tracking from both client and server sides, including absence-detection timeouts to forcefully close dead connections.
*   **SowDelete Variants**: Deleting not just by filter (`sow_delete`) but specifically optimized commands like [SowDeleteByData](file:///g:/dev/repos/amps-client-go/amps/client.go#1638-1670) (providing a payload for AMPS to infer keys) and [SowDeleteByKeys](file:///g:/dev/repos/amps-client-go/amps/client.go#1671-1703) (comma separated list of keys).

## 11. Advanced HA Client Interactions
*   **ServerChooser**: The HA Client relies on a sophisticated [ServerChooser](file:///g:/dev/repos/amps-client-go/amps/ha_client.go#320-329) to orchestrate failover behavior to different URIs, load balancing, and connection scoring. `fakeamps` clustering logic operates primarily on a hardcoded list of peers.
*   **ReconnectDelayStrategy**: Exponential backoff routing and reconnect strategies are essential for massive clusters encountering network blips. `fakeamps` lacks the server-side awareness to help clients gracefully negotiate these backoffs (e.g. through 'redirect' logon responses).
*   **Memory vs. File Backed HA Stores**: The client allows complex HA stores (for unacked publishes or bookmarks) stored in memory or memory-mapped files. `fakeamps` does not expose any admin endpoints to inspect or manage these stores or their synchronization guarantees.

---
**Summary for Planning:**
To achieve *exact* parity, the most monumental efforts required are the **Storage/Persistence engine** (1), the **Complex Joins / Materialized Views engine** (4), and the **Full Multi-format Parsers** (5), followed closely by filling in the long tail of **Lower-level Protocol Commands** (10) and perfectly mimicking the semantics that drive the **Advanced HA Client Mechanisms** (11).
