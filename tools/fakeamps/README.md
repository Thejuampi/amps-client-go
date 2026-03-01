# fakeamps

`fakeamps` is a deterministic, stateful AMPS-protocol TCP responder for integration and performance testing of custom AMPS client implementations against the 60East AMPS wire protocol.

- Isolated under `tools/` — **not** part of the exported `amps-client-go` client API.
- Models real AMPS server behavior including stateful message journal, SOW cache, content filtering, topic wildcards, delta merge, queue topics, per-topic message types, views, actions, replication exercises, authentication, and administration.

## Architecture

Modeled after real 60East AMPS's multi-threaded design ("an army of threads"):

```
┌──────────────────────────────────────────────────────────────────────┐
│                         fakeamps server                              │
│                                                                      │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────┐  │
│  │ Message Journal │  │   SOW Cache    │  │   Topic Configs        │  │
│  │ (bounded ring   │  │ (topic→key→rec │  │ (topic→message_type)   │  │
│  │  buffer for     │  │  with filter,  │  │ (json, protobuf,      │  │
│  │  bookmark       │  │  top_n, delta  │  │  nvfix, fix, xml,     │  │
│  │  replay)        │  │  merge, TTL,   │  │  binary)              │  │
│  │                 │  │  eviction)     │  │                        │  │
│  └────────────────┘  └────────────────┘  └────────────────────────┘  │
│           ▲                  ▲                     ▲                  │
│  ┌────────┴──────────────────┴─────────────────────┴──────────────┐  │
│  │                    Per-Connection (× N)                         │  │
│  │  ┌──────────────┐   channel   ┌──────────────┐                  │  │
│  │  │    Reader     │────────────▶│    Writer     │──▶ TCP out       │  │
│  │  │  goroutine    │   (async    │  goroutine    │  (write          │  │
│  │  │  (parse,      │    queue)   │  (coalesce,   │   coalescing)    │  │
│  │  │   dispatch,   │            │   flush)      │                  │  │
│  │  │   filter)     │            └──────────────┘                  │  │
│  │  └──────────────┘                                                │  │
│  │       │                                                          │  │
│  │       ├── Fan-out: async enqueue to each subscriber's writer     │  │
│  │       ├── Content filter evaluation per subscriber               │  │
│  │       ├── OOF-on-filter-mismatch for delta subscribers           │  │
│  │       ├── Topic wildcard matching                                │  │
│  │       ├── Delta merge into SOW cache                             │  │
│  │       └── Heartbeat liveness watchdog                            │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────┐  │
│  │   Admin API     │  │   Views        │  │   Actions              │  │
│  │ (REST on        │  │ (passthrough + │  │ (route, log,           │  │
│  │  separate port) │  │  aggregation)  │  │  transform)            │  │
│  └────────────────┘  └────────────────┘  └────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

## Stateful Features

### Message Journal (Transaction Log)
- Bounded ring-buffer log of all published messages
- Bookmark format: `epoch_us|publisher|seq|`
- `BOOKMARK_EPOCH ("0")` replays from the beginning
- Reconnect-resume: client reconnects, re-subscribes with last bookmark, gets only new messages
- Content filters applied during replay

### SOW Cache (State-of-the-World)
- Per-topic, per-sow-key storage of last-known record values
- SOW queries return real cached records with group_begin/group_end batching
- **Filtered queries**: content filter expressions evaluated against SOW data
- **top_n**: limit result count; `top_n=0` returns total count + topic_matches with no records (per AMPS spec)
- **orderBy**: sort SOW results by field (ascending and descending)
- **Delta merge**: `delta_publish` merges into existing SOW record (RFC 7396 JSON merge-patch) — recursive for nested objects
- **SOW delete**: by key, by sow_keys list, by filter expression, or by payload data (SowDeleteByData)
- **Expiration / TTL**: records with expiration are auto-expired on query + periodic GC sweep every 30s
- **OOF**: out-of-focus messages sent to delta subscribers on record removal AND on filter mismatch
- **Eviction policies**: configurable capacity-based eviction: none, LRU, oldest, capacity
- Record counts in completed acks: `records_returned`, `records_inserted`, `records_updated`, `records_deleted`
- Automatic sow_key extraction from JSON payloads (`_sow_key`, `id`, `key` fields)

### Per-Topic Message Types
- Each topic is bound to a message type (json, protobuf, nvfix, fix, xml, binary, etc.)
- Message type is set on first publish and echoed on all subsequent deliveries
- Default message type is `json`

### Content Filtering
- Filter expressions evaluated on subscribe fan-out, SOW queries, bookmark replay, and SOW delete
- Supported operators: `=`, `==`, `!=`, `>`, `<`, `>=`, `<=`
- **NOT** prefix: `NOT /field = value`
- **IN / NOT IN**: `/field IN ('a','b','c')`
- **IS NULL / IS NOT NULL**: `/field IS NULL`
- **BETWEEN**: `/field BETWEEN a AND b`
- **LIKE**: with `%` and `_` wildcards
- **Regex**: `/field ~ 'pattern'`
- **Nested paths**: `/parent/child = value`
- **Parenthesized groups**: `(expr) AND (expr)`
- **AND / OR** logical operators with precedence-aware parsing
- **Math operations**: `+`, `-`, `*`, `/`, `%`, `NaN`, `INF` in expressions
- **String functions**: `BEGINS WITH`, `ENDS WITH`, `CONTAINS`, `UPPER()`, `LOWER()`, `LEN()`, `INSTR()`, `SUBSTR()`
- **Array quantifiers**: `[ANY] /items/field ...` and `[ALL] /items/field ...`

### Topic Matching
- Exact match: `orders` matches only `orders`
- Dot-hierarchy wildcard: `orders.>` matches `orders.us`, `orders.eu.west`
- Double-dot wildcard: `orders..product` matches `orders.us.product`
- Catch-all: `>` matches everything

### Views & Aggregation
- Passthrough views: virtual topics from filtered source topics
- Aggregation views: COUNT, SUM, AVG, MIN, MAX with GROUP BY
- **JOIN views**: INNER, LEFT, RIGHT joins across multiple source topics
  - Format: `view_name:source1,source2:join=inner:left_key=field1,right_key=field2`
- **HAVING clause**: post-aggregation filtering (e.g., `having=[/count > 5]`)
- Projection fields

### Subscriptions
- **Pause / Resume**: `pause` and `resume` commands to suspend/unsuspend delivery
- **Conflation**: per-subscription message merge for slow consumers with configurable interval
- **Conflation key**: `conflation_key=/field` to merge by a specific JSON field instead of SOW key
- **Replace option**: `options=replace` replaces existing subscription with same subID
- **Multi-unsubscribe**: `sids` header field for unsubscribing multiple subscriptions at once

## Supported Protocol Features

| Feature | Description |
|---------|-------------|
| **Logon** | Processed ack with `version`, `client_name`, correlation ID echo |
| **Subscribe** | With content filter, topic wildcards, replace option; processed ack |
| **Delta Subscribe** | Receives OOF on record delete AND filter mismatch |
| **Bookmark Subscribe** | Journal replay from position with content filtering |
| **Unsubscribe** | By subID, by multiple subIDs (sids), or "all" |
| **Publish** | Processed + persisted acks (echo seq ID); journal + SOW upsert |
| **Delta Publish** | Recursive JSON merge-patch into existing SOW record |
| **SOW** | Cached records with group_begin/end, filter, top_n, orderBy |
| **SOW and Subscribe** | SOW snapshot + live subscription (registered first) |
| **SOW and Delta Subscribe** | SOW snapshot + delta subscription with OOF |
| **SOW Delete** | By key, sow_keys, filter, or payload data; records_deleted count; OOF to delta subs |
| **Flush** | Processed + completed acks |
| **Heartbeat** | Processed ack + beat echo; server-side liveness watchdog |
| **Queue Topics** | `queue://` prefix adds `lease_period`; supports `max_backlog=<n>` backpressure and `pull` delivery mode via `sow_and_subscribe` + `top_n` |
| **Client Ack** | Accepted silently |
| **Group Begin/End** | SOW batch markers + accepted as commands |
| **Start/Stop Timer** | Processed ack |
| **Pause / Resume** | Suspend/resume subscription delivery |
| **Stats Ack** | Synthetic stats response |
| **Received Ack** | Returned when `a:received` requested |
| **OOF** | Sent to delta subscribers on SOW delete + filter mismatch |
| **Bookmarks** | Unique `epoch_us\|publisher\|seq\|` per message |
| **Timestamps** | AMPS-format `YYYYMMDDTHHMMSSnnnnnnnnnn` |
| **SowKey** | Auto-extracted or generated per publish |
| **Sequence ID Echo** | Persisted ack echoes `s` for publish store discard |
| **Correlation ID** | Echoed in logon ack |
| **Message Types** | Per-topic: json, protobuf, nvfix, fix, xml, binary |
| **Expiration** | TTL on SOW records with periodic GC |
| **Eviction** | LRU, oldest, capacity-based SOW eviction policies |
| **Disk Persistence** | Journal and SOW can be backed by disk files |
| **Compression** | zlib compression enabled via logon options (`c` or `compress`) |
| **Challenge Logon** | Optional two-step challenge-response flow with `-auth-challenge` |

## Flags

```
-addr            listen address (default "127.0.0.1:19000")
-version         AMPS server version in logon ack (default "6.3.1.0")
-fanout          fan-out publishes to subscribers (default true)
-echo            echo publishes back to same connection (default false)
-sow             enable in-memory SOW cache (default true)
-sow-max         max SOW records per topic, 0=unlimited (default 0)
-sow-eviction    SOW eviction policy: none, lru, oldest, capacity (default "oldest")
-sow-disk        directory for disk-backed SOW persistence (default "")
-journal         enable message journal for bookmark replay (default true)
-journal-max     max journal entries before eviction (default 1000000)
-journal-disk    directory for disk-backed journal persistence (default "")
-queue           enable queue:// support (default true)
-lease           default queue lease period (default 30s)
-auth            enable auth with user:pass pairs (default "")
-auth-challenge  require two-step challenge-response logon when auth is enabled (default false)
-peers           comma-separated peer addresses for HA replication (default "")
-repl-id         unique replication instance ID (default "instance-1")
-admin           admin REST API listen address (default "")
-nodelay         set TCP_NODELAY (default true)
-read-buf        TCP read buffer size (default 65536)
-write-buf       TCP write buffer size (default 65536)
-out-depth       per-connection outbound channel depth (default 65536)
-latency         artificial per-message latency (default 0)
-stats           enable per-connection throughput logging
-stats-interval  stats logging interval (default 5s)
-log-conn        log connect/disconnect events (default true)
-view            register a view (repeatable)
-action          register an action (repeatable)
```

## Admin REST API

When started with `-admin :8085`, provides:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/admin/status` | GET | Server status, uptime, memory, config |
| `/admin/stats` | GET | Global connection stats |
| `/admin/sow` | GET | SOW summary per topic |
| `/admin/sow` | DELETE | Clear all SOW data |
| `/admin/sow/:topic` | GET | SOW records for a specific topic |
| `/admin/sow/:topic` | DELETE | Clear SOW for a specific topic |
| `/admin/subscriptions` | GET | List all active subscriptions |
| `/admin/views` | GET | List registered views |
| `/admin/actions` | GET | List registered actions |
| `/admin/journal` | GET | Journal statistics |

## Files

| File | Description |
|------|-------------|
| `main.go` | Server entry point, flag parsing, listener, signal handling |
| `handler.go` | Per-connection reader, command dispatch, fan-out, topic config |
| `writer.go` | Per-connection writer goroutine with write coalescing |
| `protocol.go` | Frame builders, JSON header parser, ack construction |
| `subscription.go` | Subscriber registry with filter + wildcard + pause/resume |
| `journal.go` | Message journal (transaction log) with bookmark replay |
| `sow.go` | SOW cache with filter, top_n, delta merge, expiration, eviction |
| `filter.go` | Content filter expression evaluator (NOT, IN, BETWEEN, regex) |
| `topic.go` | Topic wildcard/hierarchy matching |
| `delta.go` | Recursive JSON delta merge (RFC 7396 merge-patch) |
| `conflation.go` | Conflation buffer with configurable key support |
| `aggregation.go` | Aggregation/projection engine with HAVING clause |
| `views.go` | View definitions, passthrough + aggregation views |
| `actions.go` | On-publish / on-deliver actions (route, log, transform) |
| `auth.go` | Authentication and entitlements |
| `replication.go` | HA replication between peers |
| `admin.go` | Admin REST API |

## Scope & Limitations

This is a **test harness**, not a production AMPS replacement.

Out of scope for this project (intentional non-goals):

- Distributed deployment semantics (cluster leadership, quorum, consensus)
- Enterprise durability guarantees across nodes (synchronous replicated commit semantics)
- Production-grade HA orchestration and failover coordination

Still intentionally limited (single-process parity harness focus):

- Full XPath/SQL filter surface beyond currently supported operators/functions
- Deep multi-format server-side parsing/validation (FIX/XML/BSON/Protobuf)
- Pluggable enterprise identity providers (PAM/LDAP/Kerberos)
- Field-level/row-level security policies beyond configured topic/filter entitlements
