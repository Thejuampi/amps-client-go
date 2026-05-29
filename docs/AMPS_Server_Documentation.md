# AMPS Server 5.3.5 - Complete Documentation

> Consolidated reference of all AMPS 5.3.5 server documentation from 60East Technologies.
> Source: <https://crankuptheamps.com/docs>

## Welcome to AMPS 5.3.5

Welcome to the AMPS 5.3.5 documentation. This guide covers the AMPS server and links to developer guides for the AMPS client libraries. A PDF version is also available: [Download PDF](https://devnull.crankuptheamps.com/docs/offline/AMPS_server_5.3.5_documentation.pdf).

## Introduction to AMPS

This guide focuses on a broad overview of the most-commonly used features in AMPS. For detailed information, see the *AMPS User Guide*.

### Galvanometer and RESTful Statistics

When the `Admin` interface is configured, you can monitor the AMPS instance using the Galvanometer web UI or the RESTful statistics interface.

| Interface | URI |
| --- | --- |
| Galvanometer | `<http://<host>>:<port>/` |
| RESTful Statistics | `<http://<host>>:<port>/amps` |

In the URIs above, `<host>` is the host the AMPS instance is running on and `<port>` is the administration port configured in the configuration file (this is `8085` in the sample configuration).

For more information, see [Monitoring AMPS](/docs/amps-user-guide/monitoring) in the *AMPS User Guide* and the [AMPS Monitoring Guide](/docs/amps-monitoring-guide).

### Further Reading

- **Event Logging**: AMPS provides a rich logging framework supporting console, syslog, and file targets with uniquely identifiable messages; see the [Logging](/docs/amps-user-guide/logging) section in the *AMPS User Guide*.
- **Conflation**: AMPS supports limiting update volume via conflated topics (server-side) and conflated subscriptions (per-client); see the *User Guide* for details.
- **View Topics and Aggregation**: AMPS includes a high-performance aggregation engine that can project one topic onto another, including JOINs across different message types.
- **Paginated Subscriptions**: Applications can request a subset of records to save bandwidth and CPU, receiving notifications when records in the page change.
- **Historical SOW Query**: AMPS can retain historical SOW state at configurable granularity and query for the state at a point in time.
- **Utilities**: AMPS provides diagnostic utilities including `spark` (command-line client), `ampserr` (error lookup), `amps-grep` (log/journal search), `amps_sow_dump` (SOW inspection), and `amps_journal_dump` (journal inspection).
- **Monitoring Interface**: AMPS exposes host and instance statistics via a RESTful interface and can persist them to an SQLite database.
- **High Availability**: AMPS provides replication, reliable publishing, and resumable subscriptions for failover; see the [Replication](/docs/amps-user-guide/replication) and [HA](/docs/amps-user-guide/ha) chapters in the *User Guide*.

### Scenario and Feature Reference

AMPS offers a wide array of messaging features to solve a variety of messaging scenarios. This section presents some basic mappings between common messaging scenarios and the AMPS features that support those scenarios. Of course, this list is just a sampling of the types of applications that use AMPS.

| Scenario | AMPS Feature(s) |
| --- | --- |
| Simple, low-latency publish and subscribe (many to many messaging) with no need to persist messages. | Ad hoc [Publish and Subscribe](/docs/amps-user-guide/pub-sub) |
| Publish and subscribe with a replayable audit trail. | [Transaction Log and Bookmark Subscription](/docs/amps-user-guide/txlog/transaction-log-basics) |
| Snapshot of the current state of a set of messages (for example, graphing the elapsed time for all pending orders). | [State of the World (SOW)](/docs/amps-user-guide/sow) |
| Creating a view server that aggregates information about a high-velocity data feed for reporting. | [State of the World (SOW)](/docs/amps-user-guide/sow) [Views and Aggregation](/docs/amps-user-guide/views) [Transaction Log](/docs/amps-user-guide/txlog) and [Replication](/docs/amps-user-guide/replication) |
| Snapshot of the current state of a set of messages followed by updates to those messages (for example, showing the current status of a set of orders when a UI starts and then showing real-time updates to those messages). | [State of the World (SOW)](/docs/amps-user-guide/sow) [SOW and Subscribe](/docs/amps-user-guide/sow-queries/query-and-subscribe) from client application |
| Ensuring that a given message is processed once, by a single subscriber (for example, a workload distribution system). | [Message Queues](/docs/amps-user-guide/queues) and [Transaction Log](/docs/amps-user-guide/txlog/configuring-a-transaction-log) (Queues use the Transaction Log) |
| Replaying messages from a point in time. | [Transaction Log and Bookmark Subscription](/docs/amps-user-guide/txlog/transaction-log-basics) |
| Transforming messages as they are published to AMPS. | [State of the World (SOW)](/docs/amps-user-guide/sow) and [Enrichment](/docs/amps-user-guide/enrichment) |
| Producing aggregate data for a stream of messages. | [State of the World (SOW)](/docs/amps-user-guide/sow) and [Views](/docs/amps-user-guide/views) or [Aggregated Subscriptions](/docs/amps-user-guide/views/aggregated-subscriptions) |
| Coordinating work across a set of independent workers who are each assigned discrete tasks. | [Message Queues](/docs/amps-user-guide/queues) and [Transaction Log](/docs/amps-user-guide/txlog/configuring-a-transaction-log) |
| Dividing work among a set of workers who each update a portion of a record. | [State of the World (SOW)](/docs/amps-user-guide/sow) and [Delta Publish](/docs/amps-user-guide/delta-publish) |
| Providing highly available messaging with multiple servers providing failover. | [Transaction Log](/docs/amps-user-guide/txlog) and [Replication](/docs/amps-user-guide/replication) |

The scenarios above describe just a few of the more common scenarios in which AMPS is used. For messaging scenarios that aren't described above, contact 60East at <http://support.crankuptheamps.com/> for advice and guidance.

### Recovery Strategies

The AMPS server and the AMPS client libraries provide various options for recovering and resuming subscriptions. Use this cross-reference to choose the recovery strategy that best matches the needs of your application.

| Scenario | AMPS Feature(s) |
| --- | --- |
| Automatically recover subscription without replaying missed messages. | [HAClient](/docs/amps-user-guide/ha) / [Subscribe](/docs/amps-user-guide/pub-sub) |
| Recover subscription and replay any messages missed while application is offline. | [HAClient](/docs/amps-user-guide/ha) / [Transaction Log](/docs/amps-user-guide/txlog) / Bookmark Subscription (refer to the relevant Client Guide) / Bookmark Store (refer to the relevant Client Guide) |
| Recover subscription, get current state of a set of messages upon recovery and receive updates to that state. | [HAClient](/docs/amps-user-guide/ha) / [State of the World (SOW)](/docs/amps-user-guide/sow) / [SOW and Subscribe](/docs/amps-user-guide/sow-queries/query-and-subscribe) command |

The scenarios above describe just a few of the most common recovery scenarios for a subscription. For recovery scenarios that aren't described above, contact 60East at <http://support.crankuptheamps.com/> for advice and guidance.

### JSON Messages - A Quick Primer

AMPS supports a wide variety of message types; this guide uses JSON for examples because the format is simple, easily readable, and widely used. JSON has two basic constructs: objects that consist of key/value pairs, and arrays of values. JSON supports hierarchical construction where values can be nested objects or arrays.

```json
{
    "id" : 73,
    "character" : {
        "name" : "Han Solo",
        "occupation" : "smuggler",
        "ship" : {
            "name"  : "Millennium Falcon",
            "speed" : ".5 past light speed",
            "cargo" : [ "widgets", "baskets", "spice"]
        }
    }
}
```

Many AMPS applications use JSON as the payload. In addition, the `amps` protocol represents commands in a simplified subset of JSON. For example, a publish command might look like:

```json
{"c":"publish","t":"test-topic"}{ "id" : 1, "message" : "Hello, World!" }
```

The command header is followed by the message body, the payload of the command. While the `amps` protocol header is JSON, you can use any message type for the body:

```json
{ "c":"publish","t":"xml-topic"}<example><id>1</id><message>Hello, world!</message></example>
```

The AMPS client libraries create and parse AMPS headers automatically. Applications use the `Message` and `Command` interfaces of the client libraries to work with headers; there is no need for your application to parse or serialize headers directly. The client libraries do not parse or interpret the payload data on a received `Message`; instead the payload is returned as a sequence of bytes (or as a string). The one exception is the JavaScript client, which can optionally deserialize JSON messages into objects.

### Feature Highlights

- Topic based publish and subscribe, including full support for regular expressions to specify topic names.
- Content filtering based on XPath identifiers (to specify the fields of a message) and SQL-92 (to form a predicate), with added support for Perl-Compatible Regular Expressions (PCRE2).
- Message queues including content filtering for both publishers and subscribers, configurable strategies for delivery fairness, and truly distributed queues that can efficiently enforce queue semantics and delivery guarantees across a replicated network of AMPS instances.
- Content-aware messaging support for a wide range of message types, including standard formats such as JSON, FIX, MessagePack, XML, Google Protocol Buffers, and BSON. AMPS also supports simple key/value pairs in FIX format (called NVFIX to emphasize that the format uses name/value pairs rather than FIX tags), and a high-performance binary protocol called BFlat. AMPS also supports uninterpreted binary messages, and allows you to create composite message types from existing types to easily combine messages of different types in a single payload.
- An integrated database and record-aware current value storage (called State of the World, or SOW), with optional historical query capability.
- Historical replay of message streams, including the ability to preserve the total message ordering across independent topics.
- Integrated replication and high availability, including automatic resynchronization for instances that fail over.
- Aggregation and Complex Event Processing (CEP), including the ability to aggregate information across different message streams and message streams of different formats.
- Advanced messaging capabilities such as atomic query-and-subscribe, incremental (delta) updates, and out-of-focus notifications that tell a subscription when a record no longer matches.
- Built in statistics and monitoring, with data provided via a standard RESTful interface.
- Integrated authentication and entitlement across all AMPS features.
- Actions for automating AMPS functionality, including both routine maintenance tasks and dataflow-aware processing (such as alerting in response to slowdowns or invalid data).
- Client development kits for popular programming languages such as Java, C#/.NET, C++, Python, JavaScript, and Go.
- Extensibility API in the AMPS server for adding message types, extending the functions available to the AMPS query language, adding new actions, integrating with enterprise authentication and entitlement systems, and more.

### When Should I Store a Topic in the SOW

Storing a topic in the State of the World is most useful when your application needs to use the current state of the data being tracked. Storing a topic in the State of the World can be especially useful if your application would benefit from automatically receiving updates as soon as they are made (described in more detail in the [Atomic Query and Subscribe](/docs/intro-guide/sow/subscriptions) topic).


## Glossary

| Term | Definition |
| --- | --- |
| Acknowledgment | Receiver informs sender of message receipt. In AMPS: commands are asynchronous with acknowledgment responses; queue consumers acknowledge processed messages. |
| Authentication | Establishing proven identity for a connection. |
| Bookmark | Unique message identifier: publisher session ID + sequence number. Two updates to same record have different bookmarks. |
| Conflated Topic | SOW topic copy that conflates updates on a specified interval. |
| Conflation | Merging multiple messages into one (e.g., deliver most recent update every 300ms). |
| Delta | Message containing only differences between previous and new state. Supported for both publish and subscribe. |
| Entitlement | Assigning permissions based on connection identity. |
| Expression | Text string producing a value. Used in filters, enrichment, view projection. |
| Filter | AMPS expression returning TRUE/FALSE to match a message subset. |
| Message Expiration | Limiting lifespan of SOW/queue records. |
| Message Type | Data format for messages. Each message and each connection uses a single message type. |
| Module | Shared object extending AMPS (authentication, entitlement, message types, expression functions). Default modules load automatically; others via config. |
| oof (out of focus) | Notification that a previous SOW/filter result has expired, been deleted, or no longer matches. |
| Queue | Topic providing competitive consumption (message processed once regardless of consumer count). Transaction log provides reliable replay and ordering. |
| Replication | Duplicating messages to additional AMPS instances on a command-by-command basis for low latency. |
| Replication Destination | Instance receiving messages from a replication source. |
| Replication Source | Instance sending messages to one or more replication destinations. |
| Replication Transport | `amps-replication` transport for incoming replication messages. |
| Slow Client | Client whose outgoing network buffer is full (due to processing speed, network slowdown, or AMPS producing more than network can transmit). |
| SOW (State of the World) Topic | Last value cache / message database. Key fields determine uniqueness. |
| SOW Key | Unique record identifier in SOW topic. Can be content-based, provided on publish, or generated by module. Same key = same record. |
| Topic | Label grouping messages for routing. Same message type, persistence, delivery paradigm within a topic. |
| Transaction Log | History of all published messages for configurable topics. Preserves processing order within and across topics. Queryable and replayable. |
| Transport | Network protocol for message transfer (publishers, subscribers, replication). |
| View | In-memory topic constructed from one or more SOW topics. Can aggregate, transform, and use different message format. Auto-updates with underlying SOW changes. |

### Replication Validation

Each `Topic` in a replication `Destination` can configure validation checks. By default, all are applied.

| Check | Description | Excludable |
| --- | --- | --- |
| `txlog` | Topic must be in downstream transaction log | Yes |
| `replicate` | Topic must be replicated back from downstream | Yes |
| `sow` | If SOW/Topic on this instance, must also be SOW/Topic on downstream | Yes |
| `cascade` | Downstream must enforce same validation checks | Yes |
| `queue` | If queue here, must be queue downstream | **No (mandatory)** |
| `keys` | SOW/Topic on both sides must use same `Key` definitions | Yes |
| `replicate_filter` | Replication filter must match on downstream | Yes |
| `queue_passthrough` | Downstream must support passthrough from this group | Yes |
| `queue_underlying` | Queue must use same underlying topic/filters downstream | **No (mandatory)** |

```xml
<Destination>
    ...
    <Topic>
        <MessageType>json</MessageType>
        <Name>MyStuff-VIEW</Name>
        <ExcludeValidation>replicate,cascade</ExcludeValidation>
    </Topic>
    ...
</Destination>
```

### Special Characters in Configuration

**SOW File Name:** `%n` substitutes message type and topic name.

```xml
<SOW>
    <Topic>
        <Topic>Customers</Topic>
        <FileName>./sow/%n.sow</FileName>
        <MessageType>json</MessageType>
        <Key>/customerId</Key>
    </Topic>
</SOW>
```

**Log Rotation:** `%n` produces sequential log file names (e.g., `log-1.log`, `log-2.log`).

```xml
<Logging>
    <Target>
        <Protocol>file</Protocol>
        <Level>info</Level>
        <FileName>log/log-%n.log</FileName>
        <RotationThreshold>2G</RotationThreshold>
    </Target>
</Logging>
```

**Date Tokens:** Full `strftime` support (except `%n`).

| Token | Description | Example |
| --- | --- | --- |
| %a | Short weekday | Fri |
| %A | Full weekday | Friday |
| %b | Short month | Feb |
| %B | Full month | February |
| %c | Date and time | Fri Feb 14 17:25:00 2014 |
| %C | Century | 20 |
| %d | Day (leading zero) | 05 |
| %D | MM/DD/YY | 02/20/14 |
| %e | Day (leading space) | 5 |
| %F | YYYY-MM-DD | 2014-02-20 |
| %H | Hour 00-23 | 17 |
| %I | Hour 00-12 | 05 |
| %j | Day of year 001-366 | 051 |
| %m | Month 01-12 | 02 |
| %p | AM/PM | PM |
| %r | 12h time | 05:25:00 pm |
| %R | 24h time | 17:25 |
| %T | ISO 8601 time | 17:25:00 |
| %u | ISO 8601 weekday 1-7 (Mon=1) | 5 |
| %V | ISO 8601 week 00-53 | 07 |
| %y | 2-digit year | 14 |
| %Y | 4-digit year | 2014 |
| %Z | Timezone | PST |

```xml
<Logging>
    <Target>
        <Protocol>file</Protocol>
        <Level>info</Level>
        <FileName>log/log-%Y-%m-%dT%H%M%S.log</FileName>
        <RotationThreshold>2G</RotationThreshold>
    </Target>
</Logging>
```

## Units in Configuration

**Time units:**

| Units | Description |
| --- | --- |
| `ns` | nanoseconds |
| `us` | microseconds |
| `ms` | milliseconds |
| `s` | seconds |
| `m` | minutes |
| `h` | hours |
| `d` | days |
| `w` | weeks |

**Byte units:**

| Units | Description |
| --- | --- |
| `kb` | kilobytes |
| `mb` | megabytes |
| `gb` | gigabytes |
| `tb` | terabytes |

**Exponent units (case-insensitive):**

| Units | Description |
| --- | --- |
| k | 10^3 |
| M | 10^6 |

## Environment Variables

Use `${VAR_NAME}` syntax. Set via OS environment or `-D` on command line.

```xml
<Logging>
    <Target>
        <Protocol>file</Protocol>
        <FileName>${ENV_LOG}</FileName>
        <Level>info</Level>
        <RotationThreshold>2G</RotationThreshold>
    </Target>
</Logging>
```

### Internal Environment Variables

| Variable | Contains |
| --- | --- |
| `AMPS_CONFIG_DIRECTORY` | Directory of configuration file |
| `AMPS_CONFIG_PATH` | Full path to configuration file including filename |
| `AMPS_VERSION` | Full AMPS server version number |

```xml
<Logging>
    <Target>
        <Protocol>file</Protocol>
        <FileName>${AMPS_CONFIG_DIRECTORY}/logs/infoLog.log</FileName>
        <Level>info</Level>
        <RotationThreshold>2G</RotationThreshold>
    </Target>
</Logging>
```

## Software Requirements

- Linux 64-bit (kernel 2.6+) on x86
- Distribution is self-contained (no additional dependencies)
- `spark` requires Java 1.7+
- `amps_sow_dump`, `amps_clients_ack_dump`, `amps-grep` require Python

## AMPS Version Format

```
MAJOR.MINOR.FEATURE.HOTFIX.TIMESTAMP.TAG
```

| Component | Description | Verification |
| --- | --- | --- |
| `MAJOR` | Backward-incompatible changes, deprecated removals, major new functionality | Megacert |
| `MINOR` | Backward-compatible additions, deprecations | Megacert |
| `FEATURE` | Previews; `0` = long-term stable, >0 = preview | Kilocert |
| `HOTFIX` | Critical defect fix, 100% compatible with same MAJOR.MINOR.FEATURE | Cert |
| `TIMESTAMP` | Build timestamp | N/A |
| `TAG` | Code identifier | N/A |

| Certification | Description | Time |
| --- | --- | --- |
| Megacert | Performance, long-haul, full regression + stress + replication + unit tests | <2 weeks |
| Kilocert | Full regression + stress + replication + unit tests | <1 week |
| Cert | Full unit tests + replication tests if affected | 4 hours |

## Installing and Starting AMPS

Directory structure:

| Directory | Description |
| --- | --- |
| bin | AMPS engine binaries and utilities |
| docs | Documentation |
| lib | Library dependencies |
| sdk | AMPS extension API headers |

```bash
$AMPSDIR/bin/ampServer --sample-config > $AMPSDIR/amps_config.xml
$AMPSDIR/bin/ampServer $AMPSDIR/amps_config.xml
```

### Command Line Options

| Option | Effect |
| --- | --- |
| `--verify-config` | Parse and verify config, then exit |
| `--sample-config` | Produce minimal config to stdout, then exit |
| `--dump-config` | Expand config (includes + env vars) to stdout |
| `--version` | Print version, exit |
| `--help` | Print usage, exit |
| `--daemon` | Run as daemon |
| `-D<var>=<val>` | Set environment variable (repeatable) |

## Configuration File

AMPS reads, expands env vars, and processes includes at startup. Changes on disk after startup have no effect—restart required.

### Minimal Configuration

```xml
<AMPSConfig>
    <Name>test-AMPS-1</Name>
</AMPSConfig>
```

### Including External Files

`Include` directive inserts external file contents. Cycles are rejected. Each file is individually parsed (XML entities don't cross include boundaries). Use `ConfigIncludeCommentDefault` or `comment` attribute to add source comments.

```xml
<AMPSConfig>
    ...
    <Logging>
        <Include comment="true">filetarget.xml</Include>
    </Logging>
    ...
</AMPSConfig>
```

Expanded result:

```xml
<AMPSConfig>
    ...
    <Logging>
        <!-- Start <Include>filetarget.xml</Include> -->
        <Target>
            <Protocol>file</Protocol>
            <FileName>/var/log/amps-log-%n.log</FileName>
            <Level>info</Level>
        </Target>
        <!-- End <Include>filetarget.xml</Include> -->
    </Logging>
    ...
</AMPSConfig>
```

## Instance-Level Configuration

### AMPS Process Options

| Element | Required | Default | Description |
| --- | --- | --- | --- |
| `Name` | Yes | None | Instance name. Must be unique within replicated instances. No spaces, `/`, `\`, `$`, `~`. |
| `Group` | No | Instance `Name` | Replication group. Same `Group` = equivalent for failover/replication. |
| `ProcessName` | No | Executable name | Linux process name (useful for multi-instance systems). |
| `Description` | No | None | Instance description for monitoring tools. |
| `Environment` | No | None | Environment info for monitoring tools. |
| `SuggestedMinimumVersion` | No | None | Warns if AMPS version is lower. |
| `RequiredMinimumVersion` | No | None | Errors and refuses to start if AMPS version is lower. |
| `ConfigIncludeCommentDefault` | No | `false` | Default for `Include` source comments. |
| `ConfigCycleDetectionThreshold` | No | `5MB` | Max expanded config size (prevents include cycles). |

Example:

```xml
<AMPSConfig>
    <Name>AMPS</Name>
    <Group>Sample-AMPS</Group>
</AMPSConfig>
```

### UserDefinedFunctions

Registers custom scalar functions for expressions. Each `Function` entry:

| Element | Description |
| --- | --- |
| `Function/Name` | Name exposed to expression authors (case-insensitive at runtime) |
| `Function/Module` | Name of module in `<Modules>` section |
| `Function/Symbol` | Exported symbol (`extern "C"` for C++) |
| `Function/ParameterCount` | Expected arguments. Omit for variadic (`AMPS_UDF_VARIADIC_PARAMETER_COUNT`). |

```xml
<Modules>
    <Module>
        <Name>pricing-udf</Name>
        <Library>libpricing_functions.so</Library>
    </Module>
</Modules>

<UserDefinedFunctions>
    <Function>
        <Name>FWD_PRICE</Name>
        <Module>pricing-udf</Module>
        <Symbol>amps_udf_forward_price</Symbol>
        <ParameterCount>3</ParameterCount>
    </Function>
    <Function>
        <Name>NORMALIZE_TAGS</Name>
        <Module>pricing-udf</Module>
        <Symbol>amps_udf_normalize_tags</Symbol>
    </Function>
</UserDefinedFunctions>
```

UDF changes require instance restart.

## Slow Client Policies

### Instance-Wide Options

| Element | Default | Description |
| --- | --- | --- |
| `MessageMemoryLimit` | 10% of host memory or 10% of `ulimit -m`, whichever is lower | Total memory before offlining messages to disk. |
| `MessageDiskLimit` | `1GB` or `MessageMemoryLimit`, whichever is higher | Total disk before disconnecting clients. |
| `MessageDiskPath` | `/var/tmp` | Path for offline files. |

### Per-Client Options

| Element | Default | Description |
| --- | --- | --- |
| `ClientMessageAgeLimit` | No limit | Max lag time before disconnect (e.g., `30s`, `1h`). |
| `ClientMaxCapacity` | `50%` | Percentage of total capacity a single client can consume. Pre-5.3.4 default was `100%`. |

## Minidump Settings

| Element | Default | Description |
| --- | --- | --- |
| `MiniDumpDirectory` | `/tmp` | Storage location. `disabled` disables minidumps. |
| `MiniDumpFileMask` | `0640` | Octal permissions mask (chmod format). |

```xml
<AMPSConfig>
    ...
    <MiniDumpDirectory>/var/tmp</MiniDumpDirectory>
    <MiniDumpFileMask>0644</MiniDumpFileMask>
    ...
</AMPSConfig>
```

File mask examples:

- `0444` — readable by all
- `0440` — readable by owner + group
- `0400` — readable by owner only
- `0664` — read/write owner+group, read all
- `0644` — read/write owner, read group+all

## Tuning

| Element | Default | Description |
| --- | --- | --- |
| `NUMA/Enabled` | `enabled` | NUMA thread affinity. Disable for multi-instance or CPU-contended systems. Also set via `AMPS_NUMA` env var. |
| `Replication/MinSyncDestinations` | Unset | Min sync destinations before actions can downgrade to async. Does not upgrade on disconnect. |
| `Queue/QueueDeliveryFlushInterval` | `250us` | Max wait for queue delivery thread. Min: `1us`. Lower = lower latency, lower throughput. |
| `Statistics/Indexing/Enabled` | Unset | Creates index on `static_id` for DYNAMIC stats tables. Increases startup time on first enable. Indexes not removed if option removed. |

```xml
<AMPSConfig>
    <Tuning>
        <NUMA><Enabled>enabled</Enabled></NUMA>
        <Replication><MinSyncDestinations>AMPS_A</MinSyncDestinations></Replication>
        <Queue><QueueDeliveryFlushInterval>250us</QueueDeliveryFlushInterval></Queue>
        <Admin>
            <Statistics>
                <Indexing><Enabled>enabled</Enabled></Indexing>
            </Statistics>
        </Admin>
    </Tuning>
</AMPSConfig>
```

## Externals

Override external library paths:

| Element | Default | Description |
| --- | --- | --- |
| `SSL/Library` | `libopenssl.so` | SSL library (OpenSSL 1.1 compatible) |
| `Crypto/Library` | `libcrypto.so` | Crypto library |
| `Curl/Library` | `libcurl.so` | libcurl library |

```xml
<AMPSConfig>
    <Externals>
        <SSL><Library>/opt/audited/libopenssl.so</Library></SSL>
        <Crypto><Library>/opt/audited/libcrypto.so</Library></Crypto>
        <Curl><Library>/opt/resolver/lib/libcurl.so</Library></Curl>
    </Externals>
</AMPSConfig>
```

## Specialized Instance Options

| Element | Default | Description |
| --- | --- | --- |
| `SOWStatisticsInterval` | Unset | Interval for `/AMPS/SOWStats` updates. |
| `RegexTopicSupport` | `true` | Allow regex topic matching. When `false`, regex chars are literal. |
| `ConfigValidation` | `enabled` | Validate config on startup. `disabled` allows invalid configs (risky). |

## Working with Configuration Files

```bash
ampServer --sample-config > config.xml
ampServer --verify-config config.xml
ampServer --dump-config config.xml > expanded.xml
```

## Transports

Transports configure incoming connections. Two types: **Client Connections** (publishers/subscribers) and **Replication Connections** (inbound replication).

Each transport controls authentication, entitlements, and slow client policies.

### Connection Types

- **TCP** (`tcp`): Standard TCP/IP. Optional compression. If `PrivateKey`+`Certificate` present, SSL is required even with `tcp` type.
- **TLS/SSL** (`tcps`): Requires certificate + private key. Optional compression.
- **WebSocket**: Uses `websocket` protocol over `tcp`/`tcps`. Persistent full-duplex, not RESTful. AMPS messages within WebSocket frames.
- **IPv6**: Supported since 5.3.3. Both IPv4/IPv6 for transport addresses. Default: listen on both if no address specified.
- **Unix Domain Sockets** (`amps-unix`): For same-system low-latency. Requires `FileName`.

### Protocols

Preconfigured protocols:

| Protocol | Description |
| --- | --- |
| `amps` | Standard AMPS messaging, compact JSON-based headers. `json` is synonym. |
| `websocket` | WebSocket protocol, JSON headers. |

Legacy protocols (backward compat only, no new features):

| Protocol | Description |
| --- | --- |
| `fix` | FIX format headers |
| `fix-session` | FIX session protocol |
| `nvfix` | NVFIX format headers |
| `soap` | SOAP format headers |
| `xml` | XML format headers |

### Protocol Configuration

| Element | Description |
| --- | --- |
| `Name` | Name for customized protocol (used in Transport reference) |
| `Module` | Protocol module name (e.g., `amps`, `websocket`) |

#### Websocket Protocol Options

| Element | Default | Description |
| --- | --- | --- |
| `WWWAuthenticate` | None | `Negotiate` or `Basic realm="<realm>"` |
| `TrustedAdmin` | `false` | Accept admin-authenticated connections |
| `HTTPHeader` | None | Custom response header (repeatable) |

```xml
<AMPSConfig>
    <Protocols>
        <Protocol>
            <Name>websocket-portal</Name>
            <Module>websocket</Module>
            <WWWAuthenticate>Basic realm="AMPS Admin"</WWWAuthenticate>
            <TrustedAdmin>enabled</TrustedAdmin>
        </Protocol>
    </Protocols>
</AMPSConfig>
```

### Transport Configuration

| Element | Required | Default | Description |
| --- | --- | --- | --- |
| `Name` | Yes | None | Transport name (appears in logs). For replication transports, should match `Type`. |
| `Protocol` | Yes | None | `amps`, `websocket`, or legacy protocol name. |
| `Type` | Yes | None | `tcp`, `tcps`, `amps-replication`, `amps-replication-secure`, `amps-unix`. |
| `InetAddr` | Yes* | All addresses | Port or `IP:Port`. `0.0.0.0:port` = IPv4 only, `[::]:port` = IPv6 only. Not required for `amps-unix`. |
| `MessageType` | No | All types (`amps` protocol) | Restricts to single message type. Legacy protocols **must** specify. Replication transports ignore this. |
| `InitialState` | No | `enabled` | `disabled` = transport doesn't listen until explicitly enabled. |
| `Entitlement` | No | Instance default | Entitlement module (`Module` + optional `Options`). |
| `Authentication` | No | Instance default | Authentication module (`Module` + optional `Options`). |
| `MessageMemoryLimit` | No | Instance default | Per-transport memory limit before offlining. |
| `MessageDiskLimit` | No | Instance default | Per-transport disk limit before disconnect. |
| `MessageDiskPath` | No | Instance default `/var/tmp` | Path for offline files. |
| `TransportFilter` | No | None | Filter module with `Module` + optional `Options`. Multiple filters run in config order. |
| `ClientMessageAgeLimit` | No | Instance default | Max client lag before disconnect. |
| `ClientMaxCapacity` | No | `50%` | Single client capacity percentage. |

Default message types loaded: `fix`, `nvfix`, `xml`, `json`, `msgpack`, `bson`, `bflat`, `binary`.

### TLS/SSL Transport Options

| Element | Required | Default | Description |
| --- | --- | --- | --- |
| `Certificate` | Yes (for SSL) | None | Server certificate file. |
| `PrivateKey` | Yes (for SSL) | None | Server private key. |
| `Ciphers` | No | None | OpenSSL cipher list (passed through to OpenSSL). |
| `SecureSocketProtocols` | No | `TLSv1.1 TLSv1.2 TLSv1.3` | Space-delimited accepted protocols: `SSLv2`, `SSLv3`, `TLSv1`, `TLSv1.1`, `TLSv1.2`, `TLSv1.3`. |
| `VerifyClient` | No | `False` | Require client certificate. If `True`, must set `CAFile` or `CAPath`. |
| `CAFile` | No | None | PEM file with trusted certificates. |
| `CAPath` | No | None | Directory of PEM files with trusted certificates. |

### Unix Domain Socket Options

| Element | Required | Default | Description |
| --- | --- | --- | --- |
| `FileName` | Yes | None | Filesystem path for socket creation. |
| `FileMask` | No | None | Octal permissions (chmod format). Same values as minidump masks. |

### WebSocket Transport Options

| Element | Default | Description |
| --- | --- | --- |
| `PerMessageDeflate` | `enabled` | Per-message deflation. `disabled` = off. |
| `HTTPHeader` | None | Header returned during handshake (repeatable). |

### Example Transport Configurations

#### Slow Client Management

```xml
<AMPSConfig>
    <MessageMemoryLimit>10GB</MessageMemoryLimit>
    <MessageDiskPath>/mnt/fastio/AMPS/offline</MessageDiskPath>
    <ClientMessageAgeLimit>30s</ClientMessageAgeLimit>

    <Transports>
        <Transport>
            <Name>regular-tcp</Name>
            <Type>tcp</Type>
            <InetAddr>9007</InetAddr>
            <Protocol>amps</Protocol>
        </Transport>
        <Transport>
            <Name>regular-websocket</Name>
            <Type>tcp</Type>
            <InetAddr>9008</InetAddr>
            <Protocol>websocket</Protocol>
        </Transport>
        <Transport>
            <Name>highpri-tcp</Name>
            <Type>tcp</Type>
            <InetAddr>9995</InetAddr>
            <MessageMemoryLimit>35GB</MessageMemoryLimit>
            <MessageDiskLimit>70GB</MessageDiskLimit>
            <Protocol>amps</Protocol>
        </Transport>
    </Transports>
</AMPSConfig>
```

#### Transport Filter

```xml
<AMPSConfig>
    <Transports>
        <Transport>
            <Name>translate-legacy-topics</Name>
            <Type>tcp</Type>
            <InetAddr>9017</InetAddr>
            <Protocol>amps</Protocol>
            <TransportFilter>
                <Module>amps-topic-translator</Module>
                <Options>
                   <Topic>orders_for_northamerica:NAOrders</Topic>
                   <Topic>catalog_items:Catalog</Topic>
                   <Topic>customer_.*:Customers</Topic>
                </Options>
            </TransportFilter>
        </Transport>
    </Transports>
</AMPSConfig>
```

#### TLS/SSL Transport

```xml
<AMPSConfig>
    <Transports>
        <Transport>
            <Name>ssl-all-message-types</Name>
            <Type>tcps</Type>
            <InetAddr>9007</InetAddr>
            <Protocol>amps</Protocol>
            <Certificate>${AMPS_INSTALL}/cert.pem</Certificate>
            <PrivateKey>${AMPS_INSTALL}/key.pem</PrivateKey>
            <Ciphers>HIGH:!aNULL</Ciphers>
        </Transport>
    </Transports>
</AMPSConfig>
```

## HTTP Preflight

Allows TCP clients to connect through HTTP proxy via HTTP Upgrade handshake. No server-side changes needed. Add `?http_preflight=true` to connection string:

```java
Client client = new Client("example");
client.connect("tcps://localhost:443/amps/json?&http_preflight=true");
```

Optional custom headers:

```java
client.addHttpPreflightHeader("Cookie: a=1; b=2");      // raw form
client.addHttpPreflightHeader("Token", "token_value");  // key-value form
```

Note: AMPS still requires separate ports for tcp/amps, tcp/websocket, and Admin.

## Replication Connections

Transport type `amps-replication` or `amps-replication-secure`. Cannot be used for application connections. Accepts any message type, services multiple upstream instances. Configured as part of HA plan.

## Transport Filters

Filters run on each incoming command in config order. Can modify message data/headers, stop processing, or disconnect connection.

### Built-in Filters

**`amps-topic-translator`** — Translates topic names. Option format: `original:translated` (original can be PCRE regex).

```xml
<Options>
  <Topic>legacy:new</Topic>
</Options>
```

```xml
<Options>
  <Topic>^/orders/northamerica:NAorders</Topic>
</Options>
```

**`amps-conflated-topic-translator`** — Translates subscribe/sow_and_subscribe/delta_subscribe/sow_and_delta_subscribe topic names and adds conflation interval. Option format: `original:translated:interval`.

```xml
<Options>
  <Topic>orders-C:orders:500ms</Topic>
</Options>
```

```xml
<Options>
  <Topic>slowUpdates:updates:2s</Topic>
  <Topic>verySlowUpdates:updates:2s</Topic>
</Options>
```

### Optional: Correlation ID Timestamper

Writes ISO 8601 timestamp to correlation ID on `publish`/`delta_publish`. Must be explicitly loaded.

```xml
<Modules>
    <Module>
        <Name>transport-filter-correlation-id-timestamper</Name>
        <Library>libamps_transport_filter_correlation_id_timestamper.so</Library>
    </Module>
</Modules>

<Transports>
    <Transport>
        <Name>primary</Name>
        <Type>tcp</Type>
        ...
        <TransportFilter>
            <Module>transport-filter-correlation-id-timestamper</Module>
            <Options>
                <Override>True</Override>
            </Options>
        </TransportFilter>
    </Transport>
</Transports>
```

Option: `Override`=`True` overwrites existing correlation ID. Default: does not overwrite.

Limitations: bookmark subscriptions may replay old data; views generate messages internally (no filter applied); replicated messages not re-stamped. Host clocks must be in sync for valid latency measurement.

## Message Types

Message types define data format. AMPS is message-type agnostic—all types use same internal representation. Max message size: ~200MB.

### BFlat

Schemaless tag/value pairs with binary data support and full numeric precision. All AMPS features supported.

**Numeric types:**

| Type | Description |
| --- | --- |
| int8 | 8-bit integer |
| int16 | 16-bit integer |
| int32 | 32-bit integer |
| int64 | 64-bit integer |
| double | 64-bit IEEE 754 float |
| datetime | UTC ms since Unix epoch (64-bit) |
| leb128 | Signed LEB128 variable-length integer |

**Non-numeric types:**

| Type | Description |
| --- | --- |
| null | Empty field |
| string | Byte string (encoding application-defined) |
| binary | Untyped byte sequence |

BFlat supports arrays. Serializers should use most compact representation; parsers should not assume a fixed type for a field.

### Composite Messages

Combines existing message types. Two modules:
- `composite-local`: preserves part info for filtering/aggregation/projection
- `composite-global`: treats parts as single document

Restrictions:
- Delta subscribe/publish not supported for `composite-global`
- Views/joins/aggregation cannot project `composite-global` (but can be UnderlyingTopic/Join source)
- Auto-constructed messages (AMPS/.* topics, stats acks) not supported for any composite type

**Unparsed payload:** All composite types provide an unparsed payload section (bytes at end of message). Not parsed by AMPS. Not included in delta publish/subscribe or serialized representations.


## Content Filtering with Composite Message Types

Composite message types support content filtering via XPath identifiers on the composite message. These conventions apply to content filters, SOW keys, views, aggregates, and conflated topics.

### composite-global

`composite-global` combines all parsable parts into a unified set of XPath identifiers. Duplicate identifiers across parts are treated as arrays of values. Unparsable parts are ignored.

Example message (two `json` parts + one `binary` part):

```json
{"id":1,"data":"sample","message":"part one message"}  
{"message":"another part","customer":"Awesome Amalgamated, Ltd."}  
0xDEEA0934DF23A37780934...
```

| Identifier | Value |
| --- | --- |
| `/id` | `1` |
| `/data` | `"sample"` |
| `/message` | `["part one message", "another part"]` |
| `/customer` | `"Awesome Amalgamated, Ltd."` |

### composite-local

`composite-local` creates distinct XPath identifiers per part, prefixed with the part position. Unparsable parts are skipped.

Same message produces:

| Identifier | Value |
| --- | --- |
| `/0/id` | `1` |
| `/0/data` | `"sample"` |
| `/0/message` | `"part one message"` |
| `/1/message` | `"another part"` |
| `/1/customer` | `"Awesome Amalgamated, Ltd."` |

## Choosing a Composite Type

- Use `composite-local` for: delta messaging, redundant field names across parts, views.
- Otherwise `composite-global` is simpler (no part specifiers needed in filters).

### Configuring Message Types

`MessageTypes` defines supported message types. AMPS auto-loads: `fix`, `nvfix`, `json`, `bflat`, `msgpack`, `bson`, `xml`, `binary`. Only define a `MessageType` for these if settings need changing.

**`Name`** (required) — Name used to reference the message type in `Transport`, `TransactionLog`, `SOW`, etc.

**`Module`** — Module name. Default loaded modules: `fix`, `nvfix`, `json`, `bflat`, `msgpack`, `bson`, `xml`, `protobuf`, `binary`, `composite-global`, `composite-local`.

**`AMPSVersionCompliance`** — FIX version compatibility for `/AMPS/SOWStats`:

| Value | Behavior |
| --- | --- |
| `2` | AMPS 2.X FIX field tags |
| `4` | Default AMPS 4.X tags (different numbering for `SOWStats` vs `ClientStatus`) |
| `5` | Unified FIX tags across `SOWStats` and `ClientStatus` (5.X+) |

Default: `4`. No difference between `4` and `5` for non-FIX types.

**`Options`** — Custom XML options passed to a custom message type module.

### Message Type Specific Options

#### FIX/NVFIX Options

**`FieldSeparator`** — ASCII value of char separating field items.
**`HeaderSeparator`** — ASCII value of char separating header from body.
**`MessageSeparator`** — ASCII value of char separating message items.

```text
<MessageTypes>  
    <MessageType>  
        <Name>fix-custom</Name>  
        <Module>fix</Module>  
        <FieldSeparator>1</FieldSeparator>  
        <HeaderSeparator>2</HeaderSeparator>  
        <MessageSeparator>5</MessageSeparator>  
    </MessageType>  
</MessageTypes>
```

#### JSON Option

**`EarlyTerminationOptimization`** — When `true` (default), AMPS may partially parse JSON and reports the first value for duplicate fields. When `false`, AMPS fully parses and reports the last value.

Default: `true`

```text
    <MessageType>  
        <Name>json-custom</Name>  
        <Module>json</Module>  
        <EarlyTerminationOptimization>false</EarlyTerminationOptimization>  
    </MessageType>
```

#### Composite-Local and Composite-Global Option

**`MessageType`** (required, within composite) — One or more message type declarations specifying the contained types.

```text
<MessageTypes>  
    <MessageType>  
        <Name>images</Name>  
        <Module>composite-global</Module>  
        <MessageType>json</MessageType>  
        <MessageType>binary</MessageType>  
        <MessageType>binary</MessageType>  
    </MessageType>  
</MessageTypes>
```

```text
<MessageTypes>  
    <MessageType>  
        <Name>custom-composite</Name>  
        <Module>composite-local</Module>  
        <MessageType>json</MessageType>  
        <MessageType>custom-payload</MessageType>  
    </MessageType>  
</MessageTypes>
```

#### Google Protocol Buffer Options

Requires a `MessageType` definition with `.proto` files. 60East recommends explicitly declaring `syntax` version; AMPS defaults to proto2 syntax if undeclared.

**`Type`** (required) — Package-qualified type name within the `.proto` file (e.g., `my.package.Message`).

**`ProtoPath`** (required) — Search path for `.proto` files. Syntax: `alias;full-path`. Omit alias or use `;` prefix for the empty alias. Multiple `ProtoPath` declarations allowed.

**`ProtoFile`** (required) — `.proto` file name, optionally prefixed with alias.

```text
<MessageTypes>  
    <MessageType>  
        <Name>my-protobuf-messages</Name>  
        <Module>protobuf</Module>  
        <ProtoPath>proto-archive;/mnt/shared/protofiles</ProtoPath>  
        <ProtoFile>proto-archive/person.proto</ProtoFile>  
        <Type>MyNamespace.Message</Type>  
    </MessageType>  
</MessageTypes>
```

#### Struct Message Type Option

**`Field`** (required) — Defines binary format and AMPS identifier: `/fieldName = format_specifier`. Fields are interpreted in definition order.

#### Custom Message Types

```text
<MessageTypes>  
    <MessageType>  
        <Name>custom-payload</Name>  
        <Module>type-module</Module>  
    </MessageType>  
</MessageTypes>
```

### Default Message Types

| Message Type | Description |
| --- | --- |
| `bson` | Binary JSON |
| `bflat` | Schemaless key-value format with binary numeric support |
| `fix` | FIX numeric-tag messages |
| `json` | JSON messages |
| `msgpack` | MessagePack serialization format |
| `nvfix` | NVFIX (name/value FIX), tags can contain any byte not `=` or field separator |
| `xml` | XML, preserves element names/attributes, 64 levels nesting limit |
| `binary` | Uninterpreted binary payload. No content filtering, views, aggregates, delta messaging, `/AMPS/.*` subscriptions, or `stats` acks |
| `protobuf` | Google protocol buffers (v2/v3). Requires `MessageType` configuration |
| `struct` | Binary C `struct` format. Requires `MessageType` configuration |

`protobuf` and `struct` require additional configuration. AMPS parses messages only as needed and may not detect corruption beyond the parsed portion.

| Message Type Name | Description |
| --- | --- |
| `composite-global` | Combines message parts into single XPath set (no part distinction) |
| `composite-local` | Combines message parts, preserving part-position prefixes in XPath |

### MessagePack Messages

| MessagePack Type | AMPS Representation |
| --- | --- |
| nil | NULL |
| bool | Boolean |
| int (all widths) | Integer |
| float (all widths) | Float |
| str (all widths) | String |
| bin (all widths) | String |
| array (all widths) | Array of AMPS values |
| map (all widths) | Nested AMPS values |
| ext (all widths) | String |

### Protobuf Message Types

AMPS supports Google protobuf v2 and v3. Each message type requires a separate `MessageType` definition with `.proto` file references.

### Filtering with Protobuf Messages

Use member names as XPath identifiers. For nested messages, chain names: `/person/personID > 1000`.

### Working with Multiple Protocol Buffer Types

Two approaches:
1. **Separate types** — Each type on a separate connection. Independent `.proto` maintenance.
2. **Container type** — Single connection using a `oneof` containing the needed types.

```json
message Container {  
    oneof {  
      Order      order_type = 1;  
      Payment    payment_type = 2;  
    }  
}  
message Order { required string customer_id = 1; ... }  
message Payment { required string customer_id = 1; ... }
```

### Union Types

Navigate union by top-level element names: `/order_type IS NOT NULL`, `/payment_type/customer_id = '42'`.

### Protobuf Message Type Limitations

- No View with `protobuf` as destination `MessageType` (can aggregate and project as another type)
- No aggregated subscription on `protobuf` topics
- No subscriptions to AMPS internal topics (`/AMPS/ClientStatus`)
- No enrichment or preprocessing
- Proto v3: no delta publish/subscribe (fixed defaults make missing fields ambiguous)
- Proto v2: no select lists (fields can be marked required)

### Working with Optional Default Values

- Most uses: AMPS treats optional default values as present with the default value. Works for filtering, SOW keys, aggregation.
- Delta messaging (proto v2): AMPS treats optional defaults as absent. Must explicitly provide the default value in serialized message to set it. Unchanged non-default values are not emitted.

### Struct Message Types

#### Configuring a Struct Message Type

Field specifier syntax: `field_name = data_format_specifier`. Fields in declaration order. Format: optional byte order + data type specifier + optional count.

| Specifier | C Type | Size (bytes) | AMPS Type |
| --- | --- | --- | --- |
| `x` | n/a | must be specified | padding (ignored) |
| `c` | `char` | 1 (may specify count) | string |
| `b` | `signed char` | 1 | integer |
| `B` | `unsigned char` | 1 | integer |
| `?` | `bool` | 1 | boolean |
| `h` | `short` | 2 | integer |
| `H` | `unsigned short` | 2 | integer |
| `i` | `int` | 4 | integer |
| `I` | `unsigned int` | 4 | integer |
| `l` | `long` | 4 | integer |
| `L` | `unsigned long` | 4 | integer |
| `q` | `long long` | 8 | integer |
| `Q` | `unsigned long long` | 8 | integer |
| `n` | `ssize_t` | platform | integer |
| `N` | `size_t` | platform | integer |
| `f` | `float` | 4 | double |
| `d` | `double` | 8 | double |
| `s` | `char[]` | must be specified | string (all bytes) |
| `S` | `char[]` | must be specified | string (up to first NULL or count) |
| `p` | `uint8_t` + `char[]` | must be specified | string (first byte = length) |

Byte order specifiers:

| Specifier | Byte Order |
| --- | --- |
| `@` | Native (little-endian) |
| `=` | Native (little-endian) |
| `<` | Little-endian |
| `>` | Big-endian |

Default: little-endian.

```text
<MessageType>  
   <Name>sample_struct_type</Name>  
   <Module>struct</Module>  
   <Field>/id = i</Field>  
   <Field>/ignored = 4x</Field>  
   <Field>/price = f</Field>  
   <Field>/ignored = 32x</Field>  
   <Field>/code = 16s</Field>  
</MessageType>
```

### Limitations of Struct Message Types

- No View with `struct` as destination `MessageType`
- No aggregated subscription on `struct` topics
- No subscriptions to AMPS internal topics
- No enrichment or preprocessing
- No delta publish/subscribe
- No select lists

### Conflated Subscriptions

AMPS retains messages for a subscription for a *conflation interval*, delivering at most one update per unique key per interval. The latest message replaces earlier ones.

```json
{ "tickerId" : "IBM", "price" : 150.34 }  
{ "tickerId" : "IBM", "price" : 149.76 }  
{ "tickerId" : "IBM", "price" : 149.32 }  
{ "tickerId" : "IBM", "price" : 151.10 }
```

Delivers only: `{ "tickerId" : "IBM", "price" : 151.10 }`

Conflated subscriptions do not guarantee delivery order. The `timestamp` option provides the timestamp of the first message conflated.

### When to Use Conflated Subscriptions

- Network bandwidth is at a premium
- Each subscription has different conflation needs (different intervals or fields)
- Conflation needs are predictable and consistent

### Requesting Conflation on a Subscription

| Option | Description |
| --- | --- |
| `conflation=n` | Enables conflation. Value: time interval (`100ms`, `1s`, `1m`), `auto`, or `none`. Default: `none`. |
| `conflation_key=[keys]` | Comma-delimited XPath identifiers in brackets for message uniqueness. Default: SOW key fields for SOW topics. Required for non-SOW topics. Invalid with `oof` unless keys match topic keys. Requires `conflation` to be set. |

Example: `conflation=10s,conflation_key=[/orderId]`

### Filtering Subscriptions by Content

AMPS routes messages based on content using XPath identifiers and SQL-92 operators.

```text
(/Order/Instrument/Symbol == 'IBM') AND  
(/Order/Px >= 90.00 AND /Order/Px < 91.00)
```

```text
(/FIXML/Order/Instrmt/@Sym == 'IBM') AND  
(/FIXML/Order/@Px >= 90.00 AND /FIXML/Order/@Px < 91.0)
```

```text
/35 < 10 AND /34 == /9
```

### Messages in AMPS

Each AMPS message has a type, headers, and optional payload. Headers are formatted per protocol (standard `amps` protocol uses JSON). Payload format is per message type.

Max message size: **200MB**.

### Introduction to AMPS Headers

| Header | Description |
| --- | --- |
| Topic | Topic the message applies to |
| Command | Command type (`sow`, `publish`, `ack`, etc.) |
| CommandId | Correlates responses with commands. Used to update/remove subscriptions. |
| SowKey | SOW record identifier. Included by default, omitted with `no_sowkey` option. |
| CorrelationId | User-specified identifier. Limited to Base64 characters. AMPS does not interpret. |
| Status | `ack` result (e.g., `Success`, `Failure`) |
| Reason | `ack` reason for status |
| Timestamp | ISO-8601 time AMPS processed the message. Requires `timestamp` option on subscription/query. |

### Message Ordering

AMPS guarantees per-instance total order per topic: each subscription receives messages in the order AMPS received them. Exceptions: redelivered queue messages, query results.

This guarantee does **not** apply to views, queues, or conflated topics across topics. Views produce messages per-topic as soon as computed.

### Replicated Message Ordering

Each instance records messages in local transaction log order. Replication preserves per-publisher order but does not enforce global total ordering across instances.

### Replacing Subscriptions

AMPS supports atomic subscription replacement (filter, topic, or options) using the same CommandId. No missed or duplicate messages. For `sow_and_subscribe`, AMPS re-runs the SOW query and provides new messages.

### Replacing the Content Filter on a Subscription

```text
/region = 'WesternUS'
```

replaced with:

```text
/region IN ('WesternUS', 'Alaska', 'Hawaii')
```

### Replacing the Topic on a Subscription

AMPS re-evaluates the subscription. If updated topic lacks subscribe permission, replacement succeeds but no messages are delivered for that topic.

### Replacing the Options on a Subscription

New options apply from the replace point forward. No replay of previous messages. Paginated subscriptions must include full pagination options; topic cannot be changed.

### Select Lists

Select lists control which fields are retrieved, specified as `select=[field_directives]`.

Each directive: inclusion specifier (`+` include, `-` exclude) + AMPS identifier.

Special directives: `-/` (exclude all), `+/` (include all). Undirected fields are included. Most specific directive wins. First match wins for identical identifiers.

```text
select=[-/,+/id,+/complaint]
```

```text
select=[-/,+/name,+/pocket_contents/left]
```

```text
select=[-/pocket_contents]
```

Not available for `struct` message types.

### Topics

Topic = string identifying a subject for routing. AMPS supports regex topic matching for subscriptions. Publishers must use literal topic names.

Topics beginning with `/AMPS` are reserved. Max message size: 200MB. Each topic has an associated message type; connections can only publish/subscribe to topics of the same message type.

Regex topic examples:

| Topic | Behavior |
| --- | --- |
| `^trade$` | Matches only "trade" |
| `^client.*` | Matches "client", "clients", "client001", etc. |
| `.*trade.*` | Matches "NYSEtrades", "ICEtrade", etc. |
| `trade.info` | Matches "trade/info", "trade-info", etc. |

Use `non_regex_topic` option to disable regex interpretation.

### Ad Hoc Topics

Unconfigured topics work for pub/sub but provide no persistence, replay, aggregation, SOW, or other stateful features.

### State of the World (SOW)

SOW persists the most recent update per unique message per topic. Subscribers can query current state. SOW topics support: caching, snapshots, key/value stores, aggregation, delta messaging, out-of-focus notifications, historical queries.

SOW can be persistent (filesystem, survives restart) or transient (in-memory, rebuilt from transaction log on restart).

### Configuring the State of the World (SOW)

SOW section supports: `Topic` (last-value cache), `Queue`/`LocalQueue`/`GroupLocalQueue`, `View` (aggregation/joins), `ConflatedTopic`.

### Configuring Topics in a SOW

**`Name`** (required) — SOW topic name. `Topic` accepted as synonym for pre-5.0 compatibility. If `Pattern` is used, `Name` defines physical topic only.

**`MessageType`** (required) — Must support content filtering for AMPS-generated SOW keys. `binary` only works with explicit keys.

```xml
<SOW>  
    <Topic>  
        <Name>orders</Name>  
        <Key>/orderId</Key>  
        <MessageType>nvfix</MessageType>  
        <FileName>./sow/%n.sow</FileName>  
    </Topic>  
</SOW>
```

#### Storage and Recovery

**`FileName`** (required if `Durability` is `persistent`) — SOW data file path. `%n` replaced with `Name` and `MessageType`. Must be unique per topic and per AMPS instance.

**`Durability`** — `persistent` (default, filesystem) or `transient` (in-memory, rebuilt from txlog on restart). Synonym: `Duration`. Default: `persistent`.

**`RecoveryPoint`** — For txlog-covered topics, recovery start point: `epoch` (default) or `now`.

**`Expiration`** — Record lifetime. Accepts interval format, `disabled` (default, no expiration), or `enabled` (per-message expiration). Must be `disabled` if `History` is enabled.

#### Record Identity Definition

**`Key`** — XPath identifier(s) for SOW key generation. Multiple `Key` elements create composite keys. No default.

**`KeyDomain`** — Seed for SOW key generation. Default: topic `Name`. Set to same value across topics for consistent keys for same field values. Only valid with `Key` fields.

**`KeyGenerator`** — Custom SOW key generator module. Contains `Module` (required) and `Options` elements.

#### Memory and File Growth

**`SlabSize`** — Allocation size in bytes. Max message size guarantee. Default: `5MB`. Maximum: `1GB`.

**`InitialSlabCount`** — Slabs allocated on startup. Default: `1`. Maximum: `1024`.

#### Indexing Options

**`HashIndex`** — Contains `Key` element(s) for exact-match hash index. Used for exact string match on all index fields. Not used for range/regex queries. AMPS auto-creates hash index for SOW `Key` fields.

**`Index`** — Pre-creates memo indexes on specified fields at startup.

**`ExpectedKeyCountHint`** — Pre-sizes internal structures. AMPS rounds up to power of 2. Does not limit key count. No default.

```xml
<SOW>  
    <Topic>  
        <Name>customers</Name>  
        <Key>/customerId</Key>  
        <MessageType>json</MessageType>  
        <FileName>./sow/%n.sow</FileName>  
        <HashIndex>  
            <Key>/customerName</Key>  
        </HashIndex>  
        <HashIndex>  
            <Key>/zipCode</Key>  
            <Key>/customerType</Key>  
        </HashIndex>  
    </Topic>  
</SOW>
```

#### Historical Query

**`History`** element requires:

- **`Window`** (required) — History retention period (e.g., `1w`).
- **`Granularity`** (required) — Resolution of state saves. `0s` = every update. `1m` = at most once per minute.

```xml
<SOW>  
    <Topic>  
        <Name>catalog</Name>  
        <Key>/sku</Key>  
        <MessageType>json</MessageType>  
        <FileName>./sow/%n.sow</FileName>  
        <History>  
            <Window>7d</Window>  
            <Granularity>15m</Granularity>  
        </History>  
    </Topic>  
</SOW>
```

#### Message Enrichment

**`Preprocessing`** — Enrichment before SOW key determination. Contains `Field` elements.
**`Enrichment`** — Enrichment after SOW key determination. Contains `Field` elements.

```xml
<SOW>  
    <Topic>  
        <Name>sales-reps</Name>  
        <Key>/employeeId</Key>  
        <MessageType>bflat</MessageType>  
        <Enrichment>  
            <Field>CONCAT(/firstName, " ", /lastName) AS /fullName</Field>  
        </Enrichment>  
    </Topic>  
</SOW>
```

#### Multiple Logical Topics in One Physical Topic

**`Pattern`** — Regex pattern for storing multiple logical topics in one physical SOW file. Cannot specify `History`. Legacy protocols do not support querying/subscribing to pattern topics.

### How Does the SOW Work

SOW key = unique identifier per record (like a primary key). First publish = insert; same key = update (full record replacement). SOW key is distinct from message content; stored with record.

SOW key generation methods:
1. AMPS-generated from `Key` fields (recommended) — checksum of key domain + field values
2. Publisher-provided SOW key (no `Key` or `KeyGenerator` configured)
3. Custom `KeyGenerator` module

Persistent SOW: memory-mapped file, survives restart, separate from txlog. On restart, AMPS replays txlog entries newer than SOW. Transient SOW: rebuilt from txlog, recovery starts at `RecoveryPoint` (default: `epoch`).

Each persistent topic requires a separate file. Only one AMPS instance per file.

### Storing Multiple Logical Topics in One Physical Topic

`Pattern` element creates a container SOW topic. All logical topics share the same configuration. Single in-memory SOW + single file. Efficient for many topics with few records each.

### Limitations When Storing Multiple Logical Topics in a Physical Topic

- `sow`/`sow_and_subscribe` with regex: messages delivered within single `group_begin`/`group_end` pair, order not guaranteed
- Cannot be underlying topic for a view
- Can be underlying topic for a conflated topic (must be configured)
- All logical topics share the same permissions

### Configuration File Precedence

Standalone definitions matching a `Pattern` must appear **before** the pattern definition.

```xml
<SOW>  
  <Topic>  
     <Name>/orders/specialHandling</Name>  
     <MessageType>json</MessageType>  
     <Key>/orderId</Key>  
     <Preprocessing>  
       <Field>COALESCE(/orderId,  
                  CONCAT(/customerName, /customerSerialNumber)) as /orderId</Field>  
     </Preprocessing>  
     <FileName>./sow/%n.sow</FileName>  
  </Topic>  
  <Topic>  
     <Name>RegexOrders</Name>  
     <Pattern>^/orders/</Pattern>  
     <MessageType>json</MessageType>  
     <Key>/orderId</Key>  
     <FileName>./sow/%n.sow</FileName>  
  </Topic>  
</SOW>
```

| Message Published to Topic | Results |
| --- | --- |
| `/orders/specialHandling` | Matches standalone `Topic`, preprocessing runs |
| `/orders/RHAT` | Matches regex topic |
| `/orders/specialHandling/oops` | Matches regex topic (standalone is exact match only) |
| `/customer/orders/timothy_someone` | No match, not in SOW |

### Entitlements for Logical Topics

All logical topics share permissions of the physical topic `Name`. To restrict access: create a separate `Pattern` topic or use entitlement filters with `TOPIC_NAME()`.

### Indexing SOW Topics

1. **Memo indices** — Auto-created on first use. Support all query types (regex, range, comparisons). Can pre-create with `Index` directive.
2. **Hash indices** — Configured, significantly faster. Exact string match or `IN` operator only, on exact set of index fields. Auto-created for SOW `Key` fields.

Hash index requirements: all fields in index must be matched with exact string comparison (not numeric). AMPS 5.3.1.0+: compound filters use hash index if first clause is `IN` and remaining clauses use `AND`.

### AMPS-Generated SOW Keys

Based on `Key` fields + key domain (default: topic name). AMPS concatenates values with separator, calculates checksum. Composite keys via multiple `Key` elements.

`KeyDomain` allows consistent keys across different topics for the same field values.

Preprocessor enrichment runs before SOW key generation.

Custom key generator:

```xml
<AMPSConfig>  
    <Modules>  
        <Module>  
            <Name>key-generator</Name>  
            <Library>libmy_key_generator.so</Library>  
        </Module>  
    </Modules>  
    <SOW>  
        <Topic>  
            <Name>custom-keyed-sow</Name>  
            <FileName>./sow/%n.sow</FileName>  
            <KeyGenerator>  
                <Module>key-generator</Module>  
                <Options>  
                    <OptionOne>module-specific-option</OptionOne>  
                    <OptionTwo>another-specific-option</OptionTwo>  
                </Options>  
            </KeyGenerator>  
        </Topic>  
    </SOW>  
</AMPSConfig>
```

### User-Generated SOW Keys

Publisher provides SOW key. No `Key` or `KeyGenerator` in topic config. Keys must be Base64 characters. All publishers must use consistent generation. Particularly useful for `binary` message type.

### Programmatically Deleting Records from the Topic State

`sow_delete` command. Three methods:
1. Content filter (e.g., `1=1` for all)
2. SOW key list
3. Message data (AMPS parses to derive SOW key)

Deletion sends OOF messages, updates views, removes from conflated topics at next interval. With `History`, only current value is removed; historical state preserved.

Most efficient: delete by SOW key or by filter matching primary key / hash index.

### SOW Maintenance

Two aspects:
1. Capacity planning (disk, memory)
2. Data retention policy

Retention approaches:
- Stable-size topics: no explicit management needed
- Time-limited data: per-message expiration (`Expiration` config)
- Condition-based: application-initiated `sow_delete` or scheduled maintenance actions

### Setting Per-Message Lifetime

Expiration disabled by default. Two levels:
- Topic-level default via `Expiration` config
- Per-message via message header

Per-message expiration overrides topic default. No expiration set = no expiry. Expiration of `0` in header = no expiry for that message.


## Enabling Expiration for a Topic

AMPS supports default message expiration for SOW topics via `<Expiration>`.

```xml
<SOW>
    <Topic>
        <Name>ORDERS</Name>
        <FileName>sow/%n.sow</FileName>

        <Expiration>30s</Expiration>

        <Key>/55</Key>
        <Key>/109</Key>
        <MessageType>fix</MessageType>
  </Topic>
</SOW>
```

Messages without an expiration use the topic default. Message-level expiration overrides the default. Each publish or delta publish resets the expiration time.

To enable expiration only for messages that have message-level expiration set:

```xml
<SOW>
    <Topic>
        <Name>ORDERS</Name>
        <FileName>sow/%n.sow</FileName>

        <Expiration>enabled</Expiration>

        <Key>/55</Key>
        <Key>/109</Key>
        <MessageType>fix</MessageType>
    </Topic>
</SOW>
```

With this configuration, expiration is enabled but the lifetime must be specified on each message. When expiration is disabled, AMPS preserves message-level expiration settings but does not expire messages. AMPS processes expirations during startup.

## Setting Expiration for a Message

When expiration is enabled for a topic, messages expire at the configured default unless overridden by the message. A message-level expiration value overrides the topic default; `0` disables expiration for that message.

AMPS calculates and stores an expiration timestamp with each message. Updates reset the expiration lifespan. When a message expires, it is deleted from the SOW (triggering delete processing).

## Recovery and Expiration

AMPS stores an expiration timestamp per message. On recovery, expired messages are removed. Changing the default expiration does not affect timestamps already stored. If expiration is disabled for the topic, messages will not expire.

## Replication and Expiration

The expiration time is replicated with each message; downstream instances do not reset it. Expiration is processed locally per instance.

### Creating a Maintenance Schedule for a Topic

For scheduled removal (e.g., end of trading day), use AMPS actions. See the [Actions](/docs/amps-user-guide/actions) documentation.

### Sample Maintenance Plan

The following SOW topic and action remove `ORDERS` records where `/status = 'closed'` and `LAST_UPDATED()` is older than 24 hours, running daily at `02:00`.

```xml
<SOW>
    <Topic>
        <Name>ORDERS</Name>
        <FileName>sow/%n.sow</FileName>
        <Key>/orderId</Key>
        <MessageType>nvfix</MessageType>
  </Topic>
</SOW>
```

```json
<Actions>
  <Action>
    <On>
      <Module>amps-action-on-schedule</Module>
      <Options>
        <Every>02:00</Every>
        <Name>Maintenance for ORDERS topic</Name>
      </Options>
    </On>
    <Do>
      <Module>amps-action-do-delete-sow</Module>
      <Options>
         <Topic>ORDERS</Topic>
         <MessageType>nvfix</MessageType>
         <Filter>/status = 'closed'
                 AND LAST_UPDATED() &lt; ({{AMPS_UNIX_TIMESTAMP}} - 86400)</Filter>
       </Options>
     </Do>
   </Action>
</Actions>
```

### Using the State of the World

SOW topics support queries, atomic query/subscribe, advanced messaging features (OOF, incremental updates, aggregation, delta subscribe), and various application scenarios.

### Queries / Point in Time Database

Clients issue `sow` commands to retrieve messages matching a topic and optional content filter. The topic can be a literal name or regular expression. Results are returned atomically as a `group_begin`, matching records, then `group_end` sequence. Use `QueryId` to correlate requests and responses. Ordering is undefined unless `OrderBy` is specified. AMPS returns an error if queried on a non-SOW topic.

### Query and Subscribe

`sow_and_subscribe` executes a SOW query and atomically subscribes to updates, preventing gaps between query and subscription. The query results are sent first, followed by subscription messages. On non-SOW topics, no messages are delivered between `group_begin` and `group_end`.

### Historical SOW Query and Subscribe

For topics with `History` enabled and recorded in the transaction log, `sow_and_subscribe` can begin with a historical SOW query. AMPS returns the SOW state at the requested point in time, replays transaction log messages from that point, then delivers live messages.

### Conflated Subscriptions with SOW and Subscribe

Conflation options apply to the subscription portion only; SOW query results are not conflated.

### Replacing Subscriptions with SOW and Subscribe

When replacing a `sow_and_subscribe`, AMPS re-runs the SOW query and delivers messages in scope with the new filter but not previously delivered. If OOF is enabled, OOF messages are delivered for messages that matched the previous filter but not the new one.

### Batching Query Results

AMPS supports batching SOW query results via the `BatchSize` parameter (default `1`, client libraries default to `10`). Each response contains a `BatchSize` header indicating messages in the batch. Maximum is `10,000`. Batching improves network utilization and parsing efficiency; larger batches may add latency.

For `sow_and_subscribe`, AMPS returns `group_begin` and `group_end` before the live subscription begins.

### Historical SOW Topic Queries

Historical queries retrieve the SOW state at a specific point in time by providing a timestamp in the `Bookmark` header of a `sow` command. A filter can refine results.

**When to use:**
- Historical SOW query: snapshot at a specific point in time.
- Transaction log replay: exact message sequence without point-in-time query needs.
- Both: historical SOW query plus transaction log replay.

**Configuring History:**

Add the `History` element to the topic configuration with two options:

- `Window`: duration to retain historical versions. Versions older than `Window` are eligible for removal. The most current state is always retained.
- `Granularity`: interval at which historical copies are retained. A value of `0s` retains every change during the `Window`. Deleted messages are recorded as deleted within the `Window`.

A historical `sow_and_subscribe` returns the SOW state at the next oldest granularity, then replays from the transaction log. The transaction log and SOW history are maintained separately. Topics with history do not support SOW expiration; use explicit deletes or scheduled actions for cleanup.

**Pagination with Historical SOW Queries**

Topics with `History` support paginated queries from a point in time. When covered by the transaction log, `sow_and_subscribe` also supports paginated subscriptions from a point in time.

### Managing Result Sets

SOW query options and headers:

| Option / Header | Result |
| --- | --- |
| `top_n` (option) | Limits results to the specified number. Creates a paginated subscription when combined with `skip_n`. |
| `skip_n` (option) | Skips the specified number of messages before returning results. Must be used with `top_n`. |
| `OrderBy` (command header) | Orders results. Requires comma-separated identifiers of the form `/field [ASC|DESC]`. |

When replacing a subscription with `top_n`, `skip_n`, or `OrderBy`, those options must be provided on the replacement command.

### Replication and Expiration: Paginated SOW and Subscribe

When `top_n` and `skip_n` are specified on `sow_and_subscribe`, AMPS creates a *paginated subscription*. AMPS maintains the result set and delivers only records within the window. If `OrderBy` is specified, records are sorted accordingly; otherwise by `SowKey`.

Subscribers receive messages only within the pagination window. OOF notifications are sent when messages leave the window. AMPS shares the result set memory across subscriptions using the same topic; per-subscription window state is separate.

### Aggregated SOW Queries

Provide `grouping` and `projection` options with a `sow` query to aggregate results.

| Option | Description |
| --- | --- |
| `grouping=[keys]` | Comma-delimited list of XPath identifiers within brackets. Requires `projection`. Can be used with a bookmark on historical topics. |
| `projection=[fields]` | Comma-delimited set of fields within brackets. Must include every field in the aggregated message. Requires `grouping`. Can be used with a bookmark on historical topics. |

### Enable Advanced Messaging Features

SOW enables:
- **Out-of-Focus Messages**: subscribe to be notified when a message is removed or no longer matches.
- **Publishing Incremental Updates**: use `delta_publish` to merge partial updates into existing SOW records. See Delta Publish.
- **Aggregation and Analysis**: foundation for views. See Aggregation and Analytics.
- **Receiving Updated Fields Only**: use `sow_and_delta_subscribe` to receive only changed fields. See Receiving Only Updated Fields.

### Application Scenarios

See [When Should I Store a Topic in the SOW](/docs/intro-guide/sow/why_use_sow) and [Scenario and Feature Reference](/docs/intro-guide/feature_guide).

### Command Acknowledgment

AMPS command processing is asynchronous. Acknowledgments (`ack`) are optional and returned at various checkpoints. The AMPS client libraries request necessary acknowledgments automatically.

| Acknowledgment Type | Description |
| --- | --- |
| `completed` | The command (or portion) has completed. |
| `persisted` | Results persisted to durable storage. |
| `processed` | AMPS has processed the command. |
| `received` | AMPS has received the command. |
| `stats` | Statistics associated with the command. |

Acknowledgments may not arrive in command submission order. For example, a synchronous replication `publish` may delay `persisted` until replication destinations acknowledge, while a subsequent `subscribe` `processed` ack may return earlier. See the [AMPS Command Reference](/docs/amps-command-reference) for per-command details.

### Bookmark Subscriptions and Completed Acknowledgments

For bookmark subscriptions, `completed` indicates replay from the transaction log has finished. Messages after `completed` are new publishes.

### Bookmark Subscriptions and Persisted Acknowledgments

Bookmark subscriptions typically request `persisted` (clients do this automatically). AMPS periodically returns the last bookmark written to the local transaction log and acknowledged by all `sync` replication destinations. This aids recovery and failover.

The bookmark in the persisted ack is the last persisted point; it does not need to match the subscription filter.

### Acknowledgment Conflation and Publish Acknowledgments

AMPS conflates acknowledgments for some commands. Conflated `persisted` acks for `publish` and `sow_delete` contain the last client sequence number applied rather than individual command identifiers. Default conflation interval is approximately one second.

Configuration options:
- Per-client: `ack_conflation` option in logon options string.
- Replication: `AckConflationInterval` in the replication `Destination`.

To reduce latency, both client and server options typically need adjustment. Client libraries use `persisted` acknowledgments to manage reliable publishing when a publish store is configured.

### Receiving Acknowledgments

Acknowledgments are delivered in the message stream for the command with command type `ack`. The `CommandId` matches the client's command identifier. When acknowledgments are conflated, no `CommandId` is included.

### Requesting Acknowledgments

Acknowledgments are optional. Client libraries request and process required acknowledgments automatically. Applications can request additional acknowledgments explicitly.

Non-conflated acknowledgments are delivered to the command's message handler. Conflated acknowledgments must be handled by a last chance or global command type handler.

### Conflated Topics

A *conflated topic* reduces update frequency by retaining updates for a configured interval and delivering the latest state per message at interval end. Subscribers receive at most one update per message per interval. This is more efficient than per-subscription conflation when all subscribers benefit.

The underlying topic can be a `Topic`, `View`, or another `ConflatedTopic`. Applications cannot publish directly to a conflated topic. SOW keys are preserved from the underlying topic. AMPS indexes conflated topics automatically and supports declared `HashIndex` entries.

Conflation strategy:
- First update for a key begins the interval.
- Subsequent updates replace (subscription) or merge (delta subscription) the pending message.
- OOF notifications for undelivered keys prevent delivery.
- At interval end, the current version is delivered unless suppressed.

### Configuring Conflated Topics in a SOW

Parameters for defining a `ConflatedTopic` (synonym: `ReplicaDefinition`) within the SOW section:

- `Name` (required): topic name. Synonym: `Topic` (for pre-5.0 compatibility).
- `UnderlyingTopic` (required): exact name of the source SOW topic. For regex topics, match the `Name`, not `Pattern`; use `TopicFormat` to name resulting topics.
- `MessageType` (required): must match the underlying topic's message type.
- `Interval`: update frequency. Default `5s`, minimum `100ms`.
- `Filter`: content filter applied to the underlying topic.
- `HashIndex`: fast lookup indexes. Key fields are specified with `Key` elements. AMPS auto-creates a hash index for the underlying topic's `Key` fields.
- `Enrichment`: message enrichment fields. Must contain one or more `Field` elements. `OF PREVIOUS` is not supported in enrichment fields.
- `TopicFormat` (required if `UnderlyingTopic` uses `Pattern`): format string containing `%n` replaced with the underlying topic name. Cannot contain regex metacharacters.

Example:

```text
<ConflatedTopic>
    <Name>FastPublishTopic-C</Name>
    <MessageType>nvfix</MessageType>
    <UnderlyingTopic>FastPublishTopic</UnderlyingTopic>
    <Interval>5s</Interval>
    <Filter>/region = 'A'</Filter>
</ConflatedTopic>
<ConflatedTopic>
    <Name>LongIntervalTopic-C</Name>
    <MessageType>json</MessageType>
    <UnderlyingTopic>FastPublishTopic</UnderlyingTopic>
    <Interval>120s</Interval>
    <HashIndex>
       <Key>/order/status</Key>
    </HashIndex>
</ConflatedTopic>
<ConflatedTopic>
    <Name>ConflatedEnrichmentTopic</Name>
    <MessageType>json</MessageType>
    <UnderlyingTopic>market</UnderlyingTopic>
    <Interval>1s</Interval>
    <Enrichment>
       <Field>UPPER(/ticker) as /ticker</Field>
    </Enrichment>
</ConflatedTopic>
<ConflatedTopic>
   <Name>ConflateUnderlyingRegex</Name>
   <MessageType>bflat</MessageType>
   <UnderlyingTopic>TheRegexTopic</UnderlyingTopic>
   <Interval>20s</Interval>
</ConflatedTopic>
```

### Delta Publish

`delta_publish` merges a partial message into an existing SOW record. If no record exists, the message is added. The message type and topic must support delta publish. All built-in types except `binary` and `struct` support delta publish; protobuf v3 does not. Composite types support delta publish with `composite-local` definition.

Delta publish behavior:
- Parsed identifiers in the update replace the corresponding field in the existing message. Updating an anonymous element or root of a subdocument replaces the entire value.
- Missing fields mean unchanged; fields cannot be removed via delta publish. To remove a field, republish the full message.
- AMPS does not skip processing when a field value is unchanged; the publish is still processed and delivered.
- Nested elements are processed per compound type rules.

```json
   {"id":42, "contents":{"packages":[{"box":"chocolates"},
                                     {"bowl":"noodles"}]}}
```

An update to `packages` replaces the subdocument:

```json
   {"id":42, "contents":{"packages":[{"basket":"eggs"}]}}
```

Result:

```json
   {"id":42, "contents":{"packages":[{"basket":"eggs"}]}}
```

To remove a field, republish the full message.

| Command | Result |
| --- | --- |
| `delta_publish` | Publish a delta message. If no record exists in the SOW, add the message. If a record exists, merge the data into the existing record. |

**Transaction Log Replay and Delta Publish**

The transaction log stores the fully merged message. Replication replicates the fully merged message.

### Receiving Only Updated Fields (Delta Subscribe)

`sow_and_delta_subscribe` and `delta_subscribe` deliver only changed fields. AMPS compares the new and old message states and sends the difference. The SOW key and changed fields are included. If `oof` is specified, the full message is delivered when a previously out-of-focus message comes into focus.

Delta subscribe is independent of delta publish: subscribers can receive deltas regardless of whether publishers publish full messages or deltas.

| Command | Result |
| --- | --- |
| `delta_subscribe` | Register a delta subscription, starting with newly received messages. |
| `sow_and_delta_subscribe` | Replay the SOW state and atomically register a delta subscription. |

**Delta Subscribe Support**

Message type and topic must support delta subscribe. Built-in types except `binary`, `struct`, and protobuf v3 support it. Composite types support delta subscribe with `composite-local`. Queues and bookmark subscriptions do not support delta subscribe.

**Multiple Subscriptions and Delta Subscribe**

When one connection has multiple subscriptions, AMPS sends a single message containing the data requested by all matching subscriptions. If one matching subscription requests full messages and another requests deltas, the connection receives a full message. Avoid mixing delta and non-delta subscriptions for the same messages on the same connection if deltas are required.

**Conflated Subscriptions and Delta Subscribe**

Conflation merges successive delta messages during the interval. The delivered message is the merge of all deltas. A field is included if it appeared in any delta during the interval, with its final value. Fields unchanged across the interval are omitted.

Example record:

```json
{
    "id": 99,
    "status":"open",
    "notes":"none",
    "xref":82
}
```

Updates during conflation interval:

```json
{"id": 99, "status":"questioned", "notes":"none", "xref":82}
{"id": 99, "status":"questioned", "notes":"jcarlo hold", "xref":82}
{"id": 99, "status":"cleared", "notes":"none", "xref":82}
{"id": 99, "status":"open", "notes":"none", "xref":82}
```

Delivered delta:

```json
{
    "id": 99,
    "status":"open",
    "notes":"none"
}
```

**Identifying Changed Records**

Delta messages contain a `SowKey` header by default. Alternatively, SOW key fields are included in the body when `send_keys` is enabled (default since AMPS 4.0 unless `no_empties` is used). OOF notifications are supported for delta subscriptions.

**Receiving Only Changes that Update Values**

By default, delta subscriptions send a message for every publish, even if no field values change. Use the `no_empties` option to suppress messages when no data fields have changed.

**Options for Delta Subscribe**

| Option | Result |
| --- | --- |
| `no_empties` | Do not send messages if no data fields have been updated. |
| `no_sowkey` | Do not include the AMPS generated SowKey with messages. |
| `send_keys` | Include the SOW key fields in the message. Default behavior unless `no_empties` is used. |
| `oof` | Deliver out of focus messages. Also delivers the full message when a previously out-of-focus message comes into focus. |

Delta subscriptions also support regular subscription options, including timestamp and conflation.

**Select Lists and Delta Subscribe**

When a `delta_subscribe` or `sow_and_delta_subscribe` provides a select list and `no_empties`, only changes to selected fields trigger delivery.

### Aggregation and Analytics

AMPS provides a high-performance aggregation engine for projecting SOW topics, similar to `CREATE VIEW` in RDBMS. Views support delta subscriptions, OOF tracking, and can underlie other views. For ad hoc or single-subscriber aggregation, use aggregated subscriptions.

### Aggregated Subscriptions

Aggregated subscriptions compute aggregates per subscription. They are appropriate for unique/unpredictable needs, rapid development, or infrequently used expensive aggregations. Otherwise, use Views.

To request an aggregated subscription, provide `projection` and `grouping` options.

| Option | Description |
| --- | --- |
| `projection=[field specifications]` | Comma-delimited fields within brackets. Must include every field in the aggregated message. When a field is not grouped or aggregated, the last processed value is used. |
| `grouping=[keys]` | Comma-delimited XPath identifiers within brackets. |

Example:

```text
projection=[COUNT(/orderId)AS /orderCount, /customer AS /customer],grouping=[/customer]
```

**When to Use Aggregated Subscriptions**

- Unique and unpredictable aggregation needs.
- Rapid development/iteration before defining a view.
- Expensive, seldom-needed aggregations.

**Considerations for Aggregated Subscriptions**

- Source is a single SOW topic of the same message type as the output. Topic must not be a regex.
- Subscribing to a queue browses without removing messages.
- Filters apply to original messages, not projected results.
- `replace` is supported only for changing pagination options.
- Cannot be a bookmark subscription.
- Select lists cannot be used.

### Configuring Views in a SOW

Parameters for defining a `View` (synonym: `ViewDefinition`) within the SOW section:

- `Name` (required): topic name. Synonym: `Topic` (pre-5.0 compatibility).
- `MessageType` (required): output message type. Must support views (`binary` and some others are unsupported as output types).
- `UnderlyingTopic` (required): single topic name or `Join` elements.
- `Projection/Field` (required): fields to project. Can include aggregation functions.
- `Grouping/Field` (required): grouping fields.
- `KeyDomain`: seed for `SowKeys`. Default is topic name.
- `Join`: join specifications for multiple topics.
- `Conflation`: `none` (default) or `inline` to conflate updates.
- `Filter`: filter for single-topic views only.
- `HashIndex`: fast lookup indexes. AMPS auto-creates an index for `Grouping` fields.
- `JoinNullEquivalency`: `enabled` to treat NULL, empty string, and missing values as equivalent in joins. Default `disabled`.
- `FileName`: unused in this version.

Example:

```xml
<SOW>
    <!-- Single topic aggregation. -->
    <Topic>
        <Topic>/ett/order</Topic>
        <MessageType>fix</MessageType>
        <Key>/orderId</Key>
    </Topic>
    <View>
        <MessageType>nvfix</MessageType>
        <Topic>TOTAL_VALUE</Topic>
        <UnderlyingTopic>/ett/order</UnderlyingTopic>
        <Projection>
            <Field>/109</Field>
            <Field>SUM(/14 * /6) AS /71406</Field>
        </Projection>
        <Grouping>
            <Field>/109</Field>
        </Grouping>
    </View>

    <!-- Single topic aggregation with filter. -->
    <Topic>
        <Name>orders</Name>
        <MessageType>json</MessageType>
        <Key>/orderId</Key>
        <FileName>./sow/%n.sow</FileName>
    </Topic>
    <View>
        <Name>CompleteByRegion</Name>
        <UnderlyingTopic>orders</UnderlyingTopic>
        <MessageType>json</MessageType>
        <Projection>
            <Field>COUNT(/orderId) AS /completedOrders</Field>
            <Field>/region AS /region</Field>
        </Projection>
        <Grouping>
            <Field>/region</Field>
        </Grouping>
        <Filter>/status = 'complete'</Filter>
    </View>

  <!-- Single topic aggregation with a hash index
       for faster query. -->
  <Topic>
      <Name>source-for-hash-sample</Name>
      <MessageType>json</MessageType>
      <Key>/id</Key>
      <FileName>./sow/%n.sow</FileName>
  </Topic>
  <View>
      <Name>hash-sample-view</Name>
      <MessageType>json</MessageType>
      <UnderlyingTopic>source-for-hash-sample</UnderlyingTopic>
      <Projection>
         <Field>SUM(/qty) as /quantity</Field>
         <Field>/customerName</Field>
         <Field>/orderType</Field>
       </Projection>
       <Grouping>
         <Field>/customerName</Field>
         <Field>/orderType</Field>
       </Grouping>
       <!-- Provide fast query for exact matches on
            *either* name or orderType. -->
       <HashIndex>
          <Key>/name</Key>
       </HashIndex>
       <HashIndex>
          <Key>/orderType</Key>
       </HashIndex>
   </View>

    <!-- Project from one message type to another. -->
    <Topic>
        <Name>example</Name>
        <MessageType>json</MessageType>
        <Key>/id</Key>
        <FileName>./sow/%n.sow</FileName>
    </Topic>
    <View>
        <!-- Notice that the nvfix topic named 'example' is not the
             same topic as the json topic named 'example'. -->
        <Name>example</Name>
        <MessageType>nvfix</MessageType>
        <UnderlyingTopic>[json].[example]</UnderlyingTopic>
        <Projection>
            <Field>[json].[example]./id AS /id</Field>
        </Projection>
        <Grouping>
            <Field>[json].[example]./id</Field>
        </Grouping>
    </View>

   <!-- JOIN topics -->
   <Topic>
        <Name>ORDERS</Name>
        <MessageType>nvfix</MessageType>
        <Key>/OrderID</Key>
        <FileName>./sow/%n.sow</FileName>
    </Topic>
    <Topic>
        <Name>COMPANIES</Name>
        <MessageType>nvfix</MessageType>
        <Key>/CompanyId</Key>
        <FileName>./sow/%n.sow</FileName>
    </Topic>
    <View>
        <Name>TOTAL_COMPANY_VOLUME</Name>
        <UnderlyingTopic>
            <Join>[ORDERS]./Tick = [COMPANIES]./Tick</Join>
        </UnderlyingTopic>
        <MessageType>nvfix</MessageType>
        <Projection>
            <Field>[COMPANIES]./CompanyId</Field>
            <Field>[COMPANIES]./Tick</Field>
            <Field>[COMPANIES]./Name</Field>
            <Field>SUM([ORDERS]./Shares) AS /TotalVolume</Field>
        </Projection>
        <Grouping>
            <Field>[ORDERS]./Tick</Field>
        </Grouping>
    </View>
</SOW>
```

### Constructing Field Contents

View fields use the AMPS expression language. See [AMPS Expressions](/docs/amps-user-guide/amps-expressions) and [Constructing View Fields](/docs/amps-user-guide/builtin_functions/constructing-fields#constructing-view-fields).

### Defining Views and Aggregations

Multiple topic aggregation joins topics to enrich data. Each topic must maintain a SOW. Views can be built from views.

Message type support:
- `binary` cannot be underlying or view type.
- `protobuf` can be underlying but not view type.
- `composite-global` can be underlying but not view type.
- `struct` can be underlying but not view type.

### Single Topic Aggregation: UnderlyingTopic

```text
<UnderlyingTopic>MyOriginalTopic</UnderlyingTopic>
```

### Multiple Topic Aggregation: Join

`Join` relates topics with equality comparisons:

```text
[topic].[field]=[topic].[field]
```

Square brackets around topic are optional unless omitted, in which case the first `/` starts the field. Values are compared as strings. For different message types:

```text
[messagetype].[topic].[field]=[messagetype].[topic].[field]
```

This is equivalent to a `LEFT OUTER JOIN`. NULL values (including empty strings) do not match unless `JoinNullEquivalency` is `enabled`.

Multiple `Join` elements are combined with logical `AND`:

```text
<UnderlyingTopic>
   <Join>[Orders].[/CustomerID]=[Addresses].[/CustomerID]</Join>
</UnderlyingTopic>
```

```text
<Join>[nvfix].[Orders].[/CustomerID]=[json].[Addresses].[/CustomerID]</Join>
<Join>[nvfix].[Orders].[/ItemID]=[nvfix].[Catalog].[/ItemID]</Join>
```

Multi-field join example:

```text
<UnderlyingTopic>
  <Join>[Orders].[/OrderId]=[OrderExtraInfo].[/OrderId]</Join>
  <Join>[Orders].[/OrderType]=[OrderExtraInfo].[/OrderType]</Join>
</UnderlyingTopic>
```

### Setting the Message Type

```text
<MessageType>json</MessageType>
```

The view message type does not need to match underlying types, but if different, fully qualify underlying field references.

### Defining Projections

Specify each projected field with `Projection/Field`:

```text
<Projection>
    <Field>[Orders].[/CustomerID]</Field>
    <Field>[Addresses].[/ShippingAddress] AS /DestinationAddress</Field>
    <Field>SUM([Orders].[/TotalPrice]) AS /AccountTotal</Field>
</Projection>
```

Every field in the view must be explicitly projected. AMPS does not implicitly add fields.

### Data Types and Projections

AMPS converts values to internal types during projection. For types with type markers (e.g., `bson`), projected fields may reflect AMPS internal types rather than original types (typically widening numerics to 64-bit). Complex or nested types are typically projected as string equivalents. Project individual nested fields explicitly when needed.

Example messages:

```json
{"orderId":42, "line":1, "detail":{"product":"AAPL", "qty":40}}

{"orderId":42, "line":2, "detail":{"product":"AAPL", "qty":60}}
```

Projection:

```text
<Projection>
   <Field>/orderId</Field>
   <Field>/detail/product</Field>
   <Field>SUM(/detail/qty) as /detail/qty</Field>
 </Projection>
 <Grouping>
    <Field>/orderId</Field>
    <Field>/detail/product</Field>
 </Grouping>
```

Result:

```json
{"detail":{"product":"AAPL","qty":100.0},"orderId":42}
```

### Grouping

```text
<Grouping>
    <Field>[Orders].[/CustomerID]</Field>
</Grouping>
```

Fields in `Grouping` must be from underlying topics. Each projection field should be an aggregate or in `Grouping`; otherwise AMPS returns the last processed value. Unlike ANSI SQL, AMPS allows non-grouped fields in projections. Upon recovery, AMPS enforces consistent ordering when rebuilding the view.

### Inline Update Conflation

Enable with `<Conflation>inline</Conflation>` in the view configuration. When enabled, if a new update arrives for a group already pending, the pending update is replaced. Not every underlying update produces a view update, and order may differ from publication order, but the final state is consistent.

```xml
<SOW>
    <View>
        ...
        <Conflation>inline</Conflation>
        ...
    </View>
</SOW>
```

### Filtering Single Topic Aggregations

`Filter` is supported only for single-topic views:

```xml
<SOW>
    ...
    <Topic>
        <Name>orders</Name>
        <MessageType>json</MessageType>
        <Key>/orderId</Key>
        <FileName>./sow/%n.sow</FileName>
    </Topic>
    <View>
        <Name>CompleteByRegion</Name>
        <UnderlyingTopic>orders</UnderlyingTopic>
        <MessageType>json</MessageType>
        <Projection>
            <Field>COUNT(/orderId) AS /completedOrders</Field>
            <Field>/region AS /region</Field>
        </Projection>
        <Grouping>
            <Field>/region</Field>
        </Grouping>
        <Filter>/status = 'complete'</Filter>
    </View>
    ...
</SOW>
```

### Understanding Views

Views aggregate one or more SOW topics and present results as a new SOW topic. They are updated asynchronously after each underlying publish/delta publish is persisted. Views can underlie other views. Underlying topics must be defined before the view. Queues as underlying topics show only unleased messages.

### Best Practices for Views

- Minimize view count and calculation expense.
- For joins, consider update fan-out: updating a topic with few messages that join to many messages in another topic generates many view updates.
- Use `<Conflation>inline</Conflation>` when subscribers only need final states for high-velocity records.
- Avoid republishing unchanged values to underlying topics.

### View Examples

#### Simple Aggregate View Example

Schema for `ORDERS`:

| NVFIX Tag | Description |
| --- | --- |
| OrderID | Unique order identifier |
| Tick | Symbol |
| ClientId | Unique client identifier |
| Shares | Currently executed shares for the chain of orders |
| Price | Average price for the chain of orders |

Equivalent SQL:

```sql
CREATE VIEW TOTAL_VALUE AS
SELECT ClientId, SUM(Shares * Price) AS TotalCost,
                 SUM(Shares * Price)/SUM(Shares) AS WeightedAveragePrice
FROM ORDERS
GROUP BY ClientId
```

AMPS configuration:

```xml
<SOW>
    <Topic>
        <Name>ORDERS</Name>
        <MessageType>nvfix</MessageType>
        <Key>/OrderID</Key>
        <FileName>./sow/%n.sow</FileName>
    </Topic>
    <View>
        <Name>TOTAL_VALUE</Name>
        <UnderlyingTopic>ORDERS</UnderlyingTopic>
        <MessageType>nvfix</MessageType>
        <Projection>
            <Field>/ClientId</Field>
            <Field>SUM(/Shares * /Price) AS /TotalCost</Field>
            <Field>SUM(/Shares * /Price) / SUM(/Shares) AS /WeightedAveragePrice</Field>
        </Projection>
        <Grouping>
            <Field>/ClientId</Field>
        </Grouping>
    </View>
</SOW>
```

Non-grouped projection fields use the last processed value. A zero or null in an aggregate field usually means the value is zero or `NaN` (numeric aggregates return `NaN` if a non-numeric field is included).

Special characters in projections must be escaped with XML entities or wrapped in CDATA:

```xml
<Field><![CDATA[SUM(IF(/shares * /price > 1000000, /shares * /price, NULL)) AS /AggregateValue]]></Field>
<Field>SUM(IF(/Shares * /Price &gt; 1000000, /Shares * /Price, NULL)) AS /AggregateValue2</Field>
```

#### Multiple Topic Aggregate Example

Schema for `COMPANIES`:

| NVFIX Tag | Description |
| --- | --- |
| CompanyId | Unique identifier for the company |
| Tick | Symbol |
| Name | Company name |

Equivalent SQL:

```sql
CREATE VIEW TOTAL_COMPANY_VOLUME AS
SELECT COMPANIES.CompanyId, COMPANIES.Tick, COMPANIES.Name, SUM(ORDERS.Shares) AS TotalVolume
FROM COMPANIES LEFT OUTER JOIN ORDERS
    ON COMPANIES.Tick = ORDERS.Tick
GROUP BY ORDERS.Tick
```

Configuration:

```xml
<SOW>
    <Topic>
        <Name>ORDERS</Name>
        <MessageType>nvfix</MessageType>
        <Key>/OrderID</Key>
        <FileName>./sow/%n.sow</FileName>
    </Topic>
    <Topic>
        <Name>COMPANIES</Name>
        <MessageType>nvfix</MessageType>
        <Key>/CompanyId</Key>
        <FileName>./sow/%n.sow</FileName>
    </Topic>
    <View>
        <Name>TOTAL_COMPANY_VOLUME</Name>
        <UnderlyingTopic>
            <Join>[ORDERS]./Tick = [COMPANIES]./Tick</Join>
        </UnderlyingTopic>
        <MessageType>nvfix</MessageType>
        <Projection>
            <Field>[COMPANIES]./CompanyId</Field>
            <Field>[COMPANIES]./Tick</Field>
            <Field>[COMPANIES]./Name</Field>
            <Field>SUM([ORDERS]./Shares) AS /TotalVolume</Field>
        </Projection>
        <Grouping>
            <Field>[ORDERS]./Tick</Field>
        </Grouping>
    </View>
</SOW>
```

#### View Projected Into Different Message Type

When projecting into a different message type, fully qualify all underlying references with the message type.

Schema same as `ORDERS` above.

Configuration:

```xml
<SOW>
    <Topic>
        <Name>ORDERS</Name>
        <MessageType>nvfix</MessageType>
        <Key>/OrderID</Key>
        <FileName>./sow/%n.sow</FileName>
    </Topic>
    <View>
        <Name>TOTAL_VALUE</Name>
        <UnderlyingTopic>[nvfix].[ORDERS]</UnderlyingTopic>
        <MessageType>json</MessageType>
        <Projection>
            <Field>[nvfix].[ORDERS]./ClientId AS /ClientId</Field>
            <Field>SUM([nvfix].[ORDERS]./Shares * [nvfix].[ORDERS]./Price) AS /TotalCost</Field>
        </Projection>
        <Grouping>
            <Field>[nvfix].[ORDERS]./ClientId</Field>
        </Grouping>
    </View>
</SOW>
```

### State of the World Message Enrichment

SOW topics support inline enrichment during publish. There are two stages:

- **Preprocessing**: occurs before SOW key calculation. Use when enriched fields are part of the SOW key. No access to previous message state.
- **Enrichment**: occurs after SOW key calculation, before delta merge, SOW storage, transaction log write, view update, and delivery. Has access to previous state via `OF PREVIOUS`.

Entitlement filters are applied before either stage. Enrichment affects message data only, not metadata. Messages received over replication are not enriched again.

#### Preprocessing Messages

Use `Preprocessing` to alter a message before SOW key calculation. Field directives construct fields from the incoming message and merge results in. Cannot specify topic or message type in XPath identifiers.

`HINT OPTIONAL` removes a field if the result is `NULL`:

```text
<Field>IF(/source IN ('a','e','f'), /source, NULL)
       AS /source HINT OPTIONAL</Field>
```

See [Constructing Preprocessing Fields](/docs/amps-user-guide/builtin_functions/constructing-fields#constructing-preprocessing-fields).

#### Enriching Messages

Use `Enrichment` after SOW key calculation. Field directives construct fields from the incoming message and merge results in. Cannot specify topic or message type in XPath identifiers.

Special XPath modifiers:

| Modifier | Description |
| --- | --- |
| `OF CURRENT` | Refers to the incoming message. |
| `OF PREVIOUS` | Refers to the previous state of the message in the SOW. Returns `NULL` if no record exists. |

`HINT OPTIONAL` removes a field if the result is `NULL`:

```text
<Field>IF(/source IN ('a','e','f'), /source, NULL)
       AS /source HINT OPTIONAL</Field>
```

See [Constructing Enrichment Fields](/docs/amps-user-guide/builtin_functions/constructing-fields#constructing-enrichment-fields).

### SOW Update and Enrichment Processing

Processing sequence:
- Publish entitlement/filter is applied before preprocessing, enrichment, or delta merge.
- The enriched/merged message is recorded in the transaction log and SOW; the original is not preserved.
- Content filtering applies to the final enriched/merged message.
- Replication replicates the enriched message. Messages received over replication are not enriched again.

### SOW Preprocessing and Enrichment Examples

#### Add A Field

```xml
<SOW>

   <Topic>
      <Name>enrichment-example</Name>
      <MessageType>json</MessageType>
      <Key>/id</Key>
      <FileName>./sow/%n.sow</FileName>
      <Enrichment>
        <Field>AMPS_INSTANCE_NAME() as /publishSource</Field>
      </Enrichment>
   </Topic>

<SOW>
```

#### Set a Field to a Default Value

```xml
<SOW>

   <Topic>
      <Name>enrichment-example-default-value</Name>
      <MessageType>json</MessageType>
      <Key>/id</Key>
      <FileName>./sow/%n.sow</FileName>
      <Enrichment>
        <Field>COALESCE(/important OF CURRENT, /important OF PREVIOUS, 'default') as /important</Field>
      </Enrichment>
   </Topic>

<SOW>
```

#### Create a Key Field

```xml
<SOW>

   <Topic>
      <Name>key-creation-example</Name>
      <MessageType>json</MessageType>
      <Key>/bucket</Key>
      <FileName>./sow/%n.sow</FileName>
      <Preprocessing>
         <Field>CRC32(CONCAT(/name, /orderId)) % 10 as /bucket</Field>
      </Preprocessing>
    </Topic>

</SOW>
```

### Queues and Views: Message Queues

AMPS includes high-performance queuing built on the messaging engine and transaction log. Queues provide:

- Exactly-once processing.
- Fair distribution across workers.
- Guaranteed delivery and redelivery on failure.
- Retry limits and dead-letter actions.
- Replication with delivery guarantees.
- Views and aggregates based on current queue contents.
- Filtering and aggregation into/out of queues.

Use queues when ensuring a message is processed once by a single consumer. Use pub/sub when distributing to many consumers.

The underlying topic must be recorded in the transaction log. The queue itself does not need to be defined in the SOW section. Queues use the transaction log for enqueueing and acknowledgments.


### Advanced Messaging and Queues

Queues are implemented as AMPS topics, enabling content filtering, views, and bookmark subscriptions over queue data.

### Querying Queues as a View

Each queue provides a read-only view of currently available messages via a `sow` command. Querying does not lease or remove messages.

### Topic in the SOW as an Underlying Topic for a Queue

A SOW topic can be an underlying topic for a queue. Each publish creates a new queue message. Out-of-focus messages are not added. Deleting or expiring a message from the SOW does not remove it from the queue. When a message is published to a SOW topic with expiration, the message is stamped with the expiration value before being written to the transaction log. The `ExpirationModel` parameter determines how the queue manages expiration.

### Delta Messaging with Queues

AMPS supports delta publish to a SOW underlying topic; the full merged message is added to the queue. Delta subscriptions to a queue receive the full message, as each update is treated as a new publish.

### Views and Aggregated Subscriptions over Queues

Views and aggregated subscriptions over queues operate on currently available messages. Leased messages do not appear; returned or expired messages do. They are read-only and do not affect delivery.

### Bookmark Subscriptions to Queues

A queue supports `at_most_once` or `at_least_once` delivery but does not replay acknowledged messages. A bookmark subscription to a queue is translated to a bookmark subscription to the underlying topic, without queue delivery semantics. To get queuing semantics, do not include a bookmark on subscriptions to a queue.

### Advanced Queue Configuration

#### Using Multiple Underlying Topics

AMPS queues can include messages from any number of underlying topics using a regular expression. Provide a `DefaultPublishTarget` for publishes directly to the queue topic.

For example, you might configure a set of topics as follows:

```xml
<SOW>  
  ...  
  <Queue>  
     <Name>ORDERS_ANALYTICS</Name>  
     <MessageType>json</MessageType>  
     <UnderlyingTopic>^ORDERS$|^ORDERS_ANALYTICS_DIRECT$</UnderlyingTopic>  
     <DefaultPublishTarget>ORDERS_ANALYTICS_DIRECT</DefaultPublishTarget>  
  </Queue>  
  <Queue>  
     <Name>ORDERS_RISK</Name>  
     <MessageType>json</MessageType>  
     <UnderlyingTopic>^ORDERS$|^ORDERS_RISK_DIRECT$</UnderlyingTopic>  
     <DefaultPublishTarget>ORDERS_RISK_DIRECT</DefaultPublishTarget>  
  </Queue>  
  ...  
</SOW>
```

In this case, when a message is published to the `ORDERS` topic, both the `ORDERS_ANALYTICS` and the `ORDERS_RISK` queues deliver the message. However, a publisher can also publish directly to each queue by publishing a message to the `_DIRECT` topic for that queue. Furthermore, any publish to the name of the queue will be routed to the appropriate `_DIRECT` topic.

The following table demonstrates how messages are provided to topics with this configuration:

| Publish To | Results |
| --- | --- |
| `ORDERS` | Both `ORDERS_ANALYTICS` and `ORDERS_RISK` enqueue the message, since `ORDERS` matches the `UnderlyingTopic` of both queues. |
| `ORDERS_ANALYTICS` | The message is published to the `DefaultPublishTarget` of `ORDERS_ANALYTICS`, which is `ORDERS_ANALYTICS_DIRECT`.    The message is then enqueued to `ORDERS_ANALYTICS`, since `ORDERS_ANALYTICS_DIRECT` matches the `UnderlyingTopic` of `ORDERS_ANALYTICS`. |
| `ORDERS_RISK` | The message is published to the `DefaultPublishTarget` of `ORDERS_RISK`, which is `ORDERS_RISK_DIRECT`.    The message is then enqueued to `ORDERS_RISK`, since `ORDERS_RISK_DIRECT` matches the `UnderlyingTopic` `ORDERS_RISK`. |
| `ORDERS_ANALYTICS_DIRECT` | The message is published to `ORDERS_ANALYTICS_DIRECT`, and is then enqueued to `ORDERS_ANALYTICS`. |
| `ORDERS_RISK_DIRECT` | The message is published to `ORDERS_RISK_DIRECT`, and is then enqueued to `ORDERS_RISK`. |

#### Priority Queues

Add the `Priority` tag to deliver messages in descending priority order rather than publication order. The expression is evaluated as an `unsigned long`. Non-numeric or `NULL` values have the lowest priority.

```xml
<AMPSConfig>  
    ...  
    <SOW>  
        ...  
        <Queue>  
            <Name>OrderPriority</Name>  
            <MessageType>json</MessageType>  
            <Semantics>at-least-once</Semantics>  
            <UnderlyingTopic>Orders</UnderlyingTopic>  
            <Priority>/price * /qty</Priority>  
        </Queue>  
    </SOW>  
    ...  
</AMPSConfig>
```

#### Synchronizing Work with Barrier Messages

Barrier messages synchronize subscribers by requiring all previous messages to be acknowledged before delivery. When a message matches the `BarrierExpression` filter:

- AMPS does not deliver the message until all previous messages in the queue are acknowledged.
- AMPS does not deliver messages after the barrier until the barrier message is sent to subscribers.
- When all previous messages are acknowledged, AMPS delivers the barrier message to all current subscribers.
- AMPS immediately removes the barrier message from the queue, without requiring acknowledgment.
- A barrier message does not count toward backlog and bypasses backlog limits.
- Content and entitlement filters still apply; subscribers that do not match will not receive the barrier but will still see delivery pause until the barrier is released.

Each instance manages barrier delivery independently for its subscribers.

#### Limiting Currently Deliverable Messages

The `TargetQueueDepth` option limits the number of messages AMPS considers actively deliverable. Messages beyond this depth are inactive: not deliverable, queryable, or deletable by filter. As messages are acknowledged, AMPS adds messages to the active set up to the target. Inactive messages are not read from the transaction log and do not maintain queue state.

If a message beyond the depth is acknowledged before becoming active, AMPS preserves the acknowledgment and skips the message when it enters the active set. If a transfer request arrives for an inactive message, AMPS processes the queue until it reaches that message.

`TargetQueueDepth` cannot be used with `Priority` or `BarrierExpression`.

### Configuring Queues in a SOW

AMPS provides three queue types: `Queue`, `LocalQueue`, and `GroupLocalQueue`.

#### Defining Queue Replication Type

- `Queue`: Distributed queue. Consumable from any instance hosting it.
- `LocalQueue`: Local instance only, cannot be replicated.
- `GroupLocalQueue`: Restricted to a subset of instances in a replicated set.

`QueueDefinition` is accepted as a synonym for `Queue`.

#### Queue Configuration

`Name` (required)

The name of the queue topic. AMPS accepts `Topic` as a synonym for `Name` in the `Queue` definition.

`MessageType` (required)

The message type of the queue.

`UnderlyingTopic`

A topic name or regular expression for the topic that contains the messages to capture in the queue. These topics must be recorded in a transaction log, and all must be of the same message type as the queue. Default: The `Name` of the queue.

`DefaultPublishTarget` (required if `UnderlyingTopic` contains regular expression characters)

The topic to publish to when an application publishes a message to the queue. The `DefaultPublishTarget` must be one of the topics included in the queue.

`LeasePeriod`

The amount of time that a subscriber has ownership of the message before the message is returned to the queue. Default: unset (no expiration).

`Semantics`

The delivery semantics to use for this queue. AMPS queues deliver a given message to a single subscriber at a time.

- `at-least-once` - AMPS delivers the message to one subscriber at a time. The subscriber must explicitly remove the message from the queue once the message is processed. If the subscriber connection closes before acknowledging the message, if the subscriber returns the message to the queue, or the lease expires, AMPS can deliver the message again.
- `at-most-once` - AMPS removes the message from the queue immediately when AMPS sends the message.

Default: `at-least-once`

`MaxBacklog`

The maximum number of outstanding, unacknowledged messages in the queue at any one time. This backlog number is applied *per instance* of the queue. Default: unset (no limit).

`MaxPerSubscriptionBacklog`

The maximum number of outstanding, unacknowledged messages in the queue for an individual subscription. Subscribers can declare the maximum number of messages that the subscription is prepared to lease at a given time. This maximum defaults to `1` when there is no maximum explicitly specified for a subscription. AMPS will lease the number specified in the subscription or the maximum set for the queue, whichever is *lower*. Default: `1`.

`Expiration`

Sets the queue default value for the length of time an individual message can remain in the queue before AMPS considers the message to be undeliverable. Messages may expire while a subscriber has a lease on the message. AMPS does not send an additional notification in this case. Default: unset (no limit).

`ExpirationModel`

Manages how AMPS applies the queue expiration period and the message expiration period to determine the expiration for the message.

- `queue` - Always use the queue expiration value.
- `latest` - Use whichever expiration value is greatest.
- `earliest` - Use whichever expiration value is smallest.
- `default` - Use the message expiration if set; otherwise use the queue expiration.

Default: `default`

`Filter`

An AMPS `Filter` that is applied to the `UnderlyingTopic`. When a `Filter` is specified, only messages matching the `Filter` appear in the queue. By default, there is no filter.

`RecoveryPoint`

This option allows you to specify the point at which AMPS begins reviewing the transaction log to recover the state of the queue when AMPS restarts and there is no existing information about the state of the queue. Default: `epoch`.

If AMPS has a record of the last point in the transaction log at which all previous messages are acknowledged (stored in the `queues.ack` file), and the recovery point specified in the configuration file has not changed, AMPS will recover from that point rather than the configured `RecoveryPoint`.

The `RecoveryPoint` can be one of the following:

- `epoch` - Recovery begins at the beginning of the transaction log.
- `now` - Recovery begins at the time AMPS starts queue recovery, so only new messages are added to the queue.
- `creation` - Recovery begins at the time the queue was created. In current releases of AMPS, this is identical to specifying `now`.
- AMPS Bookmark - When an AMPS bookmark is provided, AMPS starts recovery at the specified bookmark.
- ISO-8601 Timestamp - When a timestamp is provided, AMPS starts recovery at the specified timestamp. The timestamp must be provided in the format AMPS uses for timestamp bookmarks.

`FairnessModel`

AMPS provides different methods to distribute messages across active subscriptions:

- `fast` - AMPS delivers to the first subscription found that can process the message.
- `round-robin` - AMPS distributes to the next subscription found that can process the message.
- `proportional` - AMPS delivers to the subscription with the lowest ratio of active messages to available backlog.

Each instance of AMPS independently manages the fairness model for subscriptions on that instance. Fairness model information is not replicated across instances.

Default: `proportional` for `at-least-once` queues, `round-robin` for `at-most-once` queues.

`Leasing`

Ownership model for leased messages.

- `strict` - AMPS allows a client to acknowledge (`sow_delete`) only messages that are leased to the client or currently unleased. If a client acknowledges a message leased to another client, there is no effect.
- `sublet` - AMPS allows any client to acknowledge any message, regardless of whether another client has a lease on the message.

Default: `sublet`

`MaxDeliveries`

Specifies an upper bound to the number of times AMPS may deliver a queue message before automatically expiring it. This counter is reset if the server restarts, and the counter is not replicated to other instances. Default: No maximum (`0`).

`MaxCancels`

Specifies a limit to the number of times a subscriber may cancel a lease on a message before it is expired. This counter is reset if the server restarts, and the counter is not replicated to other instances. Default: No maximum (`0`).

`Priority`

Specifies the order in which messages will be distributed from the queue. Higher priority messages are delivered first, regardless of the order in which messages have been published to the queue. The contents of the element can be either the name of a field in the message or an AMPS expression. Either way, the result is treated as an `unsigned long` value.

This option cannot be specified on a queue if `BarrierExpression` is specified. Default: not specified (delivery order is publication order).

`BarrierExpression`

Specifies the filter used to identify a barrier message (synchronization point) for this queue. When a message matches this expression, it will be delivered to *all* current subscribers on the queue when every previous message in the queue has been acknowledged.

The `BarrierExpression` filter is evaluated when AMPS adds the message to the in-memory state of the queue, and is not re-evaluated (unless the in-memory state of the queue is rebuilt after an instance restart).

This option cannot be specified on a queue if `Priority` is specified. Default: not specified.

#### Managing Queue Metadata

`FileBackedMetadata`

Specifies whether AMPS should persist metadata about the queue in the journal directory. This could reduce the active memory footprint of an AMPS instance in cases where a queue has a large number of messages, but it is not being actively consumed. In cases where the queue has a large number of unacknowledged messages when AMPS is restarted, this may also improve recovery time. This option can be unset or set to a value of `enabled`. Default: unset.

`TargetQueueDepth`

The target number of messages to keep active state on. Providing this option limits the amount of state that AMPS keeps for unacknowledged messages. Messages that are present in the transaction log, but for which AMPS does not currently have active state, are not deliverable from the queue topic, cannot be queried, cannot be deleted using a `sow_delete` by filter, and so on. By default, this option is not set. When unset, all messages in the queue are active. If `FileBackedMetadata` is enabled, this option is typically unnecessary. This option *cannot* be set if `Priority` or `BarrierExpression` is specified. When this parameter is set, it should be set to a number that accounts for several seconds of traffic to the queue, considering replication and acknowledgment speeds. Default: unset (no limit). Minimum: `1000`.

`DeferredAckExpiration`

Specifies the amount of time for AMPS to retain information about an acknowledgment (`sow_delete`) message received when the corresponding message is not in the queue. This element is configured as an interval, for example, `15m` or `2h`. Default: `1d`.

#### Group Local Queue Configuration

`InitialOwner` (required for `GroupLocalQueue`)

For a group local queue, provides the instance `Name` of the instance that will own the message when the message is first published. This configuration element is required for a `GroupLocalQueue`. All instances in a replicated system that define this queue must define the same `InitialOwner`. There is no default for this element.

`GroupLocalQueueDomain`

For a group local queue, provides a way to allow a queue that is hosted in instances that are in different replication groups to be identified as the same queue and function as a single distributed queue. By default, a `GroupLocalQueue` is identified using the queue `Name` and the `Group` of the AMPS instance that hosts the queue. When this element is defined, the `GroupLocalQueue` is identified using the queue `Name` and the `GroupLocalQueueDomain` rather than the `Group` of the instance that hosts the queue. This element was introduced in version 5.3.4.

The following configuration snippet shows one way to configure a queue:

```xml
<!-- Notice that the topics to use for the queue (ORDERS_.*) must be  
     recorded in a transaction log. -->  
<SOW>  
    <Queue>  
        <Name>MQ</Name>  
        <MessageType>json</MessageType>  
        <UnderlyingTopic>ORDERS_.*</UnderlyingTopic>  
        <DefaultPublishTarget>ORDERS_DIRECT</DefaultPublishTarget>  
        <LeasePeriod>60s</LeasePeriod>  
        <Expiration>1d</Expiration>  
        <MaxBacklog>3</MaxBacklog>  
    </Queue>  
</SOW>
```

The following configuration snippet shows the configuration for a `GroupLocalQueue`:

```xml
<SOW>  
    <GroupLocalQueue>  
        <Name>GroupQueue</Name>  
        <MessageType>json</MessageType>  
        <UnderlyingTopic>ORDERS_.*</UnderlyingTopic>  
        <DefaultPublishTarget>ORDERS_DIRECT</DefaultPublishTarget>  
        <GroupLocalQueueDomain>CONSUME_INSTANCES</GroupLocalQueueDomain>  
    </GroupLocalQueue>  
</SOW>
```

### Getting Started with AMPS Queues

To add a simple queue to AMPS, first create a transaction log that records the messages for the queue:

```xml
<AMPSConfig>  
    ...  
    <TransactionLog>  
         <JournalDirectory>./journals</JournalDirectory>  
         <Topic>  
            <Name>Work</Name>  
            <MessageType>json</MessageType>  
         </Topic>  
         <Topic>  
            <Name>WorkToDo</Name>  
            <MessageType>json</MessageType>  
         </Topic>  
    </TransactionLog>  
   ...  
</AMPSConfig>
```

Next, declare the queue topic itself in the SOW element:

```xml
<AMPSConfig>  
    ...  
    <SOW>  
        <Queue>  
            <Name>WorkToDo</Name>  
            <MessageType>json</MessageType>  
            <Semantics>at-most-once</Semantics>  
            <UnderlyingTopic>Work</UnderlyingTopic>  
        </Queue>  
    </SOW>  
    ...  
</AMPSConfig>
```

This simple queue provides each message to at most one subscriber. After AMPS delivers the message to one subscriber, AMPS removes the message from the queue without waiting for the subscriber to acknowledge the message.

### Queue Replication Types

AMPS supports three different replication types for a queue.

| Configuration Tag | Replication Type | Description | Message Ownership |
| --- | --- | --- | --- |
| `Queue` | Distributed queue | Fully distributed queue. | Instance that received the initial publish from an application.  Other instances may request ownership. |
| `LocalQueue` | Local queue | Queue exists on a single instance, delivery guarantees only apply to that instance.  Cannot be replicated. | Instance that contains the queue.  No other instance may request ownership. |
| `GroupLocalQueue` | Queue replicated to specific instances, most often in the same replication `Group`. | Distributed queue on a specific set of instances.  In version 5.3.4 and higher, the instances that contain the queue must all be in the same `Group` *or* define the same `GroupLocalQueueDomain` tag.  In earlier versions of AMPS, every queue with the same name in the replication fabric is treated as an instance of the same queue and must have the same definition and the same `InitialOwner`. | Instance specified in the `InitialOwner` tag for the queue.  Other instances may request ownership. |

By default, AMPS queues are *distributed queues*. AMPS provides local queues when a queue is defined with the `LocalQueue` tag. AMPS also supports distributed queues restricted to a set of instances within a mesh using the `GroupLocalQueue` tag. Each instance of AMPS manages its own subscriptions and backlog. Delivery guarantees are provided by managing the ownership of each message in the queue. Only messages owned by the current instance will be delivered to subscribers.

### Handling Unprocessed Messages

Configure `MaxCancels` and `MaxDeliveries` to limit delivery attempts. Combine with the `amps-action-on-sow-expire-message` action to move expired messages to a dead-letter queue.

```xml
<AMPSConfig>  
    ...  
    <SOW>  
        <Queue>  
            <Name>Jobs</Name>  
            <MessageType>json</MessageType>  
            <MaxCancels>3</MaxCancels>  
            <MaxDeliveries>10</MaxDeliveries>  
        </Queue>  
        <Queue>  
            <Name>FailedJobs</Name>  
            <MessageType>json</MessageType>  
        </Queue>  
    </SOW>  
</AMPSConfig>
```

```xml
<AMPSConfig>  
    ...  
    <Actions>  
        <Action>  
            <On>  
                <Module>amps-action-on-sow-expire-message</Module>  
                <Options>  
                    <Topic>Jobs</Topic>  
                    <MessageType>json</MessageType>  
                </Options>  
            </On>  
            <Do>  
                <Module>amps-action-do-publish-message</Module>  
                <Options>  
                    <Topic>FailedJobs</Topic>  
                    <MessageType>json</MessageType>  
                    <Data>{ "message": {{AMPS_DATA}}, "reason": "{{AMPS_REASON}}" }</Data>  
                </Options>  
            </Do>  
        </Action>  
    </Actions>  
    ...  
</AMPSConfig>
```

### Queue Subscriptions Compared to Bookmark Replays

- **Delivery Model**: Bookmark subscriptions deliver to any number of subscribers; queue subscriptions guarantee a message is processed only once.
- **Delivery Limits**: Bookmark subscriptions deliver as fast as possible; queues limit delivery to ensure timely processing.
- **Acknowledgment**: Bookmark subscriptions track progress client-side; queues track processing server-side.

### Replacing Queue Subscriptions

Queue subscriptions support atomic `replace` for filter, topic, or options. AMPS does not break existing leases or adjust unacknowledged counts for messages that no longer match. The backlog may change if `max_backlog` or topic changes, which can result in more outstanding messages than the current maximum allows.

### Understanding AMPS Queuing

AMPS queues are views over underlying topics recorded in the transaction log. Publishers publish to the underlying topic; consumers subscribe to the queue. AMPS tracks delivery and processing state. Memory consumption is approximately 200 bytes per message.

#### Delivery Semantics

AMPS delivers a message to a single subscriber at a time.

- **`at-least-once`**: The subscriber must explicitly remove the message with a `sow_delete` within the `LeasePeriod`. If the lease expires, the subscriber disconnects, or the message is canceled, AMPS returns the message to the queue (subject to `MaxCancels` and `MaxDeliveries`).
- **`at-most-once`**: AMPS removes the message immediately upon sending. The subscriber still acknowledges to track backlog.

During normal processing, each message is delivered exactly once. Use `at-most-once` for ephemeral data where loss is preferable to duplicates. Use `at-least-once` for higher-value data where duplicates are preferable to loss. Use `at-least-once` with `MaxDeliveries` and a dead-letter action for data requiring manual reconciliation.

#### Subscription Backlog

AMPS limits the number of unacknowledged messages per subscription to the minimum of the queue's `MaxPerSubscriptionBacklog` and the subscription's `max_backlog` option. If a subscriber does not specify `max_backlog`, it defaults to `1`. Use a backlog of at least `2` for efficient pipelined delivery.

#### Delivery Fairness

`at-least-once` queues support three fairness algorithms:

| Algorithm | Description |
| --- | --- |
| `fast` | This strategy optimizes for the lowest latency.    AMPS delivers the message to the first subscription found that does not have a full backlog. With this algorithm, AMPS tries to minimize the time spent determining which subscription receives the message without attempting to distribute messages fairly across subscriptions. |
| `round-robin` | This strategy optimizes for general fairness across subscriptions.    AMPS delivers the message to the next available subscription that does not have a full backlog. With this algorithm, AMPS delivers messages evenly among the subscribers that have space in their backlog. |
| `proportional` | This strategy optimizes for delivery to subscriptions with the most unused capacity.    AMPS delivers the message to the subscription that has the highest proportion of backlog capacity unused. AMPS determines this by taking the ratio of unacknowledged messages to the maximum backlog.    For example, if there are three active subscribers for the queue, with backlog settings and outstanding messages as follows:   - Subscriber `Inky`: `max_backlog=2`, currently leased 1 message - Subscriber `Blinky`: `max_backlog=4`, currently leased 3 messages - Subscriber `Clyde`: `max_backlog=10`, currently leased 4 messages   In this case, with `proportional` delivery, a new message for the queue will be delivered to Clyde, since that subscriber has only filled 40% of the backlog, as compared with 50% for Inky and 75% for Blinky.    If more than one subscription has the same unused capacity, AMPS delivers the message to the first subscription found with that capacity. |

`at-most-once` queues support only `round-robin`. Defaults: `proportional` for `at-least-once`, `round-robin` for `at-most-once`. Each instance manages fairness independently.

#### Acknowledging Messages

Subscribers acknowledge messages via `sow_delete` with the bookmark. Multiple bookmarks can be provided in a comma-delimited list. A filter can acknowledge all matching messages (subject to the `Leasing` model).

For `at-least-once` queues:

| Option | Result |
| --- | --- |
| <none> | Message is considered to be successfully processed and removed from the queue. |
| `cancel` | Message is returned to the queue.    The count of cancels for this message is incremented, and if the count is greater than the configured `MaxCancels` for the queue, the message is expired. |
| `expire` | Message is immediately expired from the queue. |

For `at-most-once` queues, these options have no effect.

#### Persisting Metadata for Queues

Enable `FileBackedMetadata` to save queue state in the journal directory, reducing memory usage for large queues.

#### Optional Message Delivery Behaviors

- `Priority`: Delivers messages in priority order.
- `BarrierExpression`: Provides a synchronization point.

#### Message Flow for Queues

**`at-most-once` delivery:**

1. Publisher publishes to underlying topic.
2. Message becomes available.
3. For non-barrier messages, delivered when: subscription matches, subscriber is entitled, message is oldest/highest priority, subscription has backlog capacity, and fairness selects the subscription. AMPS removes the message immediately.
4. For barrier messages, delivered only when all previous messages are acknowledged; subsequent messages pause until the barrier is delivered.
5. If expiration time passes, message is removed.
6. Subscriber acknowledges to indicate capacity.

**`at-least-once` delivery:**

1. Publisher publishes to underlying topic.
2. Message becomes available.
3. For non-barrier messages, delivered when: subscription matches, message is oldest/highest priority, subscription has backlog capacity, and fairness selects the subscription. AMPS calculates and provides the lease time.
4. For barrier messages, delivery pauses until all previous messages are acknowledged.
5. If expiration passes with no lease, message is removed.
6. If lease expires, message is returned to the queue if under expiration and `MaxDeliveries`; otherwise removed.
7. Subscriber processes and acknowledges with `sow_delete`.

#### Startup and Recovery for Queues

AMPS recovers queue state from the transaction log at startup. If the queue is new or `RecoveryPoint` changed, recovery begins at the configured point (default: `epoch`). Otherwise, recovery starts from the most recent point where all previous messages were acknowledged (stored in `queues.ack`). If that point is older than available journals, recovery begins from the start of the log. AMPS determines the oldest recovery point for all queues and reads journals from that point forward.

### Record and Replay Messages

The transaction log provides persistence, replay, and the foundation for replication and queues. Topics covered by a transaction log provide atomic broadcast with repeatable ordering, no gaps, and no duplicates.

Enabling a transaction log requires unique client names per connection. Duplicate names cause disconnection of the existing connection. Persisted publish acknowledgments are conflated.

### Configuring a Transaction Log

`JournalDirectory` (required)

Filesystem location for journal files. Dedicated to a single instance.

`JournalArchiveDirectory`

Filesystem location for archived journal files. Should be on a different device than `JournalDirectory`. Use AMPS actions to move files.

`PreallocatedJournalFiles`

The number of journal files AMPS will create as part of the server startup. Default: `2`. Minimum: `1`.

`JournalSize`

Sets the target size for AMPS to use when calculating the size of journal files. AMPS allocates journal files based on the size of an internal buffer. AMPS accepts `MinJournalSize` as a synonym for `JournalSize`. Default: `1GB`. Minimum: `10M`.

`Topic`

The topic to include in the transaction log. When no `Topic` is specified, AMPS initializes transaction log management for the instance, but does not persist messages. Multiple `Topic` elements can be included. To capture logical topics stored in a physical SOW topic, the `Topic` directive should match the `Name` of the physical topic. There is no default.

`FlushInterval`

AMPS batches writes to the transaction log. The interval at which messages will be flushed to the journal file during periods of slow activity. Default: `100ms`. Maximum: `1000ms`. Minimum: `1us`.

`O_DIRECT`

Where supported, `O_DIRECT` will perform DMA directly from/to physical memory to a user space buffer. Default: `enabled`.

`InactiveClientAckExpiration`

Sets the amount of time to retain records for an inactive publisher. Default: Retain records indefinitely.

`CompressedJournalCacheMemoryLimit`

Sets the maximum amount of server memory to use for caching compressed journal files. Default: 10% of server memory or 10GB, whichever is *lower*.

If `Topic` is included in the `TransactionLog` configuration, it must contain the following elements:

`Name` (required)

The name of the topic to record. This element can be a literal name or a regular expression.

`MessageType` (required)

The message type of the topic. This must be one of the message types loaded by default or a message type declared in the configuration file.

```text
<!-- All transaction log definitions are contained within the TransactionLog block.  
     The following global settings apply to all Topic blocks defined within the  
     TransactionLog: JournalDirectory, PreallocatedJournalFiles, and JournalSize. -->  
<TransactionLog>  
    <JournalDirectory>./amps/journal/</JournalDirectory>  
    <JournalArchiveDirectory>/mnt/somedev0/amps/journal</JournalArchiveDirectory>  
    <PreallocatedJournalFiles>1</PreallocatedJournalFiles>  
    <JournalSize>10MB</JournalSize>  
    <Topic>  
        <Name>orders</Name>  
        <MessageType>nvfix</MessageType>  
    </Topic>  
    <Topic>  
        <Name>LOGGED_.*</Name>  
        <MessageType>json</MessageType>  
    </Topic>  
    <Topic>  
        <Name>bucket</Name>  
        <MessageType>json</MessageType>  
    </Topic>  
</TransactionLog>  
<SOW>  
   ... other configuration here ...  
   <Topic>  
      <Name>bucket</Name>  
      <MessageType>json</MessageType>  
      <Pattern>.*-cached-values</Pattern>  
      <Key>/msgId</Key>  
      <FileName>./sow/%n.sow</FileName>  
   </Topic>  
</SOW>
```

### Managing Journal Files

AMPS can archive, compress, and remove journal files while running via actions. Once deleted, messages are unavailable for replay or SOW recreation.

Use `amps-action-do-remove-journal`, `amps-action-do-archive-journal`, and `amps-action-do-compress-journal` for safe management.

Do not remove journal files with gaps. If removing files while shut down, ensure sequential removal without gaps.

### Reference to File Types

| Extension | File Type | Description |
| --- | --- | --- |
| `.journal` | Journal file | These files contain the messages that comprise the transaction log.    AMPS always writes new messages to an uncompressed journal file. |
| `.journal.gz` | Compressed journal file | These files contain messages that comprise the transaction log.    These files have been compressed by AMPS as a result of the `amps-action-do-compress-journal` action.    Other than being compressed, they are treated identically to uncompressed journal files. |
| `.index.gz` | Journal index file | These files are used during recovery to help AMPS quickly rebuild its references to the content of the transaction log without having to completely reprocess each file.    Each index file contains index information for the corresponding journal file.    These files do not contain messages. |
| `.topic.index` | Topic index file | These files are used during replay to help AMPS quickly locate messages for a given topic.    Creating a topic index is optional, and is enabled through instance level `Tuning` configuration.    These files do not contain messages. |
| `.clients.ack` | Clients acknowledgment cache | Used during recovery to help AMPS quickly identify the last message persisted from each publisher without having to reprocess each journal file.    These files do not contain messages.  When present, AMPS will use this file to determine the last message received from each publisher rather than scanning the journals to determine the last message received.  The publishers tracked in this file include replication sources. |
| `.queues.ack` | Queue acknowledgment cache | For each queue, this file stores the point in the transaction log for which that queue has been completely processed (that is, all messages prior to that point in the transaction log have been acknowledged or expired).  On recovery, AMPS can begin restoring the state of the queue from that point rather than reprocessing the entire transaction log.  These files do not contain messages.  When present, AMPS will use this file to determine the last point in the journal for a given queue where all queue messages were fully consumed (acknowledged or expired). Queue recovery will begin at that point for each queue. If this file is not present, AMPS will scan journals from the beginning to recover the queue. |
| `.queue.cache` | Queue metadata cache | For a queue that specifies that metadata be cached in a file, this is the file that contains the cache.    If this file is removed, the queue state will be restored from the transaction log (using the recovery point stored in the `queues.ack` file).    These files do not contain messages or message headers. They contain delivery state for messages in the queue and the location of those messages in the transaction log. |

### Replaying Messages with Bookmark Subscription

A bookmark subscription replays messages from the transaction log starting at a specific bookmark. Bookmarks are monotonically increasing opaque identifiers. AMPS delivers messages in recorded order. If the topic is not in the transaction log, AMPS returns an error.

If `completed` acknowledgment is requested, it is delivered once replay finishes. Clients can submit a comma-delimited list of bookmarks; AMPS begins at the oldest. The server does not track subscription progress; clients manage resume state (typically via a bookmark store). Bookmark subscriptions pace themselves to the subscriber without triggering slow client offlining.

#### Replay of Full Transaction Log

Request bookmark `0` or `EPOCH` to replay from the beginning, then cut over to live messages.

#### Bookmark Replay from NOW

Request `0|1|` or `NOW` to begin at the live stream without replaying history.

#### Bookmark Replay with a Bookmark

Provide the last processed bookmark to resume. If a bookmark is unknown, AMPS defaults to `NOW`. Starting with 5.3.5, use `bookmark_not_found` to control this behavior.

`MOST_RECENT` is a client library constant that looks up the appropriate recovery point; it is never sent to the server.

#### Bookmark Replay from a Moment in Time

Use an ISO-8601 timestamp: `YYYYmmddTHHMMSS[Z]`. AMPS begins at the closest point, or `EPOCH` if prior to the log.

For example, a timestamp for January 2nd, 2015, at 12:35:

```text
20150102T123500Z
```

#### Bookmark Replay with a Starting and Stopping Point

As of version 5.3.2, AMPS allows a subscriber to specify the point at which a bookmark replay should stop.

```text
<begin_interval_specifier> <begin_bookmarks> : <end_bookmarks> <end_interval_specifier>
```

The *begin\_interval\_specifier* is one of:

| Specifier | Behavior |
| --- | --- |
| `(` | *Exclusive replay.* Begin immediately *after* the bookmark or range specified. The specified bookmark will *not* be present in the replay. |
| `[` | *Inclusive replay.* Begin immediately *before* the bookmark or range specified. The specified bookmark will be present in the replay. |

The *end\_interval\_specifier* is one of:

| Specifier | Behavior |
| --- | --- |
| `)` | *Exclusive replay.* End immediately *before* the bookmark or range specified. The specified bookmark will *not* be present in the replay. |
| `]` | *Inclusive replay.* End immediately *after* the bookmark or range specified. The specified bookmark will be present in the replay. |

For example, to replay messages received by the instance on June 4, 2020 we could construct an interval beginning at midnight on June 4 UTC (inclusive) and ending at midnight on June 5 UTC (exclusive), as follows:

```text
[20200604T000000:20200605T000000)
```

Future timestamps are allowed. An inclusive bookmark with the NOW bookmark `0|1` as the starting point will evaluate the subscription against the last entry in the transaction log.

#### Content and Topic Filtering

Bookmark subscriptions support content filtering. Only topics recorded in the transaction log are provided, ensuring consistency between replay and live streams.

#### Delivery Rate Control

Subscribers can limit replay rate with the `rate` option:

```text
rate=1000
```

To limit delivery to 500KB per second:

```text
rate=500KB
```

To limit delivery to double the original publish speed:

```text
rate=2X
```

To limit delivery to half the original publish speed:

```text
rate=.5X
```

Use `rate_max_gap` to skip long gaps. Example:

```text
rate=5X,rate_max_gap=10s
```

#### Pausing and Resuming Bookmark Subscriptions

As of version 5.0, subscriptions can be paused and resumed. Pause stops delivery; resume continues from the maintained position. Useful for synchronizing multiple subscriptions. AMPS can combine replay for subscriptions delivered to the same client, paused at the same bookmark, at the same rate, and resumed together.

A paused subscription is removed if the subscriber disconnects.

#### Conflation and Bookmark Subscriptions

Conflation works the same as regular subscriptions. The conflation interval applies to the replay message timeline. The bookmark on a conflated message is the first message's bookmark during the interval.

#### Requesting Message Timestamps

Provide the `timestamp` option to populate the timestamp header on replayed messages.

#### Selecting Message Durability Options

Default: messages are delivered when persisted to the local transaction log.

- `fully_durable`: Deliver only after local persistence and all synchronous downstream replication destinations acknowledge. Adds latency; if a sync destination is offline, delivery pauses until it recovers or is downgraded.
- `live`: Deliver before persistence for lower latency. Increases failover inconsistency risk. Not compatible with `rate`, `pause`, or `resume`. Subject to slow client offlining after replay.

#### Managing Replay Restart Behavior

As of 5.3.5, the `bookmark_not_found` option controls behavior when no provided bookmark exists:

| value | result if no provided bookmark is found |
| --- | --- |
| `epoch` | Begin replay at the start of the transaction log. |
| `now` | Begin replay at the end of the transaction log. (Default for bookmark subscriptions if this option is not provided.) |
| `fail` | Report a failure if no bookmark in the bookmark string is available. |

### Understanding Transaction Log Message Persistence

Publishers assign sequence numbers. AMPS acknowledges the most recent persisted message, indicating all previous messages are persisted. AMPS writes to the local transaction log before acknowledging; for synchronous replication, all sync destinations must persist first.

Publishers should not wait for acknowledgment; they retain unacknowledged messages and republish on failover. Client libraries manage this via `PublishStore`.

### Client Names and the Transaction Log

When a transaction log is configured, each connection must have a unique client name. If a duplicate connects:
- Same user ID: existing connection is disconnected.
- Different user ID: new connection is refused.

### Message Sequence Numbers

Every message has a bookmark combining publisher identifier and sequence number. AMPS discards duplicates with sequence numbers equal to or lower than the highest seen. Messages without sequence numbers receive a unique publisher identifier based on the instance name.

### Using amps-grep to Find Messages in the Transaction Log

`amps-grep` searches journal files and includes headers.

**Finding Messages from a Specific Client:**

```bash
$amps-grep --client=client_name journal_directory/*.journal > out.txt
```

**Finding Messages for a Specific Topic:**

```bash
$ amps-grep topic_name journal_directory/*.journal > out.txt
```

**Finding Messages with Specific Data:**

```bash
$amps-grep "data" journal_directory/*.journal > out.txt
```

### Replication and Expiration: High Availability

AMPS provides high availability through transaction logging, replication, and heartbeat monitoring. Instances do not share state outside of replication. There is no quorum or controller; each instance processes messages independently.

Client features include heartbeat monitoring, automatic reconnection/failover, reliable publication with persistent stores, and subscription recovery via bookmark replay.

### Example: Regional Distribution

Each region replicates topics of interest to other regions using `async` acknowledgment. Clients connect only to their local instance.

![Diagram showing replication between geographic regions](/assets/ha-regional-distribution-light.svg)![Diagram showing replication between geographic regions](/assets/ha-regional-distribution-dark.svg)

### Example: Pair of Instances for Failover

Two instances replicate to each other with `sync` acknowledgment for a hot-hot pair. Clients treat both instances as equivalent failover targets.

![Diagram showing synchronous replication between two AMPS instances](/assets/ha-failover-pair-light.svg)![Diagram showing synchronous replication between two AMPS instances](/assets/ha-failover-pair-dark.svg)

### Example: Regional Distribution with HA

Combine regional distribution with HA pairs in each region. Within a region, instances use `sync`; between regions, `async`. Each region is a `Group`. Clients fail over only within their region.

![Diagram showing replication across regions with high availability within each](/assets/ha-regional-group-light.svg)![Diagram showing replication across regions with high availability within each](/assets/ha-regional-group-dark.svg)

| Server | Group | Destinations | PassThrough |
| --- | --- | --- | --- |
| NewYork 1 | NewYork | - NewYork 2 /  sync ack | `.*` |
|  |  | - [London 1, London 2] / async ack | NewYork |
| NewYork 2 | NewYork | - NewYork 1 /  sync ack | `.*` |
|  |  | - [London 1, London 2] / async ack | NewYork |
| London 1 | London | - London 2 /  sync ack | `.*` |
|  |  | - [NewYork 1, NewYork 2] / async ack | London |
| London 2 | London | - London 1 /  sync ack | `.*` |
|  |  | - [NewYork 1, NewYork 2] / async ack | London |

### Guaranteed Publishing

Guaranteed publishing ensures the client retains messages until AMPS acknowledges persistence (including sync replication destinations). The unique message identifier is a bookmark formed from client name and sequence number.

Client libraries manage sequence numbers via `PublishStore`. The `logon` command's `processed` acknowledgment returns the last persisted sequence number. Connections must request `processed` acknowledgment with every `logon`.

All publishers must set a unique client name.

### Durable Publication and Subscriptions

**Durable Subscriptions**: Use bookmark subscriptions with persisted bookmarks to resume from the exact point after interruption.

**Durable Publishing**: Maintain a persistent record of outgoing messages until `persisted` acknowledgment is received. On failover, resend unacknowledged messages.

### Heartbeat in High Availability

Heartbeats detect connection failures quickly. Initialized by the client sending a `heartbeat` message.

### Message Ordering Considerations

AMPS preserves order per publisher per topic. Across publishers, order may differ on different instances. Client bookmark stores handle failover across replicated instances safely.

### Example: Hub and Spoke / Expandable Mesh

A hub and spoke topology uses:
- **Ingestion instances**: Accept publishes, replicate to each other and hubs using `sync`. No SOW.
- **Hub instances**: Accept from ingestion, replicate to application instances. No SOW, no replication back.
- **Application instances**: Provide messages to apps. Define SOW, views, queues as needed. Replicate to each other with `sync` if multiple instances per app.

Advantages: easy scaling, autonomous groups, resilience, reduced bandwidth.
Limitations: higher latency for some topologies, no fully distributed queues, requires dedicated instances, requires exclusion of replication validation to hub.

### Slow Client Management and Capacity Limits

AMPS manages memory for slow clients via offlining (buffer to disk) and disconnection. Resource pools protect instance capacity; client-level policies identify unresponsive clients.

#### Resource Pool Policies

| Element | Description |
| --- | --- |
| `MessageMemoryLimit` | The total amount of memory to allocate to messages before offlining clients.  Default: 10% of total host memory or 10% of the amount of host memory AMPS is allowed to consume (as reported by `ulimit -m` ), whichever is *lowest*. |
| `MessageDiskLimit` | The total amount of disk space to allocate to messages before disconnecting clients.  Default: 1GB or the amount specified in the `MessageMemoryLimit`, whichever is *highest*. |
| `MessageDiskPath` | The path to use to write offline files.  Default: `/var/tmp` |

#### Individual Client Policies

| Element | Description |
| --- | --- |
| `ClientMessageAgeLimit` | The maximum amount of time for the client to lag behind. If a message for the client has been held longer than this time, the client will be disconnected. This parameter is an AMPS time interval (for example, `30s` for 30 seconds, or `1h` for 1 hour).  Notice that this policy applies to *all* messages and *all* connections.  If you have applications that will consume large result sets (SOW queries) over low-bandwidth network connections, consider creating a separate transport with the age limit set higher to allow those operations to complete.  Default: No age limit |
| `ClientMaxCapacity` | The amount of available capacity a single client can consume. Before a client is offlined, this limit applies to the `MessageMemoryLimit`. After a client is offlined, this limit applies to the `MessageDiskLimit`. This parameter is a percentage of the total.  Default: `50%` (previous versions defaulted to `100%`) |

### Replicating Messages Between Instances

Replication copies messages to downstream instances after persistence. It uses a leaderless, "all nodes hot" model. Any instance can accept publishes; no quorum is required.

AMPS replicates `publish`, `delta_publish`, and `sow_delete` commands as recorded in the transaction log. Replication guarantees delivery: journal files are not removed until all destinations acknowledge.

Two acknowledgment modes:
- `sync`: Acknowledge to publisher only after local persistence and all sync destinations acknowledge.
- `async`: Acknowledge after local persistence.

Any instance accepting publishes, SOW deletes, or queue acknowledgments should have at least one `sync` destination.

### Configuring Incoming Replication Transports

Define an `amps-replication` transport (or `amps-replication-secure` for SSL) in `Transports`. Only one incoming replication transport is allowed per instance.

### Configuring Outgoing Replication Destinations

Define `Destination` blocks within the `Replication` section. Each `Destination` specifies one outgoing replication connection.

### Configuring Replication

All replicated topics must be in the transaction log. Instances in the same `Group` must be fully equivalent. Each instance must have a unique `Name`.

### Replication Setup Example

```xml
<AMPSConfig>  
    <Name>amps-1</Name>  
    <Group>DataCenter-NYC-1</Group>  
    ...  
    <Transports>  
        <Transport>  
            <Name>amps-replication</Name>  
            <Type>amps-replication</Type>  
            <InetAddr>10004</InetAddr>  
        </Transport>  
        ... transports for client use here ...  
    </Transports>  
    ...  
    <Replication>  
        <Destination>  
            <Topic>  
                <MessageType>fix</MessageType>  
                <Name>topic</Name>  
            </Topic>  
            <Topic>  
              <MessageType>json</MessageType>  
              <Name>^/orders/</Name>  
            </Topic>  
            <Name>amps-2</Name>  
            <PassThrough>.*</PassThrough>  
            <Group>DataCenter-NYC-1</Group>  
            <SyncType>sync</SyncType>  
            <Transport>  
                <InetAddr>amps-2-server.example.com:10005</InetAddr>  
                <Type>amps-replication</Type>  
            </Transport>  
        </Destination>  
    </Replication>  
    ...  
</AMPSConfig>
```

For `amps-2`:

```xml
<AMPSConfig>  
    <Name>amps-2</Name>  
    <Group>DataCenter-NYC-1</Group>  
    ...  
    <Transports>  
        <Transport>  
            <Name>amps-replication</Name>  
            <Type>amps-replication</Type>  
            <InetAddr>10005</InetAddr>  
        </Transport>  
    </Transports>  
    ...  
    <Replication>  
        <Destination>  
            <Topic>  
                <MessageType>fix</MessageType>  
                <Name>topic</Name>  
            </Topic>  
            <Topic>  
              <MessageType>json</MessageType>  
              <Name>^/orders/</Name>  
            </Topic>  
            <Name>amps-1</Name>  
            <PassThrough>.*</PassThrough>  
            <Group>DataCenter-NYC-1</Group>  
            <SyncType>sync</SyncType>  
            <Transport>  
                <InetAddr>amps-1-server.example.com:10004</InetAddr>  
                <Type>amps-replication</Type>  
            </Transport>  
        </Destination>  
    </Replication>  
    ...  
</AMPSConfig>
```

### Downstream Persistence Acknowledgment: Sync vs Async

`SyncType` controls when the publisher receives `persisted` acknowledgment.

With `sync`, the publisher waits until the message is stored locally and all sync destinations acknowledge. With `async`, acknowledgment happens after local storage.

The acknowledgment type does not affect replication speed or delivery to subscribers. `fully_durable` bookmark subscriptions wait for all sync destinations.

A `sync` destination can be downgraded to `async` while running.

### Downgrading Acknowledgments for a Destination

Downgrading a link to `async` relieves publisher pressure but reduces durability guarantees. A downgraded link is unsafe for publisher or bookmark subscriber failover.

AMPS can automatically downgrade and upgrade links via actions:

```xml
<AMPSConfig>  
    ...  
    <Actions>  
        <Action>  
            <On>  
                <Module>amps-action-on-schedule</Module>  
                <Options>  
                    <Every>15s</Every>  
                </Options>  
            </On>  
            <Do>  
                <Module>amps-action-do-downgrade-replication</Module>  
                <Options>  
                    <Age>300s</Age>  
                </Options>  
            </Do>  
            <Do>  
                <Module>amps-action-do-upgrade-replication</Module>  
                <Options>  
                    <Age>10s</Age>  
                </Options>  
            </Do>  
        </Action>  
    </Actions>  
   ...  
</AMPSConfig>
```

With the example configuration (check every 15s, downgrade at 300s, upgrade at 10s), a publisher at 10,000 msg/s needs a publish store holding at least 750,000 messages.

An instance-level tuning parameter can prevent downgrade if it would reduce sync destinations below a minimum.

### Destination Server Failover

Two approaches:
1. **Wide IP**: Transparent but may take several seconds.
2. **AMPS Failover**: Specify multiple addresses in `InetAddr`. AMPS tries each in priority order. If an incoming connection exists from a listed server, AMPS uses it.

### Guarantees on Ordering

AMPS preserves order per publisher per topic and per instance overall. Absolute order across topics is preserved except for views, queues, and conflated topics.

### PassThrough Replication

By default, instances only replicate messages published directly to them. `PassThrough` replicates messages received via replication based on the originating group name. Use `.*` to replicate the full transaction log.

For replicated queues, passthrough must be enabled for any incoming group that replicates the queue topic, including the local group.

```xml
<Replication>  
    <Destination>  
        <Name>AMPS2-HKG</Name>  
        <Transport>  
            <Name>amps-replication</Name>  
            <Type>amps-replication</Type>  
            <InetAddr>secondaryhost:10010</InetAddr>  
        </Transport>  
        <Topic>  
            <Name>/rep_topic</Name>  
            <MessageType>fix</MessageType>  
        </Topic>  
        <Topic>  
            <Name>/rep_topic2</Name>  
            <MessageType>fix</MessageType>  
        </Topic>  
        <SyncType>sync</SyncType>  
        <PassThrough>^((?!HKG).)*$</PassThrough>  
     </Destination>  
</Replication>
```

### Replicated Queues

AMPS replicates `publish` commands to the underlying topic, `sow_delete` acknowledgments, and internal queue management commands.

### Queue Message Ownership

Only one instance owns a message at a time.

- `Queue`: Owner is the instance that received the initial publish.
- `GroupLocalQueue`: Owner is the `InitialOwner` instance.
- `LocalQueue`: Not replicated; each instance owns its copy.

| Queue Type | Initial Owner |
| --- | --- |
| `Queue` | Instance where the message was published. |
| `GroupLocalQueue` | Instance specified in the `InitialOwner` tag. |
| `LocalQueue`  *cannot be replicated* | *N/A*  Each instance owns its copy of the message. The queue is not replicated. Each instance will independently deliver its copy of the message. |

An instance requests ownership transfer if it has matching subscriptions with available backlog and the message has been present longer than typical acknowledgment time. The owning instance grants transfer if the request is first and no local subscriber can accept the message.

#### Disaster Recovery and Queue Message Ownership

If an owner is offline, enable `enable_proxied_transfer` to allow another instance to act as an ownership proxy. Use with care: choose one remaining instance, fail over clients, enable proxied transfer, recover the offline instance without client connections, disable proxied transfer, then re-enable client connections.

### Configuration for Queue Replication

Requirements for distributed queue replication:
1. Bidirectional replication between instances.
2. If a topic is a queue on one instance, it must be a queue on all.
3. Same underlying topic definitions and filters on all instances. Same `InitialOwner` for `GroupLocalQueue`.
4. Underlying topics must be replicated to all instances.
5. Passthrough must be provided for instances that replicate queues.

`LocalQueue` cannot be replicated.

### Replication Basics

- **Replication is point-to-point**. Each replication connection involves exactly two AMPS instances: a source and a destination.
- **Replication is always "push" replication**. The source configures a destination and pushes messages to that destination.
- **Replication is one-link by default**. By default, an instance only replicates messages published directly to that instance by a client. `PassThrough` replicates messages received via replication.
- **Replication relies on the transaction log**. AMPS replicates commands as preserved in the transaction log. Replication always provides messages to a destination in the order in which the messages are recorded in the transaction log.
- **Replication provides a command stream**. AMPS replicates the results of `publish`, `delta_publish` and `sow_delete` commands once those results are written to the transaction log.
- **Replication is customizable by topic, message type, and content**.
- **Replication guarantees delivery**. AMPS will not remove a journal file until *all* messages in that journal file have been replicated to, and acknowledged by, the destination.
- **Replication is composable**.
- **Replication acknowledgment is configurable**. `async` acknowledgment provides durability guarantees for the local instance, whereas `sync` acknowledgment provides durability guarantees for the local instance and the downstream instance.
- **Group identifies a set of instances that are intended to be fully equivalent.**

### Benefits of Replication

Replication provides fault tolerance and remote site delivery. AMPS guarantees messages are not removed from the transaction log until all destinations acknowledge.

### Replication Best Practices

- Every client must have a distinct client name.
- Use replication filters with caution, especially for queues.
- Do not manually set client sequence numbers.
- Default to `PassThrough` for every group.
- Do not allow failover between instances using `async` acknowledgment.

### Replication Compression

Configure compression at the replication source. Receiving instances automatically detect compressed connections.

### Replication Configuration Validation

Starting in 5.0, AMPS validates replication configuration on connection. Differences that could cause message loss or inconsistent behavior are reported as errors. Validation can be excluded per topic.

AMPS performs the following checks:

`txlog`

Validates that the topic is contained in the transaction log of the downstream instance.

`replicate`

Validates that the topic is replicated from the downstream instance back to this instance.

`sow`

Validates that if the topic is a `SOW/Topic` in this instance, it must also be a `SOW/Topic` in the downstream instance.

`cascade`

Validates that the downstream instance must enforce the same set of validation checks for this `Topic` as this instance does.

`queue` (mandatory, cannot be excluded)

Validates that if the topic is a queue in this instance, it must also be a queue in the downstream instance.

`keys`

Validates that if the topic is a `SOW/Topic` in this instance, it must also be a `SOW/Topic` in the downstream instance and the `SOW/Topic` in the downstream instance must use the same `Key` definitions.

`replicate_filter`

Validates that if this topic uses a replication filter, the downstream instance must use the same replication filter for replication back to this instance.

`queue_passthrough`

Validates that if the topic is a queue in this instance, the downstream instance must support passthrough from this group to its replication destinations.

`queue_underlying` (mandatory, cannot be excluded)

Validates that if the topic is a queue in this instance, it must use the same underlying topic definition and filters in the downstream instance.

### Replication Configuration Validation: Example

```text
<Destination>  
    ...  
    <Topic>  
        <MessageType>json</MessageType>  
        <Name>MyStuff-VIEW</Name>  
        <ExcludeValidation>replicate,cascade</ExcludeValidation>  
    </Topic>  
    ...  
</Destination>
```

### Replication Resynchronization

On connection, the upstream instance replays messages the downstream may have missed. As of 5.3.3.0, instances exchange checkpoint information to find the earliest missing message. Older versions resync from the last received point.

### Replication Security

Configure `Authentication` and `Entitlement` on the incoming replication transport. Configure `Authenticator` on the outgoing destination transport.

```xml
<Transports>  
    <Transport>  
        <Name>amps-replication</Name>  
        <Type>amps-replication</Type>  
        <InetAddr>10005</InetAddr>  
        <Entitlement>  
            <Module>amps-default-entitlement-module</Module>  
        </Entitlement>  
        <Authentication>  
            <Module>amps-default-authentication-module</Module>  
       </Authentication>  
    </Transport>  
 ...  
</Transports>
```

```xml
<Replication>  
    <Destination>  
        <Topic>  
            <MessageType>fix</MessageType>  
            <Name>topic</Name>  
        </Topic>  
        <Name>amps-1</Name>  
        <SyncType>async</SyncType>  
        <Transport>  
            <InetAddr>amps-1-server.example.com:10004</InetAddr>  
            <Type>amps-replication</Type>  
            <Authenticator>  
                <Module>amps-default-authenticator-module</Module>  
            </Authenticator>  
        </Transport>  
    </Destination>  
</Replication>
```

### Two-Way Replication

Each instance defines a replication `Transport` and `Destination` to the other. Use `sync` acknowledgment for failover partners. Starting in 5.0, AMPS optimizes to a single network connection for back replication.

### Understanding Replication Message Routing

A message is replicated to a destination when:
1. It is recorded in the transaction log.
2. The destination is configured for the topic and message type (and matches content filter).
3. It was published directly, or `PassThrough` matches the originating group.
4. It has not previously passed through that destination.

### AMPS Distribution Layout

| Item | Description |
| --- | --- |
| `/bin` | AMPS binaries: the AMPS server, daemon deployment scripts, AMPS utilities, and `spark`. |
| `/docs` | AMPS base documentation. Current versions of the documentation and additional guides are available from the 60East website. |
| HISTORY | Information on the AMPS revision history. In current distributions, this provides a link to the entry for this release of AMPS within the full revision history on the 60East web site. |
| `/lib` | Libraries used by the AMPS binary. |
| LICENSE | The AMPS license. |
| README | The README file for AMPS. |
| `/sdk` | Headers used for modules that extend AMPS. |

### /bin directory

| Item | Description |
| --- | --- |
| `amps_bio_perf_test` | Diagnostic tool for testing the performance of I/O systems. |
| `amps_clients_ack_dump` | Utility for showing the contents of the AMPS clients.ack file, containing persistent per-client information. |
| `ampserr` | Utility for looking up details on AMPS log file items. |
| `ampServer` | The AMPS server binary. |
| `ampServer-compat` | The downward compatible version of the AMPS server binary. This version avoids using some of the hardware capabilities present in newer CPU architectures. |
| `amps_file` | A utility for identifying the type of AMPS files and the file format that the file uses. |
| `amps-init-script` | Part of the AMPS service installation. This script is installed into the init.d directory when the AMPS service is installed. |
| `amps_journal_dump` | Utility for extracting the contents of AMPS transaction log journal files. |
| `amps_mt_perf_test` | Diagnostic tool for performance testing of the AMPS engine parsing infrastructure. |
| `amps_sow_dump` | Utility for extracting the contents of AMPS SOW files. |
| `amps-sqlite3` | Convenience wrapper for querying an AMPS statistics database. |
| `amps_upgrade` | Utility for upgrading data files from previous versions of AMPS to the current version. |
| `install-amps-daemon.sh` | Installation script for installing AMPS as a Linux service. |
| `/lib` | Directory containing the libraries used by the `spark` utility. |
| `spark` | Utility that provides a command-line interface to AMPS. |
| `uninstall-amps-daemon.sh` | Installation script for removing the AMPS Linux service from the system. |

### Securing AMPS

AMPS security uses a plugin model for authentication and entitlement. Three aspects: authentication (identity), entitlement (permissions), and providing credentials to downstream instances.

#### Loadable Authentication/Entitlements Modules

- `libamps_http_entitlement`: RESTful web service authentication and entitlement.
- `libamps_multi_authentication`: LDAP and Kerberos authentication.
- `libamps_simple_access_entitlement`: Restricts access to specific resources.
- `libamps_oauth_authentication.so`: OAuth 2.0 authentication.

#### Authentication

AMPS assigns an identity via explicit `logon` or implicit logon (disabled by default in 5.0+). The transport's authentication module is used, falling back to the instance module, defaulting to `amps-default-authentication-module`.

Default modules:

| Module | Description |
| --- | --- |
| `amps-default-authentication-module` | Allows any username and password. Does not allow implicit logon by default. Does not provide the username to AMPS by default. |
| `amps-implicit-authentication-module` | Allows any username and password. Allows implicit logon by default. Does not provide the username to AMPS by default. |
| `amps-default-no-authentication-module` | Does not allow authentication regardless of the username and password provided.  This can be useful for testing application behavior when logon is denied, or for setting a policy for the instance that individual transports must override. |

#### Enabling Implicit Logon

```xml
<AMPSConfig>  
  ...  
  <Authentication>  
     <Module>amps-implicit-authentication-module</Module>  
  </Authentication>  
  ...  
</AMPSConfig>
```

#### Loadable Authenticator Modules

- `libamps_multi_authenticator`: Credentials for outgoing replication.
- `libamps_exec_authenticator`: External command credentials.

#### Configuring Authentication

`Module`: Name of the authentication module.
`Options`: Module-specific options.

#### Authentication Modules Loaded by Default

`amps-default-authentication-module`
- `AllowSpoofing`: Provide username to AMPS. Default: `false`.
- `RequireLogon`: Disallow implicit logon. Default: `true`.
- `RequireUsername`: Require username. Default: `false`.

`amps-implicit-authentication-module`
- `AllowSpoofing`: Default: `false`.

`amps-default-no-authentication-module`
Denies all authentication.

#### Configuring Entitlement

`Module`: Name of the entitlement module.
`Options`: Module-specific options.

#### Entitlement Modules Loaded by Default

AMPS loads `amps-default-entitlement-module` (allows all) by default.

#### Entitlement

AMPS checks entitlements per command and caches results. Clearing the cache disconnects users. Entitlements are not rechecked after a subscription is created.

#### Entitlement Resource and Permission Types

| Resource Type | Description |
| --- | --- |
| `logon` | Permission to log on to the AMPS instance. |
| `replication_logon` | Permission to log on to the AMPS instance as a replication source. |
| `topic` | Permission to receive from or publish to a specific topic. |
| `admin` | Permission to read admin statistics or perform admin functions from the web interface. |

| AMPS Command | Entitlement Type |
| --- | --- |
| `delta_subscribe`,  `sow`, `sow_and_subscribe`,  `subscribe`, `sow_and_delta_subscribe` | `read` |
| `delta_publish`, `publish`,  `sow_delete` | `write` |
| *Commands received over replication* | `replication allowed` |

#### Administrator Actions using HTTP

Admin actions are HTTP `GET` requests treated as `read` to an `admin` resource. `write` is not required.

#### Entitlement Caching

AMPS caches entitlement results. External modules must establish policies for cache reset.

#### Regular Expression Subscriptions

AMPS makes a separate entitlement request for each matching topic when a message is ready to deliver.

#### Content Filtered Entitlements

Entitlement modules can return filters for `read` and `write`. Both subscription and entitlement filters must match. For `delta_publish`, the filter applies to the delta.

For `sow_delete` with regex topics, permissions and filters are checked per topic.

#### Entitlement Select Lists

Entitlement modules can return select lists to restrict fields. Applied before subscriber select lists.

| Message | Entitlement Select List | Subscriber Select List | Result |
| --- | --- | --- | --- |
| `{"a":1, "b":2}` | `-/a` | `+/a` | `{"b":2}` |
| `{"a":1, "b":2}` | `-/,+/b` | (none) | `{"b":2}` |
| `{"a":1, "b":2, "c": {"c1":1, "c2":2, "c3":3} }` | `-/,+/b,+/c/c2` | `-/,+/c/c1,+/c/c2` | `{"c":{"c2":2}}` |

#### Queues and Views: Message Queues (Entitlements)

- `read` on a queue grants delete (acknowledge) ability.
- `write` on a queue grants publish to the queue (routed to `DefaultPublishTopic`).

#### Multiple Logical Topics in a Physical SOW Topic

Entitlements apply to the physical topic name. Use `TOPIC_NAME()` in entitlement filters to restrict logical topics.

#### Disabling Entitlement

Use `amps-do-disable-entitlements` to disable until restart or explicit re-enable. When disabled, all requests succeed, no filters or select lists are applied.

Re-enabling with `amps-action-do-enable-entitlements` initializes new contexts and clears the cache but does not disconnect existing clients or validate current subscriptions. Run an entitlement reset after re-enabling.


### Provided Entitlement Modules

AMPS loads two simple entitlement modules by default, useful for testing and development.

| Module | Description |
| --- | --- |
| `amps-default-entitlement-module` | Allows any user to access any resource. |
| `amps-default-no-entitlement-module` | Denies access to all resources for all users. |

AMPS also includes two modules that must be explicitly loaded and configured.

| Module | Description |
| --- | --- |
| `libamps_simple_access_entitlement.so` | Provides a simple allow/deny list for all users on the transport. |
| `libamps_http_entitlement.so` | Makes requests to an external RESTful service for authentication and entitlements. |

### Command Execution Authenticator

AMPS includes a module that provides credentials for outgoing replication connections using the results of an external process. This is designed for cases where a site has an authentication system that requires short-lived tokens and does not use Kerberos.

In this release, the exec authenticator module is provided with AMPS, but is not loaded by default. It must be explicitly loaded, enabled, and configured.

This module runs an external application with the credentials of the AMPS server itself. Avoid using this module unless you fully trust the application and have verified that the command line provided is correct.

Use this module when a replication connection is authenticated, when a system other than Kerberos is in use, when the credentials cannot be provided in the configuration file or stored in the filesystem, and when an executable program or script is available that can produce the credentials.

If the credentials can be provided in the configuration file or stored in a file, use the `amps-default-authenticator-module`. If Kerberos is in use, use the Multimethod Authenticator.

The exec authenticator module is included in the AMPS distribution but is not loaded by default. To load the module, add the following configuration item to the `Modules` block:

```text
<Modules>
    ...

    <Module>
        <Name>exec-authenticator</Name>
        <Library>libamps_exec_authenticator.so</Library>
    </Module>

    ...
</Modules>
```

This module does not require any options as a part of the module configuration and ignores any options provided when the module is loaded.

This module supports the following options when used in an `Authenticator` block:

| Option | Description |
| --- | --- |
| `Command` | Sets the command to run. This can be either an absolute path or a relative path based on the current working directory of the AMPS server process. The command supports the following expansions when the command runs: `AMPS_USER_NAME` - The user name that the authenticator will provide to the remote server. The authenticator will read the stdout of that command and provide the result as the authentication token for the connection. There is no default for this parameter. |
| `MaxLength` | Sets the maximum number of bytes to read from the command. Default: `1024` |
| `UserName` | Sets the user name to provide on this connection. There is no default for this parameter. |

The module must be configured with a `Command` and `UserName`. Otherwise, the module fails to initialize and AMPS will halt the startup process.

```xml
<AMPSConfig>

    <Modules>
        ...

        <Module>
            <Name>run-proc-authenticator</Name>
            <Library>libamps_exec_authenticator.so</Library>
        </Module>

        ...
    </Modules>

    ...

    <Replication>

        ...

        <Destination>
           <Transport>

              <Type>amps-replication</Type>
              <InetAddr>my-failover-partner:4000</InetAddr>

              <Authenticator>
                   <Module>run-proc-authenticator</Module>
                   <Options>
                         <UserName>${USER}</UserName>
                         <Command>./auth-widget --user "{{AMPS_USER_NAME}}" --output=stdout</Command>
                   </Options>
              </Authenticator>

            </Transport>
        </Destination>
     </Replication>

</AMPSConfig>
```

### RESTful Authentication and Entitlements

The AMPS distribution includes a module that provides authentication and entitlement via an external Web Service.

In this release, the HTTP authentication module is provided with AMPS, but is not loaded by default. It must be explicitly loaded, enabled, and configured.

When using this module, AMPS requests permissions documents from an external service using `http` or `https`. The request includes the credentials provided with the client logon. If the request succeeds, the module considers the user authenticated. When authentication succeeds, the contents of the returned document specify the permissions granted to the user.

The web service module expects that the endpoint will follow RESTful (HTTP) semantics. The module uses standard HTTP headers for authentication and expects that invalid credentials will return an HTTP 403 status code.

Use the Web Service module when the site does not have an existing authentication infrastructure for AMPS, when it is more feasible to develop a standalone web service than a server plugin, when applications need to integrate with an existing system that offers limited Linux or C/C++ support, or when an application needs an easy way to test entitlement scenarios.

#### Permissions Document Format

All documents are in JSON format. The document is not required to contain the user name, which allows systems to provide identical permissions for all users in a group.

```json
{
  "logon": true,
  "replication-logon" : false,
  "topic": [
            { "topic": "test",
              "read": "/priority = 1",
              "write": false },
            { "topic": ".*",
              "read": true,
              "write": true }
           ],
  "admin": [
            { "topic": "^/amps/instance/.*",
              "read": true,
              "write": false },
            { "topic": ".*",
              "read": false,
              "write": false }
           ]
}
```

The Web Authentication Module processes entitlements in document order. This set of entitlements specifies:

- The user is authenticated as having provided valid credentials.
- The user has permission to log on to AMPS.
- The user does not have permission to make a replication connection.
- The user has read permissions to the topic `test` for messages matching `/priority = 1`, with no write permissions.
- The user has read and write permissions to every other topic without content restrictions.
- The user has read permissions to the administrative interface under `/amps/instance`.
- The user has no other permissions to the administrative interface.

The module also allows fine-grained control of topics allowed for replication via `replicated-topics`:

```json
{
  "replication-logon": true,
  "logon": false,
  "replicated-topics":["^/orders/NYC/.*",
                       "/events/P1"],
  "user_name": "replication-user"
}
```

This document specifies that the credentials are valid for `replication-user`, who can only log on via replication connections and can replicate to `/events/P1` and topics beginning with `/orders/NYC`. The user cannot log on from an AMPS client or publish/subscribe without explicit `topic` permission.

#### Client Transport Permissions

| Field | Value |
| --- | --- |
| `logon` | Specifies permission for an application to log on to AMPS. Must be boolean `true` or `false`. |
| `topic` | Controls access to topics. Value must be a *permission list*. If absent, the user cannot publish to or subscribe to any topics. |

#### Admin Transport Permissions

| Field | Value |
| --- | --- |
| `admin` | Controls access to the administrative interface. Value must be a *permission list*. If absent, the user has no access. |

#### Replication Transport Permissions

| Field | Value |
| --- | --- |
| `replication-logon` | Controls permission for a replication connection to log on. Must be boolean `true` or `false`. |
| `replicated-topics` | Array of topics the user can replicate to. If absent, the user cannot replicate to any topics. |

#### Connection Properties

| Field | Value |
| --- | --- |
| `user_name` | Sets the authenticated user name to the provided value. Ignored in `EntitlementsOnly` mode. If absent, the logon request user name is used. |

#### Permissions Lists

| Field | Value |
| --- | --- |
| `topic` | Name of the topic, either literal or regular expression. Interpreted as regex if it contains regex characters. Supports PCRE. |
| `read` | `true`, `false`, or an AMPS filter. Grants read permission with no restrictions, denies it, or grants only for matching messages. |
| `write` | `true`, `false`, or an AMPS filter. Grants write permission with no restrictions, denies it, or grants only for matching messages. |
| `select` | Defines the entitlement select list. Should include only the select list specifier, not enclosing brackets. |

#### Indicating Authentication Failure

To indicate authentication failure, the web service should return a `403` HTTP status code and no authentication document.

To provide default permissions on failure, return a document that validates the request for a default user:

```json
{
  "user_name":"default-unauthenticated-permissions",
  "replication-logon": false,
  "logon": true,
  "topic": [
            { "topic": "^PUBLIC-",
              "read":  true,
              "write": false}
           ]
}
```

All connections for the same user name on the same Transport use the same permissions. If a logon fails, any permissions document returned should use `user_name` to set a default user name or a user name with no permissions.

#### Configuring AMPS to use Web Service Authentication and Entitlements

To load the module, add the following to the `Modules` block:

```text
<Modules>
    ...

    <Module>
        <Name>web-entitlements</Name>
        <Library>libamps_http_entitlement.so</Library>

        <!-- You may specify options here, or where the module is used.-->
    </Module>

    ...
</Modules>
```

Options may be set when the module is loaded, when used for `Authentication` or `Entitlement`, or in both places. Options set when loaded are inherited as defaults; options in `Authentication` or `Entitlement` blocks override them.

For `Authentication`, the module supports:

| Option | Description |
| --- | --- |
| `ResourceURI` | The URI to request when a user logs in. Required. Substitutes `{{USER_NAME}}` with the user being authenticated. |
| `CredentialStore` | Identifier for the entitlement cache. Default: literal value of `ResourceURI`. |
| `ConnectionTimeout` | Max time to wait for a connection, in milliseconds. Default: `2000` |
| `RequestTimeout` | Max time to wait for the server to return a permissions document, in milliseconds. Default: `5000` |
| `RetryCount` | Number of retries if retrieving the document fails. Default: `0` |
| `HTTPHeader` | Header to add to the HTTP request. Supports variable replacement. |
| `EntitlementTimeout` | Time to consider an entitlements document valid. After expiration, AMPS checks for changes and resets permissions if the document differs. |
| `ReuseConnections` | Reuse HTTP connections when possible. Only supported in `Modules` definition. Default: `disabled` |
| `ServerAcceptsEmptyAuthId` | Submit requests even when no auth ID is provided. Default: `false` |

The following tokens are expanded in `HTTPHeader`:

| Authentication Header Token | Expansion |
| --- | --- |
| `AMPS_CLIENT_NAME` | Client name provided in the logon request. |
| `AMPS_CONNECTION_NAME` | Connection name assigned by the AMPS server. |
| `AMPS_CORRELATION_ID` | Correlation ID provided on the logon request. |
| `AMPS_MESSAGE_TYPE` | Message type of the logon request. |
| `AMPS_PASSWORD` | Password provided with the logon request. |
| `AMPS_REMOTE_ADDRESS` | Remote address from which the logon request was made. |
| `AMPS_USER_NAME` | User name for the request. |
| `CORRELATION_ID` | Legacy compatibility token. |
| `USER_NAME` | Legacy compatibility token. |

For `Entitlement` blocks, one of the following is required:

| Option | Description |
| --- | --- |
| `ResourceURI` | Synonym for `CredentialStore`. Either this or `CredentialStore` must be provided. |
| `CredentialStore` | Identifier for the entitlement cache. Either this or `ResourceURI` must be provided. If both are provided, `CredentialStore` is used. |

```xml
<AMPSConfig>
    <Modules>
        ...

        <Module>
            <Name>web-entitlements</Name>
            <Library>libamps_http_entitlement.so</Library>

            <Options>
                <ResourceURI><http://permissions-server:8080/{{USER_NAME}}.json</ResourceURI>>
                <HTTPHeader>x-tracking-id: {{CORRELATION_ID}}</HTTPHeader>
                <HTTPHeader>x-origin: AMPS</HTTPHeader>
            </Options>
        </Module>

        ...
    </Modules>

    <Authentication>
        <Module>web-entitlements</Module>
    </Authentication>
    <Entitlement>
        <Module>web-entitlements</Module>
    </Entitlement>

    <Admin>
        <InetAddr>localhost:8085</InetAddr>
        <WWWAuthenticate>Basic realm="AMPS Admin"</WWWAuthenticate>
        <Authentication>
            <Module>web-entitlements</Module>
            <Options>
                <CredentialStore>AdminCreds</CredentialStore>
                <ResourceURI><http://permissions-server:8080/admin/{{USER_NAME}}.json</ResourceURI>>
            </Options>
        </Authentication>
        <Entitlement>
            <Module>web-entitlements</Module>
            <Options>
                <ResourceURI><http://permissions-server:8080/admin/{{USER_NAME}}.json</ResourceURI>>
                <CredentialStore>AdminCreds</CredentialStore>
            </Options>
        </Entitlement>
    </Admin>

    <Transports>
        <Transport>
            <Name>json-tcp</Name>
            <Type>tcp</Type>
            <InetAddr>9007</InetAddr>
            <MessageType>json</MessageType>
            <Protocol>amps</Protocol>
        </Transport>
        <Transport>
            <Name>any-tcp</Name>
            <Type>tcp</Type>
            <InetAddr>9090</InetAddr>
            <Protocol>amps</Protocol>
        </Transport>
    </Transports>
</AMPSConfig>
```

#### Using HTTPS for Authentication and Entitlement Requests

When `ResourceURI` uses `https`, the module attempts `https` connections. By default, it verifies the remote identity, requiring a `CAKey`. The certificates for outgoing `https` requests need not be the same as those for incoming SSL connections.

| Option | Description |
| --- | --- |
| `Certificate` | Certificate for the AMPS connection to the web service. |
| `Key` | Key file for the AMPS connection. |
| `CAKey` | Certificate authority key for verifying the web service identity. Required when `AllowUnverifiedPeer` is `false` (default). |
| `AllowUnverifiedPeer` | Whether AMPS requires the web service to identify itself. Default: `false` |
| `AllowSelfSigned` | Whether AMPS accepts self-signed certificates. Default: `false` |

Testing configuration (does not verify identity):

```json
<Module>
    <Name>web-entitlements</Name>
    <Library>libamps_http_entitlement.so</Library>
    <Options>
        <ResourceURI><https://permissions-server:443/{{USER_NAME}}.json</ResourceURI>>
        <AllowUnverifiedPeer>true</AllowUnverifiedPeer>
        <AllowSelfSigned>true</AllowSelfSigned>
    </Options>
</Module>
```

Full verification configuration:

```json
<Module>
    <Name>web-entitlements</Name>
    <Library>libamps_http_entitlement.so</Library>
    <Options>
        <ResourceURI><https://permissions-server:443/{{USER_NAME}}.json</ResourceURI>>
        <Certificate>/etc/security/amps-cert.pem</Certificate>
        <Key>/etc/security/amps-key.pem</Key>
        <CAKey>/etc/security/ca.pem</CAKey>
    </Options>
</Module>
```

#### Permissions Management and Request Flow

Authentication and entitlement are separate steps. The module obtains permissions during authentication and responds to entitlement requests during the entitlement step. In default mode, a user must have authenticated using the module for an entitlement request to be allowed.

**Authentication Step**

1. Logon request received from the client.
2. Module requests an entitlement document via `GET` from the Web Service, using logon credentials. Supports Basic and Digest authentication.
3. If the document cannot be retrieved, logon fails.
4. If a parsed document already exists in the `CredentialStore`, authentication succeeds immediately.
5. Otherwise, the module parses and stores the document. If parsing fails, authentication fails.

**Entitlement Step**

1. The module looks up the user in the `CredentialStore`. If no stored entitlements exist, the request is denied.
2. The module searches entitlements in document order and uses the first match. If no match, the request is denied.
3. If the entitlement disallows access, the request is denied.
4. Otherwise, the request is allowed and any filter is applied.

**Entitlement Reset**

The module caches entitlements while a user is connected. Two mechanisms update permissions:

1. Reset on disconnect: when all connections for a user close, both the AMPS entitlement cache and module cache are reset. Changes to the web service document have no effect until all connections disconnect and the user logs back in.
2. `EntitlementTimeout`: when set, a change after the timeout resets current connections, clears the cache, and replaces permissions with the newly returned document.

This is not available in `EntitlementOnly` mode.

#### Entitlement Only Mode

Starting in AMPS 5.3.1, another module can handle authentication while this module handles entitlements. Enable by adding `<EntitlementOnly/>` in the module's `Options`.

When enabled, the module makes an *unauthenticated* HTTP request the first time permissions are requested for a user ID. The returned document is cached for subsequent requests.

In this mode, the module cannot be used in an `Authentication` block. The module or `Entitlement` block must provide a `ResourceURI`.

#### Entitlement Only Request Flow

1. Entitlement request received from AMPS.
2. The module looks up the user in the `CredentialStore`. If stored entitlements exist, skip to step 6.
3. Module requests an entitlement document via `GET`. No credentials are provided.
4. If retrieval fails, the entitlement request fails.
5. The module parses the document. If parsing fails, the request fails. Otherwise, it loads permissions into the `CredentialStore`.
6. The module searches entitlements in document order and uses the first match. If no match, the request is denied.
7. If the entitlement disallows access, the request is denied.
8. If allowed, any filters are applied.

### Providing an Identity for Outbound Connections

For outgoing replication connections, AMPS uses an authenticator to provide credentials. The default `amps-default-authenticator-module` provides a user name with no password. It uses the `User` option if provided, otherwise the current user of the AMPS process, or the `USER` environment variable.

The `amps-default-authenticator-module` can send a specific password (version 5.3.0.0+):

| Option | Description |
| --- | --- |
| `Password` | Provide the contents as the password. |
| `PasswordFileName` | Read the password from the specified file. |
| `PasswordEnvironmentVariable` | Read the password from the specified environment variable. |

The Authenticator used for a replication `Destination` must provide credentials accepted by the remote instance's `Transport`.

If Kerberos is used for replication security, the AMPS distribution includes an authenticator that can provide Kerberos tokens via the Multimethod Authenticator Module. The AMPS distribution also includes the Command Execution Authenticator module.

### Multimethod Authentication Module

AMPS includes a module supporting LDAP or Kerberos authentication. It is not loaded by default and must be explicitly loaded, enabled, and configured.

This module provides authentication but not entitlements.

Use this module when integrating AMPS into an existing infrastructure that supports Kerberos or LDAP.

To enable a mechanism, provide its configuration parameters. When more than one is enabled, the module detects the mechanism from the credentials. If it cannot determine and multiple mechanisms are configured, it defaults to `DefaultAuthenticationMechanism`.

The module does not allow LDAP (arbitrary passwords) to be configured with Kerberos (specific format passwords). A given module can use Kerberos *or* LDAP, but not both.

To load the module:

```text
<Modules>
    ...

    <Module>
        <Name>multimech-authentication</Name>
        <Library>libamps_multi_authentication.so</Library>
    </Module>

    ...
</Modules>
```

This module does not require options at load time.

**Kerberos Options**

| Option | Description |
| --- | --- |
| `Kerberos.Keytab` | Keytab file path. Required when using Kerberos; `Kerberos.SPN` must also be specified. |
| `Kerberos.SPN` | Service Principal Name. Required when using Kerberos; `Kerberos.Keytab` must also be specified. |

**LDAP Options**

| Option | Description |
| --- | --- |
| `LDAP.Host` | Host name for LDAP. Required if any other `LDAP` parameter is specified. |
| `LDAP.Port` | Port for LDAP. Default: `389` |
| `LDAP.ProtocolVersion` | LDAP protocol version. Default: `2` |
| `LDAP.BaseDN` | Base Distinguished Name. Defaults to empty string. |
| `LDAP.ServiceAccountDN` | Service account DN. Defaults to empty string. |
| `LDAP.ServiceAccountPasswordFile` | File containing the service account password. Defaults to empty string. |

**General Options**

| Option | Description |
| --- | --- |
| `AllowAnonymous` | Allows logon without password, setting username to empty string. Default: `disabled` |
| `DefaultAuthenticationMechanism` | Default mechanism if AMPS cannot identify the token type. Value can be `Kerberos` or `LDAP`. |

At least one authentication method must be configured, or the module fails to initialize and AMPS halts startup.

LDAP example:

```xml
<AMPSConfig>

    <Modules>
        ...

        <Module>
            <Name>multi-auth</Name>
            <Library>libamps_multi_authentication.so</Library>
        </Module>

        ...
    </Modules>

    <Authentication>
        <Module>multi-auth</Module>
        <Options>
           <LDAP.Host>myenterprise-auth-server</LDAP.Host>
           <LDAP.Port>9389</LDAP.Port>
        </Options>
    </Authentication>

    <Admin>
        <InetAddr>localhost:8085</InetAddr>
    </Admin>

    <Transports>
        <Transport>
            <Name>json-tcp</Name>
            <Type>tcp</Type>
            <InetAddr>9007</InetAddr>
            <MessageType>json</MessageType>
            <Protocol>amps</Protocol>
        </Transport>
        <Transport>
            <Name>any-tcp</Name>
            <Type>tcp</Type>
            <InetAddr>9090</InetAddr>
            <Protocol>amps</Protocol>
        </Transport>
    </Transports>

</AMPSConfig>
```

Kerberos example:

```xml
<AMPSConfig>

    <Modules>
        ...

        <Module>
            <Name>multi-auth</Name>
            <Library>libamps_multi_authentication.so</Library>
        </Module>

        ...
    </Modules>

    <Authentication>
        <Module>multi-auth</Module>
        <Options>
            <Kerberos.SPN>AMPS/host.domain.com</Kerberos.SPN>
            <Kerberos.Keytab>/path/to/amps.keytab</Kerberos.Keytab>
       </Options>
    </Authentication>

    <Admin>
        <InetAddr>localhost:8085</InetAddr>
    </Admin>

    <Transports>
        <Transport>
            <Name>json-tcp</Name>
            <Type>tcp</Type>
            <InetAddr>9007</InetAddr>
            <MessageType>json</MessageType>
            <Protocol>amps</Protocol>
        </Transport>
        <Transport>
            <Name>any-tcp</Name>
            <Type>tcp</Type>
            <InetAddr>9090</InetAddr>
            <Protocol>amps</Protocol>
        </Transport>
    </Transports>

</AMPSConfig>
```

### Providing Replication Credentials with the AMPS Multimechanism Authenticator Module

AMPS includes a module that provides credentials for outgoing replication connections when the multimechanism authenticator module is in use at the destination.

This module can provide credentials for LDAP and Kerberos. It is not loaded by default and must be explicitly loaded, enabled, and configured.

Use this module when a replication connection is authenticated and uses the multimechanism module with Kerberos configured. It can also be useful for LDAP, though `amps-default-authenticator` may suffice.

To enable a mechanism, provide its configuration parameters.

To load the module:

```text
<Modules>
    ...

    <Module>
        <Name>multimech-authenticator</Name>
        <Library>libamps_multi_authenticator.so</Library>
    </Module>

    ...
</Modules>
```

This module does not require options at load time.

**Kerberos Options**

| Option | Description |
| --- | --- |
| `Kerberos.Keytab` | Keytab file path. Required when using Kerberos; `Kerberos.SPN` must also be specified. |
| `Kerberos.SPN` | Service Principal Name. Required when using Kerberos; `Kerberos.Keytab` must also be specified. |

**LDAP Options**

| Option | Description |
| --- | --- |
| `LDAP.Username` | Username for LDAP authentication. Required if `LDAP.PasswordFile` is specified. |
| `LDAP.PasswordFile` | File containing the password. Required if `LDAP.Username` is specified. |

At least one method must be configured, or the module fails to initialize.

```xml
<AMPSConfig>

    <Modules>
        ...

        <Module>
            <Name>multi-authenticator</Name>
            <Library>libamps_multi_authenticator.so</Library>
        </Module>

        ...
    </Modules>

    ...

    <Replication>

        ...

        <Destination>
            <Transport>

              <Type>amps-replication</Type>
              <InetAddr>my-failover-partner:4000</InetAddr>

              <Authenticator>
                   <Module>multi-authenticator</Module>
                   <Options>
                         <Kerberos.SPN>AMPS/host.domain.com</Kerberos.SPN>
                         <Kerberos.Keytab>/path/to/amps.keytab</Kerberos.Keytab>
                   </Options>
              </Authenticator>

            </Transport>
        </Destination>

     </Replication>

</AMPSConfig>
```

### OAuth Authentication

The AMPS distribution includes a module that provides authentication through OAuth 2.0. It is not loaded by default and does not provide entitlements.

Use the OAuth module when the site has an existing OAuth authorization server and wants to use it for AMPS access.

To load the module:

```text
<Modules>
    ...

    <Module>
        <Name>oauth-authentication</Name>
        <Library>libamps_oauth_authentication.so</Library>

        <!-- You may specify options here, or where the module is used.-->
    </Module>

    ...
</Modules>
```

Options may be set when the module is loaded or when used for `Authentication`. Loaded options are inherited as defaults; `Authentication` block options override them.

| Option | Description |
| --- | --- |
| `TokenEndpoint` (required) | URI endpoint of the authorization server. |
| `RedirectURI` (required) | URI to which the authorization server redirects the user. |
| `ClientID` (required) | Unique identifier issued by the authorization server. |
| `ClientSecret` (required) | Secret for AMPS to identify itself. |
| `GrantType` | Type of OAuth grant or flow. Default: `authorization_code` |
| `RequestTimeout` | Max time to wait for the server to return a result, in milliseconds. Default: `5000` |
| `RetryCount` | Number of retries if retrieving the response fails. Default: `0` |
| `HTTPHeader` | Header to add to the HTTP request. |

```xml
<AMPSConfig>
    <Modules>
        ...

        <Module>
            <Name>oauth-authentication</Name>
            <Library>libamps_oauth_authentication.so</Library>

            <Options>
                <TokenEndpoint><https://oauth.example.com/token</TokenEndpoint>>
                <ClientID>app-specific-id-from-server</ClientID>
                <ClientSecret>validation-to-server</ClientSecret>
                <RedirectURI><http://localhost:3000</RedirectURI>>
                <AllowUnverifiedPeer>true</AllowUnverifiedPeer>
            </Options>
        </Module>

        ...
    </Modules>

    <Authentication>
        <Module>oauth-authentication</Module>
        <Options>
             <TokenEndpoint><https://oauth.example.com/token</TokenEndpoint>>
             <ClientID>app-specific-id-from-server</ClientID>
             <ClientSecret>token-token-token</ClientSecret>
             <RedirectURI><http://localhost:3000</RedirectURI>>
             <AllowUnverifiedPeer>true</AllowUnverifiedPeer>
        </Options>
    </Authentication>

    <Admin>
        <Authentication>
            <Module>amps-default-authentication-module</Module>
        </Authentication>
    </Admin>

    <Transports>
        <Transport>
            <Name>json-tcp</Name>
            <Type>tcp</Type>
            <InetAddr>9007</InetAddr>
            <MessageType>json</MessageType>
            <Protocol>amps</Protocol>
        </Transport>
        <Transport>
            <Name>any-tcp</Name>
            <Type>tcp</Type>
            <InetAddr>9090</InetAddr>
            <Protocol>amps</Protocol>
        </Transport>
    </Transports>
</AMPSConfig>
```

#### Using HTTPS for OAuth Requests

When `TokenEndpoint` uses `https`, the module attempts `https`. By default it verifies the remote identity, requiring a `CAKey`.

| Option | Description |
| --- | --- |
| `Certificate` | Certificate for the AMPS connection to the web service. |
| `Key` | Key file for the AMPS connection. |
| `CAKey` | Certificate authority key for verifying the web service identity. Required when `AllowUnverifiedPeer` is `false` (default). |
| `AllowUnverifiedPeer` | Whether AMPS requires the web service to identify itself. Default: `false` |
| `AllowSelfSigned` | Whether AMPS accepts self-signed certificates. Default: `false` |

#### Authentication Request Flow

1. Logon request received from the client, including an OAuth token.
2. AMPS provides the token to the authorization server at `TokenEndpoint`.
3. The authorization server returns a reply. If it contains an access token, authentication succeeds. Otherwise, authentication fails.

This module only handles authentication and does not set any entitlement policy.

### Simple Access Entitlements Module

The AMPS distribution includes a module that grants and denies permissions based on resource name, ignoring the user name. It is not loaded by default.

Use this module when there are specific topics to allow or deny on a transport with no other restrictions, or when no other entitlement system is in use. It is commonly used to allow read-only Admin console access while denying state-modifying paths.

To load the module:

```text
<Modules>
    ...

    <Module>
        <Name>simple-access</Name>
        <Library>libamps_simple_access_entitlement.so</Library>
    </Module>

    ...
</Modules>
```

Options are set in an `Entitlement` block. `AllowedTopics` and/or `DeniedTopics` must be specified.

| Option | Description |
| --- | --- |
| `AllowedTopics` | Regex matching topics to allow. Defaults to `.*`. |
| `DeniedTopics` | Regex matching topics to deny. No default. |
| `GrantedPermissions` | When set, grants only `read` or `write` permission. No default; if absent, both are granted. |

```xml
<AMPSConfig>

    <Modules>
        ...

        <Module>
            <Name>simple-access</Name>
            <Library>libamps_simple_access_entitlement.so</Library>
        </Module>

        ...
    </Modules>

    <Admin>
        <InetAddr>localhost:8085</InetAddr>
        <Entitlement>
            <Module>simple-access</Module>
            <Options>
                <DeniedTopics>^/amps/administrator</DeniedTopics>
            </Options>
        </Entitlement>
    </Admin>

</AMPSConfig>
```

### Protecting Data in Transit Using TLS/SSL

AMPS provides SSL/TLS connections for communication with clients. AMPS uses TLS to encrypt network traffic. Encryption is independent of the AMPS authentication and entitlement system.

AMPS ships with current libraries, but an installation may load site-vetted versions of OpenSSL and Crypto libraries. See the Externals section for details.

A transport configured to use TLS defaults to accepting TLS 1.1, 1.2, and 1.3. Older protocols can be enabled with `SecureSocketProtocols`.

### Verifying Connection Identity using Mutual TLS (mTLS)

AMPS supports certificate verification for incoming connections. Add `VerifyClient` to the `Transport` and provide trusted certificates via `CAFile` or `CAPath`. If the certificate is not signed by a trusted CA, AMPS refuses the connection.

AMPS also supports certificate verification for outgoing replication connections. Provide `VerifyClient` in the `Replication` `Destination` `Transport` and trusted certificates via `CAFile` or `CAPath`. If the destination certificate is not trusted, AMPS closes the connection before logon.

### Monitoring AMPS

The AMPS monitoring interface has two components:

1. A basic monitoring interface providing statistics in machine-readable formats, with administrative functions such as enabling/disabling transports, disconnecting clients, and managing replication links.
2. The AMPS *Galvanometer*, a browser-based monitoring tool providing graphical statistics visualization, replication flow information, and query/subscription capabilities. Galvanometer uses the basic monitoring interface.

#### Statistics Collection

The monitoring interface provides health and monitoring information for the AMPS engine and host. The root URI contains:

- `host` - current operating system state
- `instance` - information about the AMPS instance
- `administration` - functions that modify instance state

Information is taken from the statistics database. Fields are described in the AMPS Monitoring Guide.

#### Galvanometer

Galvanometer provides extensive visualizations of instance state and the ability to query the instance.

Galvanometer is a JavaScript application using the administrative monitoring interface. It includes an optional lightweight read-only AMPS client.

When the `Admin` interface uses TLS/SSL, Galvanometer uses the same certificate and key. For the replication graph to display correctly, all replicated instances must either all use TLS/SSL for `Admin` or none use it.

#### Authorization and Entitlement in Galvanometer

The `WWWAuthenticate` option specifies how credentials are provided:

- `Negotiate` (Kerberos)
- `NTLM`
- `Basic realm="<SECURITY_DOMAIN>"`

```text
<Admin>
    ...

    <WWWAuthenticate>Basic realm="AMPS Admin"</WWWAuthenticate>

    ...
</Admin>
```

**Statistics Entitlement**

Galvanometer queries the HTTP admin interface. If the user lacks permission for a path, Galvanometer cannot show those statistics. Statistics retrieval is treated as a `read` request to an `admin` resource.

**Entitlement to Administrator Actions**

Administrative actions are treated as `read` requests to `admin` resources. Galvanometer checks entitlement before displaying controls. The check does not indicate the action has been performed.

**Anonymous Paths**

The `AnonymousPaths` option allows bypassing authentication and entitlement for matching `Admin` paths. It is most commonly used to allow Galvanometer to display the replication graph when using `Negotiate` or `NTLM`, since browsers disallow cross-domain authorization tokens.

```text
<Admin>
   <AnonymousPaths>^/amps/instance/replication</AnonymousPaths>
</Admin>
```

Disabled by default.

#### Enabling Queries and Subscriptions in Galvanometer

Galvanometer submits queries and subscriptions using the `websocket` protocol. Provide the name of a `Transport` of type `websocket`:

```text
<Admin>
   <SQLTransport>websocket-any</SQLTransport>
</Admin>
```

The configuration requires a `Transport` with the matching `Name` of type `websocket`:

```xml
<Transports>
   <Transport>
       <Name>websocket-any</Name>
       <Protocol>websocket</Protocol>
       <Type>tcp</Type>
       <InetAddr>9008</InetAddr>
   </Transport>
</Transports>
```

Galvanometer connects as a client; security configured for the instance or `Transport` applies. If TLS/SSL is used, certificates must be signed by a CA known to the browser.

When behind a proxy, use `SQLTransportInetAddr` to provide the URI Galvanometer uses:

```text
<Admin>
   <SQLTransportInetAddr>proxy_host_address/amps_four/wss</SQLTransportInetAddr>
</Admin>
```

#### Queries and Subscriptions with Basic Auth in Galvanometer

When Basic Auth is used, the `TrustedAdmin` option allows Galvanometer to reuse a valid session cookie for websocket connections:

```text
<Protocols>
    <Protocol>
        <Name>websocket-portal</Name>
        <Module>websocket</Module>
        <TrustedAdmin>enabled</TrustedAdmin>
    </Protocol>
</Protocols>
```

Disabled by default. Only supported by websocket-based protocols.

#### Disabling Galvanometer

To disable Galvanometer:

```text
<Admin>
   <Galvanometer>disabled</Galvanometer>
</Admin>
```

This has no effect on the basic monitoring interface.

#### Configuring Monitoring

The `Admin` tag controls the administration server and statistics collection.

`InetAddr`

Defines a port for the embedded HTTP admin server. Can specify an IP address to listen only on that address. If no IP is specified, the server listens on all available addresses. Starting with 5.3.3, both IPv4 and IPv6 are supported. `0.0.0.0:8445` listens on IPv4 only; `[::]:8445` listens on IPv6 only. No default.

`FileName`

Location for storing statistics. Default: `:memory:` (in memory).

`Interval`

Refresh interval for updating statistics. Default: `10s`. Minimum: `1s`.

`WWWAuthenticate`

HTTP authentication type for the Admin Server. Accepts `Basic realm="..."`, `NTLM`, or `Negotiate`. Default: `Negotiate`.

`Authentication`

Authentication element for the Administrative interface.

`Entitlement`

Entitlement element for the Administrative interface.

`AnonymousPaths`

Regular expression defining paths accessible anonymously. No default.

`Header`

Adds the specified HTTP header to responses. Include multiple times for multiple headers.

```text
<Header>X-Special-Information: "AMPS Admin"</Header>
<Header>X-Other-Information: "abc;123"</Header>
```

`ExternalInetAddr`

Address explicitly reported for connections to the admin interface, used by Galvanometer for replication views. Useful when the admin interface must be reached through a proxy. No default.

`AccessControlAllowOrigin`

Regular expression matching domains from which requests might come. When set and the `Origin` header matches, the response returns the request `Origin`. Otherwise, responds with the detected host address. If set, also include `<Header>Vary: Origin</Header>`. Default: `*`.

`SessionOptions`

Options for the admin cookie. Default: `max-age=86400; path=/; HttpOnly`.

`SQLTransport`

Name of the `Transport` (type `websocket`) that Galvanometer uses for queries and subscriptions. No default.

`SQLTransportInetAddr`

URI that Galvanometer uses for queries and subscriptions. No default.

AMPS supports HTTPS for the Admin interface. Provide `Certificate` and `PrivateKey` in the `Admin` block.

`Certificate`

Certificate file for the Admin Server. No default.

`PrivateKey`

Private key for the Admin Server. No default.

`Ciphers`

Cipher list passed to OpenSSL. No default.

```text
<Admin>
    <FileName>stats.db</FileName>
    <InetAddr>localhost:8085</InetAddr>
    <Interval>10s</Interval>
</Admin>
```

| URI | Description |
| --- | --- |
| `http://localhost:8085/` | Root URI for Galvanometer |
| `http://localhost:8085/amps` | Root URI for simple monitoring interface |

```text
<Admin>
    <InetAddr>9090</InetAddr>
    <FileName>stats.db</FileName>
    <Interval>20s</Interval>
    <ExternalInetAddr>proxy.example.com:8185</ExternalInetAddr>
</Admin>
```

#### Output Formatting

The monitoring interface supports XML, CSV, JSON, and RNC output formats.

**JSON Document Output**

Append `.json` to any resource. Example: `http://localhost:8085/amps/host/cpus.json`

```json
{
    "amps": {
        "host": {
            "cpus": [
                {
                    "id":"all",
                    "idle_percent":"62.452316076294",
                    "iowait_percent":"0.490463215259",
                    "system_percent":"10.681198910082",
                    "user_percent":"26.376021798365"
                },
                {
                    "id":"cpu0",
                    "idle_percent":"75.417130144605",
                    "iowait_percent":"0.333704115684",
                    "system_percent":"7.563959955506",
                    "user_percent":"16.685205784205"
                },
                {
                    "id":"cpu1",
                    "idle_percent":"50.000000000000",
                    "iowait_percent":"0.642398286938",
                    "system_percent":"13.597430406852",
                    "user_percent":"35.760171306210"
                }
            ]
        }
    }
}
```

**XML Document Output**

Append `.xml` to any resource. Example: `http://localhost:8085/amps/instance/processors/all.xml`

```text
<amps>
    <instance>
        <processors>
            <processor id='all'>
                <denied_reads>0</denied_reads>
                <denied_writes>0</denied_writes>
                <description>AMPS Aggregate Processor Stats</description>
                <last_active>1855</last_active>
                <matches_found>0</matches_found>
                <matches_found_per_sec>0</matches_found_per_sec>
                <messages_received>0</messages_received>
                <messages_received_per_sec>0</messages_received_per_sec>
                <throttle_count>0</throttle_count>
            </processor>
        </processors>
    </instance>
</amps>
```

**CSV Document Output**

Append `.csv` to any **leaf node**. Can be coupled with time range selection.

Example: `http://localhost:8085/amps/instance/processors/all/matches_found_per_sec.csv?t0=20230830T0`

```text
20230830T000000.000000Z,94244
20230830T000010.000000Z,304661
20230830T000020.000000Z,301078
20230830T000030.000000Z,304661
20230830T000040.000000Z,0
20230830T000050.000000Z,0
20230830T000100.000000Z,0
20230830T000110.000000Z,0
20230830T000120.000000Z,302390
20230830T000130.000000Z,307637
20230830T000140.000000Z,0
20230830T000150.000000Z,0
20230830T000200.000000Z,0
```

**Leaf Nodes**

A leaf node represents a single recorded statistic. Leaf nodes **do support** CSV output.

```bash
http://localhost:8085/amps/instance/processors/all/messages_received_per_sec
```

**Non-Leaf Nodes**

A non-leaf node represents an aggregate of related statistics. Non-leaf nodes **do not support** CSV output.

```bash
http://localhost:8085/amps/instance/processors/all
```

**RNC Document Output**

AMPS supports Relax NG Compact schema generation via `http://localhost:port/amps.rnc`.

Convert to XML schema with Trang:

```bash
wget http://localhost:9090/amps.rnc
trang -I rnc -O xsd amps.rnc amps.xsd
```

#### statistics-collection

Most monitoring data is collected at the configured interval. Statistics reflect the point in time when collected, not continuous samples.

A client that connects, runs a query, consumes results, and disconnects within less than the statistics interval may not appear in the statistics database at all.

#### Time Range Selection

Append `t0` and/or `t1` query parameters to a leaf node URL for historical reports.

Example: `http://localhost:8085/amps/instance/processors/all/messages_received_per_sec?t0=20111130T0&t1=20111130T232500`

```text
20111130T000000.000000Z,0
20111130T000010.000000Z,0
20111130T000020.000000Z,0
20111130T000030.000000Z,94244
20111130T000040.000000Z,304661
20111130T000050.000000Z,301078
20111130T000100.000000Z,308922
20111130T000110.000000Z,306177
20111130T000120.000000Z,302140
20111130T000130.000000Z,302390
20111130T000140.000000Z,307637
20111130T000150.000000Z,310109
20111130T000200.000000Z,309888
20111130T000210.000000Z,299993
20111130T000220.000000Z,310002
20111130T000230.000000Z,300612
20111130T000240.000000Z,299387
```

All times are ISO-8601 formatted (`YYYYMMDDThhmmss`) and in UTC.

**Time Based Query Behavior**

| Query Parameter Values | Behavior |
| --- | --- |
| Only `t0` set | Values from `t0` to latest recorded interval, inclusive. |
| Only `t1` set | Values from first recorded interval to `t1`, inclusive. |
| Both `t0` and `t1` set to different values | Values from `t0` to `t1`, inclusive. |
| Both `t0` and `t1` set to same value | Single value at that specific timestamp. Must be a timestamp present in the statistics. |

**Leaf Nodes (Reference)**

Leaf nodes fully support time-range selections with `t0` and `t1`.

```bash
http://localhost:8085/amps/instance/processors/all/messages_received_per_sec
```

**Non-Leaf Nodes (Reference)**

Non-leaf nodes do not support time-range selections. They support queries for a specific historical admin interval timestamp by setting `t0` and `t1` to the same value.

```bash
http://localhost:8085/amps/instance/transaction_log
```

### Logging

AMPS supports logging to files, syslog, and the console. Every error message is uniquely identified and can be filtered.

#### Message Categories

Error identifiers are `CC-NNNN`, where `CC` is the category.

| AMPS Code | Component |
| --- | --- |
| 00 | AMPS Startup |
| 01 | General |
| 02 | Message Processing |
| 03 | Expiration |
| 04 | Publish Engine |
| 05 | Statistics |
| 06 | Metadata |
| 07 | Client |
| 08 | Regex |
| 09 | ID Generator |
| 0A | Diff Merge |
| 0B | Out of Focus Processing |
| 0C | View |
| 0D | Message Data Cache |
| 0E | Conflated Topic |
| 0F | Message Processor Manager |
| 11 | Connectivity |
| 12 | Trace In |
| 13 | Datasource |
| 14 | Subscription Manager |
| 15 | SOW |
| 16 | Query |
| 17 | Trace Out |
| 18 | Parser |
| 19 | Administration Console |
| 1A | Evaluation Engine |
| 1B | SQLite |
| 1C | Meta Data Manager |
| 1D | Transaction Log Monitor |
| 1E | Replication Bootstrap Initialization |
| 1F | Client Session |
| 20 | Global Heartbeat |
| 21 | Transaction Replay |
| 22 | TX Completion |
| 23 | Bookmark Subscription |
| 24 | Thread Monitor |
| 25 | Authorization |
| 26 | SOW Cache |
| 28 | Memory Cache |
| 29 | Plug-in Modules |
| 2A | Message Pipeline |
| 2B | Module Manager |
| 2C | File Management |
| 2D | NUMA Module |
| 2F | SOW Update Broadcaster |
| 30 | AMPS Internal Utilities |
| 31 | AMPS Queues |
| 70 | AMPS Networking |
| FF | Shutdown |

#### Looking up Errors with ampserr

The `ampserr` utility in `$AMPSDIR/bin` provides detailed information about specific AMPS errors observed in log files.

#### Using amps-grep to Find Information in Logs

`amps-grep` extracts full multi-line AMPS event records matching a search term.

**Finding Information for a Specific Client**

```text
$ amps-grep *client_name* *log_files* > out.txt
```

Example:

```bash
$ amps-grep 'queue-processor-compute-host-39' *.log > out.txt
```

**Finding Information for a Specific Thread**

Given a minidump message containing the AMPS thread ID in brackets:

```text
2020-04-25T07:27:59.1355850-07:00 [6] critical: 01-0022 AMPS has
detected that it may not be running correctly and wrote a minidump to:
/tmp/1516e4d1-8bca-b14b-17853753-45dba87b.dmp
```

Extract every message from thread 6:

```bash
$ amps-grep ' [6] ' *.log > out.txt
```

**Tips for Using amps-grep**

1. By default `amps-grep` uses *exact* matching. Use `-E` for regular expressions.
2. When piping between `amps-grep` commands, use `-h` on the first to suppress filenames.
3. Use `-e` for multiple search terms:

```text
$ amps-grep -e 'error' -e 'warning' *.log
```

#### Message Levels

Configuring a target at a specific level captures that level and all higher severity levels.

| Level | Description |
| --- | --- |
| `developer` | Internal state information. |
| `trace` | All inbound/outbound data. |
| `stats` | Statistics messages. |
| `info` | General information messages. |
| `warning` | Problems AMPS tries to correct. |
| `error` | Events where processing was aborted. |
| `critical` | Events impacting major components. |
| `emergency` | Fatal events. AMPS will typically exit. |
| `none` | No logging. |

`IncludeErrors` includes specific messages regardless of level. `ExcludeErrors` excludes specific messages regardless of level.

Each target allows a `Level` attribute. The default `Level` is `none`.

The `Levels` attribute selects specific levels in addition to `Level`:

```xml
<AMPSConfig>
    ...
    <Logging>
        <Target>
            <Protocol>gzip</Protocol>
            <FileName>traces.log.gz</FileName>
            <Levels>trace</Levels>
        </Target>
    </Logging>
    ...
</AMPSConfig>
```

Logging only `trace` and `info`:

```xml
<AMPSConfig>
    ...
    <Logging>
    <Target>
            <Protocol>file</Protocol>
            <FileName>traces-info.log</FileName>
            <Levels>trace,info</Levels>
        </Target>
    </Logging>
    ...
</AMPSConfig>
```

Logging `trace` and `info` in addition to `error` and above:

```xml
<Target>
    <Protocol>file</Protocol>
    <FileName>traces-error-info.log</FileName>
    <Level>error</Level>
    <Levels>trace,info</Levels>
</Target>
```

The obsolete level `debug` is treated as a synonym for `info`.

#### Log Message Format

An AMPS log message contains:

- Timestamp (e.g., `2021-11-23T14:49:38.3442510-08:00`)
- AMPS Thread Identifier (e.g., `1`)
- Log Level (e.g., `info`)
- Error Identifier (e.g., `15-0008`)
- Log Message

Example:

```text
2021-11-23T14:49:38.3442510-08:00 [1] info: 00-0015 AMPS initialization completed (0 seconds).
```

Each message has a unique `CC-NNNN` identifier. Targets allow direct inclusion/exclusion by identifier:

```text
<Logging>
    <Target>
        <Protocol>stdout</Protocol>
        <IncludeErrors>00-0002</IncludeErrors>
        <ExcludeErrors>00-0001,00-0004,12-1.*</ExcludeErrors>
    </Target>
</Logging>
```

#### Configuring Logging

Add a `Logging` section with one or more `Target` definitions.

`Protocol` (required)

Valid values: `stdout`, `stderr`, `file`, `gzip`, `syslog`

`Level`

Lower bound inclusive log level. Valid values: `developer`, `trace`, `stats`, `info`, `warning`, `error`, `critical`, `emergency`, `none`. No default.

`Levels`

Comma-separated list of specific levels to include. Can be combined with `Level`.

`IncludeErrors`

Comma-delimited list of error numbers or regex to include regardless of level. No default.

`ExcludeErrors`

Comma-delimited list of error numbers or regex to exclude regardless of level. If the same error appears in both, `ExcludeErrors` takes precedence. No default.

AMPS logging is opt-in; no messages are logged by default.

#### Logging to Files

`FileName` (required for `file` and `gzip`)

The file to log to. `.log` is appended for `file`; `.gz` for `gzip`. Default: `${PWD}/%Y-%m-%dT%H%M%S.log`

`RotationThreshold`

Log size at which rotation occurs.

Example with rotation:

```xml
<AMPSConfig>
    ...
    <Logging>
        <Target>
            <Protocol>file</Protocol>
            <Level>info</Level>
            <FileName>./logs/%Y%m%d%H%M%S-%n.log</FileName>
            <RotationThreshold>2G</RotationThreshold>
        </Target>
    </Logging>
    ...
</AMPSConfig>
```

Example appending to a single file:

```xml
<AMPSConfig>
    ...
    <Logging>
        <Target>
        <Protocol>file</Protocol>
        <Level>info</Level>
        <FileName>amps.log</FileName>
        </Target>
    </Logging>
    ...
</AMPSConfig>
```

Compressed file example:

```xml
<AMPSConfig>
    ...
    <Logging>
        <Target>
            <Protocol>gzip</Protocol>
            <Level>info</Level>
            <FileName>./logs/%Y%m%d%H%M%S-%n.log.gz</FileName>
            <RotationThreshold>2G</RotationThreshold>
        </Target>
    </Logging>
    ...
</AMPSConfig>
```

#### Logging to Syslog

`Ident`

Syslog identifier. Default: AMPS Instance Name.

`Options`

Comma-separated list of syslog options. AMPS uses standard `syslog` options.

`Facility`

Syslog facility.

Example:

```xml
<AMPSConfig>
    ...
    <Logging>
        <Target>
            <Protocol>syslog</Protocol>
            <Level>critical</Level>
            <IncludeErrors>30-0000</IncludeErrors>
            <Ident>\amps dma</Ident>
            <Options>LOG_CONS,LOG_NDELAY,LOG_PID</Options>
            <Facility>LOG_USER</Facility>
        </Target>
    </Logging>
    ...
</AMPSConfig>
```

Multiple targets example:

```xml
<Logging>
    <Target>
        <Protocol>file</Protocol>
        <FileName>/var/tmp/amps/logs/%Y%m%d%H%M%S-%n.log</FileName>
        <RotationThreshold>2G</RotationThreshold>
        <Level>trace</Level>
        <Levels>critical</Levels>
    </Target>

    <Target>
        <Protocol>syslog</Protocol>
        <Level>critical</Level>
        <Ident>amps_dma</Ident>
        <Options>LOG_CONS,LOG_NDELAY,LOG_PID</Options>
        <Facility>LOG_USER</Facility>
    </Target>

    <Target>
        <Protocol>file</Protocol>
        <FileName>/var/tmp/amps/logs/initMessage</FileName>
        <IncludeErrors>00-0015</IncludeErrors>
    </Target>
</Logging>
```

#### Logging to the Console

Use `stdout` or `stderr` as the `Protocol`.

Example:

```xml
<AMPSConfig>
    ...
    <Logging>
        <Target>
            <Protocol>stdout</Protocol>
            <Levels>info,warning</Levels>
        </Target>
        <Target>
            <Protocol>stderr</Protocol>
            <Level>error</Level>
        </Target>
    </Logging>
    ...
</AMPSConfig>
```

#### Example: Development Instance Logging

```xml
<AMPSConfig>
  ...
  <Logging>
    <Target>
       <Protocol>file</Protocol>
       <Level>trace</Level>
       <FileName>./logs/trace.log</FileName>
       <RotationThreshold>250MB</RotationThreshold>
    </Target>
    <Target>
       <Protocol>stdout</Protocol>
       <Level>warning</Level>
       <IncludeErrors>00-0015</IncludeErrors>
   </Target>
   <Target>
       <Protocol>file</Protocol>
       <FileName>./logs/instance-info.log</FileName>
       <IncludeErrors>00-0001,00-0002,00-0004,00-0015,
                      00-0030,00-0033,00-0032,00-0054,
                      01-0019,2D-0005,2D-0006,2D-0008,2D-0011</IncludeErrors>
    </Target>
  </Logging>
  ...
</AMPSConfig>
```

#### Selecting a Filename

The `FileName` mask supports:

| Mask | Definition |
| --- | --- |
| `%Y` | Year |
| `%m` | Month |
| `%d` | Day |
| `%H` | Hour |
| `%M` | Minute |
| `%S` | Second |
| `%n` | Iterator starting at `00000` and incrementing on each rotation. |

If `RotationThreshold` is specified and the next filename matches an existing file, that file is truncated. To preserve history, use a timestamp or `%n` in the mask.

#### Log File Rotation

`RotationThreshold` values default to bytes. Valid suffixes:

| Unit Suffix | Base Unit | Examples |
| --- | --- | --- |
| no suffix | bytes | `1000000` = 1 million bytes |
| k or K | thousands | `50k` = 50 thousand bytes |
| m or M | millions | `10M` = 10 million bytes |
| g or G | billions | `2G` = 2 billion bytes |
| t or T | trillions | `0.5T` = 500 billion bytes |

#### Logging to a Compressed File

Compressed targets use `Protocol` `gzip`, write with gzip compression, and meter `RotationThreshold` off the *uncompressed* size.

Example:

```xml
<AMPSConfig>
    ...
    <Logging>
        <Target>
            <Protocol>gzip</Protocol>
            <Level>info</Level>
            <FileName>./logs/%Y%m%d%H%M%S-%n.log.gz</FileName>
            <RotationThreshold>2G</RotationThreshold>
        </Target>
    </Logging>
    ...
</AMPSConfig>
```

#### Syslog Severity Mapping

| AMPS Severity | Syslog Severity |
| --- | --- |
| none | LOG_DEBUG |
| developer | LOG_DEBUG |
| trace | LOG_DEBUG |
| stats | LOG_INFO |
| info | LOG_INFO |
| warning | LOG_WARNING |
| error | LOG_ERR |
| critical | LOG_CRIT |
| emergency | LOG_EMERG |

Recognized syslog flags:

| Level | Description |
| --- | --- |
| `LOG_CONS` | Write directly to system console if sending to logger fails. |
| `LOG_NDELAY` | Open the connection immediately. |
| `LOG_NOWAIT` | No effect on Linux. |
| `LOG_ODELAY` | Delay opening until `syslog()` is called (default). |
| `LOG_PERROR` | Print to standard error as well. |
| `LOG_PID` | Include PID with each message. |

Valid `Facility` values: `LOG_USER` (default), `LOG_LOCAL0` through `LOG_LOCAL7`.

### Configuring AMPS for Automation with Actions

AMPS provides the ability to run scheduled tasks or respond to events using the Actions interface. Add an `Actions` section; each `Action` contains `On` statements specifying when it occurs and `Do` statements specifying what happens.

AMPS allows variables in action parameters using `{{VARIABLE_NAME}}` syntax.

Default action variables:

| Variable | Description |
| --- | --- |
| `AMPS_INSTANCE_NAME` | The name of the AMPS instance. |
| `AMPS_BYTE_XX` | Insert byte *XX*, where *XX* is a 2-digit uppercase hex number (00-FF). |
| `AMPS_DATETIME` | Current date and time in ISO-8601 format. |
| `AMPS_UNIX_TIMESTAMP` | Current date and time as a UNIX timestamp. |

Example:

```json
<Actions>
    <Action>
        <On>
            <Module>amps-action-on-startup</Module>
        </On>
        <Do>
            <Module>amps-action-do-echo-message</Module>
            <Options>
                <Message>instance={{AMPS_INSTANCE_NAME}}</Message>
            </Options>
        </Do>
    </Action>
</Actions>
```

If a `Do` statement returns failure, subsequent `Do` statements in that action are not run.

#### On: Choosing When an Action Runs

| Condition | Modules |
| --- | --- |
| On a Schedule | `amps-action-on-schedule` |
| On AMPS Startup or Shutdown | `amps-action-on-startup`, `amps-action-on-shutdown` |
| On a Linux Signal | `amps-action-on-signal` |
| On a REST Request | `amps-action-on-admin` |
| On Minidump Creation | `amps-action-on-minidump` |
| On Client Connect or Disconnect | `amps-action-on-connect-client`, `amps-action-on-disconnect-client` |
| On Client Logon | `amps-action-on-logon-client` |
| On Client Offline Message Buffering | `amps-action-on-offline-start`, `amps-action-on-offline-stop` |
| On Subscribe or Unsubscribe | `amps-action-on-subscribe`, `amps-action-on-unsubscribe` |
| On Incoming Replication Connections | `amps-action-on-connect-incoming-replication`, `amps-action-on-disconnect-incoming-replication` |
| On Outgoing Replication Connections | `amps-action-on-connect-replication`, `amps-action-on-disconnect-replication`, `amps-action-on-replication-resync-complete`, `amps-action-on-upgrade-replication`, `amps-action-on-downgrade-replication` |
| On Message Published to AMPS | `amps-action-on-publish-message` |
| On Message Delivered to Subscriber | `amps-action-on-deliver-message` |
| On Message Affinity | `amps-action-on-message-affinity` |
| On SOW Message Expiration | `amps-action-on-sow-expire-message` |
| On SOW Message Delete | `amps-action-on-sow-delete-message` |
| On Out-of-Focus Message | `amps-action-on-oof-message` |
| On Message Condition Timeout | `amps-action-on-message-condition-timeout` |
| On Message State Change | `amps-action-on-alert` |
| On Custom Event | `amps-action-on-execute-event` |

#### Do: Choosing What an Action Does

| Action | Module |
| --- | --- |
| Rotate Error/Event Log | `amps-action-do-rotate-logs` |
| Compress Files | `amps-action-do-compress-files` |
| Truncate Statistics | `amps-action-do-truncate-statistics` |
| Manage Transaction Log Journal Files | `amps-action-do-archive-journal`, `amps-action-do-compress-journal`, `amps-action-do-remove-journal` |
| Remove Error/Event Log Files | `amps-action-do-remove-files` |
| Delete SOW Messages | `amps-action-do-delete-sow` |
| Compact SOW Topics | `amps-action-do-compact-sow` |
| Query a SOW Topic | `amps-action-do-query-sow` |
| Manage Security | `amps-action-do-disable-authentication`, `amps-action-do-enable-authentication`, `amps-action-do-reset-authentication`, `amps-action-do-reset-entitlement`, `amps-action-do-disable-entitlement`, `amps-action-do-enable-entitlement` |
| Enable or Disable Transports | `amps-action-do-enable-transport`, `amps-action-do-disable-transport` |
| Publish Message | `amps-action-do-publish-message` |
| Manage Replication Acknowledgment | `amps-action-do-downgrade-replication`, `amps-action-do-upgrade-replication` |
| Extract Values from a Message | `amps-action-do-extract-values` |
| Translate Data Within an Action | `amps-action-do-translate-data` |
| Increment Counter | `amps-action-do-increment-counter` |
| Raise a Custom Event | `amps-action-do-execute-event` |
| Execute System Command | `amps-action-do-execute-system` |
| Manage Queue Transfers | `amps-action-do-enable-proxied-transfer`, `amps-action-do-disable-proxied-transfer` |
| Create a Minidump | `amps-action-do-minidump` |
| Shut Down AMPS | `amps-action-do-shutdown` |
| Debug Action Configurations | `amps-action-do-nothing`, `amps-action-do-echo-message` |

#### Conditionally Stop an Action

| Condition | Module |
| --- | --- |
| Stop Based on File System Capacity | `amps-action-if-file-system-usage` |
| Stop Based on Evaluating an Expression | `amps-action-if-condition` |

#### Compress Files

| Module Name | Does |
| --- | --- |
| `amps-action-do-compress-files` | Compresses files matching a pattern that are older than a specified age. Does not recurse, skips open files, and does not compress `.journal` files. |

| Parameter | Description |
| --- | --- |
| `Age` (required) | Age of files to process. |
| `Pattern` (required) | Unix shell globbing pattern for files. Not a regex. |
| `Keep` | Number of matching files to leave uncompressed. |
| `Count` | Maximum number of files to compress. |

#### Create Minidump

| Module Name | Does |
| --- | --- |
| `amps-action-do-minidump` | Creates a minidump. Does not cause AMPS to exit. |

No parameters required.

#### Remove Files

| Module Name | Does |
| --- | --- |
| `amps-action-do-remove-files` | Removes files matching a pattern that are older than a specified age. Does not recurse, skips open files, and does not remove `.journal` files. |

| Parameter | Description |
| --- | --- |
| `Age` (required) | Age of files to process. |
| `Pattern` (required) | Unix shell globbing pattern. Not a regex. |
| `Keep` | Number of matching files to retain. |
| `Count` | Maximum number of files to remove. |

#### Enable or Disable Transports

| Module Name | Does |
| --- | --- |
| `amps-action-do-enable-transport` | Enables a specific transport. |
| `amps-action-do-disable-transport` | Disables a specific transport. |

| Parameter | Description |
| --- | --- |
| `Transport` | Name of the transport. If omitted, affects all transports. |

#### Execute System Command

| Parameter | Description |
| --- | --- |
| `Command` | Command to execute as a shell command. Must complete quickly or AMPS may consider the thread deadlocked. |

This module executes commands with the credentials of the AMPS process.

#### Extract Values from a Message

| Parameter | Description |
| --- | --- |
| `MessageType` (required) | Message type to parse. |
| `Value` (required) | Assignment `variable=amps-expression`. Multiple allowed. |
| `Data` | Data to parse. Variables are expanded. |

Adds the variables specified by `Value` to the context.

#### Increment Counter

| Parameter | Description |
| --- | --- |
| `Key` (required) | Counter name. |
| `Value` (required) | Variable to store the incremented value. |

Adds the counter variable to the context.

#### Manage Transaction Log Journal Files

| Module Name | Does |
| --- | --- |
| `amps-action-do-archive-journal` | Archives journal files older than a specified age. |
| `amps-action-do-compress-journal` | Compresses journal files older than a specified age. |
| `amps-action-do-remove-journal` | Deletes journal files older than a specified age. |

AMPS only removes journal files no longer needed. It ensures replays are complete, queue messages are delivered, and messages are replicated.

| Parameter | Description |
| --- | --- |
| `Age` (required) | Age of files to process. AMPS does not remove the current journal, files in use for replay/replication, or files with unacknowledged queue messages. AMPS does not allow gaps, so it only removes a file if all previous files have been removed. |

#### Manage Queue Transfers

The `amps-action-do-enable-proxied-transfer` and `amps-action-do-disable-proxied-transfer` actions manage proxied transfer for queues, allowing an instance to take ownership of messages owned by an unreachable instance. Enabling introduces risk of duplicate delivery. These actions are intended for disaster recovery, not normal maintenance.

| Parameter | Description |
| --- | --- |
| `Topic` (required) | Queue topic name. |
| `MessageType` (required) | Message type of the queue topic. |

#### Manage Security

| Module Name | Does |
| --- | --- |
| `amps-action-do-disable-authentication` | Disables authentication. |
| `amps-action-do-disable-entitlement` | Disables entitlement. |
| `amps-action-do-enable-authentication` | Enables authentication. |
| `amps-action-do-enable-entitlement` | Enables entitlement. |
| `amps-action-do-reset-authentication` | Resets authentication. |
| `amps-action-do-reset-entitlement` | Resets entitlement. |

| Parameter | Description |
| --- | --- |
| `Transport` | Transport to reset. If omitted, affects all transports. |
| `AuthenticationId` | Client authentication ID to reset entitlements for. If omitted, affects all clients. |

#### Truncate Statistics

| Module Name | Usage |
| --- | --- |
| `amps-action-do-truncate-statistics` | Removes statistics older than a specified age. Frees space but does not reduce file size. |
| `amps-action-do-vacuum-statistics` | **Deprecated**. No longer vacuums statistics. |

| Parameter | Description |
| --- | --- |
| `Age` (required) | Age of statistics to remove. |

#### Publish Message

Publishes a message into a topic. No user credentials are associated, so entitlements are not applied. The publish is recorded in the transaction log as if from an external publisher.

Caution: when running in response to `amps-action-on-publish-message` or `amps-action-on-deliver-message`, the published message could trigger the event again, causing a publish loop.

| Parameter | Description |
| --- | --- |
| `Topic` (required) | Topic to publish to. |
| `MessageType` (required) | Message type. |
| `Data` (required) | Message data. |
| `Delta` | Use delta publish when `true`. |
| `UpdateOnly` | When `true` with `Delta`, only update existing records. Default `false`. |

#### Query SOW Topic

Queries a SOW topic and stores the first result into a variable.

| Parameter | Description |
| --- | --- |
| `Topic` (required) | SOW topic, view, queue, or conflated topic. Supports regex. |
| `MessageType` (required) | Message type. |
| `Filter` (required) | Filter to apply. |
| `CaptureData` (required) | Variable to store the first returned message. |
| `DefaultData` | Value stored if no records are found. |
| `OrderBy` | Ordering expression. |
| `Options` | Valid `sow` options except `top_n`. |

Example:

```text
<Actions>
    <Action>
        <On>
            <Module>amps-action-on-schedule</Module>
            <Options>
                <Every>Saturday at 23:59</Every>
                <Name>Diagnostic_Schedule</Name>
            </Options>
        </On>
        <Do>
            <Module>amps-action-do-query-sow</Module>
            <Options>
                <MessageType>xml</MessageType>
                <Topic>SOW_TOPIC</Topic>
                <Filter>/Trans/Order/@Oname = 'PURCHASE'</Filter>
                <CaptureData>AMPS_DATA</CaptureData>
            </Options>
        </Do>
        <Do>
            <Module>amps-action-do-extract-values</Module>
            <Options>
                <MessageType>xml</MessageType>
                <Data>{{AMPS_DATA}}</Data>
                <Value>SAVED_VARIABLE=/Value</Value>
            </Options>
        </Do>
        <Do>
            <Module>amps-action-do-echo-message</Module>
            <Options>
                <Message>{{SAVED_VARIABLE}} was in the message</Message>
            </Options>
        </Do>
    </Action>
</Actions>
```

#### Raise a Custom Event

| Parameter | Description |
| --- | --- |
| `Event` (required) | Name of the event to raise. |
| `EventVariable` (required) | Variable containing the event name. |

One of `Event` or `EventVariable` is required.

#### Manage Replication Acknowledgment

| Module Name | Does |
| --- | --- |
| `amps-action-do-downgrade-replication` | Downgrades sync destinations to async if the oldest unacknowledged message exceeds the age. |
| `amps-action-do-upgrade-replication` | Upgrades previously-downgraded destinations back to sync if the oldest unacknowledged message is more recent. No effect on destinations configured as `async`. |

To avoid repeated upgrade/downgrade, set the upgrade `Age` to roughly 1/2 of the downgrade `Age`.

Downgrade options:

| Parameter | Description |
| --- | --- |
| `Age` (required) | Maximum message age before downgrading. |
| `GracePeriod` | Approximate time to wait after startup before checking. |

Upgrade options:

| Parameter | Description |
| --- | --- |
| `Age` (required) | Maximum message age for upgrading a previously-downgraded destination. |
| `GracePeriod` | Approximate time to wait after startup before checking. |

#### Rotate Error/Event Log

| Module Name | Does |
| --- | --- |
| `amps-action-do-rotate-logs` | Rotates logs older than a specified age. |

No options required.

#### Shut Down AMPS

| Module Name | Does |
| --- | --- |
| `amps-action-do-shutdown` | Shuts down AMPS. |

No parameters required.

#### Compact SOW Topic

Rearranges SOW messages to reduce unused space. Can compact a specific topic or all topics.

Updates are paused during compaction, reducing throughput.

| Parameter | Description |
| --- | --- |
| `Topic` | SOW topic to compact. Required if `MessageType` is provided. |
| `MessageType` | Message type. Required if `Topic` is provided. |

#### Delete SOW Messages

| Parameter | Description |
| --- | --- |
| `Topic` (required) | SOW topic to delete from. Supports regex. |
| `MessageType` (required) | Message type. |
| `Filter` (required) | Filter for messages to delete. |

#### Translate Data Within an Action

Translates variable values using case statements.

| Parameter | Description |
| --- | --- |
| `Data` (required) | Data to translate. |
| `Value` (required) | Variable to store the result. |
| `Case` | Translation `original_value=translated_value`. Multiple allowed. |
| `Default` | Value if no `Case` matches. Defaults to original `Data`. |

#### Based on an Expression

| Module Name | Does |
| --- | --- |
| `amps-action-if-condition` | Stops the action unless the specified AMPS filter evaluates to true. |

| Parameter | Description |
| --- | --- |
| `Condition` (required) | AMPS filter. Variables are substituted before evaluation. |

Example:

```json
<Actions>
    <Action>
        <On>
            <Module>amps-action-on-connect-client</Module>
        </On>
        <If>
            <Module>amps-action-if-condition</Module>
            <Options>
                <Condition>'{{AMPS_CLIENT_NAME}}' LIKE 'important'</Condition>
            </Options>
        </If>
        <Do>
            <Module>amps-action-do-publish-message</Module>
            <Options>
               <Topic>important-logon-notification</Topic>
               <MessageType>json</MessageType>
               <Data>{"name":"{{AMPS_CLIENT_NAME}}"}</Data>
            </Options>
        </Do>
    </Action>
</Actions>
```

#### Based on File System Capacity

| Module Name | Does |
| --- | --- |
| `amps-action-if-file-system-usage` | Stops the action unless the specified path meets the usage threshold. |

| Parameter | Description |
| --- | --- |
| `Path` (required) | Filesystem path to monitor. |
| `GreaterThan` (required) | Threshold percentage. |

Example:

```text
<Actions>
    <Action>
        <On>
            <Module>amps-action-on-schedule</Module>
            <Options>
                <Every>1m</Every>
            </Options>
        </On>
        <If>
            <Module>amps-action-if-file-system-usage</Module>
            <Options>
                <GreaterThan>90%</GreaterThan>
                <Path>/mnt/fastdrive/amps</Path>
            </Options>
        </If>
        <Do>
            <Module>amps-action-do-echo-message</Module>
            <Options>
                <Message>ALERT: You're getting low on space!</Message>
            </Options>
        </Do>
        <If>
            <Module>amps-action-if-file-system-usage</Module>
            <Options>
                <GreaterThan>98%</GreaterThan>
                <Path>/mnt/fastdrive/amps</Path>
            </Options>
        </If>
        <Do>
            <Module>amps-action-do-echo-message</Module>
            <Options>
                <Message>CRITICAL: Shutting down AMPS</Message>
            </Options>
        </Do>
        <Do>
            <Module>amps-action-do-shutdown</Module>
        </Do>
    </Action>
</Actions>
```

#### On Client Connect or Disconnect

| Variable | Description |
| --- | --- |
| `AMPS_CLIENT_NAME` | Client name. (For connect, this is the connection name since logon has not occurred.) |
| `AMPS_CONNECTION_NAME` | Connection name. |
| `AMPS_AUTHENTICATION_ID` | Authentication ID (disconnect only). |

#### On Client Offline Message Buffering

| Variable | Description |
| --- | --- |
| `AMPS_CLIENT_NAME` | Client name. |
| `AMPS_CONNECTION_NAME` | Connection name. |

#### On SOW Message Delete

| Parameter | Description |
| --- | --- |
| `Topic` (required) | Topic to monitor. Must be SOW, view, conflated topic, or queue. No regex support. |
| `MessageType` (required) | Message type. |

| Variable | Description |
| --- | --- |
| `AMPS_TOPIC` | Topic of the deleted message. |
| `AMPS_DATA` | Current data. |
| `AMPS_DATA_LENGTH` | Length in bytes. |
| `AMPS_CORRELATION_ID` | Correlation ID from publish. |

#### On Custom Event

| Parameter | Description |
| --- | --- |
| `Event` | Event name to respond to. |

#### On SOW Message Expiration

| Parameter | Description |
| --- | --- |
| `Topic` (required) | Topic to monitor. Must be SOW, view, conflated topic, or queue. No regex support. |
| `MessageType` (required) | Message type. |
| `Reason` | Comma-delimited expiration reasons to monitor: `time_limit`, `forced_expire`, `max_cancels`, `max_deliveries`. |

| Variable | Description |
| --- | --- |
| `AMPS_TOPIC` | Topic of the expired message. |
| `AMPS_DATA` | Current data. |
| `AMPS_DATA_LENGTH` | Length in bytes. |
| `AMPS_REASON` | Expiration reason(s). |
| `AMPS_CORRELATION_ID` | Correlation ID from publish. |

#### On a REST Request

Runs an action when a resource under `/amps/administrator/actions` is requested.

| Parameter | Description |
| --- | --- |
| `Path` (required) | Path under `/amps/administrator/actions`. Should contain only a resource name, no `/`. |

| Optional Element | Description |
| --- | --- |
| `RequiredParameter` | Query parameter that must be present. |
| `Name` | Display name. |
| `Description` | Display description. |

Query parameters are added to the context before `Do` steps run.

#### On Incoming Replication Connections

| Variable | Description |
| --- | --- |
| `AMPS_REPLICATION_PEER_NAME` | Instance name of the peer. |
| `AMPS_REPLICATION_CLIENT_NAME` | Client name for the connection. |
| `AMPS_REPLICATION_REMOTE_ADDRESS` | Remote address. |
| `AMPS_REPLICATION_GROUP_NAME` | Group name of the peer. |

#### On Client Logon

| Variable | Description |
| --- | --- |
| `AMPS_CLIENT_NAME` | Client name. |
| `AMPS_CONNECTION_NAME` | Connection name. |
| `AMPS_AUTHENTICATION_ID` | Authentication ID. |

#### On Message Affinity

Assigns each SOW record to a single affinitized client. A client subscribes to a control topic to participate.

| Parameter | Description |
| --- | --- |
| `MessageType` (required) | Message type of monitored and control topics. |
| `ControlTopic` (required) | Topic monitored for subscriptions. Subscribers are eligible for affinitization. |
| `DataTopic` (required) | Topic containing messages to affinitize. |
| `DataFilter` | Restricts affinitization to matching records. |

| Variable | Description |
| --- | --- |
| `AMPS_DATA` | Data of the message being affinitized. |
| `AMPS_CLIENT_NAME` | Client the message is assigned to or removed from. |
| `AMPS_AFFINITY_ACTION` | `assign` or `unassign`. |
| `AMPS_AFFINITY_REASON` | Reason for the event. |

Considerations:

- Affinitization is tracked independently on each instance.
- Can only be used for `Topic`, `RegexTopic`, `View`, or `ConflatedTopic`. Not useful for `Queue`, `LocalQueue`, or `GroupLocalQueue`.

Example:

```json
<Action>
   <On>
     <Module>amps-action-on-message-affinity</Module>
     <Options>
       <MessageType>json</MessageType>
       <DataTopic>symbols</DataTopic>
       <ControlTopic>symbol_processor_assignments</ControlTopic>
     </Options>
   </On>
   <Do>
       <Module>amps-action-do-extract-values</Module>
       <Options>
         <Data>{{AMPS_DATA}}</Data>
        <MessageType>json</MessageType>
         <Value>SYMBOL=/symbol</Value>
       </Options>
   </Do>
   <Do>
     <Module>amps-action-do-publish-message</Module>
     <Options>
       <MessageType>json</MessageType>
       <Topic>symbol_processor_assignments</Topic>
       <Data>{"client_name":"{{AMPS_CLIENT_NAME}}",
              "symbol":"{{SYMBOL}}",
              "event":"{{AMPS_AFFINITY_ACTION}}",
              "reason":"{{AMPS_AFFINITY_REASON}}"}</Data>
     </Options>
   </Do>
   <If>
     <Module>amps-action-if-condition</Module>
     <Options>
        <Condition>"{{AMPS_AFFINITY_ACTION}}" == "unassign"</Condition>
     </Options>
   </If>
   <Do>
     <Module>amps-action-do-delete-sow</Module>
     <Options>
        <Topic>symbol_processor_assignments</Topic>
        <MessageType>json</MessageType>
        <Filter>/symbol = "{{SYMBOL}}"</Filter>
     </Options>
   </Do>
 </Action>
```


   </View>
   <Topic>
      <Name>symbol_processor_assignments</Name>
      <MessageType>json</MessageType>
      <Key>/symbol</Key>
      <Durability>transient</Durability>
  </Topic>
</SOW>
```

Balances messages across processors. If a processor unsubscribes, its messages redistribute to others. No rebalancing among running processors. A SOW Key remains affinitized until processor unsubscribes or record is deleted.

### On Message Delivered to Subscriber

`amps-action-on-deliver-message` — runs actions when AMPS delivers a `publish` message to subscribers.

**Required parameters:**

| Parameter | Description |
| --- | --- |
| `Topic` (required) | Topic to monitor for delivery. Supports regex. No default. |
| `MessageType` (required) | Message type to monitor. No default. |

**Context variables:**

| Variable | Description |
| --- | --- |
| `AMPS_TOPIC` | Message topic. |
| `AMPS_DATA` | Message data. |
| `AMPS_DATA_LENGTH` | Data length. |
| `AMPS_BOOKMARK` | Message bookmark (empty string if none). |
| `AMPS_CLIENT_NAME` | Client name delivered to. |
| `AMPS_CORRELATION_ID` | Correlation ID if set on publish. |

### On Message Published to AMPS

`amps-action-on-publish-message` — runs actions when a message is published to AMPS.

- Only active when instance is active (not during recovery, no replay of pre-existing messages).
- Treated as an internal subscription.
- **Do not use with queue topics** — use the underlying topic instead, or it will lease messages without acking.

**Parameters:**

| Parameter | Description |
| --- | --- |
| `Topic` (required) | Topic to monitor. Supports regex. No default. |
| `MessageType` (required) | Message type to monitor. No default. |
| `MessageSource` | `all` (default), `local`, or `replicated`. |
| `Filter` | Only matching messages trigger the action. |
| `Options` | Any subscribe command option (without bookmark). |

**Context variables:**

| Variable | Description |
| --- | --- |
| `AMPS_TOPIC` | Message topic. |
| `AMPS_DATA` | Message data. |
| `AMPS_DATA_LENGTH` | Data length. |
| `AMPS_BOOKMARK` | Message bookmark. |
| `AMPS_TIMESTAMP` | Time AMPS processed the message. |
| `AMPS_CLIENT_NAME` | Publishing client name. |

### On Minidump Creation

`amps-action-on-minidump` — runs actions when AMPS generates a minidump. No parameters required.

**Context variable:**

| Variable | Description |
| --- | --- |
| `AMPS_MINIDUMP_PATH` | Path to the minidump. |

### On Message State Change

`amps-action-on-alert` — monitors a SOW topic; triggers on timeout **or** OOF (message no longer tracked).

`amps-action-on-message-condition-timeout` — monitors a SOW topic; triggers on timeout only.

Use `amps-action-on-alert` if both timeouts and tracking matter; otherwise use `amps-action-on-message-condition-timeout`.

Both use the OOF mechanism: when a message matches the filter, tracking begins. If no OOF within the timeout, action fires (timeout). If OOF received before timeout, action fires (no longer tracked).

Triggers exactly once per message per timeout. On restart, if a previously-triggered message still exists and matches, action fires immediately after initialization.

`amps-action-on-alert` uses the custom event system to indicate timeout vs. OOF. The event raised is stored in a context variable for use with `amps-action-do-event`.

**`amps-action-on-alert` parameters:**

| Parameter | Description |
| --- | --- |
| `Topic` (required) | SOW topic, view, or conflated topic. No regex. No queues. No default. |
| `MessageType` (required) | Message type. No default. |
| `OOFEvent` (required) | Value stored when OOF received. No default. |
| `TimeoutEvent` (required) | Value stored when timeout exceeded. No default. |
| `EventVariable` (required) | Context variable name for the reason (`OOFEvent` or `TimeoutEvent`). No default. |
| `Duration` | Time to wait for OOF before triggering. |
| `Filter` | Only matching messages monitored. No filter = all messages. |

**`amps-action-on-alert` context variables:**

| Variable | Description |
| --- | --- |
| `AMPS_TOPIC` | Triggering message topic. |
| `AMPS_DATA` | Current message data. |
| `AMPS_DATA_LENGTH` | Data length in bytes. |
| `AMPS_BOOKMARK` | Bookmark (empty if none). |
| `AMPS_TIMESTAMP` | Timestamp when tracking began. |
| `AMPS_CLIENT_NAME` | Client name of current message value. |
| `AMPS_SOW_KEY` | Current SowKey. |
| `EventVariable` value | `OOFEvent` or `TimeoutEvent`. |

### On Message Condition Timeout

`amps-action-on-message-condition-timeout` — monitors SOW topic; triggers for each message remaining matched on filter beyond the specified duration.

Uses OOF mechanism. Triggers exactly once per message per timeout. On restart, fires immediately for previously-triggered messages still in SOW and matching filter.

**Parameters:**

| Parameter | Description |
| --- | --- |
| `Topic` (required) | SOW topic, view, or conflated topic. No regex. No queues. No default. |
| `MessageType` (required) | Message type. No default. |
| `Duration` | Time to wait for OOF. |
| `Filter` | Only matching messages monitored. No filter = all messages. |

**Context variables:**

| Variable | Description |
| --- | --- |
| `AMPS_TOPIC` | Triggering message topic. |
| `AMPS_DATA` | Current message data. |
| `AMPS_DATA_LENGTH` | Data length in bytes. |
| `AMPS_BOOKMARK` | Bookmark (empty if none). |
| `AMPS_TIMESTAMP` | Timestamp when tracking began. |
| `AMPS_CLIENT_NAME` | Client name of current message value. |
| `AMPS_SOW_KEY` | Current SowKey. |

### On OOF Message

`amps-action-on-oof-message` — runs actions when an OOF message is produced for a subscription.

- Treated as an internal subscription.
- **Do not use with queue topics** — will lease messages without acking.
- Queue subscriptions never produce OOF messages (each publish is distinct).

**Parameters:**

| Parameter | Description |
| --- | --- |
| `Topic` (required) | SOW topic, view, or conflated topic. Supports regex. No queues. No default. |
| `MessageType` (required) | Message type. Supports regex. No default. |
| `Filter` | Filter for the internal subscription generating OOF messages. |
| `Type` | OOF type: `match`, `delete`, `expire`, or `all`. Default: `all` |

**Context variables:**

| Variable | Description |
| --- | --- |
| `AMPS_TOPIC` | OOF message topic. |
| `AMPS_DATA` | OOF message data. |
| `AMPS_DATA_LENGTH` | Data length. |
| `AMPS_PREVIOUS_DATA` | Previous record data. |
| `AMPS_PREVIOUS_DATA_LENGTH` | Previous data length. |

### On Outgoing Replication Connections

- `amps-action-on-connect-replication` — outgoing `Destination` connected.
- `amps-action-on-disconnect-replication` — outgoing `Destination` disconnected.
- `amps-action-on-replication-resync-complete` — `Destination` brought up to date with transaction log.
- `amps-action-on-upgrade-replication` — connection upgraded from `async` to `sync` ack.
- `amps-action-on-downgrade-replication` — connection downgraded from `sync` to `async` ack.

No parameters required.

**Context variables:**

| Variable | Description |
| --- | --- |
| `AMPS_REPLICATION_PEER_NAME` | Peer instance name (if available). |
| `AMPS_REPLICATION_CLIENT_NAME` | Client name for the connection. |
| `AMPS_REPLICATION_REMOTE_ADDRESS` | Remote address. |
| `AMPS_REPLICATION_GROUP_NAME` | Peer group name (if available). |
| `AMPS_REPLICATION_TRANSPORT_NAME` | Transport name. |

### On a Schedule

`amps-action-on-schedule` — runs actions on a specified schedule.

| Parameter | Description |
| --- | --- |
| `Every` (required) | Three forms: **Timer** — duration like `4h` or `1d`, starts on instance start, resets after firing. **Daily** — time of day like `00:32` or `17:47` (24h notation). **Weekly** — day + time like `Saturday at 11:00`. Append `Z` for UTC (e.g. `11:32Z`); otherwise local time. Default: `unknown` |
| `Name` | Schedule name for log messages. Default: `unknown` |

No context variables added.

### On a Linux Signal

`amps-action-on-signal` — runs actions when AMPS receives a specified signal.

| Parameter | Description |
| --- | --- |
| `Signal` (required) | Standard Linux signal name (e.g. `SIGUSR1`, `SIGHUP`). `SIGQUIT` is reserved for minidumps and cannot be overridden. |

No context variables added.

### Default Signal Actions

| On Event | Action |
| --- | --- |
| `SIGUSR1` | `amps-action-do-disable-authentication` |
| `SIGUSR1` | `amps-action-do-disable-entitlement` |
| `SIGUSR2` | `amps-action-do-enable-authentication` |
| `SIGUSR2` | `amps-action-do-enable-entitlement` |
| `SIGINT` | `amps-action-do-shutdown` |
| `SIGTERM` | `amps-action-do-shutdown` |
| `SIGHUP` | `amps-action-do-shutdown` |

Overridable via explicit configuration.

| On Event | Action |
| --- | --- |
| `SIGQUIT` | `amps-action-do-minidump` |

**Not** overridable.

### On AMPS Startup or Shutdown

- `amps-action-on-startup` — runs as last step in startup.
- `amps-action-on-shutdown` — runs as first step in shutdown.

Actions run in configuration file order. No parameters. No context variables.

### On Subscribe or Unsubscribe

- `amps-action-on-subscribe` — runs on subscription command.
- `amps-action-on-unsubscribe` — runs on unsubscribe command or disconnect.

**Parameters:**

| Parameter | Description |
| --- | --- |
| `Topic` (required) | Topic or regex pattern to monitor. |
| `MessageType` (required) | Message type. No default. |

**Context variables:**

| Variable | Description |
| --- | --- |
| `AMPS_TOPIC` | Topic from the command. |
| `AMPS_CLIENT_NAME` | Client name. |
| `AMPS_OPTIONS` | Subscription options. |
| `AMPS_FILTER` | Subscription filter. |

### Archive Journals Once a Week

Archives transaction log journals older than 1 week, every Saturday at 00:30. Moves files from `JournalDirectory` to `JournalArchiveDirectory` while keeping them active for replay/replication.

```text
<Actions>
    <Action>
        <On>
            <Module>amps-action-on-schedule</Module>
                <Options>
                    <Every>Saturday at 00:30</Every>
                    <Name>Saturday Night Fever</Name>
                </Options>
        </On>
        <Do>
            <Module>amps-action-do-archive-journal</Module>
            <Options>
                <Age>7d</Age>
            </Options>
        </Do>
    </Action>
</Actions>
```

### Archive Journals On RESTful Command

Archives journals via HTTP request to `/amps/administrator/actions/archive_journals` with required `AGE` query parameter.

```json
<Actions>
    <Action>
        <On>
            <Module>amps-action-on-admin</Module>
            <Options>
                <Path>archive_journals</Path>
                <RequiredParameter>AGE</RequiredParameter>
                <Name>Archive Journals</Name>
            </Options>
        </On>
        <Do>
            <Module>amps-action-do-archive-journal</Module>
            <Options>
                <Age>{{AGE}}</Age>
            </Options>
        </Do>
    </Action>
</Actions>
```

Missing `AGE` → request refused. Invalid interval → logged error, request refused.

### Record Expired Queue Messages to a Dead Letter Topic

Detects expired queue messages and publishes to a dead letter topic.

```text
<Actions>
    <Action>
        <On>
            <Module>amps-action-on-sow-expire-message</Module>
            <Options>
                <Topic>interesting-queue</Topic>
                <MessageType>json</MessageType>
            </Options>
        </On>
        <On>
            <Module>amps-action-on-sow-expire-message</Module>
            <Options>
                <Topic>another-interesting-queue</Topic>
                <MessageType>json</MessageType>
            </Options>
        </On>
        <Do>
            <Module>amps-action-do-publish-message</Module>
            <Options>
                <Topic>dead-letter</Topic>
                <MessageType>json</MessageType>
                <Data>{"topic":"{{AMPS_TOPIC}}","message":{{AMPS_DATA}} }</Data>
            </Options>
        </Do>
    </Action>
</Actions>
```

### Extract Values from a Published Message

Extracts values from XML messages and stores in context as `VALUE` and `QTY`.

```json
<Actions>
    <Action>
        <On>
            <Module>amps-action-on-publish-message</Module>
            <Options>
                <Topic>message-sow</Topic>
                <MessageType>xml</MessageType>
                <MessageSource>local</MessageSource>
            </Options>
        </On>
        <Do>
            <Module>amps-action-do-extract-values</Module>
            <Options>
                <MessageType>xml</MessageType>
                <Data>{{AMPS_DATA}}</Data>
                <Value>VALUE = /info/value</Value>
                <Value>QTY = /info/quantity</Value>
            </Options>
        </Do>
    </Action>
</Actions>
```

### Increment a Counter and Echo a Message

Increments counter on `SIGUSR1` and echoes the value.

```json
<Actions>
    <Action>
        <On>
            <Module>amps-action-on-signal</Module>
            <Options>
                <Signal>SIGUSR1</Signal>
            </Options>
        </On>
        <Do>
            <Module>amps-action-do-increment-counter</Module>
            <Options>
                <Key>MY_COUNTER</Key>
                <Value>CURRENT_COUNTER_VALUE</Value>
            </Options>
        </Do>
        <Do>
            <Module>amps-action-do-echo-message</Module>
            <Options>
                <Message>AMPS has gotten {{CURRENT_COUNTER_VALUE}}
                    SIGUSR1 signals.</Message>
            </Options>
        </Do>
    </Action>
</Actions>
```

### Copy Messages that Exceed a Timeout to a Different Topic

Copies messages from `Orders` to `Orders_Stale` when `/status = 'PENDING'` for > 5s. Works for `SOW/Topic`, `SOW/View`, `SOW/ConflatedTopic`.

```json
<Actions>
    <Action>
        <On>
            <Module>amps-action-on-message-condition-timeout</Module>
            <Options>
                <MessageType>nvfix</MessageType>
                <Topic>Orders</Topic>
                <Filter>/status = 'PENDING'</Filter>
                <Duration>5s</Duration>
            </Options>
        </On>
        <Do>
            <Module>amps-action-do-publish-message</Module>
            <Options>
                <MessageType>nvfix</MessageType>
                <Topic>Orders_Stale</Topic>
                <Data>{{AMPS_DATA}}</Data>
            </Options>
        </Do>
    </Action>
</Actions>
```

### Copy Messages to a Different Topic

Republishes all `Orders` messages to `DuplicateOrders`. Same `amps-action-on-publish-message` limitations apply: not active during recovery, no replay, use underlying topic for queues.

```json
<Actions>
    <Action>
        <On>
            <Module>amps-action-on-publish-message</Module>
            <Options>
                <MessageType>nvfix</MessageType>
                <Topic>Orders</Topic>
            </Options>
        </On>
        <Do>
            <Module>amps-action-do-publish-message</Module>
            <Options>
                <MessageType>nvfix</MessageType>
                <Topic>DuplicateOrders</Topic>
                <Data>{{AMPS_DATA}}</Data>
            </Options>
        </Do>
    </Action>
</Actions>
```

### Reset Entitlements for a Disconnected Client

Resets entitlement cache for a user on disconnect.

```json
<Actions>
    <Action>
        <On>
            <Module>amps-action-on-disconnect-client</Module>
        </On>
        <Do>
            <Module>amps-action-do-reset-entitlement</Module>
            <Options>
                <AuthenticationId>{{AMPS_AUTHENTICATION_ID}}</AuthenticationId>
            </Options>
        </Do>
    </Action>
</Actions>
```

### Shut Down AMPS When a Filesystem Is Full

Graceful shutdown when filesystem usage > 99%, checked every 3s.

```text
<Actions>
    <Action>
        <On>
            <Module>amps-action-on-schedule</Module>
            <Options>
                <Every>3s</Every>
            </Options>
        </On>
        <If>
         <Module>amps-action-if-file-system-usage</Module>
            <Options>
                <Path>./</Path>
                <GreaterThan>99%</GreaterThan>
            </Options>
        </If>
        <Do>
            <Module>amps-action-do-shutdown</Module>
       </Do>
    </Action>
</Actions>
```

### Deactivate and Reactivate Security on Signals

Disables auth/entitlement on `SIGUSR1`, re-enables on `SIGUSR2` (equivalent to default behavior).

```text
<Actions>
    <Action>
        <On>
            <Module>amps-action-on-signal</Module>
            <Options>
                <Signal>SIGUSR1</Signal>
            </Options>
        </On>
        <Do>
            <Module>amps-action-do-disable-authentication</Module>
        </Do>
        <Do>
            <Module>amps-action-do-disable-entitlement</Module>
        </Do>
    </Action>
    <Action>
        <On>
            <Module>amps-action-on-signal</Module>
            <Options>
                <Signal>SIGUSR2</Signal>
            </Options>
        </On>
        <Do>
            <Module>amps-action-do-enable-authentication</Module>
        </Do>
        <Do>
            <Module>amps-action-do-enable-entitlement</Module>
        </Do>
    </Action>
</Actions>
```

### Out-of-Focus Messages (OOF)

OOF messages notify subscribers when a previously-matching record no longer matches a subscription. Opt-in: subscribers must explicitly request OOF.

OOF produced when a record:
- Is deleted
- Expires
- No longer matches filter criteria
- Leaves pagination window (paginated subscriptions)
- Subscriber loses entitlement to view the updated record

Each OOF contains the reason and updated/previous data. Only supported for SOW topics, conflated topics, and views.

**Data in OOF body:** updated state of the record. Exceptions:
- `delta_publish` causing OOF → fully merged record.
- No updated message (delete, expire, entitlement change) → previous state.
- Conflated view/subscription: last data subscriber received. If OOF and return-to-focus occur in same conflation interval, subscriber receives end-of-interval state only (no intermediate OOF).

### Out-of-Focus Reason Codes

| Reason Field | Description | Message Contents |
| --- | --- | --- |
| `deleted` | Message deleted from topic. | previous message |
| `expired` | Message expired from topic. | previous message |
| `match` | No longer matches filter or outside record set. | updated message |
| `entitlement` | User no longer entitled. | previous message |

### Out-of-Focus Reason Codes: Usage

SOW topic with key `/buyer/id`:

```xml
<SOW>
    <Topic>
        <Name>buyer</Name>
        <MessageType>xml</MessageType>
        <Key>/buyer/id</Key>
    </Topic>
</SOW>
```

Original message persisted:

```text
<buyer>
    <id>100</id>
    <loc>NY</loc>
</buyer>
```

Client issues `sow_and_subscribe` with filter `/buyer/loc="NY"` and `oof` option. Then an update changes `loc` to `LN`:

```text
<buyer>
    <id>100</id>
    <loc>LN</loc>
</buyer>
```

Client receives OOF message with header:

| Field | Value |
| --- | --- |
| Command | `oof` |
| Topic | `buyer` |
| Reason | `match` |
| SowKey | `6387219447538349146` |

Body contains the updated message (loc=LN). If deleted, body would contain the deleted message with reason `deleted`.

### Out-of-Focus Reason Codes: Example

**Client-Side Filtering:** `sow_and_subscribe` on `orders` with `/Client="Adam"` — client must filter `State` changes locally. High volume can make GUI unresponsive.

**AMPS Filtering:** Filter `/Client = "Adam" AND /State = "Open"` — AMPS filters, but when `State` changes to `Filled`, the client never receives the update (no longer matches filter).

**OOF Processing:** Same filter with `oof` option:

```text
/Client = "Adam" AND /State = "Open"
options: oof
```

AMPS sends OOF when a matching record changes so it no longer matches. Reason field in header indicates why (`match`, `deleted`, `expired`).

### Event Topics

AMPS publishes events to internal topics prefixed with `/AMPS/`. Subscribe using content filters. Available on any connection with a message type that supports views (all default types + `bson`, excluding `binary`).

Messages delivered in the connection's message type.

### Client Status Events

Published to `/AMPS/ClientStatus` on: connect, logon, disconnect, subscribe, unsubscribe, SOW query, `sow_delete`, failed auth. On disconnect, one message per active subscription.

Example JSON for a SOW query:

```json
{
 "ClientStatus":{
    "timestamp":"20250909T171919.976304Z",
    "event":"sow",
    "client_name":"test_client",
    "connection_name":"AMPS-Sample-any-tcp-9-242891694350073019",
    "correlation_id":null,
    "query_id":"1",
    "topic":"order",
    "filter":"/item/qty > 50",
    "options":"send_empties",
    "sub_id":"1",
    "auth_id":null,
    "entitlement_filter":null
 }
}
```

**Header fields:**

| FIX | XML | JSON/BSON/MsgPack | Description |
| --- | --- | --- | --- |
| 20062 | `Reason` | `reason` | Event reason (e.g. disconnect reason). |
| 20065 | `Timestamp` | `timestamp` | Processing timestamp. |
| 20066 | `Event` | `event` | Command executed. |
| 20067 | `ClientName` | `client_name` | Client name. |
| 20068 | `Tpc` | `topic` | Topic (if applicable). |
| 20069 | `Filter` | `filter` | Filter (if applicable). |
| 20070 | `SubId` | `sub_id` | Subscription ID (if applicable). |
| 20071 | `ConnName` | `connection_name` | Internal connection name. |
| 20072 | `Options` | `options` | Subscription options (if applicable). |
| 20073 | `QId` | `query_id` | Query ID (if applicable). |
| 20074 | `CorrelationID` | `correlation_id` | Correlation ID (if applicable). |
| 20080 | `ClientAddr` | `client_address` | Client remote address. |
| 20081 | `AuthId` | `auth_id` | Authenticated identity (if applicable). |
| 20082 | `EntitlementFilter` | `entitlement_filter` | Entitlement filter (if applicable). |

For regex commands, fields like `entitlement_filter` may vary by topic and may not be available at status message time.

`/AMPS/ClientStatus` unavailable for `protobuf`, `struct`, `binary`, and composite message types (fixed schema or no fixed serialization).

### Persisting Event Topics

Event topics are not persisted to SOW by default. Add a SOW `Topic` definition with appropriate `Key` to persist.

Key must match field name for the message type:
- JSON: `<Key>/client_name</Key>`
- FIX: `<Key>/20067</Key>`

Example — persist `/AMPS/SOWStats` in FIX, JSON, XML:

```xml
<SOW>
    <Topic>
        <Name>/AMPS/SOWStats</Name>
        <FileName>./sow/sowstats.fix.sow</FileName>
        <MessageType>fix</MessageType>
        <Key>/20066</Key>
    </Topic>
    <Topic>
        <Name>/AMPS/SOWStats</Name>
        <FileName>./sow/sowstats.json.sow</FileName>
        <MessageType>json</MessageType>
        <Key>/topic</Key>
    </Topic>
    <Topic>
        <Name>/AMPS/SOWStats</Name>
        <FileName>./sow/sowstats.xml.sow</FileName>
        <MessageType>xml</MessageType>
        <Key>/Topic</Key>
    </Topic>
</SOW>
```

Each update overwrites the record with the same key value.

### SOW Statistics Events

Enable with `SOWStatsInterval` in config. Publishes to `/AMPS/SOWStats` at the specified interval.

```xml
<AMPSConfig>
    ...
    <SOWStatsInterval>5s</SOWStatsInterval>
    ...
</AMPSConfig>
```

Example JSON message:

```json
{
 "SOWStats":{
    "message_type":"bflat",
    "topic":"a-sample-topic",
    "record_count":15021,
    "timestamp":"20161108T225452.280650Z"
 }
}
```

**Header fields:**

| FIX | XML | JSON/BSON/MsgPack | Definition |
| --- | --- | --- | --- |
| 20007 | `MessageType` | `message_type` | Topic message type. |
| 20065 | `Timestamp` | `timestamp` | Timestamp AMPS sent message. |
| 20066 | `Topic` | `topic` | Topic name. |
| 20067 | `Records` | `record_count` | Record count. |

With `AMPSVersionCompliance` set to `5`, unified FIX tags:

| FIX | Definition |
| --- | --- |
| 20007 | Topic message type. |
| 20065 | Timestamp. |
| 20068 | Topic name. |
| 20075 | Record count. |

`/AMPS/SOWStats` unavailable for `protobuf`, composite, `binary`, `struct` message types.

### Chaining Key Generator

`libamps_id_chaining_key_generator` — generates the same SOW key for all messages in a chain. Messages must have a field identifying the current message and a field identifying the previous message.

Messages with same chain → single SOW record. Error if a message resolves to two different chains.

### Chained Message Sample Case

Default SOW key generator: four distinct records:

```json
delta_publish: {"DocumentNumber":1, "Status":"Started"}
delta_publish: {"DocumentNumber":2, "ParentDocument":1, "Order":"Antivenom"}
delta_publish: {"DocumentNumber":3, "ParentDocument":2, "Order":"Sandwich"}
delta_publish: {"DocumentNumber":4, "ParentDocument":1, "Status":"Pending"}
```

With chaining key generator → single record:

```json
{"DocumentNumber":4, "ParentDocument":1 , "Order":"Sandwich", "Status":"Pending"}
```

### Configuring the Chaining Key Generator

```xml
<AMPSConfig>
    ...
    <Modules>
         ...
        <Module>
            <Name>key-chaining</Name>
            <Library>libamps_id_chaining_key_generator.so</Library>
        </Module>
    </Modules>
</AMPSConfig>
```

**Parameters:**

| Parameter | Description |
| --- | --- |
| `Key` | Chaining field. First `Key` is **primary field** — creates new chain when value not in existing chain. Subsequent `Key` elements are **secondary fields** — generates SOW key as if primary field had this value. Requires AMPS field identifier (e.g. `/11`, `/Order/ClOrdID`). Requires primary + at least one secondary. |
| `FileName` | File for persisting chain data across restarts. Created if nonexistent. |
| `Primary` | Synonym for `Key`, explicitly marks primary field. When present, all `Key` elements become secondary. |
| `Secondary` | Synonym for `Key`, explicitly marks secondary field. |
| `Validation` | `true`/`1` enables detection of two distinct chains sharing identifiers. Default: `false`. |

Example:

```xml
<Modules>
   ...
    <Module>
        <Name>key-chaining</Name>
        <Library>libamps_id_chaining_key_generator.so</Library>
    </Module>
</Modules>

<SOW>
    ...
    <Topic>
        <Name>Orders</Name>
        <MessageType>json</MessageType>
        <KeyGenerator>
            <Module>key-chaining</Module>
            <Options>
                <Primary>/DocumentNumber</Primary>
                <Key>/ParentDocument</Key>
                <Key>/RelatedDocument</Key>
                <FileName>./sow/Orders.chain</FileName>
            </Options>
        </KeyGenerator>
        <FileName>./sow/%n.sow</FileName>
    </Topic>
    <Topic>
        <Name>ExternalOrders</Name>
        <MessageType>fix</MessageType>
        <KeyGenerator>
            <Module>key-chaining</Module>
            <Options>
                <Key>/11</Key>
                <Key>/41</Key>
                <FileName>./sow/ExternalOrders.chain</FileName>
            </Options>
        </KeyGenerator>
        <FileName>./sow/%n.sow</FileName>
    </Topic>
</SOW>
```

### Configuring Modules

`Modules` section loads/configures plug-in modules. Steps:
1. Load module and declare name.
2. Define AMPS object, name it, pass options.
3. Use module in context.

For `Authentication`/`Entitlement`, steps 2–3 are combined. For `MessageType` (used across Transport, SOW, View, replication), they are separate.

**Module fields:**

- `Name` (required) — reference name, used in logging.
- `Library` (required) — shared object path. Relative paths evaluated from AMPS working directory. Files in AMPS `lib` directory can be referenced by filename alone.
- `Options` — module-specific options as child elements.

Example — authentication + entitlement modules:

```xml
<AMPSConfig>
    ...
    <Modules>
        <Module>
            <Name>authentication1</Name>
            <Library>libauthenticate_customer001.so</Library>
            <Options>
                <LogLevel>info</LogLevel>
                <Mode>debugging</Mode>
            </Options>
        </Module>
        <Module>
            <Name>entitlement1</Name>
            <Library>libentitlement_customer001.so</Library>
            <Options>
                <LogLevel>error</LogLevel>
                <Mode>prod</Mode>
            </Options>
        </Module>
    </Modules>
    ...
</AMPSConfig>
```

Transport-level modules override instance-level defaults:

```xml
<AMPSConfig>
    ...
    <Authentication>
        <Module>my_default_security</Module>
    </Authentication>
    <Entitlement>
        <Module>my_default_security</Module>
    </Entitlement>
    ...
    <Transports>
        <Transport>
            <Name>fix-tcp-001</Name>
            ...
            <Authentication>
                <Module>authenticate_customer001</Module>
            </Authentication>
            <Entitlement>
                <Module>entitlement_customer001</Module>
            </Entitlement>
        </Transport>
        <Transport>
            <Name>json-tcp</Name>
            <!-- uses instance-level modules -->
            ...
        </Transport>
    </Transports>
    ...
</AMPSConfig>
```

### Special-Purpose Functions

Experimental UDF module: `libamps_udf_experimental.so`. Contains `VALUE_LOOKUP`.

### Preprocessing/Enrichment — VALUE_LOOKUP

Looks up a value in another topic during preprocessing/enrichment.

Lookup declaration (`Lookup` option, semicolon-delimited, all required):

| Option | Definition |
| --- | --- |
| Name | Lookup name. |
| Parameter Count | Number of lookup fields. |
| Message Type | Topic message type. |
| Topic Name | Topic name. |
| Return Field | Field to return. |
| Lookup Fields | Fields for lookup. |

Single-parameter example: `<Lookup>return-value-by-id;1;json;data;/value;/id</Lookup>`

Two-parameter example: `<Lookup>getNotes;2;nvfix;orders;/notes;/customerId;/orderId</Lookup>`

Usage: `VALUE_LOOKUP("return-value-by-id", /thisId)`

Exact match on lookup fields. Returns string (coercible to number). Multiple matches → one arbitrary result. No matches → `NULL`.

| Function | Parameters | Description |
| --- | --- | --- |
| `VALUE_LOOKUP` | *lookup name*, *values matching definition* | Returns value from referenced topic as string. Multiple matches → one result. No matches → `NULL`. |

**Limitations:**
- No performance guarantees; use small, infrequently updated lookup topics.
- Caches results; memory footprint ≈ lookup values + return values + 32 bytes/record + overhead per lookup.
- Returns AMPS string (coercible to number).
- Multiple matches: returns one, no guarantee which.
- Cannot be used in view definitions or SOW topics with `transient` durability and recovery point other than `now`.
- Cache updated asynchronously; immediate publish + lookup may or may not return new value.
- Non-deterministic (depends on SOW state).

### Loading the Experimental UDF Module

```text
<Modules>
  <Module>
    <Name>experimental-udf</Name>
    <Library>libamps_udf_experimental.so</Library>
    <Options>
      <Lookup>value-by-id;1;json;source-sow;/value;/id</Lookup>
      <Lookup>customer-id-by-order-id;1;nvfix;orders;/id;/customerId</Lookup>
    </Options>
  </Module>
</Modules>
```

### Loadable Function Modules

- `libamps_udf_legacy_compatibility` — legacy date/time functions.
- `libamps_udf_experimental` — special-purpose functions.

### Legacy Messaging Compatibility Functions

Not loaded by default. Add to `Modules` block:

```xml
<AMPSConfig>
    ...
    <Modules>
        ...
        <Module>
            <Name>compatibility-functions-module</Name>
            <Library>libamps_udf_legacy_compatibility.so</Library>
        </Module>
    </Modules>
</AMPSConfig>
```

| Function | Parameters | Description |
| --- | --- | --- |
| `TIMEZONEOFFSET` | (none) | UTC offset in seconds (long). |
| `YEAR` | *timestamp* | Year in UTC. |
| `MONTH` | *timestamp* | Month in UTC. |
| `DAY` | *timestamp* | Day in UTC. |
| `DATE_UTC` | *timestamp* | Start of day (00:00:00) in UTC, in seconds. |
| `DATE` | *timestamp* | Start of day (00:00:00) in local timezone, in seconds. |
| `TODAY_UTC` | (none) | Start of current day in UTC, in seconds. |
| `TODAY` | (none) | Start of current day in local timezone, in seconds. |

### Optional SOW Key Generator

- `libamps_id_chaining_generator` — chained SOW key generation.

### AMPS Expressions

AMPS includes an expression language combining XPath and SQL-92 `WHERE` clause. Used whenever AMPS refers to message contents.


## AMPS Expressions and Functions

AMPS expressions are used for content filtering, message enrichment field construction, and view projection fields. An expression produces a value; filters match when it returns `true`, field constructors use the returned value.

### AMPS Data Types

| Type | Description | Examples |
| --- | --- | --- |
| NULL | Unknown, untyped (SQL-92 semantics) | `a=<SOH>`, `{"a":null}`, `<a/>` |
| Boolean | `true` (1) or `false` (0) | `{"e":true}` |
| Integer | Signed 64-bit; unsigned 64-bit for values > LONG_MAX | `b=24`, `{"b":24}`, `<b>24</b>` |
| Floating Point | 64-bit double | `c=24.0`, `{"c":24.0}`, `<c>24.0</c>` |
| String | Byte sequence of specific length; empty string = NULL | `d=Grilled cheese sandwich<SOH>` |

Operators/functions auto-convert types: `*` converts to numeric, `CONCAT` converts to string.

### Numeric Types and Literals

- Integers: all numerals, no decimal point, 64-bit range. Examples: `42`, `149`, `-273`, `18446744073709551610`
- Floats: numerals with decimal point, double-precision. Examples: `3.1415926535`, `98.6`, `-273.0`
- Scientific notation: `31.4e-1`, `6.022E23`, `2.998e8`
- Strings with numeric values auto-convert when used with numeric operators/functions.

**Type Promotion Rules:**
1. If any value is `NaN`, result is `NaN`.
2. If any value is floating point, result is floating point.
3. Otherwise (all integers), result is integer.

Note: `1 / 5` = `0` (integer); `1.0 / 5` = `0.2` (float).

### String Literals

Single or double quotes. Escape sequences:

| Escape | Definition |
| --- | --- |
| `\a` | Alert |
| `\b` | Backspace |
| `\t` | Horizontal tab |
| `\n` | Newline |
| `\f` | Form feed |
| `\r` | Carriage return |
| `\xHH` | Hex digit (0-9, a-f, A-F) |
| `\OOO` | Octal digit (0-7) |

Any character following `\` is treated as literal. AMPS string ops handle embedded `NULL` (`\x00`) and non-ASCII; not unicode-aware.

### NULL, NaN, IS NULL

- XPath to empty/nonexistent field = `NULL` (SQL-92 semantics).
- Comparisons with `NULL` are never true (`/a == NULL` false, `/a != NULL` also false).
- Zero-length string = `NULL`.

**AND truth table with NULL:**

| Op1 | | Op2 | Result |
| --- | --- | --- | --- |
| TRUE | AND | NULL | NULL |
| FALSE | AND | NULL | FALSE |
| NULL | AND | NULL | NULL |
| NULL | AND | TRUE | NULL |
| NULL | AND | FALSE | NULL |

**OR truth table with NULL:**

| Op1 | | Op2 | Result |
| --- | --- | --- | --- |
| TRUE | OR | NULL | TRUE |
| FALSE | OR | NULL | NULL |
| NULL | OR | NULL | NULL |
| NULL | OR | TRUE | NULL |
| NULL | OR | FALSE | NULL |

Predicates: `IS NULL`, `IS NOT NULL`, `IS NAN`.

`COALESCE()` accepts a set of values, returns the first non-NULL. Not array-aware (uses first array element).

```text
COALESCE(/userCategory, /employeeCategory, /vendorCategory, 'restricted') != 'restricted'
```

### Compound Types

AMPS parses nested structures as paths to scalar values. Intermediate containers have no explicit scalar value. Duplicate paths become arrays.

Example:
```json
{"outer": {"middle": {"inner": 5}}}
```
→ Path: `/outer/middle/inner`, Value: `5`

Complex example:
```json
{"outer": {
   "array": ["a1", "a2", "a3"],
   "compound": {"A": "middle-A", "B": "middle-B",
                 "C": [{"C1":"first-C1","D1":"first-D1"},
                        {"C1":"second-C1","D1":"second-D1"}]}
}}
```

| Path | Value | Notes |
| --- | --- | --- |
| `/outer/array` | `['a1','a2','a3']` | `/outer/array[0]` = `'a1'` |
| `/outer/compound/A` | `'middle-A'` | |
| `/outer/compound/B` | `'middle-B'` | |
| `/outer/compound/C/C1` | `['first-C1','second-C1']` | `/outer/compound/C/C1[0]` = `'first-C1'` |
| `/outer/compound/C/D1` | `['first-D1','second-D1']` | `/outer/compound/C/D1[0]` = `'first-D1'` |

### Arithmetic Operators

`+`, `-`, `*`, `/`, `%`, `MOD`. NULL operand → NULL result. Mixed types: integer promoted to float.

```text
/6 * /14 < 1000
/Order/@Qty * /Order/@Prc >= 1000000
```

`MOD`/`%` preserves sign of first argument: `-5 % 3` = `-2`, `5 % -3` = `2`.

Separate math operators from XPath with whitespace (e.g., `/` is both division and XPath separator).

### Comparison Operators

Equality/ordering: `==` (= `=`), `>`, `>=`, `<`, `<=`, `!=`, `<>`. Mixed types: AMPS tries string→number conversion; unconvertible strings > numbers; empty string = NULL.

| Expression | Result |
| --- | --- |
| `1 < 2` | TRUE |
| `10 < '2'` | FALSE (`'2'` converts to number) |
| `'2.000' <> '2.0'` | TRUE (both strings, no numeric conversion) |
| `2 = 2.0` | TRUE (numeric comparison) |
| `10 < 'Crank It Up'` | TRUE (strings > numbers) |
| `10 < ''` | FALSE (empty = NULL) |
| `'' = ''` | FALSE (both NULL) |
| `'' IS NULL` | TRUE |

**BETWEEN** — inclusive range: `/A BETWEEN 0 AND 100` ≡ `/A >= 0 AND /A <= 100`.

```text
/FIXML/Order/@Px NOT BETWEEN 90.0 AND 90.5
(/price * /qty) BETWEEN 0 AND 100000
```

**IN** — membership test, equivalent to OR-ed `=` comparisons. NULL in field or set → false. Case-sensitive for strings.

```text
/Trade/OwnerID NOT IN ('JMB', 'BLH', 'CJB')
/customer IN ('Bob', 'Phil', 'Brent')
```

`/data NOT IN (1,2,3)` ≡ `NOT /data IN (1,2,3)` ≡ `NOT ((/data == 1) OR (/data == 2) OR (/data == 3))`.

`IN` typically performs better than equivalent OR chains.

### Conditional Operator: IF

```text
IF(BOOLEAN_CONDITION, VALUE_TRUE, VALUE_FALSE)
```

| Function | Parameters | Description |
| --- | --- | --- |
| `IF` | Conditional, value if true, value if false | Returns one of two values based on condition. AMPS 5.3.4+ conditionally evaluates branches. |

```text
SUM(IF((/FIXML/Order/OrdQty/@Qty > 500) AND (/FIXML/Order/Instrmt/@Sym = 'MSFT'), 1, 0))
SUM(/FIXML/Order/Instrmt/@Qty * IF(/FIXML/Order/Instmt/@Price IS NOT NULL, 1, 0))
```

### Grouping and Order of Evaluation

Parentheses group subexpressions. Within a group: left-to-right, `AND` before `OR`. AMPS may short-circuit: `A_FUNCTION(/a) OR B_FUNCTION(/b)` — `B_FUNCTION` only evaluated if `A_FUNCTION` returns false.

### Identifiers

XPath subset for field references. No wildcards, relative paths, predicates, or functions.

- XML element: `/Order/Symbol`
- XML attribute: `/Order/@update`
- FIX/NVFIX tag: `/55`
- JSON nesting: `/outer/inner`

**Bracketed identifiers**: `[/Not XPath Name]` — allows spaces and special chars in field names.

Identifiers are syntax-checked but not validated against message type at parse time. `composite-local` adds part number prefix (e.g., `/0/name`).

### LIKE Operator

PCRE regex matching on strings. Pattern must be a literal. Case-sensitive by default. Not unicode-aware.

```text
/state LIKE '(.)\1'
```

| Function | Parameters | Description |
| --- | --- | --- |
| `LIKE` | String, pattern | Returns true if string matches PCRE pattern. |

Prefer dedicated string functions (`BEGINS WITH`, `INSTR`, etc.) over `LIKE` for simple comparisons — they're faster.

### Logical Operators

`NOT`, `AND`, `OR` (descending precedence).

```text
/FIXML/Order/Instrmt/@Sym = 'IBM' OR /FIXML/Order/Instrmt/@Sym = 'MSFT'
(/orderType = 'rush' AND /customerType IN ('silver', 'gold')) OR /customerType = 'platinum'
```

### Performance Considerations

- **Short-circuiting**: `OR` evaluates RHS only if LHS is false. Put cheaper comparisons on the left.
- **Redundant expressions**: AMPS doesn't reorder or combine. Merge manually (e.g., combine overlapping `IN` sets).
- **Specialized operators**: `BEGINS WITH` > `LIKE '^...'`, `INSTR` > `LIKE '...'`, etc.

| Regex | AMPS Equivalent |
| --- | --- |
| `^something` | `BEGINS WITH('something')` |
| `something$` | `ENDS WITH('something')` |
| `something` | `INSTR(/field, 'something') != 0` |
| `(?i)something` | `INSTR_I(/field, 'something') != 0` |
| `(?i)^something$` | `STREQUAL_I(/field, 'something') != 0` |
| `^a$` | `= 'a'` |

- **Partial parsing**: AMPS stops parsing after finding all referenced fields. Not effective with `delta_subscribe`/`delta_publish` or filters referencing absent fields.
- **SOW indexing**: Use exact string match + hash index. AMPS creates memo indexes for unmapped XPaths on first query. Indexes used only during `sow` phase, not during subscription phase.

### Regular Expressions

PCRE library. Syntax reference: <http://perldoc.perl.org/perlre.html>.

**Metacharacters:**

| Char | Meaning |
| --- | --- |
| `^` | Start of string |
| `$` | End of string |
| `.` | Any char except newline |
| `*` | Match previous 0+ times |
| `?` | Match previous 0 or 1 times |
| `()` | Grouping |
| `[]` | Character set |
| `{}` | Repetition modifier |
| `\` | Escape |

**Repetition:**

| Construct | Meaning |
| --- | --- |
| `a*` | Zero or more |
| `a?` | Zero or one |
| `a{m}` | Exactly m |
| `a{m,}` | At least m |
| `a{m,n}` | At least m, at most n |

**Modifiers:**

| Modifier | Meaning |
| --- | --- |
| `i` | Case insensitive |
| `m` | Multi-line |
| `s` | `.` matches newlines |
| `x` | Ignore unescaped whitespace |
| `A` | Anchor to start |
| `U` | Non-greedy quantifiers |

Examples:
```text
(/FIXML/Order/Instrmt/@Sym LIKE "^IB.?$") AND (/FIXML/Order/@Px LIKE "^90\..*" AND /FIXML/Order/@Px < 91.0)
(/client/country LIKE "(?i)^us$")
(/55 LIKE "TRADE$")
(/109 LIKE "(?i)^US.*TRADE$")
```

### Raw Strings

Prefix with `r` or `R`: backslash characters are literal, not escape sequences.

```text
/FIXML/Language LIKE r'C++'
```
Equivalent to: `/FIXML/Language LIKE 'C\+\+'`

### Regex Topic Subscriptions

Topic names containing regex characters are interpreted as regex. Use `non_regex_topic` option to disable. `^` anchors start, `$` anchors end.

Results returned in configuration file order. No ordering guarantees within a topic.

### Expression Syntax

Expressions combine identifiers, literals, and operators/functions. Every expression produces a value.

### Typed Value Construction Functions

| Function | Parameters | Description |
| --- | --- | --- |
| `FALSE_VALUE` | none | Returns boolean false (equivalent to `0`) |
| `TRUE_VALUE` | none | Returns boolean true (equivalent to `1`) |
| `NAN_VALUE` | none | Returns NaN |
| `CHAR_VALUE` | integer (0-255) | Returns character/byte for integer. `'\x01'` is more efficient than `CHAR_VALUE(1)` for literals, but `CHAR_VALUE(/code)` works for field-based construction |

### Working with Arrays

Principles:
1. Binary operators yielding true/false (`=`, `<`, `LIKE`) and `IN` are **array-aware** — evaluate every element.
2. Arithmetic operators, functions, UDFs, scalar operators are **not** array-aware — use first element.
3. Empty array evaluates to `NULL`.

Sample data:
```json
{"data": [1, 2, 3, "zebra", 5], "other": [14, 34, 23, 5]}
```

**Any element matching:**

| Filter | Result |
| --- | --- |
| `/data = 1` | TRUE |
| `/data = 'zebra'` | TRUE |
| `/data != 'zebra'` | TRUE (has non-zebra element) |
| `/data = 42` | FALSE |
| `/data LIKE 'z'` | TRUE |
| `/other > 30` | TRUE |
| `/other > 50` | FALSE |

**Specific position (subscript `[]`):**

| Filter | Result |
| --- | --- |
| `/data[0] = 1` | TRUE |
| `/data[3] = "zebra"` | TRUE |
| `/data[1] != 1` | TRUE |
| `/other[1] LIKE '4'` | TRUE |

**Array vs array:**

| Filter | Result |
| --- | --- |
| `/data = /other` | TRUE (shared value `5`) |
| `/data != /other` | TRUE (differing values exist) |

**Array with IN:**

| Filter | Result |
| --- | --- |
| `3 IN (/data)` | TRUE |
| `/data IN (1, 2, 3)` | TRUE |
| `/data IN ("zebra", "antelope", "lion")` | TRUE |
| `NOT /data IN ("zebra", "antelope", "lion")` | FALSE |

### AMPS Functions Overview

Functions can be used anywhere an identifier or literal is used. All return a single value. Results can be nested: `REVERSE(SUBSTR('fandango',5)) == 'ogna'`.

| Category | Functions |
| --- | --- |
| String | String Comparison, `ARRAY_TO_STRING`, `CONCAT`, `UPPER`/`LOWER`, `REPLACE`/`REGEXP_REPLACE`, `SUBSTR` |
| Date/Time | `STRFTIME`, Date/Time functions, Legacy Messaging Compatibility |
| Array Reduce | Array Reduce functions |
| Geospatial | `GEO_DISTANCE` |
| Numeric | `ABS` |
| Checksum | `CRC32` |
| Message | `MESSAGE_SIZE`, `TOPIC_NAME()` |
| Client | `CLIENT_NAME` |
| NULL Handling | `COALESCE` |
| AMPS Info | `AMPS_INSTANCE_NAME` |
| Typed Values | `FALSE_VALUE`, `TRUE_VALUE`, `NAN_VALUE`, `CHAR_VALUE` |

### Deterministic vs Non-Deterministic Functions

**Deterministic**: consistent result for same message. No restrictions on use.

**Non-deterministic** (`LAST_READ`, `UNIX_TIMESTAMP`, `VALUE_LOOKUP`): may return different values per call. Cannot be used in:
- OOF subscription filters
- Aggregated subscription filters (allowed in aggregated *query* filters)
- Aggregate functions
- Paginated `sow_and_subscribe` filters (`top_n`/`skip_n`/`OrderBy`)
- Queue filters or barrier expressions
- View/conflated topic filters
- Replication filters

### Aggregate Functions

Available in view `Field` constructors and aggregated subscription `projection`. Return one value per distinct group. Not available for filters or SOW enrichment.

```text
<Projection>
   <Field>/oid</Field>
   <Field>SUM(/qty) AS /totalOrderQty</Field>
   <Field>SUM(IF((/qty % 10) == 0,1,0)) AS /evenTensOrderCount</Field>
</Projection>
<Grouping>
   <Field>/oid</Field>
</Grouping>
```

### Constructing Fields

Format: `<source expression> AS <destination identifier>`

```text
<Field>/price * /qty AS /total</Field>
```

**Preprocessing fields**: operate on single message, merged into incoming message. Unchanged fields preserved. Cannot specify topic/message type. Evaluated during preprocessing — cannot refer to previous message state.

**HINT directives for preprocessing/enrichment:**
- `HINT OPTIONAL`: if result is NULL, remove field instead of serializing NULL.
- `HINT SET_CURRENT`: immediately update current message, making value available to subsequent `Field` declarations. Forces sequential processing.
- Combined: `HINT SET_CURRENT,OPTIONAL`

```text
<Field>EXPENSIVE_UDF_CALL(/dataSet1, /dataSet2) AS /processedData HINT SET_CURRENT</Field>
<Field>IF(/processedData > 1000000, 'A', 'B') AS /resultClass</Field>
```

**Enrichment fields**: same as preprocessing, but can reference previous message state:

| Modifier | Description |
| --- | --- |
| `OF CURRENT` | XPath refers to incoming message |
| `OF PREVIOUS` | XPath refers to previous SOW state. Returns NULL if no prior record. Not supported on `ConflatedTopic` |

**View fields**: operate over message groups. Identifiers must be from underlying topics. With `Join` of different message types, include both: `[nvfix].[orders]./quantity * [json].[items]./price AS /total`.

### String Comparison Functions

- **Case-sensitive**: `=`, `BEGINS WITH`, `ENDS WITH`, `INSTR`, `IN`
- **Case-insensitive**: `INSTR_I`, `STREQUAL_I`
- **Regex**: `LIKE` (see Regular Expressions)

```text
/status = 'available'
/Department BEGINS WITH ('Engineering', 'Research', 'Technical')
/filename ENDS WITH ('gif', 'png', 'jpg')
INSTR(/eventLevels, "critical") != 0
STREQUAL_I(/couponCode, 'QED') == 1
INSTR_I(/symbolList, 'MSFT') != 0
```

`BEGINS WITH`/`ENDS WITH` accept sets. `INSTR` returns position (1-based) or 0 if not found.

### Running AMPS as a Linux Service

Config file location: `/opt/etc/amps/config.xml`. Use absolute paths. Log warning+ to syslog:

```text
<Logging>
    <Target>
        <Protocol>syslog</Protocol>
        <Level>warning</Level>
        <Ident>amps</Ident>
        <Options>LOG_CONS,LOG_NDELAY,LOG_PID</Options>
        <Facility>LOG_USER</Facility>
    </Target>
</Logging>
```

**Install**: `sudo ./install-amps-daemon.sh` — installs to `/opt/amps`, creates `/opt/etc/amps`, registers service (SystemV: `/etc/init.d/amps` or SystemD: `amps.service`).

**Manage**:
- Start: `sudo /etc/init.d/amps start` or `sudo systemctl start amps`
- Stop: `sudo /etc/init.d/amps stop` or `sudo systemctl stop amps`
- Restart: `sudo /etc/init.d/amps restart` or `sudo systemctl restart amps`
- Status: `sudo /etc/init.d/amps status` or `sudo systemctl status amps`

**Uninstall**: `sudo ./uninstall-amps-daemon.sh` (does not remove config or runtime data).

One instance per system via this script.

### Operation and Deployment

Capacity planning considers: Memory, Storage, CPU, Network. Factors: AMPS configuration parameters, data characteristics, usage patterns.


## Capacity Planning

### System Goals

Define the instance purpose and SLA (dev exploration vs. core infrastructure, latency vs. query throughput priorities).

#### Single-Tenant or Multi-Tenant

Plan for *highest* simultaneous traffic across all applications. Provision for peak load of **all** applications combined.

For multi-tenant or multi-application hosts, disable AMPS NUMA tuning:

```xml
<AMPSConfig>
  <Tuning>
    <NUMA>
      <Enabled>disabled</Enabled>
    </NUMA>
  </Tuning>
</AMPSConfig>
```

#### Physical Server, VMs, Containers

AMPS runs on physical hardware, VMs, or containers. For lowest latency, deploy on physical hardware (single AMPS instance per server). On VMs/containers, disable NUMA tuning (same XML as above). Do not overcommit underlying hardware.

For VMs: total memory/CPU/network/storage across all VMs must not exceed physical host capacity.

For containers: ensure host supports all containers at peak capacity (CPU, memory, networking, storage).

Monitor both virtual/container environment and physical host, correlating activity between them.

Do not use live VM migration for latency-sensitive applications (migration pauses ~1s, causing effective outage).

### Memory

AMPS uses memory for performance. Ensure sufficient physical memory.

AMPS binary + startup: <1GB. Typical active production footprint: ~5GB.

#### AMPS Instance Memory Estimate

```
5GB + SowSizeEstimate + (C × 4096 bytes) + TMemLimit + (J × 2) + (Q × 250 bytes) [+ (QA × 20 bytes)]
```

Where:
- **SowSizeEstimate** = SOW topic size estimate (bytes)
- **C** = Number of Clients
- **TMemLimit** = Total of all MessageMemoryLimit settings
- **J** = JournalSize setting
- **Q** = Total active unacknowledged messages in queues
- **QA** = Total acknowledgments for messages not yet in queue

With `TargetQueueDepth`, active unacknowledged messages are typically limited to that depth.

**SowSizeEstimate** per Topic/View/ConflatedTopic:

```
(2 × (S + 128 bytes) × M) + (16 bytes × M × H)
```

Where:
- **S** = Average message size (bytes)
- **M** = Maximum expected message count
- **H** = Number of hash indexes

**Example** (3 topics including 1 view, 200 clients, 10GB TMemLimit, 1GB journal, 750K unacked msgs):

| Component | S | M | H |
|---|---|---|---|
| Topic 1 | 1024 | 4,750,000 | 2 |
| View | 512 | 3,000,000 | 0 |
| Topic 2 | 1024 | 8,000,000 | 4 |

Result: ~52GB minimum physical memory for AMPS process.

#### Overall System Capacity

Add OS, monitoring, security, other apps. Linux needs 10-20% memory headroom.

Recommendation: size physical memory to handle **200%** of estimated capacity while retaining 10-20% free RAM.

| Scenario | Sizing |
|---|---|
| Production, strict SLA | 128GB (2× estimated, 10-20% free) |
| Production, stable usage | 96GB |
| Shared dev, minimal SLA | 64GB |

### Storage

#### Error and Event Log Files

- `trace` level: storage for every published + sent message + 20%
- `info` level: ~2MB per 10M messages published
- Use log rotation with same filename to cap size

#### SOW Topics

**Minimum SOW size:**

```
Min = (MsgSize × MsgCount) + (Cores × SlabSize)
```

**Maximum SOW size:**

```
MsgSlabMin = SlabSize / MsgSize / 2
Max = (MaxMsgCount / MsgSlabMin × SlabSize) + (Cores × SlabSize)
```

Where: MsgSize = average message size, Cores = CPU cores, SlabSize = configured slab size.

AMPS reserves `SlabSize` per core when first written to from that core.

**Max message size in SOW:** `SlabSize - 64` bytes.

#### Transaction Logs

```
TxLog = (S + 512) × N + Jsize + Tindex
```

Where:
- **S** = Average message size
- **N** = Number of messages to retain
- **Jsize** = Journal file size
- **Tindex** = Topic Index (if configured): max(200MB, 64 × NumberOfMessagesIndexed)

Size files to match aging policy. Preallocate files for latency-sensitive applications.

With replication: transaction log maintenance won't delete unreplicated messages. Calculate max storage including recovery window (e.g., 8hr failure window = 8hr of journals minimum).

#### File-Backed Queue Metadata

For queues using `FileBackedMetadata`:

```
GREATER OF (MaxMsgCount × 250 bytes) or 4MB
```

Where MaxMsgCount = max unacknowledged messages in queue (max `queue_depth` metric). With `TargetQueueDepth`, typically equals that depth.

AMPS preallocates ~4MB files, grows as needed, does not shrink while running.

#### Choosing Storage Devices

| Feature | Storage Usage |
|---|---|
| SOW | Random access read/write/update |
| Transaction Log | Sequential write / sequential read for replay, replication, queue distribution |
| Error and Event Log | Sequential write only |
| Statistics Database | Random access read/write/update |
| Persisted Queue Metadata | Random access read/write/update |

For transaction log >50MB/s, use flash storage or better. Magnetic disks lack consistent latency at this rate.

For high performance: separate stats DB + event log from SOW + transaction log + queue metadata onto different partitions/devices.

### CPU

SOW queries with content filtering are CPU-heavy. More cores = faster queries.

Highest performance: 64-bit x86 CPUs with SSE 4.2 instruction set.

Benchmark: 5-predicate filter on 1KB messages at >1,000,000 msgs/sec per core on Intel i7 3GHz.

### Network

Network capacity estimate:

```
R × (Sz + 128) × (1 + M × Sb) + Q
```

Where:
- **R** = Publish rate (msgs/sec)
- **Sz** = Average message size
- **128** = Metadata overhead (subscription IDs, bookmarks, timestamps)
- **M** = Match ratio per subscription
- **Sb** = Number of subscribers
- **Q** = Query load = Mq × S × Qs (messages per query × avg size × queries/sec)

**Example:** 5000 msgs/sec, 600B avg, 2% match, 100 subs, 5 queries/min returning 1000 msgs:
= 5000 × 600B × (1 + 0.02 × 100) + (1000 × 600B × 1/12) ≈ 9MB/s ≈ 72Mb/s

#### Replication Network Bandwidth

Estimate as though each outgoing replication destination subscribes to all replicated topics, and each incoming stream is a full publisher. Provision for full uncompressed capacity; use compression to save capacity.

#### Additional Network Considerations

Account for NAS traffic, monitoring, log collection in bandwidth planning.

### NUMA Considerations

- Install NIC closest to NUMA node 0 for lowest latency
- Single AMPS instance on physical host: leave NUMA tuning **enabled** (default)
- Multiple AMPS instances, other CPU-intensive processes, or processor-restricted AMPS: **disable** NUMA tuning
- VMs: **disable** NUMA tuning

### Linux OS Settings

#### ulimit

Configure appropriate ulimits for the AMPS process (file descriptors, core dumps, etc.).

### Operations Best Practices

### Monitoring

AMPS exposes statistics via RESTful interface at the `Admin` address in configuration.

Stuck threads: AMPS logs "stuck" messages. After 60s stuck, AMPS auto-emits a minidump to configured directory.

Monitor `dmesg` for OS-level errors (OOM kills, hardware failures).

Monitor `/amps/instance/processors/all/last_active` — if `last_active` increases >1 minute with degraded service, consider failover and restart.

### Logging

| Environment | Recommended Level |
|---|---|
| Production | `info` minimum; capacity for `trace` when troubleshooting |
| Development/UAT | `trace` |

Capture stdout and stderr for OS/runtime errors outside AMPS control.

### Stopping AMPS

AMPS runs `amps-action-do-shutdown` on `SIGHUP`, `SIGINT`, or `SIGTERM`. Configure custom shutdown actions as needed. When installed as a system service, shutdown scripts handle clean stop.

### SOW Parameters

`SlabSize` tradeoffs:
- **Small SlabSize**: frequent SOW extensions, potential throughput reduction, risk of exhausting kernel mmap region limit
- **Large SlabSize**: fewer allocations, more wasted space with few messages
- **Optimal**: minimize unused space per slab

Guidelines:
- Default SlabSize if messages < default SlabSize
- Several times max message size if messages > default SlabSize
- Ensure slab count ≥ number of CPU cores for query parallelism

Example: avg 512B, max 1.2MB → SlabSize of 2.5MB (holds ~5 avg + 2 large messages).

### Slow Clients

#### Slow Client Offlining for Large Result Sets

| Parameter | Recommendation |
|---|---|
| `MessageMemoryLimit` | Start at 10% system memory. Increase by 1-2% if needed. Caution >20%. |
| `MessageDiskLimit` | avg record size × expected records × simultaneous clients, or MessageMemoryLimit (whichever greater) |
| `MessageDiskPath` | Fast, high-capacity storage (e.g., PCIe flash). Capacity > MessageDiskLimit. |

#### WAN Traffic and Slow Client Settings

For clients over high-latency networks: adjust slow client settings or create a separate transport with higher capacity. Ensure `ClientMessageAgeLimit` allows time for network to consume SOW query results.

### Minidump

Minidumps capture thread state (call stacks, registers, processor info). They do **not** contain application memory or detailed host state.

**Generation triggers:**
1. Internal crash detection (thread stuck ~300s)
2. Admin console `minidump` link
3. `SIGQUIT` signal
4. Configured action
5. Thread monitor detects no progress for ~60s

Default directory: `/tmp`. Configurable via `MiniDumpDirectory`. Monitor this directory.

Contact 60East support for diagnosis. Remove minidumps after acknowledged submission.

### Deployment and Upgrade Plan

60East provides a deployment checklist covering:
- Capacity Planning
- OS Configuration
- AMPS Configuration
- Maintenance Plans
- Monitoring Strategy
- Patching and Upgrade Plan
- Support Plan

### Accessing AMPS Through a Proxy

AMPS client and replication connections are TCP with custom protocol. Any proxy that doesn't alter packet content works.

#### WebSocket Connections

JavaScript client supports arbitrary paths in URI for routing:

```
ws://proxyhost:8080/amps-prod-system/amps/json
```

#### Galvanometer Connections

Proxy rewrites URI for routing. Example NGINX config:

```json
server {
    listen 8085 default;
    listen [::]:8085;
    server_name AMPS;
    location /amps-prod-system/ {
        proxy_set_header X-Forwarded-Host $host;
        proxy_set_header X-Forwarded-Server $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_pass http://amps-prod-system:8085/;
    }
    location /amps-dev-system/ {
        proxy_set_header X-Forwarded-Host $host;
        proxy_set_header X-Forwarded-Server $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_pass http://amps-dev-system:8085/;
    }
}
```

`ExternalInetAddr` option advertises a public address for the admin server (useful when replication uses private network but Galvanometer uses public/proxy).

#### Load-Balancing Considerations

Publishers and queue consumers must not fail over across replication connections using async acknowledgment — risk of message loss and inconsistent transaction logs. Use synchronous acknowledgment for these cases.

### Upgrading AMPS

AMPS supports rolling upgrade from version 5.2.0.0+. For production, all replicated instances should have same MAJOR.MINOR version, preferably identical release.

#### Upgrade Steps

1. Stop running instance
2. Install new binaries
3. Upgrade data files if upgrading from <5.0.0.0 (use `amps_upgrade` utility)
4. Update configuration file if needed
5. Update applications if needed
6. Restart service

#### Data File Compatibility

- 5.0+: backward compatible data file formats (no `amps_upgrade` needed)
- <5.0: requires `amps_upgrade` utility
- Downgrading across MAJOR/MINOR versions: **not supported** (files not backward compatible)

### File Format Versions

| AMPS version | SOW version | Journal version | Acks version | Queue Metadata version |
|---|---|---|---|---|
| 3.8 | v1.0 | v3 | v1.0 | |
| 3.9 | v1.0 | v3 | v1.0 | |
| 4.0 | v3.0 | v7 | v1.0 | |
| 4.3 | v3.0 | v7 | v1.0 | |
| 5.0 | v3.0 | v8 | v1.0 | |
| 5.2–5.2.4 | v3.0 | v8 | v1.0 | |
| 5.3.0–5.3.3 | v3.0 | v8 | v1.0 | |
| 5.3.4–5.3.5 | v3.0 | v8 | v1.0 | v001 |

### Troubleshooting AMPS

### Diagnostic Utilities

| Utility | Description |
|---|---|
| `amps-grep` | Search AMPS logs (message-aware) or journal files (bookmark-aware, client name correlation) |
| `amps_journal_dump` | Extract journal file contents. Supports compressed journals. |
| `amps_journal_search` | Find specific message by exact bookmark in journals |
| `amps_sow_dump` | Extract SOW topic data |
| `amps_sqlite3` | SQL queries over AMPS statistics database |
| `amps_file` | File type and version info for AMPS files |
| `amps_clients_ack_dump` | Dump clients.ack (last persisted message per publisher) |
| `amps_queues_ack_dump` | Dump queues.ack (last acked message per queue) |
| `ampserr` | AMPS error/event information lookup |

### Finding Information in the Log

Search for errors:

```text
amps-grep -E 'warning|error|critical|emergency' log_file
```

Search by client name:

```text
amps-grep client_name log_file
grep -B2 -A10 client_name log_file
```

### Planning for Troubleshooting

1. Log at `info` or more verbose (minimum `warning` if storage restricted)
2. Use unique, traceable client names
3. Enable admin server
4. Use unique replication instance names
5. Learn normal operation patterns

### Reading Replication Log Messages

Replication client name format:

```text
source!destination!sync_setting!protocol
```

Example: `OrderServer!HotBackup!sync!amps-replication` — connection from OrderServer to HotBackup, synchronous, amps-replication protocol.

### Troubleshooting Regular Expression Subscriptions

Common issues:
- Regex doesn't match expected topics (use external regex tester; AMPS matches anywhere in topic name unless `^` anchored)
- User not entitled to matched topics

### Troubleshooting Disconnected Clients

AMPS disconnects clients for: duplicate name with txlog, missed heartbeat, slow client, entitlement cache reset, admin console disconnect, transport disabled.

#### Disconnection Reasons

| Reason | Explanation |
|---|---|
| `connection closed` | Socket closed |
| `entitlement reset` | Entitlements reset for user/transport |
| `name in use` | Duplicate client name reconnected |
| `heartbeat` | Heartbeat timeout |
| `slow client` | Capacity threshold exceeded |
| `unknown command` | Malformed/unknown command |
| `auth` | Authentication failure |
| `entitlement` | Authenticated but no login permission |

Logged in message `07-0013`.

#### Duplicate Client Name

```text
02-0025 A client logon with an 'in use' client name for the same user id forced a disconnect
```

Solution: ensure unique client names with transaction logging enabled.

#### Missed Heartbeat

```text
07-0042 AMPS heartbeat manager is disconnecting an unresponsive client: <name>
```

Causes: network congestion, client deadlock, AMPS issue.

#### Slow Client

```text
70-0011 client[name] slow consumption detected, offline messages.
70-0004 client[name] is not consuming messages, disconnecting slow client
```

Remedies: reduce message volume with content filtering, improve client processing speed, increase offlining threshold.

#### Admin Console Disconnect

```text
07-0013 client[name] disconnected.
```

#### Transport Disabled

```text
07-0047 Transport[name] being disabled.
07-0013 client[name] disconnected.
```

### Utility Reference

#### File Inspection and Search

| Utility | Description |
|---|---|
| `amps_file` | Report file type for AMPS-format files |
| `amps_grep` | Search/extract from AMPS logs or journal files |
| `amps_journal_dump` | Examine journal file contents |
| `amps_journal_search` | Find transaction log record by bookmark/txid |
| `amps_sow_dump` | Inspect SOW topic store |
| `amps_clients_ack_dump` | Inspect clients acknowledgment file |
| `amps_queues_ack_dump` | Inspect queues acknowledgment file |
| `amps_tx_topic_index_dump` | Inspect journal topic index |

#### Submitting Minidump

| Utility | Description |
|---|---|
| `amps_report_minidump` | Submit minidumps to 60East support |

#### Statistics Utilities

| Utility | Description |
|---|---|
| `amps-sqlite3-report` | Extract subset from AMPS sqlite3 stats database |
| `amps-sqlite3` | Query AMPS statistics database with AMPS-aware functions |

#### Planning and Informational

| Utility | Description |
|---|---|
| `ampserr` | Expand/examine AMPS error codes |
| `amps_bio_perf_test` | Measure sequential write performance on storage devices |

#### Minimal Client

| Utility | Description |
|---|---|
| `spark` | Minimal command-line AMPS client for adhoc diagnostics |

#### Obsolete

| Utility | Description |
|---|---|
| `amps_upgrade` | For upgrades from <5.0.0 only. Not needed for 5.0+. |

### amps_bio_perf_test

Measures raw sequential write (and optional read) performance for storage devices (simulates transaction log workload). Uses `O_DIRECT` (bypasses OS cache).

| Option | Description |
|---|---|
| `-b, --batch_size` | Messages per batch (1–256) |
| `-m, --message_size` | Message size in bytes (256–128K, multiple of 256) |
| `-f, --file_size` | Data file size in MB (max 8192) |
| `-r, --read_mix` | Read mix percentage (0–99) |
| `-f, --file` | Output file path (must be on device to test) |

**Output metrics:**

| Metric | Description |
|---|---|
| elapsed time | Active write/read duration |
| writer thread count | Always 1 |
| batch size | Messages per batch |
| submit size | Bytes per message |
| write size | Bytes per batch write |
| submit count | Total messages during test |
| read count | Total read operations |
| write count | Total write operations |
| submit rate | Average messages/sec |
| write rate | Average writes/sec |
| actual read mix | Actual read/write proportion |
| mean write latency | Average write latency (μs) |

Tool outputs CSV files with raw latency and throughput samples (appends to existing files).

**Usage:**

```bash
$ amps_bio_perf_test -b 128 -m 512 -s 8192 -r 0 -f /mnt/fastdrive/ampsdir/bio.data
```

### amps_clients_ack_dump

Inspect `clients.ack` file contents.

| Option | Description |
|---|---|
| `filename` (required) | Acks file path |
| `--version` | Show version |
| `-h, --help` | Show help |
| `-n LIMIT, --limit` | Max records to print |

### amps_file

Identify file type and version of AMPS files. Supports UNIX shell globbing.

| Option | Description |
|---|---|
| `file_name` (required) | File to report on (supports globbing) |

**Example:**

```bash
%> ./amps_file /amps_dir/sow/mytopic.sow
mytopic.sow: AMPS sow 4.0
```

### amps-grep

Search AMPS logs and journal files. Supports literal and regex search (Python `re` dialect).

| Option | Description |
|---|---|
| `files` (required) | Files to search |
| `-e, --search_term=TERM` | Exact match string |
| `-f, --file=LITERAL_TERMS_FILE` | Exact matches from file, one per line |
| `-E, --extended_regex=REGEX` | Regex pattern |
| `-n, --line-number` | Include line numbers |
| `-H, --with-filename` | Include filename |
| `-h, --no-filename` | Omit filename |
| `-i, --ignore-case` | Case-insensitive |
| `-v, --invert-match` | Select non-matching |
| `--no-data` | Omit data field in journal results |
| `--include-noops` | Include noops in journal dump |
| `--client=CLIENTS` | Search by client name or hash (repeatable) |

Reads stdin if no filenames provided.

### amps_journal_dump

Examine journal file contents for debugging.

| Option | Description |
|---|---|
| `filename` (required) | Journal file path |
| `-h, --help` | Show help |
| `-l LIMIT` | Limit output range (N:M or M for first M) |
| `--localtime` | ISO 8601 in local time |
| `--extents` | Show local and replication extents |
| `--no-data` | Omit data values |
| `--include-noops` | Include noops |

**Output sections:**

Header:

| Field | Description |
|---|---|
| File Name | Filesystem name |
| File Size | Total allocated bytes |
| Version | Journal format version |
| Extents | [extents used : blocks allocated] |

Entry fields:

| Field | Description |
|---|---|
| entry | Monotonic insertion order |
| crc32 | CRC for error checking |
| type | AMPS command type |
| file offset | Offset within journal file |
| tx byte count | Entry size in journal |
| msg byte count | Message payload bytes |
| msg type | Message type |
| local txid | Monotonic local identifier |
| previous local txid | Previous local identifier |
| source txid | Source identifier (or original client name hash for direct publishes without seq) |
| source name hash | Replicated source instance hash |
| client name hash | Publisher client name hash |
| client seq | Publisher sequence number |
| topic hash | Topic name identifier |
| sow expiration time | Message expiration in SOW/queue |
| iso8601 timestamp | Processing time (ISO-8601) |
| timestamp | Raw microsecond timestamp |
| previous byte count | Previous record size |
| topic byte count | Topic name length |
| topic | Topic name |
| data | Entry data (published message or internal data) |
| correlation id | Correlation ID if set |
| auth id | Authenticated submitter ID |
| rep path | Replication route to this instance |

Footer:

| Field | Description |
|---|---|
| Total Entries | Total journal entries |
| Total Bytes | Reserved bytes consumed |
| Remaining Bytes | Unused bytes available |

**Timestamp formatting:** Defaults to system timezone. Override with `TZ` env var:

```bash
%> TZ='America/New_York' ./amps_journal_dump A.000000000.journal
```

### amps_journal_search

Locate entries in journal files by bookmark or transaction ID.

| Option | Description |
|---|---|
| `filename` (required) | Journal file path |
| `-h, --help` | Show help |
| `<search-pattern>` | Bookmark or txid to locate (searches metadata only) |
| `-d <search-string>, --data` | Search message data (also matches bookmarks) |
| `-topic=TOPIC_NAME` | Search by topic |
| `--no-data` | Omit message data in output |
| `--client=CLIENT_HASH` | Search by client hash (repeatable) |

### amps_queues_ack_dump

Inspect `queues.ack` file contents.

| Option | Description |
|---|---|
| `filename` (required) | Acks file path |
| `--version` | Show version |
| `-h, --help` | Show help |
| `-n LIMIT, --limit` | Max records to print |

### amps_report_minidump

Submit minidumps to 60East support.

| Option | Description |
|---|---|
| `minidumps` (required) | Minidump file(s) to submit |
| `-e, --email=SENDER_EMAIL` (required) | Sender email |
| `-s, --subject=SUBJECT` (required) | Email subject |
| `-b, --body=BODY` | Email body |
| `-c, --compress` | Tar.gz before sending |
| `-t, --ticket=TICKET` | Ticket number |

**Example:**

```bash
$ amps-report-minidump -e "user@example.com" -s "Ticket 12345" \
    -b "This is the dump we discussed" -c "./*.dmp"
```

### amps_sow_dump

Inspect SOW topic store contents.

| Option | Description |
|---|---|
| `filename` (required) | SOW file path |
| `-n LIMIT` | Max records to print |
| `-v, --verbose` | Print record metadata and file summary |
| `--sizing-chart` | Print memory sizing chart (experimental) |
| `-d DELIMITER` | Data-only output with ASCII delimiter (default: 10/newline) |
| `--version` | Show version |
| `-h, --help` | Show help |

**Example:**

```bash
%> ./amps_sow_dump ./order.sow
{ "id": 0, "value": 1743 }
{ "id": 1, "value": 6554 }
{ "id": 2, "value": 3243 }
...
```

Verbose mode (`-v`) displays file structure and record metadata in addition to data.


## AMPS Monitoring Guide

This guide is a quick reference to the information monitored by the AMPS administrative interface, including the RESTful AMPS interface and the AMPS statistics database. It is organized the same way as the administrative interface: [Administrative Actions](#administrative-actions) provide the ability to change instance state and generate diagnostics; [Host Statistics](#host-statistics) collect information about the host system; and [AMPS Instance Statistics](#amps-instance-statistics) collect information about the running AMPS instance.

## Resource Mapping

To find a resource, start with the guide to locate the area, then use a browser for the RESTful interface or query the statistics database for details. For example, drive information is under the host section, so use `/amps/host/disks` in a browser or query the `HDISKS` view (or `HDISKS_*` tables).

### Administrative Actions

The `administrator` interface provides actions for this AMPS instance. Retrieving the URI for an action requests that AMPS take that action.

- **Authorization**: Selecting the `authorization` resource allows the `authentication` or `entitlement` resources to be reset.
  **Admin Path**: /amps/administrator/authorization/*<resource-type>*/*<action>*
- **Clients**: Selecting the `clients` resource lists all connected clients by identifier. Selecting a single client permits that client to be disconnected.
  **Admin Path**: /amps/administrator/clients/*<id>*/*<action>*
- **Diagnostics**: Selecting the `diagnostics` resource provides the option to write a diagnostics `dump` message to the log. This message is logged at `info` level, and is event number `31-0010`. The diagnostic message provides information on the current state of the queues configured for the instance.
  **Admin Path**: /amps/administrator/diagnostics/*<action>*
- **Minidump**: Selecting the `minidump` resource creates a minidump of the currently running AMPS instance. The minidump will be saved in a directory specified by the `MiniDumpDirectory`, or `/tmp` if no directory is specified.
  **Admin Path**: /amps/administrator/minidump
- **Queues**: Selecting the `queues` resource allows you to select whether this instance will respect the original ownership of messages in the queue, or allow the instance to claim ownership of messages if the original owner is unreachable. The options are `enable_proxied_transfer` or `disable_proxied_transfer`. By default, proxied transfer is disabled.
  **Admin Path**: /amps/administrator/queues/*<id>*/*<action>*
- **Replication**: Selecting the `replication` resource lists all currently configured replications. Selecting any individual replication destination permits the destination to be downgraded or upgraded.
  **Admin Path**: /amps/administrator/replication/*<id>*/*<action>*
- **SOW**: Selecting the `sow` resource lists all currently configured SOW topics. Selecting a topic permits you to `compact` that topic. Compacting a SOW causes AMPS to release unused space in the SOW.
  **Admin Path**: /amps/administrator/sow/*<id>*/*<action>*
- **Transaction Log**: Selecting the `transaction_log` resource allows the `journals` resources to be compressed, archived, or removed. Only journal files that are full will be displayed. Selecting a journal permits you to `compress`, `archive`, or `remove` that journal and all older journals.
  **Admin Path**: /amps/administrator/transaction\_log/journals/*<id>*/*<action>*
- **Transports**: Selecting the `transports` resource lists all currently configured transports. Selecting any individual transport permits the transport to be enabled or disabled.
  **Admin Path**: /amps/administrator/transports/*<id>*/*<action>*

### Entitlements Check for Admin Actions

The administrative interface provides a way to check access to an administrative action without running the action. This resource requires a `path` parameter with the full path of the action to check. AMPS returns `true` or `false` indicating whether the user has permission to run the action.

**Admin Path**: /amps/administrator/authorization/entitlement/transports/amps-admin/check

For example, a full entitlement check on the ability to disconnect the client with object ID 1, requesting the result as a JSON document:

```bash
<http://server:admin_port/amps/administrator/authorization/entitlement/transport/amps-admin/check.json?path=/amps/administrator/clients/1/disconnect>
```

### Host Statistics

The `host` URI contains information about the current operating system devices, such as the CPU, memory, disk and network. A host's network hostname and system timestamp are also exposed.

#### cpu (host statistics)

The `cpu` resource allows an administrator to view the CPU devices attached to the host. Selection generates a list of all CPUs: you can produce data for each individual CPU, or use the aggregate `all` option. AMPS also records CPU statistics for the AMPS process itself.

| Element | Description | Type |
| --- | --- | --- |
| `idle_percent` | Percent of CPU time that the system was waiting for an operation *other than* an I/O request to complete. | snapshot |
| `iowait_percent` | Percent of CPU time spent waiting for I/O requests to complete. | snapshot |
| `system_percent` | Percent of CPU utilization time which occurred while executing kernel processes. | snapshot |
| `user_percent` | Percent of CPU utilization time which occurred while running at the application level. | snapshot |

**Statistics Database Tables**: `HCPU_STATIC`, `HCPU_DYNAMIC`

**Admin Path**: /amps/host/cpu/*<id>*/*<metric>*

#### disks (host statistics)

The `disks` resource lists each of the disk devices attached to the host and permits the inspection of disk usage statistics.

| Element | Description | Type |
| --- | --- | --- |
| `file_system_free_percent` | Percentage of the filesystem currently free. | snapshot |
| `mount_point` | The mount point for this filesystem. | fixed |
| `in_progress` | Number of I/O requests waiting to be processed. | snapshot |
| `read_await` | Average read time completion in milliseconds. | interval average |
| `write_await` | Average write time completion in milliseconds. | interval average |
| `read_bytes_per_sec` | Average bytes per second read. | interval average |
| `write_bytes_per_sec` | Average bytes per second written. | interval average |

**Statistics Database Tables**: `HDISKS_STATIC`, `HDISKS_DYNAMIC`

**Admin Path**: /amps/host/disks/*<id>*/*<metric>*

#### memory (host statistics)

The `memory` resource gives details about the system memory statistics. All statistics reported are based on the current system statistics reported by examining the file `/proc/meminfo`.

| Element | Description | Type |
| --- | --- | --- |
| `anonymous` | The total amount of anonymous memory allocated. | snapshot |
| `available` | The total amount of memory available. This is the `MemAvailable` reported by the operating system, or the sum of free, buffers and cached if `MemAvailable` is not provided. | snapshot |
| `buffers` | The amount of physical memory available for file buffers. | snapshot |
| `cached` | The amount of physical memory used as cache memory. | snapshot |
| `free` | The amount of physical memory left unused by the system. | snapshot |
| `in_use` | The amount of memory currently in use. | snapshot |
| `swap_free` | The amount of swap memory which is unused. | snapshot |
| `swap_total` | The total amount of physical swap memory. | fixed |
| `total` | Total amount of RAM. | fixed |

**Statistics Database Tables**: `HMEMORY_STATIC`, `HMEMORY_DYNAMIC`

**Admin Path**: /amps/host/memory/*<metric>*

#### name (host statistics)

The `name` resource displays the network DNS name for the host.

**Admin Path**: /amps/host/name

#### network (host statistics)

The `network` resource allows an administrator to examine networking interface statistics on the host. Selecting the `network` resource displays a list of the network interfaces attached to the host. Selecting one of the interfaces will list the available properties.

| Element | Description | Type |
| --- | --- | --- |
| `bytes_in` | Number of bytes received by the interface. | interval snapshot |
| `bytes_in_per_second` | Rate of bytes received by the interface. | interval average |
| `bytes_out` | Number of bytes transmitted by the interface. | interval snapshot |
| `bytes_out_per_second` | Rate of bytes sent by the interface. | interval average |
| `errors` | Total errors both incoming and outgoing. This number includes packets dropped, collisions, fifo, frame, and carrier errors. | interval snapshot |
| `packets_in` | The total number of packets received by the interface. | interval snapshot |
| `packets_out` | The total number of packets sent by the interface. | interval snapshot |

**Statistics Database Tables**: `HNET_STATIC`, `HNET_DYNAMIC`

**Admin Path**: /amps/host/network/*<id>*/*<metric>*

### AMPS Instance Statistics

The `Instance` resource is the administrative overview of a running AMPS instance. All statistics in the instance interface are server-side metrics.

#### api (instance statistics)

Selecting the `api` resource lists information about the AMPS internal API.

| Metric | Description | Type |
| --- | --- | --- |
| `command_queue_depth` | The number of pending commands. | snapshot |

**Statistics Database Tables**: `IGLOBALS_STATIC`, `IGLOBALS_DYNAMIC`

**Admin Path**: /amps/instance/api/*<metric>*

#### clients (instance statistics)

Selecting the `clients` resource will list all connected clients by name. Selecting a single client will show various statistics for that client.

| Metric | Description | Type |
| --- | --- | --- |
| `authenticated_id` | The ID used to authenticate this client, if any. | fixed |
| `bytes_in` | Number of bytes received. | cumulative |
| `bytes_in_per_sec` | Rate of bytes received. | interval average |
| `bytes_out` | Number of bytes sent. | cumulative |
| `bytes_out_per_sec` | Rate of bytes sent. | interval average |
| `client_name` | Identifier for the client, set during logon. | fixed |
| `client_name_hash` | AMPS hash for the client name. | fixed |
| `client_version` | Version string provided by the client. | fixed |
| `connect_time` | UTC time client connection is established. | fixed |
| `connection_name` | Name of the connection. | fixed |
| `correlation_id` | The CorrelationId provided with the logon command, if any. | fixed |
| `denied_reads` | Number of read requests which have been denied due to an entitlement filter. | cumulative |
| `denied_writes` | Number of write requests which have been denied due to an entitlement filter. | cumulative |
| `messages_in` | Number of messages received from client. | cumulative |
| `messages_in_per_sec` | Rate of messages received. | interval average |
| `messages_out` | Number of messages sent to the client. | cumulative |
| `messages_out_per_sec` | Rate of messages sent to the client. | interval average |
| `query_time` | The amount of time spent for queries from this client. | cumulative |
| `queue_depth_out` | Number of messages queued to be sent to client. This represents a count of the messages that AMPS cannot write to the outgoing socket due to the transmit buffer being full. This does not count messages already written to the transmit buffer. (The transport\_tx\_queue has information about the transmit buffer.) | snapshot |
| `queue_max_latency` | The age of the oldest item in the queue which has not yet been sent. This is used as a measure of how far behind AMPS believes a subscribing client is. This measures the age of the oldest message that AMPS cannot write to the outgoing socket due to the transmit buffer being full. This does not count messages already written to the transmit buffer. (The transport\_tx\_queue has information about the transmit buffer.) The latency is measured in seconds at the resolution of the system clock. | snapshot |
| `queued_bytes_out` | Number of queued bytes waiting to be sent. This represents a count of the number of bytes that AMPS cannot write to the outgoing socket due to the transmit buffer being full. This does not count messages already written to the transmit buffer (transport\_tx\_queue will show messages written to the transmit buffer that have not yet been sent). | snapshot |
| `remote_address` | Address and port of the remote side of the client connection. | fixed |
| `subscription_count` | Number of subscriptions currently active for the client. | snapshot |
| `tcp_zero_window_advert` | Shows whether this client is currently advertising a zero window size. This is 1 if the client is advertising a windows size of zero, set to 0 if the client is advertising any other value *or* if the client connection does not use TCP (for example, the client uses UDS to connect to AMPS). | snapshot |
| `transport_rx_queue` | Number of bytes in the transport receive buffer (typically the TCP buffer) for this client. This measures messages arriving *from* the client. | snapshot |
| `transport_tx_queue` | Number of bytes in transport transmit buffer (typically the TCP buffer) for this client. This measures messages being sent *to* the client. | snapshot |

**Statistics Database Tables**: `ICLIENTS_STATIC`, `ICLIENTS_DYNAMIC`

**Admin Path**: /amps/instance/clients/*<id>*/*<metric>*

#### config_path (instance statistics)

Filesystem location of the configuration file.

**Admin Path**: /amps/instance/config\_path

#### config.xml (instance statistics)

Selecting this will display the current AMPS configuration file. To keep the path consistent, AMPS provides the file under the `config.xml` path, regardless of the actual name of the file.

**Admin Path**: /amps/instance/config.xml

#### conflated_topics (instance statistics)

Selecting the `conflated_topics` resource will display a list of the conflated topics in the instance.

| Metric | Description | Type |
| --- | --- | --- |
| `conflation_ratio` | Ratio representing the amount of conflation for this topic. (If there are no updates, this is not calculated and shows as 0.0.) | running average |
| `interval` | Conflation interval. | fixed |
| `message_type` | Message type for this topic. | fixed |
| `topic` | Name of the conflated topic. | fixed |
| `total_executions` | Total number of times the conflation algorithm has been executed. | cumulative |
| `total_time` | Amount of time spent processing this topic. | cumulative |
| `underlying_topic` | Name of the underlying topic that this topic conflates. | fixed |
| `filter` | The optionally configured filter for this topic. | fixed |
| `pattern` | The generated pattern for this topic (defined when the underlying topic is a regex topic). | fixed |
| `topic_format` | The topic format for this topic (defined when the underlying topic is a regex topic). | fixed |

**Statistics Database Tables**: `ICONFLATEDTOPICS_STATIC`, `ICONFLATEDTOPICS_DYNAMIC`

**Additional topic information**: `ISOW_STATIC`, `ISOW_DYNAMIC`

**Admin Path**: /amps/instance/conflated\_topics/*<id>*/*<metric>*

#### cpu (instance statistics)

The CPU resource lists properties related to overall CPU usage of the AMPS instance.

| Metric | Description | Type |
| --- | --- | --- |
| `system_percent` | Percent of CPU utilization time consumed while executing kernel processes on behalf of this AMPS instance. | snapshot |
| `total_percent` | Total percent of CPU utilization on behalf of this AMPS instance. | snapshot |
| `user_percent` | Percent of CPU utilization time consumed while processing non-I/O events on behalf of this AMPS instance. | snapshot |

**Statistics Database Tables**: `ICPUS_STATIC`, `ICPUS_DYNAMIC`

**Admin Path**: /amps/instance/cpu/*<metric>*

#### cwd (instance statistics)

The current working directory from which the AMPS instance was invoked.

**Admin Path**: /amps/instance/cwd

#### description (instance statistics)

The contents of the `Description` element in the configuration file.

**Admin Path**: /amps/instance/description

#### environment (instance statistics)

The contents of the `Environment` element in the configuration file.

**Admin Path**: /amps/instance/environment

#### lifetimes (instance statistics)

Information about the lifetime of the AMPS instance, including historical information if `stats.db` is persisted. Each time an event related to startup or shutdown is logged, AMPS creates an entry in this resource.

| Element | Description | Type |
| --- | --- | --- |
| `event` | The type of event logged. For example, `started` or `shutdown`. | fixed |
| `timestamp` | The timestamp of the event. | fixed |
| `version` | The AMPS version string for the instance that logged the event. | fixed |

**Statistics Database Tables**: `ILIFETIMES_STATIC`, `ILIFETIMES_DYNAMIC`

**Admin Path**: /amps/instance/lifetimes/*<identifier>*/*<metric>*

#### logging (instance statistics)

The `logging` resource contains information about the resources consumed during various AMPS logging processes. Selecting a logging mechanism (console, file or syslog) will first list all logs of that particular type. Drilling down into one of those logs will pull up more granular information. If a logging mechanism is not defined in the configuration, then the results will be blank when the logging resource is selected.

**Statistics Database Tables**: `ICONSOLE_LOGGERS_STATIC`, `ICONSOLE_LOGGERS_DYNAMIC`, `IFILE_LOGGERS_STATIC`, `IFILE_LOGGERS_DYNAMIC`, `ISYSLOG_LOGGERS_STATIC`, `ISYSLOG_LOGGERS_DYNAMIC`

**Admin Path**: /amps/instance/logging/*<type>*/*<identifier>*/*<metric>*

##### console

Below are the options available for reporting when `console` logging is enabled:

| Metric | Description | Type |
| --- | --- | --- |
| `bytes_written` | Number of bytes written to the console. | cumulative |
| `exclude_errors` | Errors which are excluded from logging. | fixed |
| `include_errors` | Errors which are included during logging. | fixed |
| `log_levels` | Log level used to control logging output. | fixed |
| `target` | Console to which logging output is directed. Default: `stdout` | fixed |

##### file

Below are the options available for reporting when `file` logging is enabled:

| Metric | Description | Type |
| --- | --- | --- |
| `bytes_written` | Number of bytes written to the log. | cumulative |
| `exclude_errors` | Errors which are excluded from logging. | fixed |
| `file_name` | File defined in the configuration file where the log file is written to. | fixed |
| `file_name_mask` | Mask of the logging output file name, if available. | fixed |
| `file_system_free_percent` | Percentage of free space on the file system that contains the file. | snapshot |
| `include_errors` | Errors which are included during logging. | fixed |
| `log_levels` | Log level used to control logging output. | fixed |
| `rotation` | Boolean representation denoting if log rotation is turned on. | fixed |
| `rotation_threshold` | Log size at which log rotation will occur. | fixed |

##### syslog

Below are the options available for reporting when `syslog` logging is enabled:

| Metric | Description | Type |
| --- | --- | --- |
| `bytes_written` | Number of bytes written to syslog. | cumulative |
| `exclude_errors` | Errors which are excluded from logging. | fixed |
| `facility` | Integer enumeration of the logging facility used by syslog. | fixed |
| `ident` | Syslog name of the logging instance. | fixed |
| `include_errors` | Errors which are included during logging. | fixed |
| `log_levels` | Log level used to control logging output. | fixed |
| `logopt` | Bitfield of possible log options included. These values are configured in the configuration file in the `<Options>` tag. | fixed |

#### memory (instance statistics)

AMPS can provide information regarding the process's memory usage in its RSS and VMSize via the `memory` resource in the monitoring interface.

| Metric | Description | Type |
| --- | --- | --- |
| `caches` | Information about AMPS memory caches. | (see caches) |
| `paginations` | Information about paginated result sets. | (see paginations) |
| `rss` | The resident set size of the AMPS process. | snapshot |
| `vmsize` | The virtual memory size of the AMPS process. | snapshot |

**Statistics Database Tables**: `IMEMORY_STATIC`, `IMEMORY_DYNAMIC`, `IMEMORY_CACHES_STATIC`, `IMEMORY_CACHES_DYNAMIC`

**Admin Path**: /amps/instance/memory/*<metric>*, /amps/instance/memory/caches/*<identifier>/\*<metric>*, /amps/instance/memory/paginations/*<metric>*

The `caches` element provides information about currently-active memory caches.

| Metric | Description | Type |
| --- | --- | --- |
| `allocations` | Number of memory allocations for this cache. | cumulative |
| `bytes` | Number of bytes allocated to this cache. | snapshot |
| `description` | Description of the cache. | fixed |
| `efficiency` | Ratio of hits to requests for this cache. | snapshot |
| `entries` | Number of entries in this cache. | snapshot |
| `evictions` | Count of evictions from this cache. | cumulative |
| `fetches` | Count of fetches from this cache. | cumulative |
| `overflow_bytes` | Bytes allocated for slow consumers to this cache. | cumulative |

The following caches may appear in AMPS statistics:

| Cache Name | Description |
| --- | --- |
| `byte buffer cache` | General purpose cache. |
| `byte in buffer cache` | Cache for bytes being received from clients. |
| `byte out buffer cache` | Cache for bytes being sent to clients. |
| `client cache` | Cache for client objects. |
| `client entitlement cache` | Cache for maintaining entitlement information. |
| `client session cache` | Cache for maintaining current session state. |
| `client status cache` | Cache for forming client status messages. |
| `message cache` | Cache for message objects. |
| `query context cache` | Cache for query contexts. |
| `sow update cache` | Cache for SOW updates. |
| `subscription cache` | Cache for subscription state. |
| `xpath value data buffer cache` | Cache for XPath values. |

**Statistics Database Table**: `IMEMORY_CACHES`

The `paginations` element provides information about currently-active paginated subscriptions in AMPS.

| Metric | Description | Type |
| --- | --- | --- |
| `memory_bytes` | Number of bytes consumed to maintain this paginated set. | snapshot |
| `message_type` | Message type for this paginated set. | fixed |
| `subscription_count` | Number of subscriptions using this paginated set. | snapshot |
| `topic` | Source topic for this paginated set. | fixed |

**Statistics Database Table**: `IPAGINATIONS`

#### message_types (instance statistics)

| Metric | Description | Type |
| --- | --- | --- |
| `module` | The name of the module that implements the message type. | fixed |
| `name` | The name of the message type. | fixed |
| `options` | Any options provided to the module. | fixed |
| `type` | The type configured for the message type module. *This configuration parameter is obsolete in 4.0 and later releases.* | (obsolete) |

**Admin Path**: /amps/instance/message\_type/*<identifier>*/*<metric>*

#### name (instance statistics)

Name of the AMPS Instance.

**Admin Path**: /amps/instance/name

#### name_hash (instance statistics)

**Admin Path**: /amps/instance/name\_hash

#### pid (instance statistics)

The process ID of the current `ampServer` process.

**Admin Path**: /amps/instance/pid

#### processors (instance statistics)

Selecting the `processors` resource will list all the available message processors that the AMPS instance has invoked to handle messages. Each AMPS message processor will be listed individually, or selecting the `all` resource will list an aggregate.

All AMPS message processors have the following attributes available:

| Metric | Description | Type |
| --- | --- | --- |
| `denied_reads` | Number of read requests which have been denied due to an entitlement filter. | cumulative |
| `denied_writes` | Number of write requests which have been denied. | cumulative |
| `description` | Descriptor of the processor. | fixed |
| `last_active` | Number of milliseconds since a processor was last active. For each statistics snapshot, this indicates the longest period of time between the time that the statistics were collected and the time an instance of a processor of this type marked itself as active. This counter is expected to have variation in a healthy instance. A steady increase in this counter over a number of samples could indicate that the processor is not able to become active (for example, due to CPU saturation). | snapshot |
| `matches_found` | Number of messages found. | cumulative |
| `matches_found_per_sec` | Rate of messages found. | interval average |
| `matches_found_bytes` | Number of bytes matched. | cumulative |
| `matches_found_bytes_per_sec` | Rate of bytes matched for this processor. | interval average |
| `messages_received` | Number of messages received. | cumulative |
| `messages_received_per_sec` | Rate of messages received. | interval average |
| `messages_received_bytes` | Number of bytes received. | cumulative |
| `messages_received_bytes_per_sec` | Rate of bytes received for this processor. | interval average |
| `throttle_count` | Number of times the processor had to wait to add a message to the processing pipeline due to the instance reaching capacity limits on the number of in-progress messages. This metric can indicate resource constraints on AMPS. | cumulative |

AMPS also includes information for the following *processing types*, presented as an entry for a message processor with the given name:

| Processing Type | Description |
| --- | --- |
| `bookmark` | Messages from transaction log replays (bookmark subscriptions). |
| `detached` | Messages related to subscriptions that hold messages before delivering them (such as conflated subscriptions, aggregated subscriptions, and subscriptions that use pagination). |
| `external` | Messages to and from regular publish/subscribe subscriptions (that is, not SOW queries, message queue subscriptions, or transaction log replays). |
| `internal` | Messages internally generated by AMPS. |
| `queue` | Messages to and from message queues. |
| `replication` | Messages to and from replication destinations. |
| `sow` | Messages from queries of a SOW topic. |

**Statistics Database Tables**: `IPROCESSORS_STATIC`, `IPROCESSORS_DYNAMIC`

**Admin Path**: /amps/instance/processors/*<identifier>*/*<metric>*

#### queries (instance statistics)

The `queries` resource lists all available information regarding queries of SOW topics.

#### queued\_queries

A count of all queries which have not yet completed processing at the time the last statistics snapshot was recorded.

**Admin Path**: /amps/instance/queries/*<metric>*

#### queues (instance statistics)

The `queues` resource lists available information regarding the queues defined for this instance.

| Metric | Description | Type |
| --- | --- | --- |
| `age_of_oldest_lease` | The age of the oldest current lease, in seconds. | snapshot |
| `backlog` | The number of leased messages awaiting acknowledgment. | snapshot |
| `deferred_ack_count` | The number of acknowledgments received for messages that have not yet become active in the queue. | snapshot |
| `expired_leases` | The number of leases that have expired for this queue. This counter resets when the instance is restarted. | cumulative |
| `inactive_message_count` | The number of messages for this queue that are present in the transaction log, but have not yet become active because they are beyond current active limit of the queue (as set by `TargetQueueDepth`, when that option is present). | snapshot |
| `max_backlog` | The configured `MaxBacklog` for the queue. | fixed |
| `target_queue_depth` | The configured `TargetQueueDepth` for the queue. | fixed |
| `message_type` | The message type for the queue. | fixed |
| `queue_depth` | Total number of unacknowledged messages currently active in the queue. For queues that do not set a `MaxQueueDepth`, this is the total set of messages in the queue. For queues that set a `MaxQueueDepth`, this represents only the messages that are within the specified depth. Messages that are present, but not yet active are shown in the `inactive_message_count`. | snapshot |
| `seconds_behind` | Age of the oldest unacknowledged message in the queue. This counter resets when the instance is restarted. This statistic is measured in seconds, at the resolution of the system clock. | snapshot |
| `owned` | Number of messages currently owned by this instance of the queue. | snapshot |
| `proxied_transfer` | State of the proxied transfer setting. | fixed |
| `topic` | Name of the queue topic. | fixed |
| `transferred_in` | The number of messages originally published to another instance that have been transferred to this instance for delivery from this queue. | cumulative |
| `transferred_out` | The number of messages originally published to this instance that have been transferred to another instance for delivery from the replicated instance of this queue. | cumulative |

**Statistics Database Tables**: `IQUEUES_STATIC`, `IQUEUES_DYNAMIC`

**Admin Path**: /amps/instance/queues/*<identifier>*/*<metric>*

The `queues` resource also contains the following resource that produces information on the current live state of the queue. This information is produced directly from the internal state of AMPS, and is not recorded in the statistics database.

| Metric | Description | Type |
| --- | --- | --- |
| `details` | Detailed information about the state of the queue. This information is produced in JSON format, and includes detailed internal metrics for the queue as well as the current depth of the queue and information about the individual messages at the head of the queue (up to the first 1000 messages). This information is produced on demand from the current state, and is not produced from the statistics database. | live state |

The details element returns a document in JSON format that contains the following information:

| Metric | Description | Type |
| --- | --- | --- |
| `internal_depth` | The current amount of space the queue has reserved for metadata entries. If the queue has grown and then messages have been acknowledged, this can be larger than the current number of messages in the queue. | live state |
| `insert_count` | The number of messages inserted into the queue. This is the metric that will be recorded in the SOW metrics for the queue topic. | live state |
| `delete_count` | The number of messages removed from the queue due to being acknowledged or having expired. This is the metric that will be recorded in the SOW metrics for the queue topic. | live state |
| `depth` | Current number of unacknowledged messages in the queue. This is the metric that will be recorded in the SOW metrics for the queue topic. | live state |
| `last_queued_txid` | The local transaction ID of the last transaction processed for the queue. | live state |
| `last_acked_txid` | The local transaction ID of the last acknowledged point in the queue. | live state |
| `priority_count` | The number of distinct priority values for the queue. (This will be 0 if the queue does not have a priority expression configured.) | live state |
| `seconds_behind` | The point in the transaction log of the oldest message in the queue, in seconds, as measured by the time between the time the message was added to the local transaction log and the current time. | live state |
| `age_of_oldest_lease` | The amount of time, in seconds, that the oldest current lease has been held by a client. | live state |
| `backlog` | Number of messages currently leased from the queue. | live state |
| `expired_leases` | Number of leases that have expired from this queue. | cumulative live state |
| `locally_owned` | Number of messages owned by this instance. | live state |
| `transferred_in` | Number of messages that have had ownership transferred to this instance. | cumulative live state |
| `cursors` | Details of delivery cursors for this queue. | live state |
| `messages` | Details for messages currently in this queue. | live state |

The `cursors` element of the queue details contains the following information:

| Metric | Description | Type |
| --- | --- | --- |
| `cursor_id` | The internal ID of the cursor. | live state |
| `state` | Current state of the cursor. | live state |
| `last_processed_txid` | Last local transaction ID processed by this cursor. | live state |
| `last_delivered_txid` | Last local transaction ID delivered to a subscription from this cursor. | live state |
| `last_result` | Last result recorded by the cursor when evaluating a message for delivery to a subscriber. | live state |
| `processing_count` | Count of delivery evaluations by this cursor. | live state |
| `cursor_subscriptions` | Details for the subscriptions serviced by this cursor, including the client name, filter in use by the client, current and maximum backlog for the subscription, and so on. | live state |

The `messages` element of the queue details contains the following information:

| Metric | Description | Type |
| --- | --- | --- |
| `txid` | The local transaction ID of the message. | live state |
| `bookmark` | The bookmark of the message. | live state |
| `journal` | The path to the journal file that contains the message. | live state |
| `deliverable` | Flag indicating whether the message is currently deliverable (1 is true, 0 is false). A message may not be deliverable if this instance does not currently own the message, or if it is already leased to a subscriber. | live state |
| `age` | The age of the message, as measured by the time between the time the message was added to the local transaction log and the current time. | live state |
| `locally_owned` | Flag indicating whether the message is currently owned by this instance (1 is true, 0 is false). | live state |
| `priority` | Priority value of this message. | live state |
| `leased_to` | If the message is currently leased, the client name of the connection the message is leased to. This field is not present if the message is not currently leased. | live state |
| `leased_age` | If the message is currently leased, the amount of time, in seconds, the message has been leased. This field is not present if the message is not currently leased. | live state |

**Admin Path**: /amps/instance/queues/*<identifier>*/details

#### replication (instance statistics)

Selecting the `replication` resource will display a list of available downstream replication instances used by this instance of AMPS.

| Metric | Description | Type |
| --- | --- | --- |
| `authenticated_id` | The ID used to authenticate the connection to this instance of AMPS. | fixed |
| `bytes_out` | Number of bytes sent to this destination. | cumulative |
| `bytes_out_per_sec` | Rate of bytes sent. | interval average |
| `client_name` | The client name used for this destination. | fixed |
| `client_type` | Specifies whether client is a replication source or destination. | fixed |
| `connect_time` | Time connected to this destination. | snapshot |
| `destination_admin_addr` | The admin address of the destination. | fixed |
| `destination_group_name` | The group name of the destination. | fixed |
| `destination_name` | The name of the destination. | fixed |
| `disconnect_count` | Number of times replication destination has been disconnected. | cumulative |
| `disconnect_time` | Timestamp of the last time the replication destination disconnected. | snapshot |
| `is_connected` | Boolean telling whether replication destination is currently connected. | snapshot |
| `messages_out` | Number of messages sent to this destination. | cumulative |
| `messages_out_per_sec` | Rate of messages sent to this destination. | interval average |
| `name` | Name of replication configuration. | fixed |
| `pass_through` | Boolean stating whether messages received via replication can be forwarded on this connection. | fixed |
| `replication_type` | One of either `sync` or `async`. | snapshot |
| `seconds_behind` | The current point in the transaction log that has been acknowledged by this destination. This is calculated as the difference in seconds between the time that the last message acknowledged by the destination was written to the transaction log and the time that the most recent transaction was processed. That is, if the last message that the destination has acknowledged was written to the local transaction log at `12:00:01.100` (one second and 100 ms after 12:00) and the current time is `12:00:03.212`, the seconds behind shown in the current statistics would be approximately `2.112`. Acknowledgments are transmitted at a specific interval (1s by default) from the destination instance to the source instance. AMPS rounds any value below `1` to `0`. | snapshot |

**Statistics Database Tables**: `IREPLICATIONS_STATIC`, `IREPLICATIONS_DYNAMIC`

**Admin Path**: /amps/instance/replication/*<identifier>*/*<metric>*

The `replication` resource also provides options for managing replication instances:

| Element | Description |
| --- | --- |
| `downgrade` | Change the replication type of this connection from `sync` to `async`. |
| `reconnect` | Close and reopen the connection to the remote instance. |

#### sow (instance statistics)

Clicking the `sow` link will list all available topics in the SOW for this AMPS instance. Selecting a single topic will list the following available statistics:

| Element | Description | Type |
| --- | --- | --- |
| `delete_count` | Number of deletes processed by the SOW. | cumulative |
| `deletes_per_sec` | Number of deletes per second processed by the SOW. | interval average |
| `device` | Device the topic is stored on, if applicable. | fixed |
| `historical_granularity` | The granularity at which the SOW maintains history for this topic (if set). | fixed |
| `historical_window` | The window for which the SOW maintains history for this topic (if set). | fixed |
| `insert_count` | Count of the number of new records inserted into this topic. | cumulative |
| `inserts_per_sec` | Rate of inserts into this topic. | interval average |
| `memory_bytes` | The number of bytes of memory used for this topic. This metric includes: messages stored in the topic, metadata for the messages in the topic, and indices for the topic (both memo indices and hash indices). Notice that, particularly when the topic is heavily indexed, this can be larger than the message size. Indices are not persisted, so this can also be larger than the space used for persisting the topic data. | snapshot |
| `mmaps` | The number of memory maps used for this topic in the SOW. | snapshot |
| `message_type` | Message type for this topic. | fixed |
| `path` | File system location of the SOW topics file store. | fixed |
| `queries_per_sec` | Rate of queries for this SOW topic. | interval average |
| `query_count` | Number of queries processed for this topic. | cumulative |
| `record_size` | Record size for the topic in the SOW. For SOW files created with current versions of AMPS, this will always return the same value. | fixed |
| `resident_percent` | Percentage of the storage of this topic that is currently resident in memory. Messages that are not currently in memory will need to be retrieved from storage before they are delivered. When part of a topic is not resident, either the instance is under memory pressure or the messages (if any) in that part of the topic have not been recently updated or delivered. | snapshot |
| `slab_count` | The total number of slabs allocated for this SOW topic. | snapshot |
| `slab_size` | The slab size for this SOW topic. | fixed |
| `stored_bytes` | Number of bytes stored for this topic. | snapshot |
| `topic` | Name of this SOW topic. | fixed |
| `update_count` | Number of updates to existing records processed by this topic. | cumulative |
| `updates_per_sec` | Number of updates to existing records per second. | interval average |
| `valid_keys` | Number of distinct messages in the SOW - defined by the SOW topic key. For topics that maintain a history, this shows the total number of messages that the SOW maintains, which may be larger than the number of messages that would be returned by a query at the current time, or the number of messages that would be returned by a query at a historical point in time. | snapshot |

**Statistics Database Tables**: `ISOW_STATIC`, `ISOW_DYNAMIC`

**Admin Path**: /amps/instance/sow/*<identifier>*/*<metric>*

Sample `amps-sqlite3` query:

```sql
SELECT iso8601_local(timestamp), topic, deletes_per_sec, inserts_per_sec, updates_per_sec, valid_keys
 FROM ISOW
 GROUP BY topic
 ORDER BY topic, timestamp
```

This query shows the overall activity and number of records available for each SOW topic in the instance. The activity fields are averaged over the sample interval, while the number of valid keys is a snapshot at each sample.

#### statistics (instance statistics)

The `statistics` resource contains information regarding how AMPS monitors its own statistics.

| Element | Description | Type |
| --- | --- | --- |
| `disk_per_sample` | Amount of storage the stats database has grown since the last sample interval. | snapshot |
| `file_name` | Location where statistics are stored. Default: `:memory:` which stores the statistics database in system memory. | fixed |
| `file_size` | Size on disk of the statistics database. | snapshot |
| `interval` | Time in milliseconds between statistics database updates. | fixed |
| `memory_used` | Size in bytes of the system memory consumption of the statistics database. | snapshot |
| `queries` | Number of queries processed from the statistics database. | cumulative |
| `time_per_sample` | Time taken to process each statistics database query. | snapshot |
| `total_commit_time` | Total amount of time spent committing statistics information to the database. | cumulative |
| `total_samples` | Number of statistics database updates that have taken place since the AMPS server started. | cumulative |
| `total_time` | Total amount of time spent publishing statistics, including the commit time, since the AMPS server started. | cumulative |

**Statistics Database Tables**: `ISTATISTICS_STATIC`, `ISTATISTICS_DYNAMIC`

**Admin Path**: /amps/instance/statistics/*<metric>*

#### subscriptions (instance statistics)

Each client that submits a `subscribe` command message is tracked by AMPS, and their relevant metrics are captured in the monitoring instance database.

| Metric | Description | Type |
| --- | --- | --- |
| `backlog` | The current number of messages leased on this subscription. Applies to subscriptions to a queue. | snapshot |
| `bookmark` | The bookmark the client provided when the subscription was entered, if any. | fixed |
| `client_id` | The ID of the subscribing client. | fixed |
| `cursor_id` | For bookmark subscriptions, the ID of the cursor replaying messages in the transaction log. | snapshot |
| `entitlement_filter` | The filter applied to this subscription by the entitlement module, if any. (Since a regular expression subscription can have multiple entitlement filters, one for each matching topic, this is blank for subscriptions that use a regular expression for the topic name.) | fixed |
| `filter` | The filter requested on the subscription, if any. | fixed |
| `message_type` | Message type for the subscription message. Message type for a subscription is established when the client connects to a Transport. It will be the `MessageType` of the Transport or the message type supplied in the URI in the case the Transport doesn't specify a `MessageType`. | fixed |
| `options` | The options string for the subscription. | fixed |
| `pagination_id` | For subscriptions that use pagination, the identifier of the paginated set for this subscription. | fixed |
| `seconds_behind` | For bookmark subscriptions, the age of the last message enqueued for the client. This indicates the point in the transaction log at which replay is currently happening, and does not necessarily correspond to the rate at which the client is receiving messages or the amount of time required for the client to complete replay. | snapshot |
| `sub_id` | The subscription ID for this subscription. | fixed |
| `topic` | Subscription topic. | fixed |

**Statistics Database Tables**: `ISUBSCRIPTIONS_STATIC`, `ISUBSCRIPTIONS_DYNAMIC`

**Admin Path**: /amps/instance/subscriptions/*<id>*/*<metric>*

#### timestamp (instance statistics)

The timestamps of the historical admin statistics intervals as recorded by AMPS. The interval between these timestamps is determined by the `Interval` configured in the Admin Server and Statistics block in the configuration. These values can be used to ensure valid results are returned from a Time Range Selection.

All times used for the report generation and presentation are ISO-8601 formatted: `YYYYMMDDThhmmss`, where `YYYY` is the year, `MM` is the month, `DD` is the year, `T` is a separator between the date and time, `hh` is the hours, `mm` is the minutes and `ss` is the seconds. Decimals are permitted after the `ss` units. All times are stored and returned in UTC time.

**Admin Path**: /amps/instance/timestamp

#### transaction_log (instance statistics)

| Metric | Description | Type |
| --- | --- | --- |
| `journals` | A list of all journal file names. | snapshot |
| `max_timestamp` | The largest timestamp in the transaction log. | snapshot |
| `min_timestamp` | The smallest timestamp in the transaction log. | snapshot |
| `write_latency` | Statistics covering the latency of writes to the transaction log. | snapshot |
| `write_size` | Statistics covering the size of writes to the transaction log. | snapshot |

**Statistics Database Tables**: `ITRANSACTION_LOG_DYNAMIC`, `ITRANSACTION_LOG_STATIC`, `ITRANSACTION_WRITE_LATENCY_DYNAMIC`, `ITRANSACTION_LOG_WRITE_LATENCY_STATIC`, `ITRANSACTION_WRITE_SIZE_DYNAMIC`, `ITRANSACTION_LOG_WRITE_SIZE_STATIC`

**Admin Path**: /amps/instance/transaction\_log/*<metric>*

Selecting the `journals` resource will list all journal file names. Selecting a single journal will show the following details:

| Metric | Description | Type |
| --- | --- | --- |
| `file_name` | The file name of the selected journal. | fixed |
| `min_timestamp` | The smallest timestamp of the journal. | fixed |
| `max_timestamp` | The largest timestamp of the journal. | fixed |
| `is_archived` | Whether or not the journal is archived. | snapshot |
| `is_compressed` | Whether or not the journal is compressed. | snapshot |

**Admin Path**: /amps/instance/transaction\_log/journals/*<id>*/*<metric>*

The `write_latency` and `write_size` metrics contain the following details:

| Metric | Description | Type |
| --- | --- | --- |
| `histogram` | An ASCII histogram of the monitored statistic. | snapshot |
| `minimum` | The lowest observed sample of the statistic, in microseconds or bytes. | snapshot |
| `maximum` | The largest observed sample of the statistic, in microseconds or bytes. | snapshot |

#### transports (instance statistics)

Clicking the `transports` link will give a list of the transports defined in the configuration file for the AMPS instance.

| Metric | Description | Type |
| --- | --- | --- |
| `is_enabled` | Indicates whether the transport is enabled. | snapshot |
| `message_type` | The message type for this transport. | fixed |
| `name` | The name of this transport. | fixed |
| `options` | The options provided for this transport. | fixed |
| `type` | The type of transport. | fixed |

**Statistics Database Tables**: `ITRANSPORTS_STATIC`, `ITRANSPORTS_DYNAMIC`

**Admin Path**: /amps/instance/transports/*<id>*/*<metric>*

#### tuning (instance statistics)

Clicking the `tuning` link will give a list of the tuning parameters for the instance.

| Metric | Description | Type |
| --- | --- | --- |
| `NUMA` | Indicates whether AMPS NUMA tuning is enabled. | fixed |

**Admin Path**: /amps/instance/tuning/*<metric>*

#### uptime (instance statistics)

The length of time that the AMPS instance has been running, which conforms to a `hh:mm:ss.uuuuuu` format.

| Element | Description | Type |
| --- | --- | --- |
| `hh` | Hours | snapshot |
| `mm` | Minutes | snapshot |
| `ss` | Seconds | snapshot |
| `uuuuuu` | Microseconds | snapshot |

**Admin Path**: /amps/instance/uptime

#### user_id (instance statistics)

**Admin Path**: /amps/instance/user\_id

#### version (instance statistics)

Version string of the current running instance of AMPS.

**Admin Path**: /amps/instance/version

#### views (instance statistics)

The `views` resource contains information about the views in the AMPS instance. AMPS also collects SOW statistics for views. These are available from the `sow` resource, with the name of the `view` as the topic name.

| Element | Description | Type |
| --- | --- | --- |
| `conflation` | The inline conflation mode of the view. | fixed |
| `conflation_ratio` | The ratio of incoming to conflated updates. (If there are no updates, this is not calculated and shows as 0.0.) | snapshot |
| `grouping` | List of one or more fields, which are used to determine message aggregation. | fixed |
| `message_type` | The message type of messages produced by this view. | fixed |
| `projection` | The formula defined in the AMPS config for the computed transformation of one or more fields onto a new field. | fixed |
| `queue_depth` | The number of updates to the view that are pending, but have not yet been applied. | snapshot |
| `topic` | The name of the new AMPS topic created by this view. | fixed |
| `underlying_topic` | The source topic used to compute the projected view. | fixed |

**Admin Path**: /amps/instance/views/*<id>*/*<metric>*

**Statistics Database Tables**: `IVIEWS_STATIC`, `IVIEWS_DYNAMIC`

**Additional topic information**: `ISOW_STATIC`, `ISOW_DYNAMIC`

Sample `amps-sqlite3` query:

```sql
SELECT iso8601_local(v.timestamp), v.topic, v.queue_depth, v.conflation_ratio,
       s.updates_per_sec, s.inserts_per_sec, s.deletes_per_sec
FROM IVIEWS AS v
JOIN ISOW AS s ON v.oid = s.oid AND v.timestamp=s.timestamp
WHERE (v.queue_depth + s.updates_per_sec + s.inserts_per_sec + s.deletes_per_sec) > 0
ORDER BY v.oid, v.timestamp asc
```

This query shows information about changes to the views in the instance. The query joins information from the ISOW record for the topics using matching `oid` and `timestamp` fields to show the inserts, updates, and deletes to the View (per second) for each sample. The query includes the pending updates (queue depth) at each sample. The `WHERE` clause only includes samples where there is activity to the view, to avoid showing samples where the contents of the queue are not changing.

### Statistics Types

AMPS collects statistics in several different ways. Understanding how each number is collected and calculated is important for accurately interpreting the statistic.

| Type | Description |
| --- | --- |
| Cumulative | Statistics are cumulative since the instance was started (for example, the number of bytes AMPS has sent over a given network interface since the instance started). |
| Fixed | Information that is fixed for the lifetime of the instance (for example, the process ID of the AMPS server). |
| Snapshot | The value of a metric at a specific point in time (for example, the number of active subscriptions for a client at a particular moment). |
| Interval Average | Average computed for this statistics interval (for example, the number of bytes sent per second on a given network interface for the last statistics interval). |
| Running Average | Average computed since the instance was started (for example, the conflation ratio for a conflated topic). |

### Table Reference

This section lists the statistics database tables that are related to each performance metric. Static properties are stored in a `STATIC` table, while statistics captured at each interval are stored in a `DYNAMIC` table. The `amps-sqlite3` script automatically handles the join from `STATIC` tables to `DYNAMIC` tables. For queries that use other tools, include a join between the corresponding `STATIC` and `DYNAMIC` table `static_id` fields.

**Host Metrics**

| Metric Category | Base Table Names |
| --- | --- |
| [cpu](/docs/amps-monitoring-guide/host-interface/cpu) (host level) | `HCPUS` (instance info in `ICPUS`) |
| [disk capacity and activity](/docs/amps-monitoring-guide/host-interface/disks) | `HDISKS` |
| [memory](/docs/amps-monitoring-guide/host-interface/memory) (host level) | `HMEMORY` (instance info in `IMEMORY`) |
| [network activity](/docs/amps-monitoring-guide/host-interface/network) | `HNET` |

**Instance Metrics**

| Metric Category | Base Table Names |
| --- | --- |
| [api](/docs/amps-monitoring-guide/instance-interface/api) (embedded client) | `IGLOBALS` |
| [clients](/docs/amps-monitoring-guide/instance-interface/clients) | `ICLIENTS` |
| [conflated topics](/docs/amps-monitoring-guide/instance-interface/conflated_topics) | `ICONFLATEDTOPICS` (info also in `ISOW`) |
| [cpu](/docs/amps-monitoring-guide/instance-interface/cpu) (instance) | `ICPUS` (host level info in `HCPU`) |
| [logging](/docs/amps-monitoring-guide/instance-interface/logging) | `ICONSOLE_LOGGERS`, `IFILE_LOGGERS`, `ISYSLOG_LOGGERS` |
| [memory](/docs/amps-monitoring-guide/instance-interface/memory) (instance) | `IMEMORY`, `IMEMORY_CACHES`, `IPAGINATIONS` |
| [message processors](/docs/amps-monitoring-guide/instance-interface/processors) | `IPROCESSORS` |
| [queues](/docs/amps-monitoring-guide/instance-interface/queues) | `IQUEUES` (info also in `ISOW`) |
| [replication](/docs/amps-monitoring-guide/instance-interface/replication) | `IREPLICATIONS` |
| [sow](/docs/amps-monitoring-guide/instance-interface/sow) (including information on views, conflated topics, queues) | `ISOW` |
| [statistics](/docs/amps-monitoring-guide/instance-interface/statistics) | `ISTATISTICS` |
| [subscriptions](/docs/amps-monitoring-guide/instance-interface/subscriptions) | `ISUBSCRIPTIONS` |
| [transaction log](/docs/amps-monitoring-guide/instance-interface/transaction_log) | `ITRANSACTION_LOG`, `ITRANSACTION_WRITE_LATENCY`, `ITRANSACTION_WRITE_SIZE` |
| [transports](/docs/amps-monitoring-guide/instance-interface/transports) | `ITRANSPORTS` |
| [views](/docs/amps-monitoring-guide/instance-interface/views) | `IVIEWS` |


## AMPS Command Reference

Commands and options are protocol-independent. Applications set properties on a `Command` object, then call `execute()` or `executeAsync()`. Client libraries handle formatting, heartbeating, and acknowledgment.

### Commands to AMPS

Commands can be sent via named methods (e.g., `subscribe()`) or by creating a `Command` object, setting parameters, and calling `execute()`/`executeAsync()`.

### flush command

Returns acknowledgment when all previous commands from this client have been processed. The `publishFlush` client method includes additional logic when a `PublishStore` is present.

### flush command: Header Fields

| Field | Description |
| --- | --- |
| `cmd` | Value: `flush` |
| `client_name` | Unique client ID string. |
| `ack_type` | Comma-separated: `none`, `completed`, `processed`. |

### flush command: Returns

| Acknowledgment | Description |
| --- | --- |
| `none` | Default. No `ack` returned. |
| `completed` | All previous commands processed. |
| `persisted` | All previous commands persisted (or failed with requested acks). |
| `processed` | `flush` message processed. |
| `received` | `flush` received. |
| `stats` | Not supported. |

### heartbeat command

Starts/refreshes a heartbeat timer. AMPS sends periodic heartbeat messages and disconnects clients that don't respond within the interval. Client libraries manage this automatically.

### heartbeat command: Header Fields

| Field | Description |
| --- | --- |
| `cmd` | Value: `heartbeat` |
| `opts` | `start,<interval>` to start timer (e.g., `start,5` for 5s), or `beat` to refresh. |

### heartbeat command: Returns

| Acknowledgment | Description |
| --- | --- |
| `none` | Not supported. |
| `completed` | Not supported. |
| `parsed` | Not supported. |
| `persisted` | Not supported. |
| `processed` | `heartbeat` processed. |
| `received` | `heartbeat` received. |
| `stats` | Not supported. |

### logon command

Required as first command on a new connection (one per connection). AMPS performs an implicit `logon` if not sent first. With authentication enabled, must include `username`/`password`. `ClientName` must be unique across instances sharing a transaction log; same `ClientName` + same user = reconnection (existing connection dropped); same `ClientName` + different user = failure. Recommend requesting `processed` acknowledgment. The `websocket` protocol uses a different mechanism.

### logon command: Header Fields

| Field | Description |
| --- | --- |
| `cmd` | Value: `logon` |
| `client_name` | Unique ID. Must be unique across replicated instances. No control chars, newlines, or square brackets. |
| `ack_type` | Comma-separated: `none`, `received`, `processed`. |
| `message_type` | Required if transport accepts any message type. |
| `user_id` | Username for authentication. |
| `password` | Password for authentication. |
| `correlation_id` | User string included in log and admin interface. Base64-legal characters only. |
| `version` | Client library version, logged but does not affect connection. |

### logon command: Returns

| Acknowledgment | Description |
| --- | --- |
| `none` | Default. No `ack` returned. |
| `completed` | Not supported. |
| `persisted` | Not supported. |
| `processed` | `logon` processed. Indicates successful authentication. |
| `received` | `logon` received. Response includes `ClientName`. |
| `stats` | Not supported. |

#### logon command: Options Field

| Option | Description |
| --- | --- |
| `none` | Default. |
| `ack_conflation=interval` | Interval for conflating `persisted` acks. Default: `1s`. |
| `pretty` | `true` returns formatted binary message contents. |

### Publishing to AMPS

For SOW topics: inserts new messages or updates existing ones. For transaction log topics, each publish/delta_publish stores the message as delivered to subscribers.

| Command | Usage |
| --- | --- |
| `publish` | Send a message to AMPS. |
| `delta_publish` | Send partial update; non-NULL fields overwrite SOW record, NULL fields ignored. On non-SOW topics, behaves like `publish`. |

### delta_publish command

Sends incremental update to a SOW record. AMPS extracts key fields, looks up the record, overwrites non-key fields with values from the update, and appends new fields. Behaves like `publish` if record doesn't exist or topic has no SOW store. Transparent to other clients.

#### delta_publish command: Header Fields

| Field | Description |
| --- | --- |
| `cmd` | Value: `delta_publish` |
| `topic` | SOW topic to publish to. |
| `ack_type` | Comma-separated: `none`, `received`, `processed`, `completed`, `stats`. |
| `cmd_id` | Included in non-conflated `ack` responses. |
| `expiration` | Lifetime in seconds. |
| `seq` | Monotonically increasing ID for HA environments. |
| `correlation_id` | User string passed verbatim to subscribers. Base64-legal characters. |
| `sow_key` | Explicit SOW key for topics requiring one. Base64-legal characters. |

#### delta_publish command: Returns

Recommend only requesting `persisted` acks (can be conflated).

| Acknowledgment | Description |
| --- | --- |
| `none` | Default. |
| `completed` | Message processed and enqueued for persistence/replication. Includes `processed` steps. |
| `persisted` | 1) All downstream sync replications acknowledged delivery. 2) Sent to all async replications. No replication = local persistence only. No persistence = still provided. May conflate: `seq` means that message and all previous acknowledged. |
| `processed` | Message processed to SOW (entitlement checks, parsing). Errors returned here. Does not guarantee persistence/replication. |
| `received` | Message received. |
| `stats` | Not supported. |

#### delta_publish command: Errors

Errors in `processed` ack status and log. Post-processing errors in `persisted` ack status if requested. AMPS may conflate successful `persisted` acks.

### publish command

Primary way to insert messages into AMPS. Messages forwarded to matching subscribers, and may update SOW, write to transaction log, replicate, etc.

### publish command: Header Fields

| Field | Description |
| --- | --- |
| `cmd` | Value: `publish` |
| `topic` | Topic to publish to. |
| `ack_type` | Comma-separated: `none`, `received`, `persisted`, `processed`. |
| `cmd_id` | Included in `ack` responses. |
| `expiration` | Lifetime in seconds. |
| `seq` | Monotonically increasing ID for HA. |
| `correlation_id` | User string passed verbatim to subscribers. Base64-legal characters. |
| `sow_key` | Explicit SOW key. Base64-legal characters. |

### publish command: Returns

`processed` ack available but has significant performance overhead. Recommend only `persisted` acks (can be conflated).

| Acknowledgment | Description |
| --- | --- |
| `none` | Default. |
| `completed` | Message processed and enqueued for persistence/replication. Includes `processed` steps. |
| `persisted` | Same guarantees as `delta_publish persisted`. May conflate: `seq` means that message and all previous acknowledged. |
| `processed` | Message processed (entitlement checks, parsing). Errors returned here. No persistence/replication guarantee. |
| `received` | Message received. |
| `stats` | Not supported. |

### publish command: Errors

Same as `delta_publish`: errors in `processed` ack status and log. Post-processing errors in `persisted` ack if requested.

### Subscribing to and Querying Topics

| Command | Usage |
| --- | --- |
| `subscribe` | Stream of messages from a topic. |
| `sow` | Snapshot query of SOW topic. |
| `sow_and_subscribe` | SOW query + subscription atomically. |
| `delta_subscribe` | Stream with only changed fields (SOW topics); behaves as `subscribe` otherwise. |
| `sow_and_delta_subscribe` | SOW query + delta subscription atomically. |

### delta_subscribe command

Like `subscribe` but receives only changed fields for SOW topics. Behaves as `subscribe` on non-SOW topics.

#### delta_subscribe command: Header Fields

| Field | Description |
| --- | --- |
| `cmd` | Value: `delta_subscribe` |
| `topic` | Topic to subscribe to. |
| `ack_type` | Comma-separated: `none`, `received`, `processed`, `completed`, `stats`. |
| `cmd_id` | Included in `ack` responses. |
| `data_only` | Boolean. `true` excludes envelopes (e.g., SOAP). |
| `filter` | Content filter expression. XML: wrap in `CDATA`. |
| `opts` | See Options Field below. |
| `send_empty` | Boolean. Forward empty publishes. Default: `true`. |
| `send_matching_ids` | Boolean. Send subscription IDs with matched messages. |
| `sub_id` | Subscription ID. Used for new subs, `replace`, `pause`, `resume`. Auto-generated if not provided. |

##### delta_subscribe command: Options Field

| Option | Description |
| --- | --- |
| `none` | Default. |
| `bookmark` | Return bookmarks on each publish if topic is in transaction log. Does not set starting point (use `Bookmark` header). Not required for bookmark subscriptions (always included). |
| `conflation=n` | Time interval, `auto`, or `none`. Enables conflation. `auto` = AMPS determines interval. Time specifiers: `100ms`, `1s`, `1m`. Default: `none`. |
| `conflation_key=[key]` | Comma-delimited XPath list in brackets for message uniqueness. Defaults to SOW key fields for SOW topics. Required for non-SOW topics. Not valid with `oof` unless keys match topic keys. |
| `live` | Send messages before persisted to transaction log. Only applies to bookmark subscriptions. |
| `max_backlog=n` | Queue: max unacknowledged messages to accept at once. |
| `no_empties` | Suppress empty publish messages. |
| `no_sowkey` | Don't send AMPS-generated `SowKey`. |
| `non_regex_topic` | Literal topic name match. |
| `oof` | Send OOF messages for records out of focus. With focus tracking, also sends full message when record comes into focus. |
| `pause` | Pause bookmark subscription (not `live`). Uses `SubId`. |
| `rate=n` | Max delivery rate for bookmark subscription (not `live`). Formats: `1000` (msgs/sec), `100KB` (bytes/sec), `1.5X` (multiplier). |
| `replace` | Replace subscription at `SubId`. |
| `resume` | Resume bookmark subscription (not `live`). Uses `SubId`. |
| `send_keys` | Send SOW key fields with messages. |
| `timestamp` | Include processing timestamp header. |

##### delta_subscribe command: Returns

| Command | Description |
| --- | --- |
| `publish` | Published message. |
| `oof` | Out-of-focus notification (when `oof` option set, SOW topic, non-bookmark). |
| `ack` | Acknowledgments. |

| Acknowledgment | Description |
| --- | --- |
| `none` | Default. |
| `completed` | Bookmark replay complete. Further messages are new publishes. |
| `persisted` | Most recent bookmark in server's transaction log. |
| `processed` | Filters compiled. |
| `received` | Message received. |
| `stats` | Returns `Matches`, `TopicMatches`, `RecordsReturned`. |

##### delta_subscribe command: Errors

Errors in `processed` ack `Status` field and log. Only returned if `processed` in `AckType`.

### sow command

Query SOW topic contents (including views, queues, conflated topics). Can use filters.

### sow command: Header Fields

| Field | Description |
| --- | --- |
| `cmd` | Value: `sow` |
| `topic` | SOW topic to query. |
| `ack_type` | Comma-separated: `none`, `received`, `processed`, `completed`, `stats`. |
| `batch_size` | Records per result message. Server default: 1. Client default: 10. Only applies to sow query. |
| `bookmark` | Historical SOW state bookmark. Ignored if historical query not enabled. |
| `cmd_id` | Included in `ack` responses. |
| `filter` | Content filter expression. |
| `order_by` | Comma-delimited identifiers with optional `ASC`/`DESC`. |
| `query_id` | Unique response identifier. |
| `sow_keys` | Comma-delimited SowKeys to return. |
| `top_n` | Max messages to return. |

### sow command: Options Field

| Option | Description |
| --- | --- |
| `none` | Default. |
| `no_sowkey` | Don't send AMPS-generated `SowKey`. |
| `grouping=[keys]` | Aggregated queries. Comma-delimited XPath list in brackets. Must cover all aggregated fields. Requires `projection`. Usable with bookmark for historical aggregation. |
| `oof` | Send OOF for out-of-focus records. |
| `projection=[fields]` | Aggregated queries. Comma-delimited field projections in brackets. Requires `grouping`. Max 64KB. Usable with bookmark for historical aggregation. |
| `replace` | Replace subscription at `SubId`. Runs SOW query for new sub. |
| `skip_n=n` | Skip N records. Requires `top_n` and `OrderBy`. |
| `top_n=n` | Max records to return. |
| `select=[fields]` | Fields to include in messages. Comma-delimited inclusion specifiers. |
| `send_keys` | Send SOW key data fields with messages. |
| `timestamp` | Include processing timestamp header. |

### sow command: Returns

Results in `group_begin` → sow records → `group_end` sequence.

| Acknowledgment | Description |
| --- | --- |
| `none` | Default. |
| `completed` | Query completed. |
| `persisted` | Not supported. |
| `processed` | Filters compiled. |
| `received` | Command received. |
| `stats` | Returns `Matches`, `TopicMatches`, `RecordsReturned`. |

- **TopicMatches**: Total records compared across matching SOW topics.
- **Matches**: Records matching topic regex + filter. Can exceed `RecordsReturned` when limited by `TopN`.
- **RecordsReturned**: Total records returned to client, limited by `TopN`.

### sow command: Errors

Errors in `processed` ack `Status`/`Reason` fields. Record ordering undefined without `OrderBy`.

### sow_and_delta_subscribe command

Combines `sow` + `delta_subscribe`. Queries SOW, then subscribes with only changed fields. Behaves as `sow_and_subscribe` on non-SOW topics.

#### sow_and_delta_subscribe command: Header Fields

| Field | Description |
| --- | --- |
| `cmd` | Value: `sow_and_delta_subscribe` |
| `topic` | Target SOW topic. |
| `ack_type` | Comma-separated: `none`, `received`, `processed`, `completed`, `stats`. |
| `batch_size` | Records per result message. Default: 1. |
| `cmd_id` | Included in `ack` responses. |
| `data_only` | Boolean. `true` excludes envelopes. |
| `filter` | Content filter expression. |
| `opts` | See Options Field below. |
| `orderby` | Comma-delimited identifiers with optional `ASC`/`DESC`. |
| `query_id` | SOW query identifier, added to all responses. |
| `send_empty` | Forward empty publishes. Default: `true`. |
| `send_oof` | Send OOF messages. Default: `false`. |
| `send_keys` | Receive `SowKey` back. |
| `send_matching_ids` | Send subscription IDs with matched messages. |
| `sow_keys` | Comma-delimited SowKeys to return. |
| `sub_id` | Subscription ID. Used for new subs, `replace`, `pause`, `resume`. Auto-generated if not provided. |
| `top_n` | Max messages from SOW query. |

#### sow_and_delta_subscribe command: Options Field

| Option | Description |
| --- | --- |
| `none` | Default. |
| `bookmark` | Return bookmarks on each publish (requires transaction log). Does not set starting point. Not required for bookmark subscriptions. |
| `conflation=n` | Time interval, `auto`, or `none`. Default: `none`. |
| `conflation_key=[key]` | XPath list in brackets for uniqueness. Defaults to SOW keys. Required for non-SOW topics. Not valid with `oof` unless keys match topic keys. |
| `grouping=[keys]` | Aggregated subscriptions. Requires `projection`. Cannot use with bookmark. |
| `live` | Send messages before persisted to transaction log. |
| `no_empties` | Suppress empty publishes. |
| `no_sowkey` | Don't send `SowKey`. |
| `non_regex_topic` | Literal topic name match. |
| `oof` | Send OOF for out-of-focus records. |
| `projection=[fields]` | Aggregated subscriptions. Requires `grouping`. Cannot use with bookmark. Max 64KB. |
| `replace` | Replace subscription at `SubId`. |
| `top_n=n` | Max records from result set (uses `OrderBy`). Equivalent to `TopN` header. |
| `skip_n=n` | Skip N records. Used with `top_n` for pagination. |
| `send_keys` | Send SOW key data fields with messages. |
| `select=[fields]` | Fields to include. Comma-delimited inclusion specifiers. |
| `timestamp` | Include processing timestamp header. |

#### sow_and_delta_subscribe command: Returns

| Command | Description |
| --- | --- |
| `group_begin` | SOW query results start. |
| `group_end` | SOW query results end. |
| `sow` | SOW query record. |
| `publish` | New message/update. |
| `oof` | Out-of-focus notification (`oof` option, SOW topic, non-bookmark). |
| `ack` | Acknowledgments. |

| Acknowledgment | Description |
| --- | --- |
| `none` | Default. |
| `completed` | SOW portion complete; future messages from publishes. |
| `persisted` | Not supported. |
| `processed` | Filters compiled. |
| `received` | Command received. |
| `stats` | Returns `Matches`, `TopicMatches`, `RecordsReturned`. |

- **TopicMatches**: Total records compared across matching SOW topics.
- **Matches**: Records matching topic regex + filter. Can exceed `RecordsReturned` when limited by `TopN`.
- **RecordsReturned**: Total records returned, limited by `TopN`.

#### sow_and_delta_subscribe command: Errors

Errors in `Status` field if `AckType` defined, or in AMPS log.

### sow_and_subscribe command

Combines `sow` + `subscribe`. Queries SOW, then subscribes with full messages.

#### sow_and_subscribe command: Header Fields

| Field | Description |
| --- | --- |
| `cmd` | Value: `sow_and_subscribe` |
| `topic` | Target SOW topic. |
| `ack_type` | Comma-separated: `none`, `received`, `processed`, `completed`, `stats`. |
| `batch_size` | Records per result message. Server default: 1. Client default: 10. Only applies to sow query. |
| `bookmark` | Historical SOW state bookmark. If topic has transaction log, returns SOW state at bookmark and starts bookmark subscription immediately after that state. Invalid bookmark for non-historical topics: only `NOW` valid. |
| `cmd_id` | Included in `ack` responses. |
| `data_only` | Boolean. `true` excludes envelopes. |
| `filter` | Content filter expression. |
| `opts` | See Options Field below. |
| `orderby` | Comma-delimited identifiers with optional `ASC`/`DESC`. |
| `query_id` | SOW query identifier, added to all responses. |
| `send_oof` | Send OOF messages. Default: `false`. |
| `send_keys` | Receive `SowKey` back. |
| `send_matching_ids` | Send subscription IDs with matched messages. |
| `sow_keys` | Comma-delimited SowKeys to return. |
| `sub_id` | Subscription ID. Used for new subs, `replace`, `pause`, `resume`. Auto-generated if not provided. |
| `top_n` | Max messages from SOW query. |

#### sow_and_subscribe command: Returns

| Command | Description |
| --- | --- |
| `group_begin` | SOW query results start. |
| `group_end` | SOW query results end. |
| `sow` | SOW query record. |
| `publish` | New message/update. |
| `oof` | Out-of-focus notification (`oof` option, SOW topic, non-bookmark). |
| `ack` | Acknowledgments. |

| Acknowledgment | Description |
| --- | --- |
| `none` | Default. |
| `bookmark` | Return bookmarks on each publish (requires transaction log). Does not set starting point. |
| `completed` | SOW portion complete; future messages from publishes. |
| `persisted` | Not supported. |
| `processed` | Subscription registered, SOW query started. |
| `received` | Command received. |
| `stats` | Returns `Matches`, `TopicMatches`, `RecordsReturned`. |

- **TopicMatches**: Total records compared across matching SOW topics.
- **Matches**: Records matching topic regex + filter. Can exceed `RecordsReturned` when limited by `TopN`.
- **RecordsReturned**: Total records returned, limited by `TopN`.

#### sow_and_subscribe command: Options Field

| Option | Description |
| --- | --- |
| `none` | Default. |
| `conflation=n` | Time interval, `auto`, or `none`. Default: `none`. |
| `conflation_key=[keys]` | XPath list in brackets for uniqueness. Defaults to SOW keys. Required for non-SOW topics. Not valid with `oof` unless keys match topic keys. |
| `grouping=[keys]` | Aggregated subscriptions. Requires `projection`. Cannot use with bookmark. |
| `live` | Send messages before persisted. Only for bookmark subscriptions. |
| `no_sowkey` | Don't send `SowKey`. |
| `non_regex_topic` | Literal topic name match. |
| `oof` | Send OOF for out-of-focus records. |
| `pause` | Pause bookmark subscription (not `live`). Uses `SubId`. |
| `projection=[fields]` | Aggregated subscriptions. Requires `grouping`. Cannot use with bookmark. Max 64KB. |
| `rate=n` | Max delivery rate for bookmark subscription (not `live`). Formats: `1000`, `100KB`, `1.5X`. |
| `replace` | Replace subscription at `SubId`. Runs SOW query for new sub. |
| `resume` | Resume bookmark subscription (not `live`). Uses `SubId`. |
| `top_n=n` | Max records from result set (uses `OrderBy`). Equivalent to `TopN` header. |
| `skip_n=n` | Skip N records. Used with `top_n` for pagination. |
| `send_keys` | Send SOW key data fields with messages. |
| `select=[fields]` | Fields to include. Comma-delimited inclusion specifiers. |
| `timestamp` | Include processing timestamp header. |

#### sow_and_subscribe command: Errors

Errors in `Status` field if `AckType` defined. Also logged.

### subscribe command

Primary way to retrieve messages. Supports content filtering.

### subscribe command: Header Fields

| Field | Description |
| --- | --- |
| `cmd` | Value: `subscribe` |
| `topic` | Topic to subscribe to. |
| `ack_type` | Comma-separated: `none`, `received`, `processed`, `completed`. |
| `bookmark` | Starting point in transaction log. Single or comma-delimited list (uses earliest). Non-transaction-log topic: no replay. |
| `cmd_id` | Included in `ack` responses. |
| `data_only` | Boolean. `true` excludes envelopes. |
| `filter` | CDATA-wrapped content filter expression. |
| `opts` | See Options Field below. |
| `send_matching_ids` | Boolean. Send subscription IDs with matched messages. |
| `sub_id` | Subscription ID. Used for new subs, `replace`, `pause`, `resume`. Auto-generated if not provided. |
| `top_n` | Max messages from bookmark subscription only. Invalid without bookmark. |

### subscribe command: Options Field

| Option | Description |
| --- | --- |
| `none` | Default. |
| `bookmark` | Return bookmarks on each publish (requires transaction log). Does not set starting point. Not required for bookmark subscriptions. |
| `bookmark_not_found` | Behavior when bookmark not found: `epoch` (start of log), `now` (end of log), `fail` (error). Default: `now`. Only for bookmark replays. |
| `conflation=n` | Time interval, `auto`, or `none`. Default: `none`. |
| `conflation_key=[keys]` | XPath list in brackets for uniqueness. Defaults to SOW keys. Required for non-SOW topics. Not valid with `oof` unless keys match topic keys. |
| `fully_durable` | Send only after persisted to local log + acknowledged by all sync replicas. Only for bookmark subscriptions. |
| `live` | Send messages before persisted. Only applies to bookmark subscriptions. |
| `max_backlog=n` | Queue: max unacknowledged messages to accept. Only for queue/local queue/group local queue subscriptions. |
| `non_regex_topic` | Literal topic name match. |
| `no_sowkey` | Don't send `SowKey`. |
| `oof` | OOF notifications. Only for SOW topics, views, conflated topics. |
| `pause` | Pause bookmark subscription (not `live`). Uses `SubId`. |
| `rate=n` | Max delivery rate for bookmark subscription (not `live`). Formats: `1000`, `100KB`, `1.5X`. |
| `rate_max_gap=n` | Max time between messages for rate-limited bookmark subscription. |
| `replace` | Replace subscription at `SubId`. |
| `resume` | Resume bookmark subscription (not `live`). Uses `SubId`. |
| `select=[fields]` | Fields to include. Comma-delimited inclusion specifiers. |
| `send_keys` | Not supported. |
| `timestamp` | Include processing timestamp header. |

### subscribe command: Returns

| Command | Description |
| --- | --- |
| `publish` | Published message. |
| `oof` | Out-of-focus notification (`oof` option, SOW/view/conflated topic, non-bookmark). |
| `ack` | Acknowledgments. |

| Acknowledgment | Description |
| --- | --- |
| `none` | Default. |
| `completed` | Bookmark replay complete. Includes `SubscriptionId`. Messages after this are new publishes. |
| `processed` | Subscription registered. With bookmark: about to begin replay. Returns `SubscriptionId`. |
| `persisted` | Most recent fully-persisted bookmark in server's transaction log. |
| `received` | Message received. |

### subscribe command: Errors

Errors in `processed` ack `Status` and log. Only returned if `processed` in `AckType`.

### unsubscribe command

Remove a subscription. Two methods: `SubId=all` (all SOW subscriptions) or specific `SubId` (from `processed` ack).

### unsubscribe command: Header Fields

| Field | Description |
| --- | --- |
| `cmd` | Value: `unsubscribe` |
| `sub_id` | Subscription ID or comma-delimited list. `all` unsubscribes all. With `query_id`: removes all matching subs and queries. Requires at least one of `sub_id` or `query_id`. |
| `query_id` | Query ID or comma-delimited list to cancel in-progress SOW query. Requires at least one of `sub_id` or `query_id`. |
| `ack_type` | Comma-separated: `none`, `received`, `persisted`. |
| `cmd_id` | Included in `ack` responses. |

### unsubscribe command: Returns

| Acknowledgment | Description |
| --- | --- |
| `none` | Default. |
| `completed` | Not supported. |
| `processed` | `unsubscribe` processed. |
| `persisted` | Not supported. |
| `received` | Command received. |
| `stats` | Not supported. |

### sow_delete command (Removing Messages)

Three ways to remove SOW records:
1. `publish` with `cmd=sow_delete` — reconstructs SowKey, looks up and removes.
2. `sow_delete` with comma-delimited `SowKeys` list.
3. `sow_delete` with `filter` — deletes matching records.

Also used to acknowledge queue messages (sends comma-delimited bookmarks). Only for queue topics.

`sow_keys`, `filter`, `data`, and `bookmark` are mutually exclusive.

### sow_delete command: Header Fields

| Field | Description |
| --- | --- |
| `cmd` | Value: `sow_delete` |
| `topic` | SOW topic to delete from. |
| `ack_type` | Comma-separated: `none`, `received`, `processed`, `persisted`, `completed`, `stats`. |
| `cmd_id` | Included in `ack` responses. |
| `sow_keys` | Comma-delimited internal SowKeys to delete. |
| `filter` | Content filter — removes matching records. |
| `data` | Message data identifying record to remove (looked up as if publish). |
| `bookmark` | Queue acknowledgment. Only for queue topics. Cannot use with `sow_keys`/`filter`. |
| `opts` | `cancel` returns message to queue; `expire` expires message from queue. |

### sow_delete command: Returns

| Acknowledgment | Description |
| --- | --- |
| `none` | Default. |
| `completed` | Query portion completed (requires `Filter`). |
| `persisted` | 1) All downstream sync replications acknowledged deletion. 2) Sent to async replications. |
| `processed` | Filters compiled. |
| `received` | Command received. |
| `stats` | Returns `Matches`, `TopicMatches`, `RecordsDeleted`. |

- **TopicMatches**: Total records compared across matching SOW topics.
- **Matches**: Records matching topic regex + filter.
- **RecordsDeleted**: Total records deleted.

### sow_delete command: Errors

Errors in `processed` ack and log. Typical: missing topic, missing/invalid `SowKey`.

### Command Cookbook

Quick reference for common command headers.

### Cookbook: Delta Publishing

*Command:* `delta_publish`

**Basic Delta Publish**

| Header | Comment |
| --- | --- |
| `topic` (required) | Literal topic name. Views/conflated topics cannot be published to directly. |
| `data` | Data published verbatim. |

**Delta Publish with CorrelationId**

| Header | Comment |
| --- | --- |
| `topic` (required) | Literal topic name. |
| `data` | Data published verbatim. |
| `correlation_id` | Passed to subscribers. Base64-legal characters only. |

**Delta Publish with Explicit SOW Key**

| Header | Comment |
| --- | --- |
| `topic` (required) | Literal topic name. |
| `data` | Data published verbatim. |
| `sow_key` | Only for topics requiring explicit SOW Key. |

### Cookbook: Delta Subscribe

*Command:* `delta_subscribe`

**Basic Delta Subscription**

| Header | Comment |
| --- | --- |
| `topic` (required) | Literal or regex topic name. |

**Delta Subscription with Options**

| Header | Comment |
| --- | --- |
| `topic` (required) | Literal or regex topic name. |
| `opts` | Comma-delimited options. See `delta_subscribe` options. |

**Delta Subscription with Content Filter**

| Header | Comment |
| --- | --- |
| `topic` (required) | Literal or regex topic name. |
| `filter` | Content filter. Only matching messages delivered. |

### Cookbook: Publishing

*Command:* `publish`

`publish` does not return a stream of messages. Use with async message processing and empty message handler.

**Basic Publish**

| Header | Comment |
| --- | --- |
| `topic` (required) | Literal topic name. Views/conflated topics cannot be published to directly. |
| `data` | Data published verbatim. |

**Publish with CorrelationId**

| Header | Comment |
| --- | --- |
| `topic` (required) | Literal topic name. |
| `data` | Data published verbatim. |
| `correlation_id` | Passed to subscribers. Base64-legal characters only. |

**Publish with Explicit SOW Key**

| Header | Comment |
| --- | --- |
| `topic` (required) | Literal topic name. |
| `data` | Data published verbatim. |
| `sow_key` | Only for topics requiring explicit SOW Key. |

### Cookbook: SOW

*Command:* `sow`

**Basic SOW Query**

| Header | Comment |
| --- | --- |
| `topic` (required) | Literal or regex topic name. Returns all SOW messages. |

**SOW Query with Options**

| Header | Comment |
| --- | --- |
| `topic` (required) | Literal or regex topic name. |
| `opts` | Comma-delimited options. See `sow` options. |

**SOW Query with Ordered Results**

| Header | Comment |
| --- | --- |
| `topic` (required) | Literal or regex topic name. |
| `order_by` | Comma-separated identifiers: `/field/[ASC|DESC]`. |

**SOW Query with TopN Results**

| Header | Comment |
| --- | --- |
| `topic` (required) | Literal or regex topic name. |


| `Options` | `top_n=N` — max records returned. Use with `OrderBy` for deterministic ordering. Example: `top_n=10` |
| `OrderBy` | Comma-separated list: `/field/[ASC|DESC]` |

### SOW Query with Content Filter

| Header | Comment |
| --- | --- |
| `Topic` (required) | Topic name or regex. |
| `Filter` | Content filter — only matching records returned. |

### Historical SOW Query

Requires SOW topic with `History` enabled. Set `Bookmark` (specific bookmark or timestamp).

| Header | Comment |
| --- | --- |
| `Topic` (required) | Topic name or regex. |
| `Bookmark` | Historical point to query — returns SOW state as of that time. |

### Historical SOW Query with Content Filter

Requires SOW topic with `History` enabled.

| Header | Comment |
| --- | --- |
| `Topic` (required) | Topic name or regex. |
| `Bookmark` | Historical point to query. |
| `Filter` | Content filter. |

### SOW Query for Specific Records

| Header | Comment |
| --- | --- |
| `Topic` (required) | Topic name or regex. |
| `SowKeys` | Comma-delimited `SowKey` values. Example: `1853097931817257202,10402779940201650075` |

### SOW Query with Pagination

| Header | Comment |
| --- | --- |
| `Topic` (required) | Literal topic name only (no regex). |
| `OrderBy` | `/field/[ASC|DESC]` |
| `Options` | `top_n=N,skip_n=M`. Example: `top_n=10,skip_n=30` |

### Aggregated SOW Query

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `opts` | `projection` and `grouping` options. See `sow` section. |

## Cookbook: SOW and Delta Subscribe

*Command:* `sow_and_delta_subscribe`

### Basic SOW and Delta Subscribe

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |

### SOW and Delta Subscribe with Options

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `opts` | Common: `oof` (out of focus), `timestamp`. See full options reference. |

### SOW and Delta Subscribe with Content Filter

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `filter` | Content filter. |

### Paginated SOW and Delta Subscribe

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `orderby` | Ordering. Defaults to `SowKey`. |
| `opts` | `top_n=N,skip_n=M,oof`. Example: `top_n=20,skip_n=30,oof` |

### Aggregated SOW and Delta Subscribe

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `opts` | `projection`, `grouping` plus optional: `oof`, `timestamp`, `no_empties`. |

## Cookbook: SOW and Subscribe

*Command:* `sow_and_subscribe`

### Basic SOW and Subscribe

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |

### SOW and Subscribe with Options

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `opts` | Common: `oof`, `timestamp`. See full options reference. |

### SOW and Subscribe with Select List

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `opts` | Example: `oof,select=[-/,+/id,+/ticker]` |

### SOW and Subscribe with Content Filter

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `filter` | Content filter. |

### Conflated SOW and Subscribe

No `conflation_key` needed when topic has a SOW.

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `opts` | `conflation=<interval>`. Examples: `conflation=250ms`, `conflation=1m` |

### Paginated SOW and Subscribe

| Header | Comment |
| --- | --- |
| `topic` (required) | Literal topic name only (no regex). |
| `orderby` | Ordering. Defaults to `SowKey`. |
| `opts` | `top_n=N,skip_n=M,oof`. Example: `top_n=20,skip_n=30,oof` |

### Historical SOW and Subscribe

Requires SOW topic with transaction log. `Bookmark` can be a specific bookmark, timestamp, or `NOW` (`0|1|`) / `EPOCH` (`0`). If not `NOW`, topic must have `History` enabled.

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `bookmark` | Historical point to query. |

### Historical SOW and Subscribe with Content Filter

Same requirements as Historical SOW and Subscribe.

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `bookmark` | Historical point to query. |
| `filter` | Content filter. |

### Aggregated SOW and Subscribe

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `opts` | `projection`, `grouping` plus optional: `oof`, `timestamp`. |

## Cookbook: SOW Delete

*Command:* `sow_delete`

### Delete All Records in a SOW

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic to delete from. |
| `filter` (required) | Use `1=1` to delete all records. |

### Delete SOW Records Matching a Filter

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic to delete from. |
| `filter` (required) | Filter for records to remove. |

### Delete a Specific Message by Data

Relies on `Key` definition in SOW config. Not useful for explicitly-keyed SOW topics.

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic to delete from. |
| `data` (required) | Message data to match for deletion. |

### Delete Specific Messages using Keys

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic to delete from. |
| `sow_keys` (required) | Comma-delimited SOW keys. |

### Acknowledge Messages from a Queue

Only form of `sow_delete` for queue acknowledgment. Not accepted for non-queue topics.

| Header | Comment |
| --- | --- |
| `topic` (required) | Queue topic. |
| `bookmark` (required) | Comma-delimited bookmarks to acknowledge. |

## Cookbook: Subscribe

*Command:* `subscribe`

### Basic Subscription

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |

### Subscription with Options

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `opts` | See `subscribe` options reference. |

### Subscription with Select List

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `opts` | Example: `select=[-/,+/id,+/ticker]` |

### Subscription with Content Filter

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `filter` | Content filter. |

### Conflated Subscription to a SOW Topic

No `conflation_key` needed when topic has a SOW.

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `opts` | `conflation=<interval>`. Examples: `conflation=250ms`, `conflation=1m` |

### Conflated Subscription to a Topic with No SOW

Must provide `conflation_key`.

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `opts` | `conflation=<interval>,conflation_key=[/field,...]`. Example: `conflation=250ms,conflation_key=[/id]` |

### Bookmark Subscription

Requires transaction log. `Bookmark` can be a specific bookmark, timestamp, or client constant. `MOST_RECENT` tells the client to find the appropriate bookmark from the bookmark store. AMPS accepts comma-delimited bookmarks — starts from earliest.

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `Bookmark` | Bookmark, timestamp, or client constant. Comma-delimited for multiple. |

### Rate Controlled Bookmark Subscription

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `bookmark` | Start point in transaction log. |
| `opts` | `rate=N` (msgs/sec). Example: `rate=750` |

### Rate Controlled Bookmark Subscription with Maximum Gap

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `bookmark` | Start point in transaction log. |
| `opts` | `rate=<speed>,rate_max_gap=<duration>`. Example: `rate=2X,rate_max_gap=3s` |

### Bookmark Subscription with Completed Acknowledgment

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `bookmark` | Start point in transaction log. |
| `ack_type` | Include `completed` to receive ack when replay finishes (command type `ack`, ack type `completed`). |

### Bookmark Subscription with Content Filter

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `bookmark` | Start point in transaction log. |
| `filter` | Content filter. |

### Entering a Bookmark Subscription In the Paused State

Used to enter multiple subscriptions at the same point in the transaction log, then resume together.

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `sub_id` (required) | Subscription ID to pause. |
| `opts` (required) | Must include `pause`. |

### Starting One or More Paused Bookmark Subscriptions

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `SubId` (required) | Comma-delimited subscription IDs to resume. |
| `Options` | Must include `resume`. |

### Replacing the Filter on a Subscription

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `sub_id` (required) | Original subscription ID (or `cmd_id` if no `sub_id` was provided). |
| `opts` | Must include `replace`. |
| `filter` | New content filter. |

### Subscribing to a Queue and Requesting a max_backlog

| Header | Comment |
| --- | --- |
| `topic` (required) | Topic name or regex. |
| `opts` | `max_backlog=NN`. Example: `max_backlog=7` |

## Responses from AMPS

### Content Messages

- `publish` — data from a topic (live or replay), in order.
- `sow` — data from a SOW query (current state, unordered by default; use `OrderBy` for ordering).
- `oof` — indicates a content message no longer matches a subscription (delivered in order).

## Ack Messages

Acknowledgment messages must be explicitly requested. Clients request them by default for failure detection.

| ack Type | Meaning |
| --- | --- |
| `completed` | Operation completed (e.g., transaction log replay finished). |
| `persisted` | Data persisted. |
| `processed` | AMPS processed the command (may not have executed yet). |
| `received` | AMPS received but not yet processed. |
| `stats` | Command statistics (typically after full completion). |

### ack: Header Fields

| Field | Description |
| --- | --- |
| `cmd` | Always `ack`. |
| `ack_type` | One of: `completed`, `persisted`, `processed`, `received`, `stats`. |
| `cmd_id` | Command ID this ack refers to. Not returned for conflated `persisted` acks. |
| `status` | Command status. |
| `reason` | Detail, typically for `failure` status. |

### `logon` Acknowledgment: Additional Fields

| Field | Description |
| --- | --- |
| `client_name` | Client name from command. |
| `seq` | Last sequence number fully processed/persisted. |
| `bookmark` | Last bookmark from this client. |
| `user_id` | User ID for retry status. |
| `password` | Password for retry status. |
| `version` | AMPS server version. |

### `publish`, `delta_publish`: Additional Fields

| Field | Description |
| --- | --- |
| `seq` | Last sequence number processed/persisted. |
| `bookmark` | Last bookmark persisted. |

### `delta_subscribe`: Additional Fields

| Field | Description |
| --- | --- |
| `sub_id` | Subscription ID (not returned in `processed` acks). |
| `opts` | Queue subscriptions: `max_backlog` — effective max backlog. |
| `bookmark` | For `completed` ack on bookmark subscription: point in transaction log where ack was generated. |

### `sow`, `sow_and_subscribe`, `sow_and_delta_subscribe`: Additional Fields

| Field | Description |
| --- | --- |
| `sub_id` | Subscription ID from command. |
| `query_id` | Query ID from command. |
| `records_returned` | Records returned (`stats` ack). |
| `topic_matches` | Total records compared across matching topics (`stats` ack). |
| `matches` | Records matching topic regex and filter (`stats` ack). |

### `sow_delete`: Additional Fields

| Field | Description |
| --- | --- |
| `query_id` | Query ID from command. |
| `records_deleted` | Records deleted (`stats` ack). |
| `topic_matches` | Total records compared (`stats` ack). |
| `matches` | Records matching filter (`stats` ack). |

### publish: Content from Server

Two origins:
- **Single-origin** — subscriptions to unpersisted topics, SOW topics, conflated replicas. Header info from the original publish. Not for views/conflated topics based on views.
- **Synthetic** — constructed by server (views, status messages). No origin info or `correlation_id`.

### publish: Header Fields

| Field | Description |
| --- | --- |
| `cmd` | Always `publish`. |
| `topic` | Topic published to. |
| `correlation_id` | Publisher-provided, base64 chars only. For delta publishes: uses delta's `correlation_id`, falls back to existing message's. |
| `user_id` | Publisher's user ID (subject to auth module). |
| `sids` | Matching subscription IDs. Clients split into individual `sub_id` per handler call. |
| `bookmark` | Bookmark if persisted to transaction log. |
| `timestamp` | ISO-8601, microsecond resolution. Included if `timestamp` option requested. |
| `leaseperiod` | ISO-8601 lease expiry (queue messages). |
| `msg_len` | Message body length. |
| `sow_key` | SOW key if topic uses SOW. |

### sow: Header Fields

| Field | Description |
| --- | --- |
| `cmd` | Always `sow`. |
| `topic` | Source topic. |
| `sow_key` | AMPS-created identifier. |
| `batch_size` | Records in this batch. |
| `timestamp` | ISO-8601, microsecond precision. |
| `query_id` | Query ID. |
| `msg_len` | Length of first SOW message in data portion. |

### sow Data Fields (within batch)

| Field | Description |
| --- | --- |
| `sow_key` | AMPS-created identifier. |
| `correlation_id` | User-provided, base64 chars only. |
| `msg_len` | Length of next SOW message in data. |

### group_begin / group_end: Result Set Delimiters

#### group_begin Header Fields

| Field | Description |
| --- | --- |
| `cmd` | Always `group_begin`. |
| `query_id` | Query ID (or command ID if none provided). |

#### group_end Header Fields

| Field | Description |
| --- | --- |
| `cmd` | Always `group_end`. |
| `query_id` | Query ID (or command ID if none provided). |

### oof: Out of Focus Header Fields

| Field | Description |
| --- | --- |
| `cmd` | Always `oof`. |
| `topic` | Topic of the OOF message. |
| `msg_len` | Message body length. |
| `sow_key` | SOW key of the OOF message. |
| `reason` | One of: `deleted`, `expired`, `match`, `entitlement`. |
| `sids` | Matching subscription IDs. |
| `correlation_id` | From original SOW record, base64 chars only. |

### oof Reason Values

| Reason | Description |
| --- | --- |
| `deleted` | Message was deleted. |
| `expired` | Message expired. |
| `match` | No longer matches filter or pagination window. Updated message provided in data. |
| `entitlement` | User lost permission. Previous message provided. |

## AMPS Protocol Header Reference

Sorted by name:

| AMPS Header Field | Abbreviation | Name |
| --- | --- | --- |
| ack_type | a | `AckType` |
| password | pw | `Password` |
| bookmark | bm | `Bookmark` |
| batch_size | bs | `BatchSize` |
| client_name | | `ClientName` |
| cmd | c | `Command` |
| cmd_id | cid | `CommandId` |
| correlation_id | x | `CorrelationId` |
| data_only | | `DataOnly` |
| expiration | e | `Expiration` |
| filter | f | `Filter` |
| gseq | | `GroupSequenceNumber` |
| heartbeat | | `Heartbeat` |
| leaseperiod | lp | `LeasePeriod` |
| matches | | `Matches` |
| msg_len | l | `MsgLen` |
| max_msgs | | `MaximumMessages` |
| opts | o | `Opts` |
| orderby | | `OrderBy` |
| query_id | | `QueryID` |
| reason | | `Reason` |
| records_deleted | | `RecordsDeleted` |
| records_inserted | | `RecordsInserted` |
| records_returned | | `RecordsReturned` |
| records_updated | | `RecordsUpdated` |
| seq | s | `Sequence` |
| send_empty | | `SendEmpty` |
| send_keys | | `SendKeys` |
| send_oof | | `SendOutOfFocus` |
| sow_key | k | `SowKey` |
| sow_keys | | `SowKeys` |
| status | | `Status` |
| sub_id | | `SubscriptionId` |
| sids | | `SubscriptionIds` |
| src | | `Src` |
| timeout_interval | | `TimeoutInterval` |
| timestamp | ts | `TransmissionTime` |
| top_n | | `TopNRecordsReturned` |
| topic_matches | | `TopicMatches` |
| topic | t | `Topic` |
| use_ns | | `UseNamespaces` |
| user_id | | `UserId` |
| version | v | `Version` |

## Header Fields - Reference

| Name | Type | Definition |
| --- | --- | --- |
| `AckType` | string | Acknowledgment type. |
| `BatchSize` | integer, default 1 | Messages batched per query result. |
| `Bookmark` | string | Client-originated location marker in journaled messages. |
| `ClientName` | string | Client identifier. Set with `logon`. |
| `Command` | One of: `publish`, `subscribe`, `sow`, `sow_and_subscribe`, `sow_delete`, `unsubscribe`, `flush`, `heartbeat`, `logon` | Command to execute. |
| `CommandId` | string | Client-specified ID for correlating responses. |
| `CorrelationId` | string, base64 only | Opaque token passed with message. |
| `DataOnly` | Boolean | If `true`, send raw data only (no FIX/NVFIX envelope). |
| `Expiration` | integer (seconds) | SOW expiration for publish. |
| `Filter` | string (CDATA) | Content filter expression. |
| `GracePeriod` | integer (ms) | Grace period after heartbeat exceeded. |
| `GroupSequenceNumber` | integer | Sequence number per SOW batch. |
| `Heartbeat` | One of: `start`, `stop`, `beat` | Heartbeat command. |
| `LeasePeriod` | timestamp | Queue lease expiry time. |
| `LogLevel` | `info` or `none` | Deprecated. |
| `Matches` | integer | SOW query match count. |
| `MaximumMessages` | integer > 0 | Max messages per batch publish. |
| `MessageID` | string, e.g. `MAMPS–XYZ` | AMPS-assigned message tag. |
| `MessageLength` | integer | Message body bytes. |
| `MessageType` | string | Configured AMPS message type. |
| `MsgLen` | integer | Message body bytes. |
| `Opts` | string | Comma-delimited command options. |
| `Password` | string | Authentication password. |
| `QueryID` | string | SOW query identifier. |
| `Reason` | string | Failure detail in ack. |
| `RecordsDeleted` | integer | Records deleted by `sow_delete` (`stats` ack). |
| `RecordsInserted` | integer | Records inserted (`stats` ack). |
| `RecordsUpdated` | integer | Records updated (`stats` ack). |
| `RecordsReturned` | integer | Records returned by SOW query. |
| `SendEmpty` | Boolean, default `true` | Forward empty messages to subscriptions. |
| `SendKeys` | Boolean | Return `SowKey`(s) to client. |
| `SendOutOfFocus` | Boolean | Send OOF messages for SOW query. |
| `SendSubscriptionIDs` | Boolean | If `false`, omit subscription IDs. |
| `Sequence` | integer > 0 | Publish sequence number. |
| `SowKey` | string (unsigned long digits for AMPS-generated; base64 for user-provided) | Unique SOW record identifier. |
| `SowKeys` | comma-separated `SowKey` values | Multiple SOW keys. |
| `Status` | One of: `stopped`, `alive`, `timed out`, `error` | Client status for heartbeat monitoring. |
| `SubscriptionId` | string, e.g. `SAMPS-XYZ` | Server-assigned subscription ID. |
| `SubscriptionIds` | string | Comma-delimited subscription IDs for matched publish. |
| `TimeoutInterval` | integer | Publisher timeout with heartbeat. |
| `TopNRecordsReturned` | unsigned integer | Records to return. Rounded up to `BatchSize` multiple. |
| `Topic` | string | Topic. |
| `TopicMatches` | integer | Topic match count in SOW query ack. |
| `TransmissionTime` | ISO-8601 | Server processing timestamp. |
| `UseNamespaces` | Boolean | Use SOAP XML namespaces. |
| `UserId` | string | User ID for command. |
| `Version` | string | AMPS server version. |

## Legacy Protocol Reference

### FIX/NVFIX Protocol — Sorted by Value

| FIX/NVFIX | AMPS Equivalent |
| --- | --- |
| 20000 | `cmd` |
| 20001 | `cmd_id` |
| 20002 | `client_name` |
| 20003 | `user_id` |
| 20004 | `timestamp` |
| 20005 | `topic` |
| 20006 | `filter` |
| 20007 | `message_type` |
| 20008 | `ack_type` |
| 20009 | `sub_id` |
| 20011 | `version` |
| 20012 | `expiration` |
| 20013 | N/A — obsolete (`SendSubscriptionIDs`) |
| 20014 | `data_only` |
| 20015 | `heartbeat` |
| 20016 | `timeout_interval` |
| 20017 | `lease_period` |
| 20018 | `status` |
| 20019 | `query_id` |
| 20020 | `send_oof` |
| 20021 | N/A — obsolete (`LogLevel`) |
| 20022 | `use_ns` |
| 20023 | `batch_size` |
| 20025 | `top_n` |
| 20029 | `send_empty` |
| 20031 | `max_msgs` |
| 20032 | `sow_keys` |
| 20033 | `send_keys` |
| 20034 | `src` |
| 20035 | `correlation_id` |
| 20036 | `seq` |
| 20037 | `bookmark` |
| 20038 | `password` |
| 20039 | `opts` |
| 20052 | `records_inserted` |
| 20053 | `records_updated` |
| 20054 | `records_deleted` |
| 20055 | `records_returned` |
| 20056 | `topic_matches` |
| 20057 | `matches` |
| 20058 | `msg_len` |
| 20059 | `sow_key` |
| 20060 | `gseq` |
| 20061 | `sids` |
| 20062 | `reason` |
| 20063 | N/A — obsolete (`MessageID`) |
| 20074 | `correlation_id` (AMPS/ClientStatus messages) |

### FIX/NVFIX Protocol — Sorted by Name

| FIX/NVFIX | AMPS Equivalent |
| --- | --- |
| 20008 | `ack_type` |
| 20037 | `bookmark` |
| 20023 | `batch_size` |
| 20002 | `client_name` |
| 20000 | `cmd` |
| 20001 | `cmd_id` |
| 20035 | `correlation_id` |
| 20014 | `data_only` |
| 20012 | `expiration` |
| 20006 | `filter` |
| 20060 | `gseq` |
| 20015 | `heartbeat` |
| 20017 | `leaseperiod` |
| 20021 | N/A — obsolete (`LogLevel`) |
| 20057 | `matches` |
| 20063 | N/A — obsolete (`MessageID`) |
| 20058 | `msg_len` |
| 20007 | `message_type` |
| 20031 | `max_msgs` |
| 20039 | `opts` |
| 20038 | `password` |
| 20019 | `query_id` |
| 20062 | `reason` |
| 20054 | `records_deleted` |
| 20053 | `records_inserted` |
| 20055 | `records_returned` |
| 20036 | `seq` |
| 20029 | `send_empty` |
| 20033 | `send_keys` |
| 20020 | `send_oof` |
| 20013 | N/A — obsolete (`SendSubscriptionIDs`) |
| 20059 | `sow_key` |
| 20032 | `sow_keys` |
| 20034 | `src` |
| 20018 | `status` |
| 20009 | `sub_id` |
| 20061 | `sids` |
| 20016 | `timeout_interval` |
| 20025 | `top_n` |
| 20056 | `topic_matches` |
| 20005 | `topic` |
| 20004 | `timestamp` |
| 20022 | `use_ns` |
| 20003 | `user_id` |

### XML Protocol — Sorted by Name

| XML Header | AMPS Equivalent |
| --- | --- |
| AckTyp | `ack_type` |
| BkMrk | `bookmark` |
| BtchSz | `batch_size` |
| ClntName | `client_name` |
| Cmd | `cmd` |
| CmdId | `cmd_id` |
| DatOnly | `data_only` |
| Expn | `expiration` |
| Fltr | `filter` |
| GrcPrd | N/A — obsolete (`GracePeriod`) |
| GrpSqNum | `gseq` |
| Hrtbt | `heartbeat` |
| LeasePeriod | `lease_period` |
| LogLvl | N/A — obsolete (`LogLevel`) |
| Matches | `matches` |
| MsgId | `MessageID` |
| MsgLen | `msg_len` |
| MsgTyp | `message_type` |
| MxMsgs | `max_msgs` |
| Opts | `opts` |
| PW | `password` |
| QId | `query_id` |
| Reason | `reason` |
| RecordsDeleted | `records_deleted` |
| RecordsReturned | `records_returned` |
| Seq | `seq` |
| SndEmpty | `send_empty` |
| SndKeys | `send_keys` |
| SndOOF | `send_oof` |
| SndSubIds | N/A — obsolete (`SendSubscriptionIDs`) |
| SowKey | `sow_key` |
| SowKeys | `sow_keys` |
| Status | `status` |
| SubId | `sub_id` |
| SubIds | `sids` |
| TmIntvl | `timeout_interval` |
| TopN | `top_n` |
| TopicMatches | `topic_matches` |
| Tpc | `topic` |
| TxmTm | `timestamp` |
| UseNS | `use_ns` |
| UsrId | `user_id` |
| Version | `version` |


## AMPS Developer Guides

For comprehensive developer resources, including client library guides, performance best practices, and connector documentation, visit the [AMPS Developer Portal](https://crankuptheamps.com/developers).

Detailed server behavior, configuration references, and API documentation are available in the [AMPS Server Documentation](/docs).


