# API Parity Performance Report (C vs Go)

Percentile method: `nearest-rank`

| Benchmark ID | Tier | HA | C++ Symbol | Parity Mapped | Go p50/p95/p99 (ns/op) | C p50/p95/p99 (ns/op) | Winner (p95) | Delta p95 (Go-C) | Enabled |
|---|---|---:|---|---:|---|---|---|---:|---:|
| client.parse.header.strict | offline | false | `Client::execute` | true | N/A | N/A | n/a | N/A | true |
| client.parse.sow_batch.strict | offline | false | `Client::sow` | true | N/A | N/A | n/a | N/A | true |
| client.serialize.header.strict | offline | false | `Client::send` | true | N/A | N/A | n/a | N/A | true |
| client.route.single | offline | false | `Client::addMessageHandler` | true | N/A | N/A | n/a | N/A | true |
| client.route.multi_sids | offline | false | `Client::addMessageHandler` | true | N/A | N/A | n/a | N/A | true |
| client.read.decode.dispatch | offline | false | `Client::executeAsync` | true | N/A | N/A | n/a | N/A | true |
| client.set_name | offline | false | `Client::setName` | true | N/A | N/A | n/a | N/A | true |
| client.get_name | offline | false | `Client::getName` | true | N/A | N/A | n/a | N/A | true |
| client.get_name_hash | offline | false | `Client::getNameHash` | true | N/A | N/A | n/a | N/A | true |
| client.set_logon_correlation_data | offline | false | `Client::setLogonCorrelationData` | true | N/A | N/A | n/a | N/A | true |
| client.get_logon_correlation_data | offline | false | `Client::getLogonCorrelationData` | true | N/A | N/A | n/a | N/A | true |
| client.set_auto_ack | offline | false | `Client::setAutoAck` | true | N/A | N/A | n/a | N/A | true |
| client.get_auto_ack | offline | false | `Client::getAutoAck` | true | N/A | N/A | n/a | N/A | true |
| ha.set_timeout | offline | true | `HAClient::setTimeout` | true | N/A | N/A | n/a | N/A | true |
| ha.get_timeout | offline | true | `HAClient::getTimeout` | true | N/A | N/A | n/a | N/A | true |
| ha.set_reconnect_delay | offline | true | `HAClient::setReconnectDelay` | true | N/A | N/A | n/a | N/A | true |
| ha.get_reconnect_delay | offline | true | `HAClient::getReconnectDelay` | true | N/A | N/A | n/a | N/A | true |
| ha.set_reconnect_delay_strategy | offline | true | `HAClient::setReconnectDelayStrategy` | true | N/A | N/A | n/a | N/A | true |
| ha.set_logon_options | offline | true | `HAClient::setLogonOptions` | true | N/A | N/A | n/a | N/A | true |
| ha.get_logon_options | offline | true | `HAClient::getLogonOptions` | true | N/A | N/A | n/a | N/A | true |
| ha.set_server_chooser | offline | true | `HAClient::setServerChooser` | true | N/A | N/A | n/a | N/A | true |
| ha.get_server_chooser | offline | true | `HAClient::getServerChooser` | true | N/A | N/A | n/a | N/A | true |
| ha.disconnected | offline | true | `HAClient::disconnected` | true | N/A | N/A | n/a | N/A | true |
| client.connect_logon.integration | integration | false | `Client::connect` | true | 374640.00 / 498040.00 / 655580.00 | 345888.00 / 990982.00 / 2124850.67 | go | -49.74% | true |
| client.publish.integration | integration | false | `Client::publish` | true | 43384.00 / 48151.00 / 58008.00 | 44995.83 / 121651.58 / 126046.58 | go | -60.42% | true |
| client.subscribe.integration | integration | false | `Client::subscribe` | true | 34063.00 / 40156.00 / 40626.00 | 29324.40 / 95934.80 / 101912.00 | go | -58.14% | true |
| ha.connect_and_logon.integration | integration | true | `HAClient::connectAndLogon` | true | 367540.00 / 420560.00 / 435480.00 | 360891.25 / 459820.00 / 740551.25 | go | -8.54% | true |

Comparable rows: **4**
Go wins (p95): **4**
C wins (p95): **0**
Ties (p95): **0**
Rows without both sides yet: **23**
Rows mapped in parity manifest: **27**
Rows missing parity mapping: **0**
