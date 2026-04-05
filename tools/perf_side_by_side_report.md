# API Parity Performance Report (C vs Go)

Percentile method: `nearest-rank`

| Benchmark ID | Tier | HA | C++ Symbol | Parity Mapped | Go p50/p95/p99 (ns/op) | C p50/p95/p99 (ns/op) | Winner (p95) | Delta p95 (Go-C) | Enabled |
|---|---|---:|---|---:|---|---|---|---:|---:|
| client.parse.header.strict | offline | false | `Client::execute` | true | 21.12 / 21.67 / 21.92 | 23.06 / 23.74 / 24.21 | go | -8.72% | true |
| client.parse.sow_batch.strict | offline | false | `Client::sow` | true | 106.50 / 110.90 / 113.90 | 131.54 / 137.39 / 141.19 | go | -19.28% | true |
| client.serialize.header.strict | offline | false | `Client::send` | true | 66.56 / 67.19 / 68.51 | 70.40 / 73.05 / 73.29 | go | -8.02% | true |
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
| client.connect_logon.integration | integration | false | `Client::connect` | true | N/A | N/A | n/a | N/A | false |
| client.publish.integration | integration | false | `Client::publish` | true | 167400.00 / 259300.00 / 305400.00 | 171827.92 / 364372.75 / 441580.25 | go | -28.84% | true |
| client.subscribe.integration | integration | false | `Client::subscribe` | true | 100600.00 / 144100.00 / 199100.00 | 153363.20 / 283117.70 / 328832.70 | go | -49.10% | true |
| ha.connect_and_logon.integration | integration | true | `HAClient::connectAndLogon` | true | N/A | N/A | n/a | N/A | false |

Comparable rows: **5**
Go wins (p95): **5**
C wins (p95): **0**
Ties (p95): **0**
Rows without both sides yet: **22**
Rows mapped in parity manifest: **27**
Rows missing parity mapping: **0**
