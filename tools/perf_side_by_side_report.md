# API Parity Performance Report (C vs Go)

Percentile method: `nearest-rank`

| Benchmark ID | Tier | HA | C++ Symbol | Parity Mapped | Go p50/p95/p99 (ns/op) | C p50/p95/p99 (ns/op) | Winner (p95) | Delta p95 (Go-C) | Enabled |
|---|---|---:|---|---:|---|---|---|---:|---:|
| client.parse.header.strict | offline | false | `Client::execute` | true | 19.29 / 19.86 / 19.92 | 22.01 / 22.18 / 22.25 | go | -10.46% | true |
| client.parse.sow_batch.strict | offline | false | `Client::sow` | true | 95.89 / 97.21 / 97.23 | 120.37 / 121.29 / 121.37 | go | -19.85% | true |
| client.serialize.header.strict | offline | false | `Client::send` | true | 147.60 / 149.30 / 150.70 | 68.43 / 69.54 / 69.61 | c | 114.70% | true |
| client.route.single | offline | false | `Client::addMessageHandler` | true | 139.90 / 141.30 / 145.30 | N/A | n/a | N/A | true |
| client.route.multi_sids | offline | false | `Client::addMessageHandler` | true | 177.70 / 189.90 / 195.90 | N/A | n/a | N/A | true |
| client.read.decode.dispatch | offline | false | `Client::executeAsync` | true | 174.50 / 181.80 / 184.10 | N/A | n/a | N/A | true |
| client.set_name | offline | false | `Client::setName` | true | 0.49 / 0.51 / 0.52 | N/A | n/a | N/A | true |
| client.get_name | offline | false | `Client::getName` | true | 0.44 / 0.46 / 0.47 | N/A | n/a | N/A | true |
| client.get_name_hash | offline | false | `Client::getNameHash` | true | 0.99 / 1.02 / 1.06 | N/A | n/a | N/A | true |
| client.set_logon_correlation_data | offline | false | `Client::setLogonCorrelationData` | true | 0.53 / 0.53 / 0.53 | N/A | n/a | N/A | true |
| client.get_logon_correlation_data | offline | false | `Client::getLogonCorrelationData` | true | 0.52 / 0.54 / 0.60 | N/A | n/a | N/A | true |
| client.set_auto_ack | offline | false | `Client::setAutoAck` | true | 17.72 / 18.37 / 18.82 | N/A | n/a | N/A | true |
| client.get_auto_ack | offline | false | `Client::getAutoAck` | true | 17.04 / 17.74 / 17.76 | N/A | n/a | N/A | true |
| ha.set_timeout | offline | true | `HAClient::setTimeout` | true | 11.20 / 12.04 / 12.07 | N/A | n/a | N/A | true |
| ha.get_timeout | offline | true | `HAClient::getTimeout` | true | 11.11 / 11.31 / 11.72 | N/A | n/a | N/A | true |
| ha.set_reconnect_delay | offline | true | `HAClient::setReconnectDelay` | true | 17.80 / 18.78 / 19.30 | N/A | n/a | N/A | true |
| ha.get_reconnect_delay | offline | true | `HAClient::getReconnectDelay` | true | 11.22 / 11.64 / 15.48 | N/A | n/a | N/A | true |
| ha.set_reconnect_delay_strategy | offline | true | `HAClient::setReconnectDelayStrategy` | true | 11.25 / 11.36 / 11.39 | N/A | n/a | N/A | true |
| ha.set_logon_options | offline | true | `HAClient::setLogonOptions` | true | 12.00 / 12.51 / 13.13 | N/A | n/a | N/A | true |
| ha.get_logon_options | offline | true | `HAClient::getLogonOptions` | true | 16.07 / 16.31 / 16.39 | N/A | n/a | N/A | true |
| ha.set_server_chooser | offline | true | `HAClient::setServerChooser` | true | 11.36 / 11.56 / 11.63 | N/A | n/a | N/A | true |
| ha.get_server_chooser | offline | true | `HAClient::getServerChooser` | true | 11.28 / 11.41 / 11.44 | N/A | n/a | N/A | true |
| ha.disconnected | offline | true | `HAClient::disconnected` | true | 0.44 / 0.45 / 0.48 | N/A | n/a | N/A | true |
| client.connect_logon.integration | integration | false | `Client::connect` | true | 263000.00 / 464700.00 / 557600.00 | 187976.67 / 230508.00 / 255249.33 | c | 101.60% | true |
| client.publish.integration | integration | false | `Client::publish` | true | 88100.00 / 132100.00 / 138300.00 | 25678.83 / 29874.50 / 30062.67 | c | 342.18% | true |
| client.subscribe.integration | integration | false | `Client::subscribe` | true | 121200.00 / 153800.00 / 159500.00 | 26403.10 / 28563.00 / 31622.30 | c | 438.46% | true |
| ha.connect_and_logon.integration | integration | true | `HAClient::connectAndLogon` | true | 337800.00 / 468700.00 / 551000.00 | 185640.00 / 216978.75 / 220268.75 | c | 116.01% | true |

Comparable rows: **7**
Go wins (p95): **2**
C wins (p95): **5**
Ties (p95): **0**
Rows without both sides yet: **20**
Rows mapped in parity manifest: **27**
Rows missing parity mapping: **0**
