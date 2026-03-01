# API Parity Performance Report (C vs Go)

Percentile method: `nearest-rank`

| Benchmark ID | Tier | HA | C++ Symbol | Parity Mapped | Go p50/p95/p99 (ns/op) | C p50/p95/p99 (ns/op) | Winner (p95) | Delta p95 (Go-C) | Enabled |
|---|---|---:|---|---:|---|---|---|---:|---:|
| client.parse.header.strict | offline | false | `Client::execute` | true | 20.24 / 20.64 / 20.79 | 22.12 / 22.43 / 22.95 | go | -7.98% | true |
| client.parse.sow_batch.strict | offline | false | `Client::sow` | true | 100.90 / 105.30 / 125.10 | 120.96 / 123.45 / 128.17 | go | -14.70% | true |
| client.serialize.header.strict | offline | false | `Client::send` | true | 145.00 / 185.90 / 186.40 | 68.56 / 69.20 / 69.63 | c | 168.64% | true |
| client.route.single | offline | false | `Client::addMessageHandler` | true | 65.74 / 92.56 / 95.93 | N/A | n/a | N/A | true |
| client.route.multi_sids | offline | false | `Client::addMessageHandler` | true | 107.90 / 147.20 / 152.20 | N/A | n/a | N/A | true |
| client.read.decode.dispatch | offline | false | `Client::executeAsync` | true | 96.68 / 131.00 / 132.50 | N/A | n/a | N/A | true |
| client.set_name | offline | false | `Client::setName` | true | 0.48 / 0.49 / 0.53 | N/A | n/a | N/A | true |
| client.get_name | offline | false | `Client::getName` | true | 0.48 / 0.61 / 0.62 | N/A | n/a | N/A | true |
| client.get_name_hash | offline | false | `Client::getNameHash` | true | 1.00 / 1.54 / 1.54 | N/A | n/a | N/A | true |
| client.set_logon_correlation_data | offline | false | `Client::setLogonCorrelationData` | true | 0.53 / 0.58 / 0.63 | N/A | n/a | N/A | true |
| client.get_logon_correlation_data | offline | false | `Client::getLogonCorrelationData` | true | 0.51 / 0.52 / 0.52 | N/A | n/a | N/A | true |
| client.set_auto_ack | offline | false | `Client::setAutoAck` | true | 17.63 / 19.05 / 19.06 | N/A | n/a | N/A | true |
| client.get_auto_ack | offline | false | `Client::getAutoAck` | true | 17.42 / 23.48 / 26.96 | N/A | n/a | N/A | true |
| ha.set_timeout | offline | true | `HAClient::setTimeout` | true | 11.84 / 12.89 / 13.03 | N/A | n/a | N/A | true |
| ha.get_timeout | offline | true | `HAClient::getTimeout` | true | 11.18 / 13.51 / 13.68 | N/A | n/a | N/A | true |
| ha.set_reconnect_delay | offline | true | `HAClient::setReconnectDelay` | true | 18.25 / 21.94 / 23.07 | N/A | n/a | N/A | true |
| ha.get_reconnect_delay | offline | true | `HAClient::getReconnectDelay` | true | 11.11 / 11.99 / 12.52 | N/A | n/a | N/A | true |
| ha.set_reconnect_delay_strategy | offline | true | `HAClient::setReconnectDelayStrategy` | true | 11.18 / 13.26 / 13.32 | N/A | n/a | N/A | true |
| ha.set_logon_options | offline | true | `HAClient::setLogonOptions` | true | 11.90 / 14.18 / 14.83 | N/A | n/a | N/A | true |
| ha.get_logon_options | offline | true | `HAClient::getLogonOptions` | true | 15.98 / 18.47 / 19.20 | N/A | n/a | N/A | true |
| ha.set_server_chooser | offline | true | `HAClient::setServerChooser` | true | 11.50 / 13.09 / 13.36 | N/A | n/a | N/A | true |
| ha.get_server_chooser | offline | true | `HAClient::getServerChooser` | true | 12.67 / 13.56 / 13.72 | N/A | n/a | N/A | true |
| ha.disconnected | offline | true | `HAClient::disconnected` | true | 0.44 / 0.60 / 0.64 | N/A | n/a | N/A | true |
| client.connect_logon.integration | integration | false | `Client::connect` | true | 613600.00 / 922400.00 / 1199900.00 | 566494.67 / 689634.67 / 881306.67 | c | 33.75% | true |
| client.publish.integration | integration | false | `Client::publish` | true | 131700.00 / 214700.00 / 233300.00 | 40886.08 / 46921.50 / 48558.75 | c | 357.57% | true |
| client.subscribe.integration | integration | false | `Client::subscribe` | true | 88900.00 / 146600.00 / 351800.00 | 29741.60 / 33311.20 / 45110.40 | c | 340.09% | true |
| ha.connect_and_logon.integration | integration | true | `HAClient::connectAndLogon` | true | 711500.00 / 1084500.00 / 1213600.00 | 554261.25 / 639606.25 / 640266.25 | c | 69.56% | true |

Comparable rows: **7**
Go wins (p95): **2**
C wins (p95): **5**
Ties (p95): **0**
Rows without both sides yet: **20**
Rows mapped in parity manifest: **27**
Rows missing parity mapping: **0**
