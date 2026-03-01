# API Parity Performance Report (C vs Go)

Percentile method: `nearest-rank`

| Benchmark ID | Tier | HA | C++ Symbol | Parity Mapped | Go p50/p95/p99 (ns/op) | C p50/p95/p99 (ns/op) | Winner (p95) | Delta p95 (Go-C) | Enabled |
|---|---|---:|---|---:|---|---|---|---:|---:|
| client.parse.header.strict | offline | false | `Client::execute` | true | 19.41 / 20.59 / 21.02 | 22.20 / 22.41 / 22.47 | go | -8.12% | true |
| client.parse.sow_batch.strict | offline | false | `Client::sow` | true | 97.19 / 98.59 / 101.80 | 121.97 / 125.56 / 126.13 | go | -21.48% | true |
| client.serialize.header.strict | offline | false | `Client::send` | true | 172.40 / 174.10 / 185.30 | 68.56 / 70.16 / 72.35 | c | 148.15% | true |
| client.route.single | offline | false | `Client::addMessageHandler` | true | 139.90 / 148.90 / 149.30 | N/A | n/a | N/A | true |
| client.route.multi_sids | offline | false | `Client::addMessageHandler` | true | 177.40 / 181.50 / 190.40 | N/A | n/a | N/A | true |
| client.read.decode.dispatch | offline | false | `Client::executeAsync` | true | 171.60 / 175.70 / 177.80 | N/A | n/a | N/A | true |
| client.set_name | offline | false | `Client::setName` | true | 0.48 / 0.50 / 0.54 | N/A | n/a | N/A | true |
| client.get_name | offline | false | `Client::getName` | true | 0.44 / 0.51 / 0.53 | N/A | n/a | N/A | true |
| client.get_name_hash | offline | false | `Client::getNameHash` | true | 0.98 / 0.99 / 0.99 | N/A | n/a | N/A | true |
| client.set_logon_correlation_data | offline | false | `Client::setLogonCorrelationData` | true | 0.55 / 0.63 / 0.63 | N/A | n/a | N/A | true |
| client.get_logon_correlation_data | offline | false | `Client::getLogonCorrelationData` | true | 0.53 / 0.78 / 0.80 | N/A | n/a | N/A | true |
| client.set_auto_ack | offline | false | `Client::setAutoAck` | true | 18.48 / 20.70 / 23.43 | N/A | n/a | N/A | true |
| client.get_auto_ack | offline | false | `Client::getAutoAck` | true | 17.04 / 18.23 / 18.80 | N/A | n/a | N/A | true |
| ha.set_timeout | offline | true | `HAClient::setTimeout` | true | 11.21 / 11.98 / 12.00 | N/A | n/a | N/A | true |
| ha.get_timeout | offline | true | `HAClient::getTimeout` | true | 11.30 / 11.60 / 11.70 | N/A | n/a | N/A | true |
| ha.set_reconnect_delay | offline | true | `HAClient::setReconnectDelay` | true | 19.54 / 21.76 / 21.94 | N/A | n/a | N/A | true |
| ha.get_reconnect_delay | offline | true | `HAClient::getReconnectDelay` | true | 11.17 / 11.30 / 12.12 | N/A | n/a | N/A | true |
| ha.set_reconnect_delay_strategy | offline | true | `HAClient::setReconnectDelayStrategy` | true | 11.22 / 11.36 / 11.91 | N/A | n/a | N/A | true |
| ha.set_logon_options | offline | true | `HAClient::setLogonOptions` | true | 11.90 / 12.43 / 12.59 | N/A | n/a | N/A | true |
| ha.get_logon_options | offline | true | `HAClient::getLogonOptions` | true | 15.92 / 16.58 / 16.69 | N/A | n/a | N/A | true |
| ha.set_server_chooser | offline | true | `HAClient::setServerChooser` | true | 11.27 / 11.38 / 11.65 | N/A | n/a | N/A | true |
| ha.get_server_chooser | offline | true | `HAClient::getServerChooser` | true | 11.20 / 11.96 / 12.01 | N/A | n/a | N/A | true |
| ha.disconnected | offline | true | `HAClient::disconnected` | true | 0.45 / 0.47 / 0.54 | N/A | n/a | N/A | true |
| client.connect_logon.integration | integration | false | `Client::connect` | true | N/A | 234654.00 / 252962.00 / 302878.67 | n/a | N/A | true |
| client.publish.integration | integration | false | `Client::publish` | true | N/A | 25976.25 / 37558.08 / 37888.92 | n/a | N/A | true |
| client.subscribe.integration | integration | false | `Client::subscribe` | true | N/A | 26453.00 / 30241.80 / 36901.90 | n/a | N/A | true |
| ha.connect_and_logon.integration | integration | true | `HAClient::connectAndLogon` | true | N/A | 220371.25 / 248715.00 / 425472.50 | n/a | N/A | true |

Comparable rows: **3**
Go wins (p95): **2**
C wins (p95): **1**
Ties (p95): **0**
Rows without both sides yet: **24**
Rows mapped in parity manifest: **27**
Rows missing parity mapping: **0**
