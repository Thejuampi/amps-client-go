## Thejuampi AMPS Go Client (`github.com/Thejuampi/amps-client-go/amps`)

This repository is a custom implementation of an AMPS client in Go by Thejuampi.

Current project version: `0.1.0`.

It provides an AMPS Go client with compatibility aliases and behavior parity for key C++ `Client`/`HAClient` workflows (target: 5.3.5.1), while keeping the public import path:

```go
import "github.com/Thejuampi/amps-client-go/amps"
```

## Install

```bash
go get github.com/Thejuampi/amps-client-go/amps
```

## Build With Make

Install GNU Make on Windows if needed:

```powershell
winget install --id ezwinports.make -e --accept-source-agreements --accept-package-agreements --silent
```

After install, restart your terminal so `make` is on `PATH`.

Common targets:

```bash
make help
make build
make test
make integration-test
make install
make release
```

## Quick Start

```go
package main

import (
	"fmt"

	"github.com/Thejuampi/amps-client-go/amps"
)

func main() {
	client := amps.NewClient("example-client")

	if err := client.Connect("tcp://localhost:9000/amps/json"); err != nil {
		panic(err)
	}
	if err := client.Logon(); err != nil {
		panic(err)
	}

	_, err := client.SubscribeAsync(func(message *amps.Message) error {
		fmt.Println(string(message.Data()))
		return nil
	}, "orders")
	if err != nil {
		panic(err)
	}

	if err := client.Publish("orders", `{"id":1}`); err != nil {
		panic(err)
	}
}
```

## HA Example

```go
package main

import (
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

func main() {
	ha := amps.NewHAClient("ha-client")
	chooser := amps.NewDefaultServerChooser(
		"tcp://localhost:9000/amps/json",
		"tcp://localhost:9001/amps/json",
	)
	ha.SetServerChooser(chooser)
	ha.SetReconnectDelayStrategy(amps.NewExponentialDelayStrategy(
		100*time.Millisecond,
		5*time.Second,
		2,
	))
	if err := ha.ConnectAndLogon(); err != nil {
		panic(err)
	}
}
```

## Integration Test Environment Contract

Optional integration tests use:

- `AMPS_TEST_URI`
- `AMPS_TEST_FAILOVER_URIS`
- `AMPS_TEST_PROTOCOL`
- `AMPS_TEST_USER`
- `AMPS_TEST_PASSWORD`

Run:

```bash
go test ./... -run Integration
```

## Parity Coverage Snapshot

| Coverage Area | Status | Primary Verification |
|---|---|---|
| `Client`/`HAClient` API parity surface | Complete in scope | Unit tests (`amps/*_test.go`) |
| Handler ordering and fallback chain | Verified | Unit tests (`TestOnMessageHandlerOrder`, `TestUnhandledAndLastChanceOrder`) |
| Auto-ack batching/timeout and explicit ack flow | Verified | Unit + integration (`TestAutoAck*`, `TestIntegrationQueueAutoAckBatching`) |
| Publish replay/discard behavior | Verified | Unit (`TestMemoryPublishStoreReplayAndDiscard`, `TestApplyAckBookkeepingDiscardPublishOnPersistedAck`) |
| Bookmark dedupe/resume behavior | Verified | Unit + integration (`TestMemoryBookmarkStoreDuplicateAndMostRecent`, `TestIntegrationBookmarkResumeAcrossReconnect`) |
| HA chooser/reconnect flow | Verified | Unit + integration (`TestHAReconnectDelayStrategySetters`, `TestIntegrationHAConnectAndLogonWithFailoverChooser`) |
| Timer command path (`start_timer`/`stop_timer`) | Verified | Unit + integration (`TestTimerCommandPathWritesStartAndStop`, `TestIntegrationStartStopTimerCommandPath`) |

## Legal Note

This repository is an independent custom implementation maintained by Thejuampi and is licensed under MIT (`LICENSE`).

## Additional Docs

- `docs/migration_guide.md`
- `docs/cpp_to_go_parity_matrix.md`
