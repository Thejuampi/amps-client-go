# AMPS Go Client (`github.com/Thejuampi/amps-client-go/amps`)

Version: `0.1.0`

This repository provides a custom Go implementation of an AMPS client API with parity-oriented behavior for C++ `Client`/`HAClient` 5.3.5.1.

## Install

```bash
go get github.com/Thejuampi/amps-client-go/amps
```

## Minimal Quick Start

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
	defer client.Close()

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

## Documentation Entry Point

Primary portal:

- [Documentation Index](docs/index.md)

Where to start:

- Fast integration: [Getting Started](docs/getting_started.md), [Client Entrypoints](docs/client_entrypoints.md), [Pub/Sub and SOW](docs/pub_sub_and_sow.md)
- Production hardening: [Queue Ack Semantics](docs/queue_ack_semantics.md), [Bookmarks and Replay](docs/bookmarks_and_replay.md), [HA Failover](docs/ha_failover.md), [Operational Playbook](docs/operational_playbook.md)
- Scope and constraints: [Supported Scope and Constraints](docs/supported_scope.md)
- API lookup: [Reference: Client](docs/reference_client.md), [Reference: HAClient](docs/reference_ha_client.md), [Reference: Types and Handlers](docs/reference_types_and_handlers.md)
- C++ parity mapping: [C++ to Go Parity Matrix](docs/cpp_to_go_parity_matrix.md)

## Build and Test

```bash
make build
make test
make integration-test
make release
```

## License

MIT. See [LICENSE](LICENSE).
