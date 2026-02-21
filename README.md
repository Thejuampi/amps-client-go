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
- C compatibility: [C API Compatibility Reference](docs/capi_reference.md)
- C++ utility compatibility: [C++ Compatibility Reference](docs/cppcompat_reference.md)
- C++ parity mapping: [C++ to Go Parity Matrix](docs/cpp_to_go_parity_matrix.md)

## Build and Test

```bash
make build
make test
make integration-test
make parity-check
make coverage-check
make release
```

Equivalent direct commands:

```bash
go run ./tools/paritycheck -manifest tools/parity_manifest.json
go test -count=1 ./amps/... -coverprofile=coverage.out
go run ./tools/coveragegate -profile coverage.out
```

Coverage gating is expected before merge for `./amps/...`: aggregate `>=90.0%`, pure-functional files `100.0%`, and I/O/stateful files `>=80.0%` as enforced by `tools/coveragegate/main.go`.

PowerShell note: quote the coverprofile flag if needed, for example `go test -count=1 ./amps/... '-coverprofile=coverage.out'`.

## License

MIT. See [LICENSE](LICENSE).
