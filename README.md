## AMPS Go Client (`github.com/Thejuampi/amps-client-go/amps`)

Version: `0.1.0`

This repository provides a Go implementation of an AMPS client API with C++ 5.3.5.1 parity-oriented behavior for `Client` and `HAClient`.

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

## Documentation

Primary portal:

- [Documentation Index](docs/index.md)

Common entry points:

- [Getting Started](docs/getting_started.md)
- [Client Entrypoints](docs/client_entrypoints.md)
- [HA Failover](docs/ha_failover.md)
- [Testing and Validation](docs/testing_and_validation.md)
- [C++ to Go Parity Matrix](docs/cpp_to_go_parity_matrix.md)

## Build and Test

```bash
make build
make test
make integration-test
make release
```

## License

MIT. See [LICENSE](LICENSE).
