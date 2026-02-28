# AMPS Go Client (`github.com/Thejuampi/amps-client-go/amps`)

<p align="left">
  <a href="https://github.com/Thejuampi/amps-client-go/actions/workflows/ci.yml"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/Thejuampi/amps-client-go/ci.yml?branch=main&label=CI&logo=githubactions&logoColor=white"></a>
  <a href="https://github.com/Thejuampi/amps-client-go/actions/workflows/release.yml"><img alt="Release" src="https://img.shields.io/github/actions/workflow/status/Thejuampi/amps-client-go/release.yml?label=release&logo=github"></a>
  <a href="https://github.com/Thejuampi/amps-client-go/releases"><img alt="Latest Release" src="https://img.shields.io/github/v/release/Thejuampi/amps-client-go?sort=semver&logo=github"></a>
  <a href="https://pkg.go.dev/github.com/Thejuampi/amps-client-go/amps"><img alt="Go Reference" src="https://pkg.go.dev/badge/github.com/Thejuampi/amps-client-go/amps.svg"></a>
  <a href="https://github.com/Thejuampi/amps-client-go/blob/main/go.mod"><img alt="Go Version" src="https://img.shields.io/github/go-mod/go-version/Thejuampi/amps-client-go?logo=go"></a>
  <a href="https://github.com/Thejuampi/amps-client-go/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/github/license/Thejuampi/amps-client-go"></a>
  <a href="docs/index.md"><img alt="Docs" src="https://img.shields.io/badge/docs-index-blue?logo=readthedocs&logoColor=white"></a>
  <a href="tools/coveragegate/main.go"><img alt="Coverage Gate" src="https://img.shields.io/badge/coverage%20gate-90%25%2B-brightgreen"></a>
  <a href="https://github.com/Thejuampi/amps-client-go/issues"><img alt="Open Issues" src="https://img.shields.io/github/issues/Thejuampi/amps-client-go"></a>
  <a href="https://github.com/Thejuampi/amps-client-go/pulls"><img alt="Open PRs" src="https://img.shields.io/github/issues-pr/Thejuampi/amps-client-go"></a>
  <a href="https://github.com/Thejuampi/amps-client-go/stargazers"><img alt="GitHub stars" src="https://img.shields.io/github/stars/Thejuampi/amps-client-go?style=social"></a>
</p>

Version: `0.1.7`

This repository provides a custom Go implementation of an AMPS client API with parity-oriented behavior for C++ `Client`/`HAClient` 5.3.5.1.

## Performance Profile

This client is engineered as a low-latency AMPS implementation in Go, with optimized hot paths for header parsing, SOW batch parsing, route dispatch, and header serialization. On current parity benchmarks, it performs ahead of the official C client for the equivalent strict parser scenarios on this environment, while preserving API compatibility and coverage gates. Full-suite tail behavior is tracked with committed nearest-rank percentile artifacts in `tools/perf_tail_baseline.json`, `tools/perf_tail_current.json`, and `tools/perf_tail_comparison.json` so p95/p99 regressions are visible over time.

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
