# AMPS Go Client

**A feature-complete, high-performance Go client for [AMPS](https://www.cranktheamps.com/) — built from scratch to match and outperform the official C/C++ client on critical hot paths.**

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

Version: `0.2.1`

---

## Why This Client?

AMPS is one of the fastest message brokers on the planet. Building a client worthy of that speed — in Go, without CGO — was the goal. This project delivers:

- 🏎️ **Faster than C on the hot path** — Go client outperforms the official C library on header parsing and SOW batch processing at p95/p99  
- 🔬 **253 parity-mapped symbols** — full `Client` and `HAClient` API surface, tested against C++ 5.3.5.1 behavior  
- 🛡️ **Production-grade quality gates** — 90%+ coverage, zero open parity gaps, enforced regression budgets on every PR  
- ⚡ **Zero-allocation critical paths** — header parse, uint decode, timeout poll, and string conversion all run at 0 allocs/op  
- 🔄 **HA failover built in** — reconnect strategies, bookmark replay, publish stores, and server chooser with no manual plumbing  

If you're building on AMPS and you need a Go-native client that doesn't compromise on performance, this is it.

---

## Performance: Go vs Official C Client

All benchmarks run on the same machine, same workload, same measurement methodology (nearest-rank percentiles, 20 samples). Lower is better.

### Hot-Path Parity Results (Go vs Official C)

These are the strict parity workloads we currently gate for C-vs-Go comparisons. Lower is better.

| Benchmark | Go p95 (ns/op) | C p95 (ns/op) | Delta | Winner |
|:---|---:|---:|---:|:---|
| **Header Parse** (strict parity) | **18.41** | 22.38 | **-17.7%** | Go |
| **SOW Batch Parse** (strict parity) | **93.34** | 123.14 | **-24.2%** | Go |
| **Header Serialize** (strict parity) | **68.88** | 69.60 | **-1.0%** | Go |
| **Publish Integration** (processed ack) | **101380** | 245025.75 | **-58.6%** | Go |
| **Subscribe Integration** (processed ack) | **105067** | 225831 | **-53.5%** | Go |

This is 5/5 wins on the in-scope hot-path parity suite (p95).

Connect-and-logon timings are tracked separately and treated as out of scope for this steady-state hot-path gate.

### Full-Suite Tail Latency (Go Internal Benchmarks)

Every hot path in the client is micro-benchmarked and tracked across commits. Here are the current numbers at p95 (20 samples each):

| Hot Path | p95 (ns/op) | Allocs/Op |
|:---|---:|---:|
| Header parse | 21.44 | 0 |
| SOW batch parse | 78.27 | 0 |
| Route dispatch (single) | 139.0 | — |
| Route dispatch (many subscriptions) | 137.7 | — |
| Frame decode → dispatch | 177.3 | — |
| Publish send (full frame) | 48.7 | 0 |
| Uint parse (bytes) | 8.87 | 0 |
| Stream dequeue | 75.39 | — |
| Stream timeout poll | 12.79 | 0 |
| Header reset | 0.13 | 0 |
| Ack serialization | 21.79 | — |

### How We Measure

- **Methodology**: `go test -bench=. -benchtime=1s -count=20` with nearest-rank percentile extraction  
- **C baselines**: compiled from the official AMPS C client library, run with the same fake server and payload profiles  
- **Regression gates**: PRs fail on >7% ns/op regression or >5% allocs/op regression against committed baselines  
- **Artifacts**: all raw data committed in [`tools/perf_tail_baseline.json`](tools/perf_tail_baseline.json), [`tools/perf_tail_current.json`](tools/perf_tail_current.json), [`tools/perf_tail_comparison.json`](tools/perf_tail_comparison.json), and [`tools/perf_side_by_side_baseline.json`](tools/perf_side_by_side_baseline.json)

---

## Feature Completeness

This isn't a minimal SDK. It's a full-surface client with behavior parity across the entire C++ `Client` and `HAClient` public API.

| Dimension | Status |
|:---|:---|
| Parity symbols mapped | **253** (zero gaps) |
| Behavior gaps open | **0** |
| Coverage gate (aggregate) | **≥ 90%** |
| Coverage gate (pure-functional) | **100%** |
| C compatibility layer (`amps/capi`) | ✅ Full |
| C++ utility compat (`amps/cppcompat`) | ✅ Full |

### Supported Workflows

| Workflow | Primary APIs |
|:---|:---|
| Pub/sub and delta publish | `Publish`, `DeltaPublish`, `Subscribe*`, `DeltaSubscribe*` |
| SOW queries | `Sow*`, `SowAndSubscribe*`, `SowAndDeltaSubscribe*`, `SowDelete*` |
| Queue acknowledgement | `Ack`, `SetAutoAck`, `SetAckBatchSize`, `SetAckTimeout`, `FlushAcks` |
| Bookmark replay | `BookmarkSubscribe*`, `BookmarkStore` implementations |
| Publish persistence | `PublishStore` implementations, `PublishFlush` |
| HA failover & reconnect | `HAClient.ConnectAndLogon`, server chooser, delay strategies |
| Kerberos auth (pure Go) | `amps/auth/kerberos.NewAuthenticator` |
| Transport hooks | Transport filter, receive-start callback, global handlers |

---

## Install

```bash
go get github.com/Thejuampi/amps-client-go/amps
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

📖 **[Full Documentation Index](docs/index.md)**

| Getting Started | Production | Reference |
|:---|:---|:---|
| [Getting Started](docs/getting_started.md) | [Queue Ack Semantics](docs/queue_ack_semantics.md) | [Client API](docs/reference_client.md) |
| [Client Entrypoints](docs/client_entrypoints.md) | [Bookmarks and Replay](docs/bookmarks_and_replay.md) | [HAClient API](docs/reference_ha_client.md) |
| [Pub/Sub and SOW](docs/pub_sub_and_sow.md) | [HA Failover](docs/ha_failover.md) | [Types and Handlers](docs/reference_types_and_handlers.md) |
| [Supported Scope](docs/supported_scope.md) | [Operational Playbook](docs/operational_playbook.md) | [C API Compat](docs/capi_reference.md) |
| | | [C++ Compat](docs/cppcompat_reference.md) |
| | | [C++ Parity Matrix](docs/cpp_to_go_parity_matrix.md) |

## Build and Test

```bash
make build
make test
make integration-test
make parity-check
make coverage-check
make release
```

<details>
<summary>Equivalent direct commands</summary>

```bash
go run ./tools/paritycheck -manifest tools/parity_manifest.json
go test -count=1 ./amps/... -coverprofile=coverage.out
go run ./tools/coveragegate -profile coverage.out
```

Coverage gating is expected before merge for `./amps/...`: aggregate `>=90.0%`, pure-functional files `100.0%`, and I/O/stateful files `>=80.0%` as enforced by `tools/coveragegate/main.go`.

PowerShell note: quote the coverprofile flag if needed, for example `go test -count=1 ./amps/... '-coverprofile=coverage.out'`.

</details>

## CLI Tool

**[gofer](https://github.com/Thejuampi/gofer)** is the official command-line interface built on top of this library.
It provides `ping`, `publish`, `subscribe`, `sow`, `sow_and_subscribe`, and `sow_delete` commands as a single native binary.

```bash
go install github.com/Thejuampi/gofer@latest
gofer ping -server tcp://localhost:9007/amps/json
```

---

## License

MIT. See [LICENSE](LICENSE).
