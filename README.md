# amps-client-go

**A high-performance Go client for [AMPS](https://www.cranktheamps.com/) — designed for broad behavioral parity, predictable latency, and native Go deployments without CGO.**

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

Version: `1.1.0`

See the [changelog and upgrade notes](CHANGELOG.md) for the breaking changes in 1.0.0.

Behavior review (2026-10-24): the [audit against C++ 5.3.5.14](docs/behavior_parity_audit.md)
identified 23 finding groups, now [fixed with 32 ordinary regression tests](docs/behavior_parity_fixes.md).
The [748-declaration inventory](docs/behavior_parity_inventory.md) distinguishes
reviewed contracts from compatibility limits. The [gap register](docs/gap_register.md)
has zero open registered entries; this is not certification of complete server parity.

---

## Why This Client?

AMPS is one of the fastest message brokers on the planet. Building a client worthy of that speed — in Go, without CGO — was the goal. This project delivers:

- 🏎️ **Predictable hot-path performance** — focused regression gates track latency and allocations across parsing, routing, streaming, and publish paths
- 🔬 **261 parity-mapped symbols** — API mapping plus a function-by-function behavior review against C++ 5.3.5.14
- 🛡️ **Blocking quality gates** — 90%+ coverage, tracked open parity gaps, and perf regression budgets on `main` pre-production and release workflows
- ⚡ **Zero-allocation critical paths** — header parse, uint decode, timeout poll, and string conversion all run at 0 allocs/op  
- 🔄 **HA failover built in** — reconnect strategies, bookmark replay, publish stores, and server chooser with no manual plumbing  

If you're building on AMPS and you need a Go-native client that doesn't compromise on performance, this is it.

---

## Performance: Internal Regression Benchmarks

These historical internal p95 snapshots use 20 samples each; timings depend on the host and are not a result for the current checkout:

| Hot Path | p95 (ns/op) | Allocs/Op |
|:---|---:|---:|
| Header parse | 19.22 | 0 |
| SOW batch parse | 73.66 | 0 |
| Route dispatch (single) | 54.82 | — |
| Route dispatch (many subscriptions) | 57.09 | — |
| Frame decode → dispatch | 77.28 | — |
| Publish send (full frame) | 45.36 | 0 |
| Uint parse (bytes) | 6.91 | 0 |
| Stream dequeue | 32.29 | — |
| Stream timeout poll | 10.81 | 0 |
| Header reset | 0.11 | 0 |
| Ack to string | 0.11 | — |

### How We Measure

- **Methodology**: `go test -bench=. -benchtime=1s -count=20` with nearest-rank percentile extraction  
- **Regression gates**: pull requests, pushes to `main`, nightly analysis, and releases run the paired gate against the committed baseline and reference; see the [performance gate method](docs/performance_gate.md)
- **Artifacts**: internal Go baseline data is committed in [`tools/perf_tail_baseline.json`](tools/perf_tail_baseline.json), [`tools/perf_tail_current.json`](tools/perf_tail_current.json), and [`tools/perf_tail_comparison.json`](tools/perf_tail_comparison.json)
- **Publication safety**: external-client comparison results remain untracked unless publication is expressly authorized; `make publication-scan` enforces this policy

---

## Feature Completeness

The client implements a broad C++-inspired surface. The current behavior audit
resolved the 23 reproduced queue, streaming, bookmark, publish-store and HA finding groups.
Symbol mapping and line coverage alone do not establish behavioral equivalence.

| Dimension | Status |
|:---|:---|
| Parity symbols mapped | **261** (no missing mapped symbols) |
| Behavior gaps open | **0** registered entries; [23 finding groups corrected](docs/behavior_parity_fixes.md) |
| Coverage gate (aggregate) | **≥ 90%** |
| Coverage gate (pure-functional) | **100%** |
| C compatibility layer (`amps/capi`) | Partial; some TLS/zlib entrypoints are emulations |
| C++ utility compat (`amps/cppcompat`) | Partial; see the individual behavior review |

### Supported Workflows

| Workflow | Primary APIs |
|:---|:---|
| Pub/sub and delta publish | `Publish`, `DeltaPublish`, `Subscribe*`, `DeltaSubscribe*` |
| SOW queries | `Sow*`, `SowAndSubscribe*`, `SowAndDeltaSubscribe*`, `SowDelete*` |
| Queue acknowledgement | `Ack`, `SetAutoAck`, `SetAckBatchSize`, `SetAckTimeout`, `FlushAcks` |
| Queue backlog requests | `SetMaxBacklog`, `SubscribeWithMaxBacklog`, `SubscribeAsyncWithMaxBacklog` |
| Bookmark replay | `BookmarkSubscribe*`, `BookmarkStore` implementations |
| Bookmark replay controls | `SetBookmarkNotFound*`, `SetFullyDurable`, `SOWHistoricalQueryAndSubscribe` |
| Publish persistence | `PublishStore` implementations, `PublishFlush` |
| HA failover & reconnect | `HAClient.ConnectAndLogon`, server chooser, delay strategies |
| TCP/TLS transport compression | `SetCompression`, `compression=zlib` on `tcp`/`tcps` URIs |
| Kerberos auth (pure Go) | `amps/auth/kerberos.NewAuthenticator` |
| Transport hooks | Transport filter, receive-start callback, global handlers |

---

## Install

```bash
go get github.com/Thejuampi/amps-client-go/amps
```

## Spark-Compatible CLI

`gofer` is the repository's spark-compatible command-line client.

```bash
go run ./cmd/gofer help
go run ./cmd/gofer ping -server localhost:9007 -type json
go run ./cmd/gofer publish -server localhost:9007 -type json -topic orders -data '{"id":1}'
```

## Fake AMPS Harness

`tools/fakeamps` now supports AMPS-style XML server configuration in addition to flags.

```bash
go run ./tools/fakeamps --sample-config
go run ./tools/fakeamps --verify-config config.xml
go run ./tools/fakeamps --dump-config config.xml
go run ./tools/fakeamps --config config.xml
```

It also serves a browser dashboard and monitoring API from the admin listener:

```bash
go run ./tools/fakeamps --config config.xml
# then open http://127.0.0.1:8085/
```

See [tools/fakeamps/README.md](tools/fakeamps/README.md) for the supported XML sections, dashboard/admin routes, the `Extensions/FakeAMPS` runtime block, and the deterministic validation rules for unsupported custom modules and UDFs.

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
make static-scan
make publication-scan
make leak-check
make fuzz-smoke
make test-race
make test
make integration-test
make integration-live-smoke
make stress-check
make parity-check
make coverage-check
make perf-check
make vuln-scan
make preprod-check
make release
```

`make static-scan` now combines `go vet`, `staticcheck`, `ineffassign`, `errcheck`, the repo-specific `patterncheck` analyzer, the public benchmark-disclosure check, and an expanded bug-focused `golangci-lint` lane.

<details>
<summary>Equivalent direct commands</summary>

```bash
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 -checks=SA* ./...
go run github.com/gordonklaus/ineffassign@v0.2.0 ./...
go run github.com/kisielk/errcheck@v1.10.0 -ignoretests ./...
go run ./tools/patterncheck ./...
go run github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.8 run --config .golangci.yml ./...
go test -count=1 ./amps
go test -count=1 ./tools/fakeamps -run '^(TestStartLeaseWatcher|TestStopLeaseWatcherStopsBackgroundLoop)$$'
go test ./amps -run=^$$ -fuzz=FuzzParseHeader -fuzztime=5s
go test ./amps -run=^$$ -fuzz=FuzzParseBookmarkToken -fuzztime=5s
go test ./amps -run=^$$ -fuzz=FuzzParseSocketOptions -fuzztime=5s
go test ./amps -run=^$$ -fuzz=FuzzCompositeMessageParser -fuzztime=5s
go test -race ./... -skip Integration
go test ./... -skip Integration
go test ./... -run Integration
go test -count=1 ./amps -run '^(TestIntegrationConnectLogonPublishSubscribe|TestIntegrationSOWAndSowAndSubscribeLifecycle|TestIntegrationQueueAutoAckBatching|TestIntegrationBookmarkResumeAcrossReconnect|TestIntegrationHAConnectAndLogonWithFailoverChooser)$$'
go test -race -shuffle=on -count=20 ./... -skip Integration
go run ./tools/paritycheck -manifest tools/parity_manifest.json
go test -count=1 ./amps/... -coverprofile=coverage.out
go run ./tools/coveragegate -profile coverage.out
make compat-check
make perf-compare-toolchains
go run ./tools/withtoolchain -toolchain go1.26.10 -- run ./tools/perfgate -baseline tools/perf_baseline.json
go run ./tools/withtoolchain -toolchain go1.26.10 -- run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...
```

Static analysis is enforced in CI with `make static-scan`, which includes vet, staticcheck correctness checks, ineffassign, errcheck on non-test packages, `patterncheck`, and an expanded `golangci-lint` lane for bug detectors such as aliasing hazards, unicode traps, resource leaks, loop-variable mistakes, compiler-directive misuse, shadowed variables, unused writes, nil/error contract mistakes, suspicious assignments, and duration arithmetic errors.

Leak detection, fuzz smoke, and repeated shuffled race stress runs are exposed separately through `make leak-check`, `make fuzz-smoke`, and `make stress-check`, and combined in `make preprod-check`.

`make vuln-scan` runs `govulncheck` with the pinned patched Go toolchain and is part of the blocking `make scan` merge gate.

The module keeps its Go 1.25 language floor and prefers **Go 1.26.10** for security.
Make pins that compiler for release builds, tests, scanners, performance, and
nested Go commands. `make compat-check` uses exact Go 1.25.13 separately, without
automatic upgrades. Applications consuming this module must also use a patched
Go compiler; the dependency's toolchain preference does not select theirs.

`make perf-compare-toolchains` compares the historical floor and the Go 1.27.3
candidate. For the security upgrade comparison, set
`PERF_COMPARE_NEW_TOOLCHAIN=go1.26.10 PERF_COMPARE_NEW_LABEL=go1.26.10`.
Raw outputs and benchstat results go under `.tmp/perf/`. The comparison is
diagnostic; the release must still pass `make perf-check` with the committed
baseline and unchanged limits. A performance-only promotion additionally needs
a measured client speedup without unexplained allocation regressions.

GitHub Actions now enforce an Ubuntu analysis job, a cross-platform test matrix, and a scheduled nightly pre-production workflow, with optional live AMPS smoke coverage whenever `AMPS_TEST_*` secrets are configured.

Coverage gating is expected before merge for `./amps/...`: aggregate `>=90.0%`, pure-functional files `100.0%`, and I/O/stateful files `>=80.0%` as enforced by `tools/coveragegate/main.go`.

PowerShell note: quote the coverprofile flag if needed, for example `go test -count=1 ./amps/... '-coverprofile=coverage.out'`.

</details>

## License

MIT. See [LICENSE](LICENSE).
