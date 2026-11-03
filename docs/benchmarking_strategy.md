# Benchmarking Strategy

This document defines benchmark coverage for `amps/client.go` and `amps/ha_client.go` against deterministic fake server scenarios.

## Goals

- Keep low-noise micro regressions visible in PRs.
- Measure realistic end-to-end behavior against `tools/fakeamps`.
- Track HA reconnect/failover recovery characteristics.
- Preserve reproducible internal Go baselines over time.

## Benchmark Layers

1. `micro` (PR gate)

- Isolated hot paths: send, parse, route dispatch, stream queue, ack bookkeeping.

1. `component` (PR gate or soft gate)

- One client + one fake server.
- Focus: queue pull/backlog behavior, filter variants, command lifecycle.

1. `integration` (nightly)

- End-to-end API roundtrip with fake server.
- Focus: connect/logon, publish, subscribe/sow, queue ack flow.

1. `ha_integration` (nightly)

- `HAClient` failover/reconnect/recovery with chooser and delay strategy variations.

1. `soak` (scheduled)

- Long-running reconnect + queue + replay stability checks.

## Coverage Matrix

- `Client.Connect` / `Client.Logon`: micro + integration.
- `Client.ExecuteAsync` route/ack flow: micro + component.
- `Client.Publish` / `Client.DeltaPublish`: micro + integration.
- `Client.Subscribe` / `Sow` / `SowAndSubscribe`: component + integration.
- Heartbeat lifecycle: component + soak.
- Queue lease/ack interactions: component + integration.
- `HAClient.connectAndLogon`: ha_integration + soak.
- Reconnect strategy and chooser behavior: ha_integration.
- Post-logon replay/resubscribe path: ha_integration.

## Naming Standard

Use benchmark names that encode scenario class and dimensions.

- `BenchmarkMicroClient_*`
- `BenchmarkComponentClient_*`
- `BenchmarkIntegrationClient_*`
- `BenchmarkIntegrationHA_*`
- `BenchmarkSoakHA_*`

Sub-benchmark dimensions order:
`flow/transport/auth/queue/filter/topology/payload`

## Baseline Process

1. Environment controls

- Run the pinned reference and candidate on the same runner under low background load.
- Fixed fake server flags and payload profiles.
- Fixed capture settings (`samples`, benchtime, timeout).

1. Go baseline

- Keep `tools/perf_baseline.json` unchanged and use `tools/perf_reference.json` to pin the reference commit, benchmark harness, and Go 1.26.10 toolchain.
- The module minimum remains Go 1.25; Go 1.25.13 is the compatibility toolchain and Go 1.26.10 is the release and performance toolchain.
- `make perf-check` compares six alternating same-runner pairs for each benchmark; every pair is evaluated, with mixed results blocking as inconclusive.
- Use `make perf-compare-toolchains` for a diagnostic comparison of exact Go 1.25.13 and Go 1.27.3; it stores raw outputs and a benchstat report under `.tmp/perf/`.
- Do not refresh `tools/perf_baseline.json` or promote a performance-only release for neutral or noisy comparisons, or for unexplained allocation regressions.
- Keep the existing benchmark group overrides in the baseline and the unchanged default threshold for the rest.
- Capture tails with `tools/perfreport capture-go` for broader profiles.

The full pairing, normalization, result, and artifact contract is in
[Performance Gate](performance_gate.md).

1. Internal comparison artifacts

- Save raw Go output, summarized tails (p50/p95/p99), and metadata (`profile`, `source_command`, commit SHA in CI).
- Keep toolchain comparisons under `.tmp/perf/`; do not commit generated reports.

## External Benchmark Publication Policy

- External-client benchmark results are private evaluation material unless their publication is expressly authorized in writing.
- Do not commit raw samples, summarized results, winner labels, ratios, or reports that compare this project with a vendor-supplied or third-party client.
- Keep permitted internal evaluation output under `.tmp/perf/external/`.
- Run `make publication-scan` before every release. The check rejects public comparison claims and external-client result artifacts.

## Gate Policy

The blocking micro gate applies the committed 10% default or 25% configured
group limit, plus the existing 0.1 ns comparison slack. Each candidate sample
must also meet its committed allocation budget. PR, push, nightly and release
gates use the same rules; `FAIL` and `INCONCLUSIVE` both block.

## Tooling Targets

- Benchmarks: `amps/perf_benchmark_test.go`, `amps/perf_api_integration_benchmark_test.go`, `amps/perf_client_fakeamps_benchmark_test.go`, `amps/perf_ha_fakeamps_benchmark_test.go`.
- Gating: `tools/perfgate/main.go`.
- Tail capture/reporting: `tools/perfreport/main.go`.
- Scenario profiles: `tools/perf/profiles/*.json`.
