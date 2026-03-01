# Benchmarking Strategy

This document defines benchmark coverage for `amps/client.go` and `amps/ha_client.go` against deterministic fake server scenarios, with repeatable comparison against official C benchmarks.

## Goals

- Keep low-noise micro regressions visible in PRs.
- Measure realistic end-to-end behavior against `tools/fakeamps`.
- Track HA reconnect/failover recovery characteristics.
- Maintain stable Go-vs-C side-by-side baselines over time.

## Benchmark Layers

1. `micro` (PR gate)
- Isolated hot paths: send, parse, route dispatch, stream queue, ack bookkeeping.

2. `component` (PR gate or soft gate)
- One client + one fake server.
- Focus: queue pull/backlog behavior, filter variants, command lifecycle.

3. `integration` (nightly)
- End-to-end API roundtrip with fake server.
- Focus: connect/logon, publish, subscribe/sow, queue ack flow.

4. `ha_integration` (nightly)
- `HAClient` failover/reconnect/recovery with chooser and delay strategy variations.

5. `soak` (scheduled)
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
- Fixed host class and low background load.
- Fixed fake server flags and payload profiles.
- Fixed capture settings (`samples`, benchtime, timeout).

2. Go baseline
- Keep `tools/perf_baseline.json` for stable micro gates.
- Capture tails with `tools/perfreport capture-go` for broader profiles.

3. C baseline
- Capture with `official_c_parity_benchmark` and `tools/cperf/fakeamps_c_integration_benchmark` in the same fake server profile window.

4. Comparison artifacts
- Save raw output, summarized tails (p50/p95/p99), and metadata (`profile`, `source_command`, commit SHA in CI).

## Gate Policy

PR (`micro` and selected `component`):
- `ns/op` regression > 7% fails.
- `allocs/op` regression > 5% (or +1 alloc for small baselines) fails.

Nightly (`integration`, `ha_integration`, `soak`):
- Alert on >10% regressions.
- Fail nightly after two consecutive breaches.
- HA reconnect/recovery regressions >15% follow same policy.

Go vs C tracking:
- Alert on ratio drift >12% from baseline band per benchmark ID.

## Tooling Targets

- Benchmarks: `amps/perf_benchmark_test.go`, `amps/perf_api_integration_benchmark_test.go`, `amps/perf_client_fakeamps_benchmark_test.go`, `amps/perf_ha_fakeamps_benchmark_test.go`.
- Gating: `tools/perfgate/main.go`.
- Tail capture/reporting: `tools/perfreport/main.go`.
- Scenario profiles: `tools/perf/profiles/*.json`.
