# fakeamps (Side Project)

`fakeamps` is a minimal deterministic AMPS-like TCP responder used only for benchmark scaffolding.

- It is intentionally isolated under `tools/`.
- It is **not** part of the exported `amps-client-go` client API.
- It provides a simple request/ack loop for integration-tier performance experiments.

## Run

```bash
go run ./tools/fakeamps -addr 127.0.0.1:19000
```

## C Integration Harness

`tools/cperf/fakeamps_c_integration_benchmark.c` provides synthetic C-side integration benchmarks that connect to this responder.

Compile:

```bash
gcc -O2 -o tools/cperf/fakeamps_c_integration_benchmark.exe tools/cperf/fakeamps_c_integration_benchmark.c -lws2_32
```

Run (defaults to `127.0.0.1:19000`):

```bash
tools/cperf/fakeamps_c_integration_benchmark.exe
```

Override address using `PERF_FAKEAMPS_ADDR` (format: `host:port`).

## Scope

- Reads AMPS-style framed input (`4-byte length + header(+payload)`).
- Extracts `c` and optional `cid` from the JSON header.
- Returns a framed `ack` with `status=success` for known commands.
- Returns a framed `ack` with `status=failure` for unknown commands.

This helper exists to support the two-tier performance program:
1) offline parity baselines now
2) integration/HA-style synthetic workloads next
