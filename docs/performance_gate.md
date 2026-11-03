# Performance Gate

`make perf-check` is the blocking microbenchmark gate. It compares the current
candidate with a pinned reference on the same runner. The comparison uses the
committed baseline and its original limits; it does not replace the baseline
with measurements from a faster host.

## Fixed inputs

- Baseline: `tools/perf_baseline.json`, SHA-256
  `64db5a6cdee1ad878ba73c6a2b305fc14d10c1874b93cdd3f541d0318b515539`.
- Reference: `tools/perf_reference.json` pins commit
  `831ab636ae5e76cd06880edafb55d3e39d3bd7dd` and exact Go toolchain
  `go1.26.10`.
- The reference benchmark harness `amps/perf_benchmark_test.go` has SHA-256
  `aaa21566db17cfa5d09a3acf3413dc7076dd3035c714470f987ed2cee397912e`;
  the current harness matches it.
- The candidate is compiled from the current working tree, including local
  and untracked production sources selected by `go list`, module files and
  embeds. It is copied to an immutable snapshot. A final input digest check
  rejects source changes during measurement. The report records that digest,
  commit, dirty state and both binary hashes.

Both binaries compile only the pinned benchmark file and the same generated
bootstrap. Other test files, `TestMain` and their initialization code are
excluded. PGO, compiler experiments and inherited Go flags cannot silently
change the measurement. A different reference manifest cannot replace the
pinned commit, toolchain, baseline digest or harness digest.

The reference commit must exist in the local Git history. CI and nightly use a
full checkout, and the release workflow fetches full history. A shallow local
checkout must fetch the reference history before running the gate. The
reference and candidate use the exact pinned Go version, without an automatic
upgrade. The gate checks the fixed inputs instead of silently replacing them.
Git object replacements are disabled when reading the reference and candidate
provenance, so local `refs/replace` cannot substitute a different source tree.

## Paired measurement

For each baseline benchmark, the gate executes exactly six pairs on one runner with
four Go processors and a one-second benchmark time. The order alternates
reference/candidate and candidate/reference. Every sample is retained; there
are no selective retries or medians that hide an outlying pair.

For each pair the comparison is:

```text
normalized candidate ns/op = candidate ns/op × baseline ns/op ÷ reference ns/op
```

The normalized value is evaluated against the original benchmark baseline.
The default time regression limit is 10%; existing `volatile-micro` overrides
use 25%. The existing 0.1 ns comparison slack also remains. Allocation
regression remains 10%: the existing volatile overrides affect only time.

The historical allocation baseline was generated with Go `benchmem`, whose
`allocs/op` is the integer division `MemAllocs/N`. The hard budget retains
exactly that unit and is now enforced on every sample instead of a median.
Integer division uses the raw counters, avoiding a floating-point rounding
underflow. Every approval therefore satisfies the former allocation budget
for the same observations.

The bootstrap also preserves raw `N`, duration, allocations and bytes, with
fractional rates. If all six candidate allocation rates exceed their paired
reference rates by the original allocation percentage, the gate rejects that
sustained fractional regression too. This comparison uses rational arithmetic
on the raw counters, including the exact budget boundary. Additional pairs
are rejected: they cannot erase the evidence from the fixed six observations.
An isolated sub-unit global allocation
remains visible in the report; it is not a proof of allocation per operation.
In particular, a reported zero does not prove absolute zero allocations.

For each benchmark, all six time comparisons within the limit are `PASS`, all
six outside the limit are `FAIL`, and a mixture is `INCONCLUSIVE`. An
allocation failure takes precedence. `FAIL` and `INCONCLUSIVE` both block
verification; inconclusive results use exit code 3. Missing or invalid input
or samples cannot produce a pass.

Exit code 3 belongs to the executable. `go run` and Make translate nonzero
child codes; automation using `make perf-check` must inspect `report.json` to
distinguish `FAIL` from `INCONCLUSIVE`. Both remain blocking.

A paired comparison reduces sensitivity to differences between runner hosts;
it does not eliminate scheduler noise, frequency changes, or contention.
Investigate anomalous measurements with the retained samples and runner data
before drawing a conclusion. The paired gate may reveal regressions that were
already present in v1.1; do not change the baseline or limits merely to obtain
a passing result.

## Artifacts and CI

Each run writes `report.json`, raw reference and candidate samples, and
provenance under `.tmp/perf/gate/<runid>/`. Provenance includes the CPU,
processor count, toolchain, commits, relevant hashes and the verifier executable's
SHA-256. The artifact
directory does not contain benchmark executables or source archives.

The PR and main-branch push CI workflow has an independent blocking
`performance` job; `validate` requires it to succeed. The nightly and release
workflows also run the gate. All three upload the performance artifacts even
when the gate fails or is inconclusive. A missing artifact warning after an
early setup failure does not turn the failed gate into a pass.

`make perf-compare-toolchains` is a separate diagnostic. It runs absolute
benchmarks in report-only mode for exact Go 1.25.13 and Go 1.27.3 by default,
stores raw outputs under `.tmp/perf/`, and writes a benchstat report. It does
not replace the blocking paired gate. Use the documented toolchain override to
compare the Go 1.26.10 security release when needed.

## Historical host variance

Two hosted CI attempts measured the same unchanged commit
`12824f421ae748a1961e5d81663ee58181df0c76` on different CPU models. The
following are the original five-sample medians in ns/op, not new paired gate
results:

| Benchmark | First attempt: EPYC 9V74 | Second attempt: EPYC 9V45 |
| --- | ---: | ---: |
| `BenchmarkHeaderHotParse` | 40.57 | 20.77 |
| `BenchmarkRouteHotMultiSIDs` | 123.3 | 70.96 |

The first attempt failed the then-current absolute gate, including its
confirmatory reruns; the second passed on another host without a source change.
The host and run conditions changed together, so these numbers alone cannot
assign the difference to a particular cause. Selecting the favorable retry
would not establish a code improvement or a reliable regression result. The
paired method instead evaluates reference and candidate samples from the same
run while retaining the original budgets.

## Regressions found while repairing the gate

The first paired run found four time regressions: every pair exceeded the
original budget. Header parsing repeated prefix checks and character tests;
routing entered acknowledgement and duplicate handling even when inapplicable,
and locked the global-handler map when empty. The optimized code retains
escaped-header validation, distinguishes absent and empty bookmarks, checks
duplicates after each callback, and observes global handlers installed or
removed by callbacks. Ordinary semantic tests and source mutations cover
those boundaries.

On the same Windows host with Go 1.26.10, the subsequent complete run passed
all 21 benchmarks, retaining all 252 reference/candidate observations. Its
time results for those four paths were:

| Benchmark | Before: median | After: median | After: worst pair | Original limit |
| --- | ---: | ---: | ---: | ---: |
| `BenchmarkHeaderHotParse` | 42.092 | 35.862 | 36.263 | 37.115 |
| `BenchmarkReadHotFrameDecodeDispatch` | 147.054 | 125.919 | 128.334 | 141.560 |
| `BenchmarkRouteHotMultiSIDs` | 138.084 | 110.983 | 113.149 | 114.720 |
| `BenchmarkRouteHotSingle` | 46.635 | 38.097 | 38.495 | 46.190 |

Values are normalized ns/op, not absolute time on that host. The complete
run took 5 minutes 42 seconds. These measurements establish the result for
that source and environment; each future candidate still runs the gate.
