package main

import (
	"math"
	"testing"
)

func TestMedianFloat64IncludesBothMiddleSamples(t *testing.T) {
	if got := medianFloat64([]float64{14, 8, 12, 10}); got != 11 {
		t.Fatalf("median = %v, want 11", got)
	}
}

func TestSummarizeBenchOutputDoesNotHideAllocationSpike(t *testing.T) {
	var summary = summarizeBenchOutput(map[string][]benchmarkResult{
		"BenchmarkFoo": {{NSOp: 10}, {NSOp: 10, AllocsOp: 1}, {NSOp: 10}},
	})
	if got := summary["BenchmarkFoo"].AllocsOp; got != 1 {
		t.Fatalf("allocation maximum = %v, want 1; a median must not hide an allocation", got)
	}
}

func TestBenchmarkFailuresRejectInvalidMetrics(t *testing.T) {
	var baseline = baselineFile{Benchmarks: map[string]benchmarkBaseline{
		"BenchmarkFoo": {NSOp: 10},
	}}
	var cases = []struct {
		name   string
		result benchmarkResult
	}{
		{"nan-time", benchmarkResult{NSOp: math.NaN()}},
		{"infinite-time", benchmarkResult{NSOp: math.Inf(1)}},
		{"zero-time", benchmarkResult{}},
		{"negative-time", benchmarkResult{NSOp: -1}},
		{"nan-allocations", benchmarkResult{NSOp: 10, AllocsOp: math.NaN()}},
		{"infinite-allocations", benchmarkResult{NSOp: 10, AllocsOp: math.Inf(1)}},
		{"negative-allocations", benchmarkResult{NSOp: 10, AllocsOp: -1}},
	}
	for _, entry := range cases {
		t.Run(entry.name, func(t *testing.T) {
			var failures = benchmarkFailures(baseline, map[string]benchmarkResult{"BenchmarkFoo": entry.result}, 10, []string{"BenchmarkFoo"})
			if len(failures) == 0 {
				t.Fatal("invalid measurement was accepted")
			}
		})
	}
}

func TestRunPerfGateDoesNotDiscardEarlierRegressions(t *testing.T) {
	var baseline = baselineFile{Benchmarks: map[string]benchmarkBaseline{
		"BenchmarkFoo": {NSOp: 10},
	}}
	var calls int
	var runner benchmarkRunner = func([]string, string, int) (benchmarkRun, error) {
		calls++
		var result = benchmarkResult{NSOp: 12}
		if calls > 1 {
			result.NSOp = 10
		}
		return benchmarkRun{Summary: map[string]benchmarkResult{"BenchmarkFoo": result}}, nil
	}
	var attempts, err = runPerfGate(runner, baseline, 10, []string{"BenchmarkFoo"}, "1s", 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts[len(attempts)-1].Failures) == 0 {
		t.Fatal("an initially failing measurement was replaced by a favorable retry")
	}
}
