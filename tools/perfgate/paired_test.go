package main

import (
	"math"
	"testing"
)

func constantPairs(reference, candidate float64) []benchmarkPair {
	var pairs = make([]benchmarkPair, 6)
	for index := range pairs {
		pairs[index] = benchmarkPair{
			Reference: benchmarkResult{NSOp: reference},
			Candidate: benchmarkResult{NSOp: candidate},
		}
	}
	return pairs
}

func TestPairedComparisonIsIndependentOfCommonHardwareScale(t *testing.T) {
	for _, scale := range []float64{0.5, 1, 2, 100} {
		var result, err = comparePairs("BenchmarkFoo", benchmarkBaseline{NSOp: 100}, 10, 10, constantPairs(100*scale, 105*scale))
		if err != nil || result.Verdict != "PASS" || math.Abs(result.NormalizedNS-105) > 1e-9 {
			t.Fatalf("scale %g: result=%+v err=%v", scale, result, err)
		}
	}
}

func TestPairedComparisonRejectsRegressionOnFasterHardware(t *testing.T) {
	var result, err = comparePairs("BenchmarkFoo", benchmarkBaseline{NSOp: 100}, 10, 10, constantPairs(50, 60))
	if err != nil || result.Verdict != "FAIL" {
		t.Fatalf("20%% regression on a faster CPU: result=%+v err=%v", result, err)
	}
}

func TestPairedComparisonCannotAverageAwayConflictingSamples(t *testing.T) {
	var pairs = constantPairs(100, 100)
	pairs[0].Candidate.NSOp = 120
	var result, err = comparePairs("BenchmarkFoo", benchmarkBaseline{NSOp: 100}, 10, 10, pairs)
	if err != nil || result.Verdict != "INCONCLUSIVE" {
		t.Fatalf("mixed measurements must block approval: result=%+v err=%v", result, err)
	}
}

func TestPairedComparisonPreservesExistingTimeLimits(t *testing.T) {
	var cases = []struct {
		budget  float64
		actual  float64
		verdict string
	}{
		{10, 110.1, "PASS"}, {10, 110.100001, "FAIL"},
		{25, 125.1, "PASS"}, {25, 125.100001, "FAIL"},
	}
	for _, entry := range cases {
		var result, err = comparePairs("BenchmarkFoo", benchmarkBaseline{NSOp: 100}, entry.budget, 10, constantPairs(100, entry.actual))
		if err != nil || result.Verdict != entry.verdict {
			t.Fatalf("budget=%g actual=%g: result=%+v err=%v", entry.budget, entry.actual, result, err)
		}
	}
}

func TestPairedComparisonRejectsAnyAllocationSpike(t *testing.T) {
	var pairs = constantPairs(100, 100)
	pairs[2].Candidate.AllocsOp = 1
	var result, err = comparePairs("BenchmarkFoo", benchmarkBaseline{NSOp: 100}, 10, 10, pairs)
	if err != nil || result.Verdict != "FAIL" {
		t.Fatalf("zero-allocation budget was bypassed: result=%+v err=%v", result, err)
	}
}

func TestPairedComparisonRejectsConsistentFractionalAllocationRegression(t *testing.T) {
	var pairs = constantPairs(100, 100)
	for index := range pairs {
		pairs[index].Candidate.AllocsOp = 0.5
		pairs[index].Candidate.N = 2
		pairs[index].Candidate.MemAllocs = 1
	}
	var result, err = comparePairs("BenchmarkFoo", benchmarkBaseline{NSOp: 100}, 10, 10, pairs)
	if err != nil || result.Verdict != "FAIL" {
		t.Fatalf("consistent fractional regression escaped: %+v, err=%v", result, err)
	}
}

func TestPairedComparisonRejectsSeventhPairThatWouldEraseRegression(t *testing.T) {
	var pairs = constantPairs(100, 100)
	for index := range pairs {
		pairs[index].Reference = benchmarkResult{NSOp: 100, N: 2}
		pairs[index].Candidate = benchmarkResult{NSOp: 100, N: 2, MemAllocs: 1, AllocsOp: 0.5}
	}
	var seventh = benchmarkPair{Reference: pairs[0].Candidate, Candidate: pairs[0].Candidate}
	pairs = append(pairs, seventh)
	if result, err := comparePairs("BenchmarkFoo", benchmarkBaseline{NSOp: 100}, 10, 10, pairs); err == nil {
		t.Fatalf("seventh pair erased six confirmed regressions: %+v", result)
	}
}

func TestPairedFractionalRegressionUsesExactCounterRatios(t *testing.T) {
	var pairs = constantPairs(100, 100)
	for index := range pairs {
		pairs[index].Reference = benchmarkResult{NSOp: 100, N: 1000000000, MemAllocs: 90909091, AllocsOp: float64(90909091) / 1000000000}
		pairs[index].Candidate = benchmarkResult{NSOp: 100, N: 999999999, MemAllocs: 100000000, AllocsOp: float64(100000000) / 999999999}
	}
	var result, err = comparePairs("BenchmarkFoo", benchmarkBaseline{NSOp: 100}, 10, 10, pairs)
	if err != nil || result.Verdict != "FAIL" || !result.FractionalRegression {
		t.Fatalf("exactly above 10%% was rounded into approval: %+v, err=%v", result, err)
	}
}

func TestPairedFractionalRegressionAllowsExactBudgetBoundary(t *testing.T) {
	var pairs = constantPairs(100, 100)
	for index := range pairs {
		pairs[index].Reference = benchmarkResult{NSOp: 100, N: 100, MemAllocs: 10, AllocsOp: 0.1}
		pairs[index].Candidate = benchmarkResult{NSOp: 100, N: 100, MemAllocs: 11, AllocsOp: 0.11}
	}
	var result, err = comparePairs("BenchmarkFoo", benchmarkBaseline{NSOp: 100}, 10, 10, pairs)
	if err != nil || result.Verdict != "PASS" || result.FractionalRegression {
		t.Fatalf("exactly 10%% should be within budget: %+v, err=%v", result, err)
	}
}

func TestReportedAllocationUnitsUseExactIntegerDivision(t *testing.T) {
	var result = benchmarkResult{N: 9007199254740995, MemAllocs: 27021597764222985, AllocsOp: 2.9999999999999996}
	if got := reportedAllocations(result); got != 3 {
		t.Fatalf("reported allocations = %g, want 3", got)
	}
}

func TestWholeAllocationBudgetRetainsFractionalDiagnostics(t *testing.T) {
	var pairs = constantPairs(100, 100)
	for index := range pairs {
		pairs[index].Reference.N = 1000000000
		pairs[index].Candidate.N = 1000000000
	}
	pairs[2].Candidate.MemAllocs = 1
	pairs[2].Candidate.AllocsOp = 1e-9
	var result, err = comparePairs("BenchmarkFoo", benchmarkBaseline{NSOp: 100}, 10, 10, pairs)
	if err != nil || result.Verdict != "PASS" || result.MaxAllocs != 0 || result.MaxExactAllocs != 1e-9 || result.Pairs[2].Candidate.MemAllocs != 1 {
		t.Fatalf("historical units or exact evidence lost: %+v, err=%v", result, err)
	}
}

func TestPairedAllocationApprovalImpliesHistoricalMedianBudget(t *testing.T) {
	for _, budget := range []float64{0, 2, 5} {
		for _, allocations := range []uint64{0, 1, 3, 9, 15, 18} {
			var pairs = constantPairs(100, 100)
			for index := range pairs {
				pairs[index].Reference = benchmarkResult{NSOp: 100, N: 3, MemAllocs: allocations, AllocsOp: float64(allocations) / 3}
				pairs[index].Candidate = pairs[index].Reference
			}
			var result, err = comparePairs("BenchmarkFoo", benchmarkBaseline{NSOp: 100, AllocsOp: budget}, 10, 10, pairs)
			if err != nil {
				t.Fatal(err)
			}
			if result.Verdict == "PASS" && float64(allocations/3) > budget*1.1 {
				t.Fatalf("approval relaxed historical budget %g: %+v", budget, result)
			}
		}
	}
}

func TestPairedComparisonRequiresSixValidPairs(t *testing.T) {
	var cases = []struct {
		name  string
		pairs []benchmarkPair
	}{
		{"missing", nil}, {"too-few", constantPairs(100, 100)[:5]},
		{"zero-reference", constantPairs(0, 100)},
		{"nan-reference", constantPairs(math.NaN(), 100)},
		{"nan-candidate", constantPairs(100, math.NaN())},
		{"infinite-candidate", constantPairs(100, math.Inf(1))},
	}
	for _, entry := range cases {
		t.Run(entry.name, func(t *testing.T) {
			var _, err = comparePairs("BenchmarkFoo", benchmarkBaseline{NSOp: 100}, 10, 10, entry.pairs)
			if err == nil {
				t.Fatal("incomplete or invalid observations were accepted")
			}
		})
	}
}
