package main

import "testing"

func TestParseBenchOutputCollectsRepeatedSamples(t *testing.T) {
	output := `
BenchmarkFoo-20  1000000  12.0 ns/op  0 B/op  1 allocs/op
BenchmarkFoo-20  1000000  10.0 ns/op  0 B/op  3 allocs/op
BenchmarkBar-20  1000000  20.0 ns/op  0 B/op  2 allocs/op
`

	results := parseBenchOutput(output)

	if len(results["BenchmarkFoo"]) != 2 {
		t.Fatalf("expected two BenchmarkFoo samples, got %d", len(results["BenchmarkFoo"]))
	}
	if len(results["BenchmarkBar"]) != 1 {
		t.Fatalf("expected one BenchmarkBar sample, got %d", len(results["BenchmarkBar"]))
	}
}

func TestSummarizeBenchOutputUsesMedianSamples(t *testing.T) {
	parsed := map[string][]benchmarkResult{
		"BenchmarkFoo": {
			{NSOp: 12, AllocsOp: 1},
			{NSOp: 10, AllocsOp: 3},
			{NSOp: 11, AllocsOp: 2},
		},
	}

	summary := summarizeBenchOutput(parsed)

	if summary["BenchmarkFoo"].NSOp != 11 {
		t.Fatalf("expected median ns/op 11, got %v", summary["BenchmarkFoo"].NSOp)
	}
	if summary["BenchmarkFoo"].AllocsOp != 2 {
		t.Fatalf("expected median allocs/op 2, got %v", summary["BenchmarkFoo"].AllocsOp)
	}
}
