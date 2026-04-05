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

func TestNSRegressionExceededAllowsTinySlack(t *testing.T) {
	if nsRegressionExceeded(22.87, 20.78, 10.0) {
		t.Fatalf("expected tiny overrun to stay within comparison slack")
	}
}

func TestNSRegressionExceededRejectsMeaningfulOverrun(t *testing.T) {
	if !nsRegressionExceeded(22.97, 20.78, 10.0) {
		t.Fatalf("expected larger overrun to fail")
	}
}

func TestRunPerfGatePassesAfterConfirmatoryRerun(t *testing.T) {
	baseline := baselineFile{
		MaxRegression: 10,
		Benchmarks: map[string]benchmarkBaseline{
			"BenchmarkFoo": {NSOp: 10, AllocsOp: 0},
			"BenchmarkBar": {NSOp: 20, AllocsOp: 1},
		},
	}
	callIndex := 0
	var namesPerCall [][]string
	var runner benchmarkRunner = func(names []string, benchtime string, samples int) (benchmarkRun, error) {
		callIndex++
		var copied = append([]string(nil), names...)
		namesPerCall = append(namesPerCall, copied)
		if callIndex == 1 {
			return benchmarkRun{
				Summary: map[string]benchmarkResult{
					"BenchmarkFoo": {NSOp: 11.2, AllocsOp: 0},
					"BenchmarkBar": {NSOp: 20, AllocsOp: 1},
				},
			}, nil
		}
		return benchmarkRun{
			Summary: map[string]benchmarkResult{
				"BenchmarkFoo": {NSOp: 10.4, AllocsOp: 0},
			},
		}, nil
	}

	attempts, err := runPerfGate(runner, baseline, 10, []string{"BenchmarkBar", "BenchmarkFoo"}, "1s", 5, "2s", 7, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(attempts) != 2 {
		t.Fatalf("expected two attempts, got %d", len(attempts))
	}
	if len(attempts[1].Failures) != 0 {
		t.Fatalf("expected confirmatory rerun to clear failures, got %d", len(attempts[1].Failures))
	}
	if len(namesPerCall) != 2 || len(namesPerCall[1]) != 1 || namesPerCall[1][0] != "BenchmarkFoo" {
		t.Fatalf("expected rerun to target only failing benchmark, got %#v", namesPerCall)
	}
	if attempts[1].Benchtime != "2s" || attempts[1].Samples != 7 {
		t.Fatalf("expected retry settings to apply, got benchtime=%q samples=%d", attempts[1].Benchtime, attempts[1].Samples)
	}
}

func TestRunPerfGateFailsWhenConfirmatoryRerunStillFails(t *testing.T) {
	baseline := baselineFile{
		MaxRegression: 10,
		Benchmarks: map[string]benchmarkBaseline{
			"BenchmarkFoo": {NSOp: 10, AllocsOp: 0},
		},
	}
	var runner benchmarkRunner = func(names []string, benchtime string, samples int) (benchmarkRun, error) {
		return benchmarkRun{
			Summary: map[string]benchmarkResult{
				"BenchmarkFoo": {NSOp: 11.5, AllocsOp: 0},
			},
		}, nil
	}

	attempts, err := runPerfGate(runner, baseline, 10, []string{"BenchmarkFoo"}, "1s", 5, "2s", 5, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(attempts) != 2 {
		t.Fatalf("expected two attempts, got %d", len(attempts))
	}
	if len(attempts[1].Failures) != 1 {
		t.Fatalf("expected final failure to persist, got %d", len(attempts[1].Failures))
	}
	if attempts[1].Failures[0].Name != "BenchmarkFoo" {
		t.Fatalf("unexpected failing benchmark %q", attempts[1].Failures[0].Name)
	}
}
