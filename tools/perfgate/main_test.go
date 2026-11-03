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
	if summary["BenchmarkFoo"].AllocsOp != 3 {
		t.Fatalf("expected maximum allocs/op 3, got %v", summary["BenchmarkFoo"].AllocsOp)
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

func TestFinalFailureExitCodeHonorsReportOnly(t *testing.T) {
	var failures = []gateFailure{{Name: "BenchmarkFoo", Message: "regression"}}
	if finalFailureExitCode(failures, true) != 0 {
		t.Fatalf("expected report-only failures to exit successfully")
	}
}

func TestFinalFailureExitCodeFailsRegressions(t *testing.T) {
	var failures = []gateFailure{{Name: "BenchmarkFoo", Message: "regression"}}
	if finalFailureExitCode(failures, false) != 2 {
		t.Fatalf("expected enforced failures to exit 2")
	}
}

func TestRunPerfGateCollectsOneFixedSampleWithoutFavorableRetries(t *testing.T) {
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

	attempts, err := runPerfGate(runner, baseline, 10, []string{"BenchmarkBar", "BenchmarkFoo"}, "1s", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("expected one fixed sample, got %d", len(attempts))
	}
	if len(attempts[0].Failures) != 1 {
		t.Fatalf("expected the regression to remain visible, got %d", len(attempts[0].Failures))
	}
	if len(namesPerCall) != 1 || len(namesPerCall[0]) != 2 {
		t.Fatalf("expected one collection of all benchmarks, got %#v", namesPerCall)
	}
	if attempts[0].Benchtime != "1s" || attempts[0].Samples != 5 {
		t.Fatalf("unexpected fixed collection settings: %+v", attempts[0])
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

	attempts, err := runPerfGate(runner, baseline, 10, []string{"BenchmarkFoo"}, "1s", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("expected one sample, got %d", len(attempts))
	}
	if len(attempts[0].Failures) != 1 {
		t.Fatalf("expected final failure to persist, got %d", len(attempts[0].Failures))
	}
	if attempts[0].Failures[0].Name != "BenchmarkFoo" {
		t.Fatalf("unexpected failing benchmark %q", attempts[0].Failures[0].Name)
	}
}
