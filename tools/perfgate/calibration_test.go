package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

var testBenchmarkEnvironment = benchmarkEnvironment{GOOS: "linux", GOARCH: "amd64", CPU: "cpu", Package: "amps", Procs: 4}

func testBenchmarkRun(name string, n uint64, duration time.Duration) benchmarkRun {
	var observation = benchmarkResult{NSOp: 100, N: n, DurationNS: int64(duration)}
	return benchmarkRun{Environment: testBenchmarkEnvironment, Measurements: map[string][]benchmarkResult{name: {observation}}}
}

func testBenchmarkBaseline() baselineFile {
	return baselineFile{Benchmarks: map[string]benchmarkBaseline{"BenchmarkFoo": {NSOp: 100}}}
}

func TestCollectPairsCalibratesOnceAndUsesCommonCountInEveryPair(t *testing.T) {
	var calls []benchmarkRequest
	var runner pairedRunner = func(request benchmarkRequest) (benchmarkRun, error) {
		calls = append(calls, request)
		if request.Pair < 0 {
			var n uint64 = 100
			if request.Side == "candidate" {
				n = 200
			}
			var run = testBenchmarkRun(request.Name, n, time.Second)
			run.Measurements[request.Name][0].MemBytes = n * 3
			return run, nil
		}
		return testBenchmarkRun(request.Name, request.Iterations, time.Second), nil
	}
	var results, environment, err = collectPairs(runner, testBenchmarkBaseline(), 10, 6, time.Second)
	if err != nil || environment != testBenchmarkEnvironment || len(results) != 1 {
		t.Fatalf("results=%+v environment=%+v error=%v", results, environment, err)
	}
	var result = results[0]
	if result.Verdict != "PASS" || len(result.Pairs) != 6 || result.Calibration.Iterations != 250 || result.Calibration.Reference.N != 100 || result.Calibration.Candidate.N != 200 || result.Calibration.Candidate.MemBytes != 600 {
		t.Fatalf("calibration or samples lost: %+v", result)
	}
	for index, pair := range result.Pairs {
		if pair.Reference.N != 250 || pair.Candidate.N != 250 {
			t.Fatalf("pair %d did not retain the selected count: %+v", index+1, pair)
		}
	}
	var wantSides = []string{"reference", "candidate", "reference", "candidate", "candidate", "reference", "reference", "candidate", "candidate", "reference", "reference", "candidate", "candidate", "reference"}
	var sides []string
	for index, call := range calls {
		sides = append(sides, call.Side)
		if call.Name != "BenchmarkFoo" || (index < 2 && (call.Pair != -1 || call.Iterations != 0)) || (index >= 2 && (call.Pair != (index-2)/2 || call.Iterations != 250)) {
			t.Fatalf("incorrect request %d: %+v", index, call)
		}
	}
	if !reflect.DeepEqual(sides, wantSides) {
		t.Fatalf("request order = %v", sides)
	}
}

func TestCollectPairsCalibratesEachBenchmarkSeparately(t *testing.T) {
	var baseline = baselineFile{Benchmarks: map[string]benchmarkBaseline{"BenchmarkA": {NSOp: 100}, "BenchmarkB": {NSOp: 100}}}
	var seen = make(map[string]int)
	var runner pairedRunner = func(request benchmarkRequest) (benchmarkRun, error) {
		seen[request.Name]++
		var n = request.Iterations
		if request.Pair < 0 {
			n = 100
			if request.Name == "BenchmarkB" {
				n = 200
			}
			if request.Side == "candidate" {
				n++
			}
		}
		return testBenchmarkRun(request.Name, n, time.Second), nil
	}
	var results, _, err = collectPairs(runner, baseline, 10, 6, time.Second)
	if err != nil || len(results) != 2 || results[0].Name != "BenchmarkA" || results[0].Calibration.Iterations != 127 || results[1].Name != "BenchmarkB" || results[1].Calibration.Iterations != 252 || seen["BenchmarkA"] != 14 || seen["BenchmarkB"] != 14 {
		t.Fatalf("independent plans not retained: results=%+v calls=%v error=%v", results, seen, err)
	}
}

func TestCollectPairsChoosesBoundedMaximumCalibration(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		ref, candidate, want uint64
	}{
		{"minimum", 1, 1, 2},
		{"reference is larger", 1000, 1, 1250},
		{"candidate is larger", 1, 1000, 1250},
		{"maximum", maximumBenchmarkIterations, 1, maximumBenchmarkIterations},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var runner pairedRunner = func(request benchmarkRequest) (benchmarkRun, error) {
				var n = request.Iterations
				if request.Pair < 0 {
					n = tc.ref
					if request.Side == "candidate" {
						n = tc.candidate
					}
				}
				return testBenchmarkRun(request.Name, n, time.Second), nil
			}
			var result, _, err = collectPairs(runner, testBenchmarkBaseline(), 10, 6, time.Second)
			if err != nil || len(result) != 1 || result[0].Calibration.Iterations != tc.want || result[0].Verdict != "PASS" {
				t.Fatalf("result=%+v error=%v", result, err)
			}
		})
	}
}

func TestCollectPairsRejectsInvalidCalibrationAndPreservesEvidence(t *testing.T) {
	var processErr = errors.New("calibration process failed")
	for _, tc := range []struct{ side, failure string }{
		{"reference", "zero count"}, {"candidate", "zero count"},
		{"reference", "too many"}, {"candidate", "missing"},
		{"candidate", "bad duration"}, {"candidate", "changed environment"},
		{"reference", "missing os"}, {"reference", "missing arch"},
		{"reference", "missing package"}, {"reference", "missing cpu"},
		{"reference", "bad workers"},
		{"candidate", "process error"},
	} {
		t.Run(tc.side+"/"+tc.failure, func(t *testing.T) {
			var calls []benchmarkRequest
			var runner pairedRunner = func(request benchmarkRequest) (benchmarkRun, error) {
				calls = append(calls, request)
				var run = testBenchmarkRun(request.Name, 100, time.Second)
				if request.Side == tc.side && request.Pair < 0 {
					switch tc.failure {
					case "zero count":
						run.Measurements[request.Name][0].N = 0
					case "too many":
						run.Measurements[request.Name][0].N = maximumBenchmarkIterations + 1
					case "missing":
						run.Measurements[request.Name] = nil
					case "bad duration":
						run.Measurements[request.Name][0].DurationNS = 0
					case "changed environment":
						run.Environment.CPU = "other"
					case "missing os":
						run.Environment.GOOS = ""
					case "missing arch":
						run.Environment.GOARCH = ""
					case "missing package":
						run.Environment.Package = ""
					case "missing cpu":
						run.Environment.CPU = ""
					case "bad workers":
						run.Environment.Procs = 8
					case "process error":
						return run, processErr
					}
				}
				return run, nil
			}
			var results, _, err = collectPairs(runner, testBenchmarkBaseline(), 10, 6, time.Second)
			var wantCalls = 1
			if tc.side == "candidate" {
				wantCalls = 2
			}
			if err == nil || len(results) != 1 || results[0].Verdict != "INCONCLUSIVE" || len(calls) != wantCalls || len(results[0].Pairs) != 0 {
				t.Fatalf("calls=%+v results=%+v error=%v", calls, results, err)
			}
			if tc.side == "candidate" && (results[0].Calibration.Reference == nil || results[0].Calibration.Reference.N != 100) {
				t.Fatalf("valid reference calibration lost: %+v", results[0])
			}
			if tc.failure == "process error" && !errors.Is(err, processErr) {
				t.Fatalf("process error lost: %v", err)
			}
		})
	}
}

func TestCollectPairsRejectsIncorrectOrChangingCounts(t *testing.T) {
	for _, tc := range []struct {
		name, side string
		pair       int
	}{
		{"unequal sides", "candidate", 0},
		{"count changes later", "reference", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var runner pairedRunner = func(request benchmarkRequest) (benchmarkRun, error) {
				var n = request.Iterations
				if request.Pair < 0 {
					n = 100
				} else if request.Pair == tc.pair && request.Side == tc.side {
					n++
				}
				return testBenchmarkRun(request.Name, n, time.Second), nil
			}
			var results, _, err = collectPairs(runner, testBenchmarkBaseline(), 10, 6, time.Second)
			if err == nil || !strings.Contains(err.Error(), "iteration") || len(results) != 1 || results[0].Verdict == "PASS" || results[0].Calibration.Iterations != 125 || len(results[0].Pairs) != tc.pair {
				t.Fatalf("results=%+v error=%v", results, err)
			}
		})
	}
}

func TestCollectPairsInsufficientDurationIsInconclusiveWithoutRetry(t *testing.T) {
	for _, tc := range []struct {
		name              string
		n                 uint64
		duration, minimum time.Duration
		want              string
	}{
		{"one ns short", 100, time.Second - 1, time.Second, "INCONCLUSIVE"},
		{"exact duration", 100, time.Second, time.Second, "PASS"},
		{"longer request", 100, time.Second, 2 * time.Second, "INCONCLUSIVE"},
		{"below cap", 799999999, time.Second - 1, time.Second, "INCONCLUSIVE"},
		{"at cap", maximumBenchmarkIterations, time.Nanosecond, time.Second, "PASS"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			var runner pairedRunner = func(request benchmarkRequest) (benchmarkRun, error) {
				calls++
				var n = tc.n
				if request.Pair >= 0 {
					n = request.Iterations
				}
				return testBenchmarkRun(request.Name, n, tc.duration), nil
			}
			var results, _, err = collectPairs(runner, testBenchmarkBaseline(), 10, 6, tc.minimum)
			if err != nil || len(results) != 1 || results[0].Verdict != tc.want || calls != 14 || len(results[0].Pairs) != 6 || (tc.want == "INCONCLUSIVE") != (len(results[0].MeasurementIssues) > 0) {
				t.Fatalf("calls=%d results=%+v error=%v", calls, results, err)
			}
		})
	}
}

func TestIncompleteMeasurementRetainsPlanAndCompletedHalfPair(t *testing.T) {
	var failure = errors.New("candidate process failed")
	var runner pairedRunner = func(request benchmarkRequest) (benchmarkRun, error) {
		if request.Pair == 0 && request.Side == "candidate" {
			return benchmarkRun{}, failure
		}
		var n uint64 = 100
		if request.Pair >= 0 {
			n = request.Iterations
		}
		return testBenchmarkRun(request.Name, n, time.Second), nil
	}
	var results, environment, err = collectPairs(runner, testBenchmarkBaseline(), 10, 6, time.Second)
	if !errors.Is(err, failure) || len(results) != 1 || environment != testBenchmarkEnvironment || results[0].IncompletePair == nil || results[0].IncompletePair.Reference == nil || results[0].IncompletePair.Candidate != nil {
		t.Fatalf("results=%+v environment=%+v error=%v", results, environment, err)
	}
	var data, marshalErr = json.Marshal(pairedReport{Verdict: "INCONCLUSIVE", Benchtime: "1s", PairCount: 6, Environment: environment, Results: results, Error: err.Error()})
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	var saved pairedReport
	if err = json.Unmarshal(data, &saved); err != nil || saved.Benchtime != "1s" || saved.PairCount != 6 || saved.Environment != environment || len(saved.Results) != 1 || saved.Results[0].Calibration.Reference.N != 100 || saved.Results[0].Calibration.Candidate.N != 100 || saved.Results[0].Calibration.Iterations != 125 || saved.Results[0].IncompletePair.Reference.N != 125 || !strings.Contains(saved.Error, failure.Error()) {
		t.Fatalf("saved report=%+v error=%v", saved, err)
	}
}

func TestShortMeasurementsDoNotHideValidAllocationFailure(t *testing.T) {
	var runner pairedRunner = func(request benchmarkRequest) (benchmarkRun, error) {
		var duration = time.Second
		if request.Pair == 1 && request.Side == "reference" {
			duration--
		}
		var n uint64 = 100
		if request.Pair >= 0 {
			n = request.Iterations
		}
		var run = testBenchmarkRun(request.Name, n, duration)
		if request.Pair == 0 && request.Side == "candidate" {
			run.Measurements[request.Name][0].MemAllocs = n
			run.Measurements[request.Name][0].AllocsOp = 1
		}
		return run, nil
	}
	var results, _, err = collectPairs(runner, testBenchmarkBaseline(), 10, 6, time.Second)
	if err != nil || len(results) != 1 || results[0].Verdict != "FAIL" || len(results[0].MeasurementIssues) != 1 {
		t.Fatalf("valid failure lost: %+v error=%v", results, err)
	}
}

func TestShortMeasurementAloneCannotEstablishAllocationFailure(t *testing.T) {
	var runner pairedRunner = func(request benchmarkRequest) (benchmarkRun, error) {
		var duration = time.Second
		if request.Pair == 0 && request.Side == "candidate" {
			duration--
		}
		var n uint64 = 100
		if request.Pair >= 0 {
			n = request.Iterations
		}
		var run = testBenchmarkRun(request.Name, n, duration)
		if request.Pair == 0 && request.Side == "candidate" {
			run.Measurements[request.Name][0].MemAllocs = n
			run.Measurements[request.Name][0].AllocsOp = 1
		}
		return run, nil
	}
	var results, _, err = collectPairs(runner, testBenchmarkBaseline(), 10, 6, time.Second)
	if err != nil || len(results) != 1 || results[0].Verdict != "INCONCLUSIVE" || len(results[0].MeasurementIssues) != 1 {
		t.Fatalf("invalid sample determined verdict: %+v error=%v", results, err)
	}
}

func TestParseRequestedBenchmarkRunEnforcesPlan(t *testing.T) {
	var output = measurementHeader + "PERFGATE_EXACT {\"name\":\"BenchmarkFoo\",\"n\":2,\"duration_ns\":27,\"mem_allocs\":1,\"mem_bytes\":8}\nBenchmarkFoo-4 2 13 ns/op 4 B/op 0 allocs/op\nPASS\n"
	var request = benchmarkRequest{Name: "BenchmarkFoo", Side: "candidate", Pair: 0, Iterations: 3}
	if _, err := parseRequestedBenchmarkRun(output, request); err == nil || !strings.Contains(err.Error(), "iteration") {
		t.Fatalf("unexpected count accepted: %v", err)
	}
	request.Iterations = 2
	if run, err := parseRequestedBenchmarkRun(output, request); err != nil || run.Measurements[request.Name][0].N != 2 {
		t.Fatalf("matching count rejected: %+v error=%v", run, err)
	}
	request.Pair = -1
	request.Iterations = 0
	if _, err := parseRequestedBenchmarkRun(output, request); err != nil {
		t.Fatalf("calibration must retain its measured count: %v", err)
	}
}

func TestBenchmarkLogPersistsOutputAndPreservesCommandFailure(t *testing.T) {
	var failure = errors.New("exit status 42")
	var directory = t.TempDir()
	var logPath = filepath.Join(directory, "sample.txt")
	var output = []byte("raw benchmark output\n")
	var err = recordBenchmarkOutput(logPath, output, failure)
	var saved, readErr = os.ReadFile(logPath)
	if !errors.Is(err, failure) || !strings.Contains(err.Error(), logPath) || readErr != nil || string(saved) != string(output) {
		t.Fatalf("log=%q error=%v read error=%v", saved, err, readErr)
	}
	if err = recordBenchmarkOutput(directory, output, failure); !errors.Is(err, failure) || !strings.Contains(err.Error(), directory) {
		t.Fatalf("simultaneous write failure hid command failure: %v", err)
	}
	if err = recordBenchmarkOutput(logPath, output, nil); err != nil {
		t.Fatalf("successful log write failed: %v", err)
	}
}

func TestBenchmarkArgumentsUseCalibrationDurationThenFixedIterations(t *testing.T) {
	var calibration = benchmarkRequest{Side: "reference", Name: "BenchmarkFoo", Pair: -1}
	var sample = benchmarkRequest{Side: "candidate", Name: "BenchmarkFoo", Pair: 3, Iterations: 12345}
	for _, tc := range []struct {
		request benchmarkRequest
		want    string
	}{{calibration, "-test.benchtime=2s"}, {sample, "-test.benchtime=12345x"}} {
		var args, err = benchmarkArguments(tc.request, 2*time.Second)
		if err != nil || !strings.Contains(strings.Join(args, " "), tc.want) {
			t.Fatalf("request=%+v args=%v error=%v", tc.request, args, err)
		}
	}
	for _, request := range []benchmarkRequest{
		{Side: "reference", Pair: -1, Iterations: 1},
		{Side: "candidate", Pair: 0, Iterations: 0},
		{Side: "candidate", Pair: 0, Iterations: maximumBenchmarkIterations + 1},
		{Side: "candidate", Pair: requiredPairs, Iterations: 1},
		{Side: "unknown", Pair: 0, Iterations: 1},
	} {
		if args, err := benchmarkArguments(request, time.Second); err == nil {
			t.Fatalf("invalid request accepted: %+v args=%v", request, args)
		}
	}
	if _, err := benchmarkArguments(calibration, time.Second-1); err == nil {
		t.Fatal("too-short calibration configuration accepted")
	}
}

func TestCollectorRejectsInvalidDurationBeforeCalibration(t *testing.T) {
	var calls int
	var runner pairedRunner = func(benchmarkRequest) (benchmarkRun, error) {
		calls++
		return benchmarkRun{}, nil
	}
	if _, _, err := collectPairs(runner, testBenchmarkBaseline(), 10, 6, time.Second-1); err == nil || calls != 0 {
		t.Fatalf("invalid duration started collection: calls=%d error=%v", calls, err)
	}
}

func TestCalibrationLogNamesIdentifyBenchmarkPhaseAndSide(t *testing.T) {
	for _, request := range []benchmarkRequest{
		{Side: "reference", Name: "BenchmarkFoo", Pair: -1},
		{Side: "candidate", Name: "BenchmarkFoo", Pair: 5, Iterations: 42},
	} {
		var path = benchmarkLogName(request)
		if !strings.Contains(path, request.Name) || !strings.Contains(path, request.Side) {
			t.Fatalf("missing benchmark or side from log name %q", path)
		}
		if request.Pair < 0 && !strings.Contains(path, "calibration") {
			t.Fatalf("calibration log not identified: %s", path)
		}
		if request.Pair >= 0 && !strings.Contains(path, fmt.Sprintf("%02d", request.Pair+1)) {
			t.Fatalf("pair log not identified: %s", path)
		}
	}
}
