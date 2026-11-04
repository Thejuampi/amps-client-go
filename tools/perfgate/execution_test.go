package main

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"strings"
	"testing"
	"time"
)

func TestVerifyBenchmarkBuildInfoPreservesReadFailure(t *testing.T) {
	var failure = errors.New("cannot read build info")
	if err := verifyBenchmarkBuildInfo(nil, failure, "go1.26.10"); !errors.Is(err, failure) {
		t.Fatalf("build info error was not wrapped: %v", err)
	}
}

func TestVerifyBenchmarkBuildInfoReportsToolchainMismatch(t *testing.T) {
	var err = verifyBenchmarkBuildInfo(&debug.BuildInfo{GoVersion: "go1.25.13"}, nil, "go1.26.10")
	if err == nil || !strings.Contains(err.Error(), "go1.25.13") || strings.Contains(err.Error(), "<nil>") {
		t.Fatalf("toolchain mismatch lost actual version or reported a nil error: %v", err)
	}
	if err = verifyBenchmarkBuildInfo(&debug.BuildInfo{GoVersion: "go1.26.10"}, nil, "go1.26.10"); err != nil {
		t.Fatal(err)
	}
}

func TestMatchingDigestErrorPreservesCauseAndReportsMismatch(t *testing.T) {
	var failure = errors.New("cannot read digest")
	if err := matchingDigestError("candidate source inputs changed", "expected", "actual", failure); !errors.Is(err, failure) {
		t.Fatalf("digest read error was not wrapped: %v", err)
	}
	var err = matchingDigestError("candidate source inputs changed", "expected", "actual", nil)
	if err == nil || !strings.Contains(err.Error(), "candidate source inputs changed") || strings.Contains(err.Error(), "<nil>") {
		t.Fatalf("digest mismatch was not reported correctly: %v", err)
	}
	if err = matchingDigestError("candidate source inputs changed", "expected", "expected", nil); err != nil {
		t.Fatal(err)
	}
}

func TestFinalizePairedReportDoesNotApproveUnserializableResult(t *testing.T) {
	var report = pairedReport{Verdict: "PASS", Results: []pairedResult{{NormalizedNS: math.NaN()}}}
	var called bool
	var code, err = finalizePairedReport(&report, 0, nil, func([]byte) error { called = true; return nil })
	if err == nil || code == 0 || report.Verdict != "INCONCLUSIVE" || report.Error == "" || called {
		t.Fatalf("unserializable result was approved: code=%d err=%v report=%+v persisted=%v", code, err, report, called)
	}
}

func TestFinalizePairedReportPreservesCleanupAndPersistenceErrors(t *testing.T) {
	var cleanupFailure = errors.New("cleanup failed")
	var persistenceFailure = errors.New("persistence failed")
	var report = pairedReport{Verdict: "PASS"}
	var code, err = finalizePairedReport(&report, 0, func() error { return cleanupFailure }, func([]byte) error { return persistenceFailure })
	if code == 0 || !errors.Is(err, cleanupFailure) || !errors.Is(err, persistenceFailure) || report.Verdict != "INCONCLUSIVE" {
		t.Fatalf("finalization errors were lost: code=%d err=%v report=%+v", code, err, report)
	}
}

func TestCollectPairsRejectsSeventhPairBeforeRunningCommands(t *testing.T) {
	var calls int
	var runner = func(benchmarkRequest) (benchmarkRun, error) {
		calls++
		return benchmarkRun{}, errors.New("runner should not execute")
	}
	var _, _, err = collectPairs(runner, baselineFile{Benchmarks: map[string]benchmarkBaseline{"BenchmarkFoo": {NSOp: 100}}}, 10, 7, time.Second)
	if err == nil || calls != 0 {
		t.Fatalf("seven pairs were not rejected before execution: error=%v calls=%d", err, calls)
	}
}

func TestExecutePairedRejectsSeventhPairBeforeCreatingArtifacts(t *testing.T) {
	var output = filepath.Join(t.TempDir(), "artifacts")
	var code = executePaired("missing-baseline", "missing-reference", output, "1s", 7)
	if _, err := os.Stat(output); code == 0 || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid seven-pair run created artifacts or passed: exit=%d stat=%v", code, err)
	}
}

func TestPairedReportRecordsActualVerifierDigest(t *testing.T) {
	var output = filepath.Join(t.TempDir(), "artifacts")
	if code := executePaired("missing-baseline", "missing-reference", output, "1s", 6); code == 0 {
		t.Fatal("invalid reference unexpectedly passed")
	}
	var entries, err = os.ReadDir(output)
	if err != nil || len(entries) != 1 || !entries[0].IsDir() {
		t.Fatalf("missing run artifacts: %v, %v", entries, err)
	}
	var data []byte
	data, err = os.ReadFile(filepath.Join(output, entries[0].Name(), "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report map[string]any
	if err = json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	var executable string
	executable, err = os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var want string
	want, err = fileDigest(executable)
	if err != nil || report["verifier_sha256"] != want {
		t.Fatalf("verifier digest=%v, want %s; error=%v", report["verifier_sha256"], want, err)
	}
}

func TestFinalizePairedReportDoesNotPreservePassAfterCleanupFailure(t *testing.T) {
	var report = pairedReport{Verdict: "PASS"}
	var saved pairedReport
	var code, err = finalizePairedReport(&report, 0, func() error { return errors.New("cleanup failed") }, func(data []byte) error { return json.Unmarshal(data, &saved) })
	if err == nil || code == 0 || report.Verdict != "INCONCLUSIVE" || saved.Verdict != "INCONCLUSIVE" || saved.Error == "" {
		t.Fatalf("code=%d err=%v report=%+v saved=%+v", code, err, report, saved)
	}
}

func TestFinalizePairedReportDoesNotApproveIfReportCannotBeSaved(t *testing.T) {
	var report = pairedReport{Verdict: "PASS"}
	var code, err = finalizePairedReport(&report, 0, nil, func([]byte) error { return errors.New("disk full") })
	if err == nil || code == 0 || report.Verdict != "INCONCLUSIVE" {
		t.Fatalf("code=%d err=%v report=%+v", code, err, report)
	}
}

func TestCollectPairsAlternatesOrderAndRetainsEveryObservation(t *testing.T) {
	var calls []string
	var environment = benchmarkEnvironment{GOOS: "linux", GOARCH: "amd64", CPU: "cpu", Package: "amps", Procs: 4}
	var runner pairedRunner = func(request benchmarkRequest) (benchmarkRun, error) {
		calls = append(calls, request.Side)
		var n uint64 = 100
		if request.Pair >= 0 {
			n = request.Iterations
		}
		return benchmarkRun{Environment: environment, Measurements: map[string][]benchmarkResult{request.Name: {{NSOp: 100, N: n, DurationNS: int64(time.Second)}}}}, nil
	}
	var results, _, err = collectPairs(runner, baselineFile{Benchmarks: map[string]benchmarkBaseline{"BenchmarkFoo": {NSOp: 100}}}, 10, 6, time.Second)
	var want = []string{"reference", "candidate", "reference", "candidate", "candidate", "reference", "reference", "candidate", "candidate", "reference", "reference", "candidate", "candidate", "reference"}
	if err != nil || !reflect.DeepEqual(calls, want) || len(results) != 1 || len(results[0].Pairs) != 6 || results[0].Verdict != "PASS" {
		t.Fatalf("calls=%v results=%+v error=%v", calls, results, err)
	}
}

func TestCollectPairsRejectsEnvironmentChangesAndExecutionFailure(t *testing.T) {
	for _, field := range []string{"cpu", "os", "arch", "package", "workers", "command", "samples"} {
		t.Run(field, func(t *testing.T) {
			var runner pairedRunner = func(request benchmarkRequest) (benchmarkRun, error) {
				var n uint64 = 100
				if request.Pair >= 0 {
					n = request.Iterations
				}
				var run = testBenchmarkRun(request.Name, n, time.Second)
				if request.Side == "candidate" && request.Pair == 1 {
					switch field {
					case "cpu":
						run.Environment.CPU = "faster"
					case "os":
						run.Environment.GOOS = "windows"
					case "arch":
						run.Environment.GOARCH = "arm64"
					case "package":
						run.Environment.Package = "other"
					case "workers":
						run.Environment.Procs = 8
					case "command":
						return run, errors.New("benchmark process failed")
					case "samples":
						run.Measurements[request.Name] = nil
					}
				}
				return run, nil
			}
			var _, _, err = collectPairs(runner, baselineFile{Benchmarks: map[string]benchmarkBaseline{"BenchmarkFoo": {NSOp: 100}}}, 10, 6, time.Second)
			if err == nil {
				t.Fatal("invalid comparison was accepted")
			}
		})
	}
}

func TestCollectPairsPreservesConfirmedAllocationFailureBeforeCommandError(t *testing.T) {
	var runner pairedRunner = func(request benchmarkRequest) (benchmarkRun, error) {
		if request.Pair > 0 {
			return benchmarkRun{}, errors.New("sample process failed")
		}
		var n uint64 = 100
		if request.Pair >= 0 {
			n = request.Iterations
		}
		var result = benchmarkResult{NSOp: 100, N: n, DurationNS: int64(time.Second)}
		if request.Pair == 0 && request.Side == "candidate" {
			result.AllocsOp = 1
			result.MemAllocs = n
		}
		return benchmarkRun{Environment: testBenchmarkEnvironment, Measurements: map[string][]benchmarkResult{request.Name: {result}}}, nil
	}
	var results, _, err = collectPairs(runner, baselineFile{Benchmarks: map[string]benchmarkBaseline{"BenchmarkFoo": {NSOp: 100}}}, 10, 6, time.Second)
	var verdict, code = pairedVerdict(results)
	if err == nil || verdict != "FAIL" || code != 2 {
		t.Fatalf("confirmed failure was lost: results=%+v, error=%v", results, err)
	}
}
