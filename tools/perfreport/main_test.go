package main

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRequiredGoBenchmarksForAllComparableProfile(t *testing.T) {
	var required = requiredGoBenchmarksForProfile("all-comparable")
	if len(required) == 0 {
		t.Fatalf("expected required Go benchmarks for all-comparable profile")
	}

	var expected = []string{
		"BenchmarkHeaderHotWrite",
		"BenchmarkStrictParityHeaderHotParse",
		"BenchmarkStrictParitySOWBatchParse",
		"BenchmarkAPIIntegrationClientPublish",
		"BenchmarkAPIIntegrationClientSubscribe",
	}

	for _, name := range expected {
		var found bool
		for _, candidate := range required {
			if candidate == name {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected required benchmark %s", name)
		}
	}
}

func TestRequiredCBenchmarksForAllComparableProfile(t *testing.T) {
	var required = requiredCBenchmarksForProfile("all-comparable")
	if len(required) == 0 {
		t.Fatalf("expected required C benchmarks for all-comparable profile")
	}

	var expected = []string{
		"OfficialCParityHeaderHotWrite",
		"OfficialCParityHeaderHotParse",
		"OfficialCParitySOWBatchParse",
		"OfficialCIntegrationPublish",
		"OfficialCIntegrationSubscribe",
	}

	for _, name := range expected {
		var found bool
		for _, candidate := range required {
			if candidate == name {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected required benchmark %s", name)
		}
	}
}

func TestMissingRequiredBenchmarksDetection(t *testing.T) {
	var file = tailFile{
		Benchmarks: map[string]tailBenchmarkStats{
			"OfficialCIntegrationPublish": {},
		},
	}

	var missing = missingRequiredBenchmarks(file, []string{"OfficialCIntegrationPublish", "OfficialCIntegrationSubscribe"})
	if len(missing) != 1 {
		t.Fatalf("expected one missing benchmark, got %d", len(missing))
	}
	if missing[0] != "OfficialCIntegrationSubscribe" {
		t.Fatalf("unexpected missing benchmark %s", missing[0])
	}
}

func TestInsufficientRequiredBenchmarkSamplesDetection(t *testing.T) {
	var file = tailFile{
		Benchmarks: map[string]tailBenchmarkStats{
			"BenchmarkHeaderHotWrite":                  {N: 20},
			"BenchmarkAPIIntegrationHAConnectAndLogon": {N: 12},
		},
	}

	var insufficient = insufficientRequiredBenchmarkSamples(file, []string{"BenchmarkHeaderHotWrite", "BenchmarkAPIIntegrationHAConnectAndLogon"}, 20)
	if len(insufficient) != 1 {
		t.Fatalf("expected one insufficient benchmark, got %d", len(insufficient))
	}
	if insufficient[0] != "BenchmarkAPIIntegrationHAConnectAndLogon" {
		t.Fatalf("unexpected insufficient benchmark %s", insufficient[0])
	}
}

func TestValidateComparableFloor(t *testing.T) {
	var summary = mergedSummary{Comparable: 4}
	var err = validateComparableFloor(summary, 7)
	if err == nil {
		t.Fatalf("expected comparable floor validation failure")
	}
	if !strings.Contains(err.Error(), "below required minimum") {
		t.Fatalf("unexpected comparable floor error: %v", err)
	}

	err = validateComparableFloor(mergedSummary{Comparable: 8}, 7)
	if err != nil {
		t.Fatalf("unexpected comparable floor error for satisfied summary: %v", err)
	}
}

func TestIsTransientCaptureGoErrorTrue(t *testing.T) {
	if !isTransientCaptureGoError("ConnectionRefusedError: dial tcp4 127.0.0.1:19000: connectex: Only one usage of each socket address") {
		t.Fatalf("expected transient socket exhaustion error to be retryable")
	}
}

func TestIsTransientCaptureGoErrorFalse(t *testing.T) {
	if isTransientCaptureGoError("syntax error in benchmark output") {
		t.Fatalf("expected non-network parse error to be non-retryable")
	}
}

func TestRunCommandWithRetryUsingRunnerRetriesTransientFailure(t *testing.T) {
	var attempts int
	var runner = func(command []string, timeout time.Duration, progressInterval time.Duration, progressLabel string) (string, error) {
		attempts++
		if attempts == 1 {
			return "connectex: Only one usage of each socket address", errors.New("transient")
		}
		return "ok", nil
	}

	var output, err = runCommandWithRetryUsingRunner(
		runner,
		[]string{"go", "test"},
		time.Now().Add(time.Second),
		10*time.Millisecond,
		"capture-go",
		2,
		0,
	)
	if err != nil || output != "ok" {
		t.Fatalf("expected retry to succeed, got output=%q err=%v", output, err)
	}
}

func TestRunCommandWithRetryUsingRunnerDoesNotRetryNonTransient(t *testing.T) {
	var attempts int
	var runner = func(command []string, timeout time.Duration, progressInterval time.Duration, progressLabel string) (string, error) {
		attempts++
		return "malformed benchmark output", errors.New("fatal")
	}

	_, err := runCommandWithRetryUsingRunner(
		runner,
		[]string{"go", "test"},
		time.Now().Add(time.Second),
		10*time.Millisecond,
		"capture-go",
		3,
		0,
	)
	if err == nil || attempts != 1 {
		t.Fatalf("expected immediate failure without retry, attempts=%d err=%v", attempts, err)
	}
}
