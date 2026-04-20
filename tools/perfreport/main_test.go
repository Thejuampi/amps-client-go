package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
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

func TestRequiredGoBenchmarksForContentionPublishProfile(t *testing.T) {
	var required = requiredGoBenchmarksForProfile("contention-publish")
	if len(required) != 1 {
		t.Fatalf("expected one required Go benchmark for contention-publish profile, got %d", len(required))
	}
	if required[0] != "BenchmarkAPIIntegrationClientPublishConcurrent" {
		t.Fatalf("unexpected required Go benchmark %q", required[0])
	}
}

func TestRequiredGoBenchmarksForContentionPublishNoAckProfile(t *testing.T) {
	var required = requiredGoBenchmarksForProfile("contention-publish-noack")
	if len(required) != 1 {
		t.Fatalf("expected one required Go benchmark for contention-publish-noack profile, got %d", len(required))
	}
	if required[0] != "BenchmarkAPIIntegrationClientPublishConcurrentNoAck" {
		t.Fatalf("unexpected required Go benchmark %q", required[0])
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

func TestRequiredCBenchmarksForContentionPublishProfile(t *testing.T) {
	var required = requiredCBenchmarksForProfile("contention-publish")
	if len(required) != 1 {
		t.Fatalf("expected one required C benchmark for contention-publish profile, got %d", len(required))
	}
	if required[0] != "OfficialCIntegrationPublishConcurrent" {
		t.Fatalf("unexpected required C benchmark %q", required[0])
	}
}

func TestRequiredCBenchmarksForContentionPublishNoAckProfile(t *testing.T) {
	var required = requiredCBenchmarksForProfile("contention-publish-noack")
	if len(required) != 1 {
		t.Fatalf("expected one required C benchmark for contention-publish-noack profile, got %d", len(required))
	}
	if required[0] != "OfficialCIntegrationPublishConcurrentNoAck" {
		t.Fatalf("unexpected required C benchmark %q", required[0])
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

func TestCompareMergedIncludesBaselineOnlyRows(t *testing.T) {
	tempDir := t.TempDir()
	baselinePath := filepath.Join(tempDir, "baseline.json")
	currentPath := filepath.Join(tempDir, "current.json")
	outPath := filepath.Join(tempDir, "comparison.json")

	baseline := mergedFile{
		GeneratedAtUTC: "2026-03-21T00:00:00Z",
		Rows: []mergedRow{
			{BenchmarkID: "bench-a", Tier: "offline", WinnerP95: "go", Go: mergedMetrics{P95: 10}},
			{BenchmarkID: "bench-b", Tier: "integration", WinnerP95: "c", C: mergedMetrics{P95: 20}},
		},
	}
	current := mergedFile{
		GeneratedAtUTC: "2026-03-22T00:00:00Z",
		Rows: []mergedRow{
			{BenchmarkID: "bench-a", Tier: "offline", WinnerP95: "go", Go: mergedMetrics{P95: 11}},
		},
	}

	baselineData, err := json.Marshal(baseline)
	if err != nil {
		t.Fatalf("marshal baseline: %v", err)
	}
	if err := os.WriteFile(baselinePath, baselineData, 0o600); err != nil {
		t.Fatalf("write baseline: %v", err)
	}
	currentData, err := json.Marshal(current)
	if err != nil {
		t.Fatalf("marshal current: %v", err)
	}
	if err := os.WriteFile(currentPath, currentData, 0o600); err != nil {
		t.Fatalf("write current: %v", err)
	}

	if err := commandCompareMerged([]string{"-baseline", baselinePath, "-current", currentPath, "-out", outPath}); err != nil {
		t.Fatalf("compare-merged failed: %v", err)
	}

	outData, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	var comparison struct {
		Entries []struct {
			BenchmarkID string `json:"benchmark_id"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(outData, &comparison); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if len(comparison.Entries) != 2 {
		t.Fatalf("expected baseline-only rows to be retained, got %d entries", len(comparison.Entries))
	}
	foundBaselineOnly := false
	for _, entry := range comparison.Entries {
		if entry.BenchmarkID == "bench-b" {
			foundBaselineOnly = true
			break
		}
	}
	if !foundBaselineOnly {
		t.Fatalf("expected comparison output to include baseline-only benchmark")
	}
}

func TestWriteOwnerOnlyFileUsesRestrictedPermissions(t *testing.T) {
	var path = filepath.Join(t.TempDir(), "report.json")
	if err := writeOwnerOnlyFile(path, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("writeOwnerOnlyFile() error: %v", err)
	}

	var info, err = os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(path): %v", err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("writeOwnerOnlyFile mode = %o, want 0600", info.Mode().Perm())
	}
}

func TestWriteJSONMarshalFailure(t *testing.T) {
	var path = filepath.Join(t.TempDir(), "report.json")
	var err = writeJSON(path, map[string]any{"bad": make(chan int)})
	if err == nil {
		t.Fatalf("writeJSON should fail for unsupported JSON values")
	}

	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("writeJSON should not create output on marshal failure, stat err = %v", statErr)
	}
}
