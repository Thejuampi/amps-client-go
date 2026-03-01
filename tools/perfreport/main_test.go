package main

import (
	"strings"
	"testing"
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
		"BenchmarkAPIIntegrationClientConnectLogon",
		"BenchmarkAPIIntegrationClientPublish",
		"BenchmarkAPIIntegrationClientSubscribe",
		"BenchmarkAPIIntegrationHAConnectAndLogon",
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
		"OfficialCIntegrationConnectLogon",
		"OfficialCIntegrationPublish",
		"OfficialCIntegrationSubscribe",
		"OfficialCIntegrationHAConnectAndLogon",
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
