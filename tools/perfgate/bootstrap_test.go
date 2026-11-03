package main

import (
	"strings"
	"testing"
)

func TestExactBenchmarkCountsRetainFractionalAllocations(t *testing.T) {
	var output = measurementHeader + "PERFGATE_EXACT {\"name\":\"BenchmarkFoo\",\"n\":2,\"duration_ns\":27,\"mem_allocs\":1,\"mem_bytes\":8}\nBenchmarkFoo-4 2 13 ns/op 4 B/op 0 allocs/op\nPASS\n"
	var run, err = parseExactBenchmarkRun(output, "BenchmarkFoo")
	if err != nil || len(run.Measurements["BenchmarkFoo"]) != 1 || run.Measurements["BenchmarkFoo"][0].AllocsOp != 0.5 || run.Measurements["BenchmarkFoo"][0].BytesOp != 4 || run.Measurements["BenchmarkFoo"][0].NSOp != 13.5 || run.Measurements["BenchmarkFoo"][0].MemAllocs != 1 {
		t.Fatalf("exact counters were lost: %+v, error=%v", run, err)
	}
}

func TestExactBenchmarkCountsRejectMissingDuplicateOrWrongObservations(t *testing.T) {
	var exact = "PERFGATE_EXACT {\"name\":\"BenchmarkFoo\",\"n\":2,\"duration_ns\":27,\"mem_allocs\":1,\"mem_bytes\":8}\n"
	var ordinary = measurementHeader + "BenchmarkFoo-4 2 13 ns/op 4 B/op 0 allocs/op\nPASS\n"
	for _, data := range []string{ordinary, ordinary + exact + exact, ordinary + strings.ReplaceAll(exact, "BenchmarkFoo", "BenchmarkBar"), ordinary + strings.ReplaceAll(exact, "\"n\":2,", "\"n\":0,")} {
		var _, err = parseExactBenchmarkRun(data, "BenchmarkFoo")
		if err == nil {
			t.Fatal("missing or invalid exact counters were accepted")
		}
	}
}
