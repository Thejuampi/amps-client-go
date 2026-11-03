package main

import (
	"strings"
	"testing"
)

const measurementHeader = "goos: linux\ngoarch: amd64\npkg: example/amps\ncpu: test CPU\n"

func TestParseBenchmarkRunRequiresCompleteValidObservations(t *testing.T) {
	var valid = measurementHeader + "BenchmarkFoo-4 100 12.5 ns/op 0 B/op 0 allocs/op\nPASS\n"
	var cases = []struct {
		name    string
		output  string
		samples int
		valid   bool
	}{
		{"valid", valid, 1, true},
		{"missing-sample", valid, 2, false},
		{"duplicate-sample", valid + "BenchmarkFoo-4 100 12.5 ns/op 0 B/op 0 allocs/op\n", 1, false},
		{"missing-cpu", strings.ReplaceAll(valid, "cpu: test CPU\n", ""), 1, false},
		{"wrong-workers", strings.ReplaceAll(valid, "BenchmarkFoo-4", "BenchmarkFoo-8"), 1, false},
		{"nan-time", strings.ReplaceAll(valid, "12.5 ns/op", "NaN ns/op"), 1, false},
		{"infinite-time", strings.ReplaceAll(valid, "12.5 ns/op", "+Inf ns/op"), 1, false},
		{"negative-bytes", strings.ReplaceAll(valid, "0 B/op", "-1 B/op"), 1, false},
		{"nan-allocations", strings.ReplaceAll(valid, "0 allocs/op", "NaN allocs/op"), 1, false},
		{"negative-allocations", strings.ReplaceAll(valid, "0 allocs/op", "-1 allocs/op"), 1, false},
		{"invalid-iterations", strings.ReplaceAll(valid, "100 12.5", "zero 12.5"), 1, false},
		{"unexpected-benchmark", valid + "BenchmarkBar-4 100 10 ns/op 0 B/op 0 allocs/op\n", 1, false},
		{"truncated-line", valid + strings.Repeat("x", 70*1024), 1, false},
	}
	for _, entry := range cases {
		t.Run(entry.name, func(t *testing.T) {
			var run, err = parseBenchmarkRun(entry.output, []string{"BenchmarkFoo"}, entry.samples, 4)
			if (err == nil) != entry.valid {
				t.Fatalf("valid=%v, error=%v", entry.valid, err)
			}
			if entry.valid && (run.Environment.CPU != "test CPU" || run.Summary["BenchmarkFoo"].NSOp != 12.5) {
				t.Fatalf("wrong observations: %+v", run)
			}
		})
	}
}
