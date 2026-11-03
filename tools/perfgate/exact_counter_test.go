package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Thejuampi/amps-client-go/internal/hiddenprocess"
)

func exactCounterOutput(count string, record string) string {
	return measurementHeader + "PERFGATE_EXACT " + record + "\nBenchmarkFoo-4 " + count + " 13 ns/op 4 B/op 0 allocs/op\nPASS\n"
}

func TestExactCounterParserRequiresAllFieldsAndMatchingCount(t *testing.T) {
	var good = `{"name":"BenchmarkFoo","n":2,"duration_ns":27,"mem_allocs":1,"mem_bytes":8}`
	for _, entry := range []struct {
		name, record string
	}{
		{"missing name", strings.Replace(good, `"name":"BenchmarkFoo",`, "", 1)},
		{"missing count", strings.Replace(good, `"n":2,`, "", 1)},
		{"missing duration", strings.Replace(good, `"duration_ns":27,`, "", 1)},
		{"missing allocs", strings.Replace(good, `"mem_allocs":1,`, "", 1)},
		{"missing bytes", strings.Replace(good, `,"mem_bytes":8`, "", 1)},
		{"wrong count", strings.Replace(good, `"n":2`, `"n":3`, 1)},
		{"zero count", strings.Replace(good, `"n":2`, `"n":0`, 1)},
		{"negative allocation", strings.Replace(good, `"mem_allocs":1`, `"mem_allocs":-1`, 1)},
		{"negative bytes", strings.Replace(good, `"mem_bytes":8`, `"mem_bytes":-1`, 1)},
		{"zero duration", strings.Replace(good, `"duration_ns":27`, `"duration_ns":0`, 1)},
		{"negative duration", strings.Replace(good, `"duration_ns":27`, `"duration_ns":-1`, 1)},
		{"unrepresentable historical allocations", strings.Replace(good, `"mem_allocs":1`, `"mem_allocs":9223372036854775808`, 1)},
		{"allocation overflow", strings.Replace(good, `"mem_allocs":1`, `"mem_allocs":18446744073709551616`, 1)},
		{"duration overflow", strings.Replace(good, `"duration_ns":27`, `"duration_ns":9223372036854775808`, 1)},
	} {
		t.Run(entry.name, func(t *testing.T) {
			if _, err := parseExactBenchmarkRun(exactCounterOutput("2", entry.record), "BenchmarkFoo"); err == nil {
				t.Fatalf("accepted invalid exact record %s", entry.record)
			}
		})
	}
}

func TestExactCounterParserPreservesMaximumCounters(t *testing.T) {
	var record = `{"name":"BenchmarkFoo","n":1000000000,"duration_ns":9223372036854775807,"mem_allocs":9223372036854775807,"mem_bytes":18446744073709551615}`
	var run, err = parseExactBenchmarkRun(exactCounterOutput("1000000000", record), "BenchmarkFoo")
	if err != nil {
		t.Fatal(err)
	}
	var got = run.Measurements["BenchmarkFoo"][0]
	if got.N != 1000000000 || got.MemAllocs != uint64(^uint64(0)>>1) || got.MemBytes != ^uint64(0) || got.AllocsOp != float64(uint64(^uint64(0)>>1))/1000000000 || got.BytesOp != float64(^uint64(0))/1000000000 {
		t.Fatalf("maximum counters not preserved: %+v", got)
	}
}

func TestExactCounterParserRejectsTooManyIterationsEvenWhenCountsMatch(t *testing.T) {
	var record = `{"name":"BenchmarkFoo","n":1000000001,"duration_ns":27,"mem_allocs":0,"mem_bytes":0}`
	if _, err := parseExactBenchmarkRun(exactCounterOutput("1000000001", record), "BenchmarkFoo"); err == nil {
		t.Fatal("accepted more iterations than Go's timed benchmark can run")
	}
}

func TestGeneratedBootstrapCollectsOneTimedBenchmark(t *testing.T) {
	var root = t.TempDir()
	var amps = filepath.Join(root, "amps")
	if err := os.MkdirAll(amps, 0700); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		"go.mod":        "module example.test/counter\n\ngo 1.25.0\n",
		"amps/empty.go": "package amps\n",
		"amps/perf_benchmark_test.go": `package amps
import "testing"
import "fmt"
var sink *int
func BenchmarkHalf(b *testing.B) {
 fmt.Println("TRACE benchmark invocation")
 b.ResetTimer()
 for index := 0; index < b.N; index++ { if index % 2 == 0 { sink = new(int) } }
}
`,
	} {
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(name)), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := writeBenchmarkBootstrap(root, []string{"BenchmarkHalf"}); err != nil {
		t.Fatal(err)
	}
	var cmd = exec.Command("go", "test", "-mod=readonly", "./amps", "-run", "^TestPerfGateMeasure$", "-count=1", "-benchtime=30000x", "-cpu=4", "-v")
	hiddenprocess.HideConsole(cmd)
	cmd.Dir = root
	cmd.Env = append(benchmarkEnv(runtime.Version()), "PERFGATE_BENCHMARK=BenchmarkHalf", "PERFGATE_CPU=test CPU")
	var output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run generated bootstrap: %v\n%s", err, output)
	}
	if count := strings.Count(string(output), "TRACE benchmark invocation"); count != 2 || strings.Contains(string(output), "allocation_probes") {
		t.Fatalf("expected calibration and one timed sample; got %d calls:\n%s", count, output)
	}
	var run benchmarkRun
	run, err = parseExactBenchmarkRun(string(output), "BenchmarkHalf")
	if err != nil {
		t.Fatalf("parse real exact counters: %v\n%s", err, output)
	}
	var got = run.Measurements["BenchmarkHalf"][0]
	if got.N != 30000 || got.MemAllocs < 15000 || got.AllocsOp != float64(got.MemAllocs)/float64(got.N) {
		t.Fatalf("wrong timed allocation counters: %+v", got)
	}
}
