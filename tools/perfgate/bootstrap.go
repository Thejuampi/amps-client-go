package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const maximumBenchmarkIterations = 1000000000

type exactObservation struct {
	Name       string `json:"name"`
	N          uint64 `json:"n"`
	DurationNS int64  `json:"duration_ns"`
	MemAllocs  uint64 `json:"mem_allocs"`
	MemBytes   uint64 `json:"mem_bytes"`
}

func (observation *exactObservation) UnmarshalJSON(data []byte) error {
	var input struct {
		Name       *string `json:"name"`
		N          *uint64 `json:"n"`
		DurationNS *int64  `json:"duration_ns"`
		MemAllocs  *uint64 `json:"mem_allocs"`
		MemBytes   *uint64 `json:"mem_bytes"`
	}
	var decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return err
	}
	if input.Name == nil || input.N == nil || input.DurationNS == nil || input.MemAllocs == nil || input.MemBytes == nil {
		return fmt.Errorf("exact observation requires name, timing and allocation counters")
	}
	*observation = exactObservation{
		Name: *input.Name, N: *input.N, DurationNS: *input.DurationNS,
		MemAllocs: *input.MemAllocs, MemBytes: *input.MemBytes,
	}
	return nil
}

func parseExactBenchmarkRun(output, name string) (benchmarkRun, error) {
	var run, err = parseBenchmarkRun(output, []string{name}, 1, 4)
	if err != nil {
		return run, err
	}
	var observations []exactObservation
	for _, line := range strings.Split(output, "\n") {
		if data, found := strings.CutPrefix(line, "PERFGATE_EXACT "); found {
			var observation exactObservation
			if err = json.Unmarshal([]byte(data), &observation); err != nil {
				return run, err
			}
			observations = append(observations, observation)
		}
	}
	if len(observations) != 1 || observations[0].Name != name || observations[0].N == 0 || observations[0].N > maximumBenchmarkIterations || observations[0].N != run.Measurements[name][0].N || observations[0].DurationNS <= 0 || observations[0].MemAllocs > math.MaxInt64 {
		return run, fmt.Errorf("%s: require one valid bounded allocation/time counter record", name)
	}
	var observation = observations[0]
	var result = benchmarkResult{
		NSOp:     float64(observation.DurationNS) / float64(observation.N),
		AllocsOp: float64(observation.MemAllocs) / float64(observation.N),
		BytesOp:  float64(observation.MemBytes) / float64(observation.N),
		N:        observation.N, MemAllocs: observation.MemAllocs, MemBytes: observation.MemBytes, DurationNS: observation.DurationNS,
	}
	if !validMetric(result) {
		return run, fmt.Errorf("%s: invalid exact counters", name)
	}
	run.Measurements[name] = []benchmarkResult{result}
	run.Summary[name] = result
	return run, nil
}

func writeBenchmarkBootstrap(directory string, names []string) (string, error) {
	var body strings.Builder
	body.WriteString(`package amps
import (
 "encoding/json"
 "fmt"
 "os"
 "runtime"
 "testing"
)
func TestPerfGateMeasure(t *testing.T) {
 functions := map[string]func(*testing.B){
`)
	var namePattern = regexp.MustCompile(`^Benchmark[A-Za-z0-9_]+$`)
	for _, name := range names {
		if !namePattern.MatchString(name) {
			return "", fmt.Errorf("invalid pinned benchmark name %q", name)
		}
		fmt.Fprintf(&body, "%q: %s,\n", name, name)
	}
	body.WriteString(` }
 name := os.Getenv("PERFGATE_BENCHMARK")
 fn := functions[name]
 if fn == nil { t.Fatal("unknown pinned benchmark", name) }
 result := testing.Benchmark(fn)
 if result.N <= 0 || result.T.Nanoseconds() <= 0 { t.Fatal("benchmark failed or skipped", name) }
 data, err := json.Marshal(struct{
  Name string ` + "`json:\"name\"`" + `
  N int ` + "`json:\"n\"`" + `
  DurationNS int64 ` + "`json:\"duration_ns\"`" + `
  MemAllocs uint64 ` + "`json:\"mem_allocs\"`" + `
  MemBytes uint64 ` + "`json:\"mem_bytes\"`" + `
 }{name, result.N, result.T.Nanoseconds(), result.MemAllocs, result.MemBytes})
 if err != nil { t.Fatal(err) }
 fmt.Printf("goos: %s\ngoarch: %s\npkg: github.com/Thejuampi/amps-client-go/amps\ncpu: %s\n", runtime.GOOS, runtime.GOARCH, os.Getenv("PERFGATE_CPU"))
 fmt.Printf("PERFGATE_EXACT %s\n", data)
 fmt.Printf("%s-%d %d %.17g ns/op %.17g B/op %.17g allocs/op\n", name, runtime.GOMAXPROCS(0), result.N, float64(result.T.Nanoseconds())/float64(result.N), float64(result.MemBytes)/float64(result.N), float64(result.MemAllocs)/float64(result.N))
}
`)
	var target = filepath.Join(directory, "amps", "perfgate_bootstrap_test.go")
	if err := os.WriteFile(target, []byte(body.String()), 0600); err != nil {
		return "", err
	}
	return fileDigest(target)
}
