package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

type benchmarkBaseline struct {
	NSOp     float64 `json:"ns_op"`
	AllocsOp float64 `json:"allocs_op"`
}

type baselineFile struct {
	Benchmarks map[string]benchmarkBaseline `json:"benchmarks"`
}

type benchmarkResult struct {
	NSOp     float64
	AllocsOp float64
}

func parseBenchOutput(output string) map[string]benchmarkResult {
	results := map[string]benchmarkResult{}
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}
		fields := strings.Fields(line)
		// Typical format:
		// BenchmarkName-20  N  ns/op  B/op  allocs/op
		if len(fields) < 5 {
			continue
		}
		name := fields[0]
		if dash := strings.LastIndex(name, "-"); dash > 0 {
			name = name[:dash]
		}

		// Reset parse outputs for each benchmark line.
		nsOp := 0.0
		allocsOp := 0.0
		hasNSOp := false
		hasAllocsOp := false
		for i := 0; i < len(fields)-1; i++ {
			switch fields[i+1] {
			case "ns/op":
				if parsed, err := strconv.ParseFloat(fields[i], 64); err == nil {
					nsOp = parsed
					hasNSOp = true
				}
			case "allocs/op":
				if parsed, err := strconv.ParseFloat(fields[i], 64); err == nil {
					allocsOp = parsed
					hasAllocsOp = true
				}
			}
		}
		if hasNSOp && hasAllocsOp && nsOp > 0 {
			results[name] = benchmarkResult{NSOp: nsOp, AllocsOp: allocsOp}
		}
	}
	return results
}

func main() {
	baselinePath := flag.String("baseline", "tools/perf_baseline.json", "path to benchmark baseline JSON")
	packagePath := flag.String("package", "./amps", "package path for benchmarks")
	benchtime := flag.String("benchtime", "1s", "go test benchmark duration")
	maxRegression := flag.Float64("max-regression", 10.0, "max allowed regression percentage")
	flag.Parse()

	data, err := os.ReadFile(*baselinePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "perf baseline read failed: %v\n", err)
		os.Exit(1)
	}

	baseline := baselineFile{}
	if err = json.Unmarshal(data, &baseline); err != nil {
		fmt.Fprintf(os.Stderr, "perf baseline parse failed: %v\n", err)
		os.Exit(1)
	}
	if len(baseline.Benchmarks) == 0 {
		fmt.Fprintln(os.Stderr, "perf baseline is empty")
		os.Exit(1)
	}

	benchmarkNames := make([]string, 0, len(baseline.Benchmarks))
	for name := range baseline.Benchmarks {
		benchmarkNames = append(benchmarkNames, regexp.QuoteMeta(name))
	}
	benchPattern := "^(" + strings.Join(benchmarkNames, "|") + ")$"

	command := exec.Command("go", "test", *packagePath, "-run", "^$", "-bench", benchPattern, "-benchmem", "-count=1", "-benchtime="+*benchtime) // #nosec G204 -- arguments are passed without shell expansion
	outputBytes, err := command.CombinedOutput()
	output := string(outputBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchmark command failed: %v\n%s", err, output)
		os.Exit(1)
	}

	results := parseBenchOutput(output)
	failures := []string{}
	for name, expected := range baseline.Benchmarks {
		actual, ok := results[name]
		if !ok {
			failures = append(failures, fmt.Sprintf("missing benchmark result: %s", name))
			continue
		}

		maxNS := expected.NSOp * (1.0 + (*maxRegression / 100.0))
		if actual.NSOp > maxNS {
			failures = append(failures, fmt.Sprintf("%s ns/op regression: baseline %.2f, actual %.2f, max %.2f", name, expected.NSOp, actual.NSOp, maxNS))
		}

		maxAllocs := expected.AllocsOp * (1.0 + (*maxRegression / 100.0))
		if expected.AllocsOp == 0 {
			maxAllocs = 0
		}
		if actual.AllocsOp > maxAllocs {
			failures = append(failures, fmt.Sprintf("%s allocs/op regression: baseline %.2f, actual %.2f, max %.2f", name, expected.AllocsOp, actual.AllocsOp, maxAllocs))
		}
	}

	fmt.Print(output)
	if len(failures) == 0 {
		fmt.Println("perf gate: PASS")
		return
	}

	fmt.Println("perf gate: FAIL")
	for _, failure := range failures {
		fmt.Printf("- %s\n", failure)
	}
	os.Exit(2)
}
