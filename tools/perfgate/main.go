package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"

	"github.com/Thejuampi/amps-client-go/internal/hiddenprocess"
)

type benchmarkBaseline struct {
	NSOp     float64 `json:"ns_op"`
	AllocsOp float64 `json:"allocs_op"`
	Group    string  `json:"group,omitempty"`
}

type regressionGroup struct {
	MaxRegressionPct       float64 `json:"max_regression_pct,omitempty"`
	MaxAllocsRegressionPct float64 `json:"max_allocs_regression_pct,omitempty"`
}

type baselineFile struct {
	MaxRegression float64                      `json:"max_regression,omitempty"`
	Groups        map[string]regressionGroup   `json:"groups,omitempty"`
	Benchmarks    map[string]benchmarkBaseline `json:"benchmarks"`
}

type benchmarkResult struct {
	NSOp       float64 `json:"ns_op"`
	AllocsOp   float64 `json:"allocs_op"`
	BytesOp    float64 `json:"bytes_op"`
	N          uint64  `json:"n"`
	MemAllocs  uint64  `json:"mem_allocs"`
	MemBytes   uint64  `json:"mem_bytes"`
	DurationNS int64   `json:"duration_ns"`
}

type gateFailure struct {
	Name    string
	Message string
}

type benchmarkRun struct {
	Output       string
	Summary      map[string]benchmarkResult
	Measurements map[string][]benchmarkResult
	Environment  benchmarkEnvironment
}

type gateAttempt struct {
	Name      string
	Benchtime string
	Samples   int
	Failures  []gateFailure
	Output    string
}

type benchmarkRunner func(names []string, benchtime string, samples int) (benchmarkRun, error)

const nsComparisonSlack = 0.1

func parseBenchOutput(output string) map[string][]benchmarkResult {
	results := map[string][]benchmarkResult{}
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
			results[name] = append(results[name], benchmarkResult{NSOp: nsOp, AllocsOp: allocsOp})
		}
	}
	return results
}

func medianFloat64(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sorted = slices.Clone(values)
	slices.Sort(sorted)
	var middle = len(sorted) / 2
	if len(sorted)%2 == 0 {
		return sorted[middle-1]/2 + sorted[middle]/2
	}
	return sorted[middle]
}

func summarizeBenchOutput(parsed map[string][]benchmarkResult) map[string]benchmarkResult {
	summary := make(map[string]benchmarkResult, len(parsed))
	for name, samples := range parsed {
		nsValues := make([]float64, 0, len(samples))
		var maxAllocs float64
		for _, sample := range samples {
			nsValues = append(nsValues, sample.NSOp)
			maxAllocs = max(maxAllocs, sample.AllocsOp)
		}
		summary[name] = benchmarkResult{
			NSOp:     medianFloat64(nsValues),
			AllocsOp: maxAllocs,
		}
	}
	return summary
}

func nsRegressionExceeded(actual float64, expected float64, regressionLimit float64) bool {
	var maxNS = expected * (1.0 + (regressionLimit / 100.0))
	return actual > maxNS+nsComparisonSlack
}

func benchmarkNamesForBaseline(baseline baselineFile) []string {
	var names []string
	for name := range maps.Keys(baseline.Benchmarks) {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func benchmarkPattern(names []string) string {
	var quotedNames = make([]string, 0, len(names))
	for _, name := range names {
		quotedNames = append(quotedNames, regexp.QuoteMeta(name))
	}
	return "^(" + strings.Join(quotedNames, "|") + ")$"
}

func runGoBenchmarks(packagePath string) benchmarkRunner {
	return func(names []string, benchtime string, samples int) (benchmarkRun, error) {
		if len(names) == 0 {
			return benchmarkRun{Summary: map[string]benchmarkResult{}}, nil
		}

		var pattern = benchmarkPattern(names)
		command := exec.Command("go", "test", packagePath, "-run", "^$", "-bench", pattern, "-benchmem", "-count="+strconv.Itoa(samples), "-benchtime="+benchtime) // #nosec G204 -- arguments are passed without shell expansion
		hiddenprocess.HideConsole(command)
		command.Env = benchmarkEnv(runtime.Version())
		outputBytes, err := command.CombinedOutput()
		output := string(outputBytes)
		if err != nil {
			return benchmarkRun{Output: output}, err
		}
		return parseBenchmarkRun(output, names, samples, 4)
	}
}

func benchmarkFailures(baseline baselineFile, results map[string]benchmarkResult, defaultRegression float64, names []string) []gateFailure {
	var failures []gateFailure
	for _, name := range names {
		expected, ok := baseline.Benchmarks[name]
		if !ok {
			continue
		}

		actual, ok := results[name]
		if !ok {
			failures = append(failures, gateFailure{
				Name:    name,
				Message: fmt.Sprintf("missing benchmark result: %s", name),
			})
			continue
		}
		if !validMetric(actual) || !validMetric(benchmarkResult{NSOp: expected.NSOp, AllocsOp: expected.AllocsOp}) {
			failures = append(failures, gateFailure{Name: name, Message: fmt.Sprintf("%s: invalid measurement or baseline", name)})
			continue
		}

		var regressionLimit = defaultRegression
		var allocRegressionLimit = defaultRegression
		if expected.Group != "" {
			if group, ok := baseline.Groups[expected.Group]; ok {
				if group.MaxRegressionPct > 0 {
					regressionLimit = group.MaxRegressionPct
				}
				if group.MaxAllocsRegressionPct > 0 {
					allocRegressionLimit = group.MaxAllocsRegressionPct
				}
			}
		}

		maxNS := expected.NSOp * (1.0 + (regressionLimit / 100.0))
		if nsRegressionExceeded(actual.NSOp, expected.NSOp, regressionLimit) {
			failures = append(failures, gateFailure{
				Name:    name,
				Message: fmt.Sprintf("%s ns/op regression: baseline %.2f, actual %.2f, max %.2f (+%.2f ns slack)", name, expected.NSOp, actual.NSOp, maxNS, nsComparisonSlack),
			})
		}

		maxAllocs := expected.AllocsOp * (1.0 + (allocRegressionLimit / 100.0))
		if expected.AllocsOp == 0 {
			maxAllocs = 0
		}
		if actual.AllocsOp > maxAllocs {
			failures = append(failures, gateFailure{
				Name:    name,
				Message: fmt.Sprintf("%s allocs/op regression: baseline %.2f, actual %.2f, max %.2f", name, expected.AllocsOp, actual.AllocsOp, maxAllocs),
			})
		}
	}
	return failures
}

func failureNames(failures []gateFailure) []string {
	var names []string
	for _, failure := range failures {
		names = append(names, failure.Name)
	}
	slices.Sort(names)
	return slices.Compact(names)
}

func runPerfGate(runner benchmarkRunner, baseline baselineFile, defaultRegression float64, names []string, benchtime string, samples int) ([]gateAttempt, error) {
	var run, err = runner(names, benchtime, samples)
	return []gateAttempt{{
		Name: "fixed sample", Benchtime: benchtime, Samples: samples,
		Failures: benchmarkFailures(baseline, run.Summary, defaultRegression, names), Output: run.Output,
	}}, err
}

func finalFailureExitCode(finalFailures []gateFailure, reportOnly bool) int {
	if len(finalFailures) == 0 || reportOnly {
		return 0
	}
	return 2
}

func main() {
	mode := flag.String("mode", "paired", "paired gate, or absolute diagnostic (requires -report-only)")
	baselinePath := flag.String("baseline", "tools/perf_baseline.json", "path to benchmark baseline JSON")
	referencePath := flag.String("reference", "tools/perf_reference.json", "immutable performance reference manifest")
	outputPath := flag.String("output", ".tmp/perf/gate", "parent directory for raw observations and JSON reports")
	pairs := flag.Int("pairs", requiredPairs, "number of alternating reference/candidate pairs (exactly six)")
	packagePath := flag.String("package", "./amps", "package path for benchmarks")
	benchtime := flag.String("benchtime", "1s", "go test benchmark duration")
	samples := flag.Int("samples", 5, "number of benchmark samples to collect")
	maxRegression := flag.Float64("max-regression", 10.0, "max allowed regression percentage")
	reportOnly := flag.Bool("report-only", false, "print benchmark output and threshold failures without failing on regressions")
	flag.Parse()
	if *mode != "paired" && *mode != "absolute" || *mode == "absolute" && !*reportOnly {
		fmt.Fprintln(os.Stderr, "absolute timings are diagnostic only; use the paired gate or -mode absolute -report-only")
		os.Exit(1)
	}
	if *mode == "paired" && (*reportOnly || *maxRegression != 10 || *packagePath != "./amps") {
		fmt.Fprintln(os.Stderr, "paired gate uses the committed AMPS baseline and budgets; report-only and budget/package overrides are diagnostic only")
		os.Exit(1)
	}
	if *mode == "paired" {
		os.Exit(executePaired(*baselinePath, *referencePath, *outputPath, *benchtime, *pairs))
	}

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

	var defaultRegression = *maxRegression
	if baseline.MaxRegression > 0 {
		defaultRegression = baseline.MaxRegression
	}

	var benchmarkNames = benchmarkNamesForBaseline(baseline)
	var attempts, runErr = runPerfGate(runGoBenchmarks(*packagePath), baseline, defaultRegression, benchmarkNames, *benchtime, *samples)
	for _, attempt := range attempts {
		fmt.Print(attempt.Output)
	}
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "benchmark command failed: %v\n", runErr)
		os.Exit(1)
	}

	var finalFailures = attempts[len(attempts)-1].Failures
	fmt.Println("perf gate: REPORT ONLY (absolute times are not comparable across hardware)")
	for _, failure := range finalFailures {
		fmt.Printf("- %s\n", failure.Message)
	}
	os.Exit(finalFailureExitCode(finalFailures, *reportOnly))
}
