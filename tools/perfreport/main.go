package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	defaultCaptureTimeout   = 5 * time.Minute
	defaultProgressInterval = 20 * time.Second
)

type tailBenchmarkStats struct {
	N        int     `json:"n"`
	MinNSOp  float64 `json:"min_ns_op"`
	P50NSOp  float64 `json:"p50_ns_op"`
	P95NSOp  float64 `json:"p95_ns_op"`
	P99NSOp  float64 `json:"p99_ns_op"`
	MaxNSOp  float64 `json:"max_ns_op"`
	MeanNSOp float64 `json:"mean_ns_op"`
}

type tailFile struct {
	GeneratedAtUTC   string                        `json:"generated_at_utc"`
	SourceCommand    string                        `json:"source_command"`
	PercentileMethod string                        `json:"percentile_method"`
	SamplesPerBench  int                           `json:"samples_per_benchmark"`
	Benchmarks       map[string]tailBenchmarkStats `json:"benchmarks"`
}

type comparisonEntry struct {
	P95Before   float64 `json:"p95_before"`
	P95After    float64 `json:"p95_after"`
	P95Delta    float64 `json:"p95_delta"`
	P95DeltaPct float64 `json:"p95_delta_pct"`
	P99Before   float64 `json:"p99_before"`
	P99After    float64 `json:"p99_after"`
	P99Delta    float64 `json:"p99_delta"`
	P99DeltaPct float64 `json:"p99_delta_pct"`
}

type comparisonFile struct {
	BaselineGeneratedAtUTC string                     `json:"baseline_generated_at_utc"`
	CurrentGeneratedAtUTC  string                     `json:"current_generated_at_utc"`
	Metric                 string                     `json:"metric"`
	PercentileMethod       string                     `json:"percentile_method"`
	Benchmarks             map[string]comparisonEntry `json:"benchmarks"`
}

type apiBenchmark struct {
	BenchmarkID string               `json:"benchmark_id"`
	Tier        string               `json:"tier"`
	HA          bool                 `json:"ha"`
	CPPSymbol   string               `json:"cpp_symbol"`
	GoTarget    string               `json:"go_target"`
	GoBenchmark string               `json:"go_benchmark"`
	CBenchmark  string               `json:"c_benchmark"`
	Contract    *integrationContract `json:"contract,omitempty"`
	Enabled     bool                 `json:"enabled"`
	Notes       string               `json:"notes"`
}

type integrationContract struct {
	TimingStart           string `json:"timing_start"`
	TimingEnd             string `json:"timing_end"`
	PayloadProfile        string `json:"payload_profile"`
	AckRequired           *bool  `json:"ack_required"`
	UnsubscribeCompletion string `json:"unsubscribe_completion,omitempty"`
	HAMode                string `json:"ha_mode,omitempty"`
	RetryOnDisconnect     *bool  `json:"retry_on_disconnect"`
}

type apiManifest struct {
	Version          int            `json:"version"`
	Description      string         `json:"description"`
	PercentileMethod string         `json:"percentile_method"`
	Benchmarks       []apiBenchmark `json:"benchmarks"`
}

type parityEntry struct {
	CPPSymbol string `json:"cpp_symbol"`
	GoTarget  string `json:"go_target"`
}

type parityManifest struct {
	Entries []parityEntry `json:"entries"`
}

type mergedMetrics struct {
	P50 float64 `json:"p50_ns_op,omitempty"`
	P95 float64 `json:"p95_ns_op,omitempty"`
	P99 float64 `json:"p99_ns_op,omitempty"`
}

type mergedDelta struct {
	P50NS  float64 `json:"p50_ns"`
	P50Pct float64 `json:"p50_pct"`
	P95NS  float64 `json:"p95_ns"`
	P95Pct float64 `json:"p95_pct"`
	P99NS  float64 `json:"p99_ns"`
	P99Pct float64 `json:"p99_pct"`
}

type mergedRow struct {
	BenchmarkID  string        `json:"benchmark_id"`
	Tier         string        `json:"tier"`
	HA           bool          `json:"ha"`
	CPPSymbol    string        `json:"cpp_symbol"`
	GoTarget     string        `json:"go_target"`
	ParityMapped bool          `json:"parity_mapped"`
	GoBenchmark  string        `json:"go_benchmark"`
	CBenchmark   string        `json:"c_benchmark"`
	Enabled      bool          `json:"enabled"`
	Notes        string        `json:"notes"`
	Go           mergedMetrics `json:"go,omitempty"`
	C            mergedMetrics `json:"c,omitempty"`
	Delta        *mergedDelta  `json:"delta_go_vs_c,omitempty"`
	WinnerP95    string        `json:"winner_p95"`
}

type mergedSummary struct {
	Rows               int `json:"rows"`
	Comparable         int `json:"comparable"`
	GoWinsP95          int `json:"go_wins_p95"`
	CWinsP95           int `json:"c_wins_p95"`
	TiesP95            int `json:"ties_p95"`
	Unavailable        int `json:"unavailable"`
	OfflineRows        int `json:"offline_rows"`
	IntegrationRows    int `json:"integration_rows"`
	ParityMappedRows   int `json:"parity_mapped_rows"`
	ParityUnmappedRows int `json:"parity_unmapped_rows"`
}

type mergedFile struct {
	GeneratedAtUTC   string        `json:"generated_at_utc"`
	PercentileMethod string        `json:"percentile_method"`
	ManifestVersion  int           `json:"manifest_version"`
	Summary          mergedSummary `json:"summary"`
	Rows             []mergedRow   `json:"rows"`
}

func normalizeBenchmarkName(name string) string {
	var dash = strings.LastIndex(name, "-")
	if dash <= 0 {
		return name
	}
	var suffix = name[dash+1:]
	if suffix == "" {
		return name
	}
	for _, character := range suffix {
		if character < '0' || character > '9' {
			return name
		}
	}
	return name[:dash]
}

func nearestRank(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var rank = int(math.Ceil((percentile / 100.0) * float64(len(values))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(values) {
		rank = len(values)
	}
	return values[rank-1]
}

func summarizeSamples(samples []float64) tailBenchmarkStats {
	if len(samples) == 0 {
		return tailBenchmarkStats{}
	}
	var sorted = append([]float64(nil), samples...)
	sort.Float64s(sorted)
	var total float64
	for _, value := range sorted {
		total += value
	}
	var mean = total / float64(len(sorted))
	return tailBenchmarkStats{
		N:        len(sorted),
		MinNSOp:  sorted[0],
		P50NSOp:  nearestRank(sorted, 50),
		P95NSOp:  nearestRank(sorted, 95),
		P99NSOp:  nearestRank(sorted, 99),
		MaxNSOp:  sorted[len(sorted)-1],
		MeanNSOp: math.Round(mean*10000) / 10000,
	}
}

func runCommand(command []string, timeout time.Duration, progressInterval time.Duration, progressLabel string) (string, error) {
	if len(command) == 0 {
		return "", errors.New("empty command")
	}

	if timeout <= 0 {
		timeout = defaultCaptureTimeout
	}
	if progressInterval <= 0 {
		progressInterval = defaultProgressInterval
	}

	var label = strings.TrimSpace(progressLabel)
	if label == "" {
		label = strings.Join(command, " ")
	}

	var ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var cmd = exec.CommandContext(ctx, command[0], command[1:]...) // #nosec G204 -- structured arguments only
	var done = make(chan struct{}, 1)
	var outputBytes []byte
	var runErr error
	go func() {
		outputBytes, runErr = cmd.CombinedOutput()
		done <- struct{}{}
	}()

	var startedAt = time.Now()
	var ticker = time.NewTicker(progressInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			var output = string(outputBytes)
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return output, fmt.Errorf("command timed out after %s: %s\n%s", timeout, label, output)
			}
			if runErr != nil {
				return output, fmt.Errorf("command failed: %w\n%s", runErr, output)
			}
			return output, nil
		case <-ticker.C:
			var elapsed = time.Since(startedAt).Round(time.Second)
			var remaining = time.Until(startedAt.Add(timeout)).Round(time.Second)
			if remaining < 0 {
				remaining = 0
			}
			fmt.Fprintf(os.Stderr, "[perfreport] running: %s (elapsed=%s remaining=%s)\n", label, elapsed, remaining)
		}
	}
}

func parseGoBenchSamples(output string) map[string][]float64 {
	var result = map[string][]float64{}
	var scanner = bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		var line = strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}
		var fields = strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		var benchName = normalizeBenchmarkName(fields[0])
		for index := 0; index < len(fields)-1; index++ {
			if fields[index+1] == "ns/op" {
				var value, err = strconv.ParseFloat(fields[index], 64)
				if err == nil && !math.IsNaN(value) && !math.IsInf(value, 0) {
					result[benchName] = append(result[benchName], value)
				}
			}
		}
	}
	return result
}

func parseCBenchSample(output string) map[string]float64 {
	var result = map[string]float64{}
	var matcher = regexp.MustCompile(`(?m)^(Official\S+)\s+\d+\s+([0-9]+(?:\.[0-9]+)?)\s+ns/op\r?$`)
	var matches = matcher.FindAllStringSubmatch(output, -1)
	for _, match := range matches {
		if len(match) != 3 {
			continue
		}
		var value, err = strconv.ParseFloat(match[2], 64)
		if err != nil {
			continue
		}
		result[normalizeBenchmarkName(match[1])] = value
	}
	return result
}

func writeJSON(path string, value any) error {
	var bytes, err = json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	bytes = append(bytes, '\n')
	return os.WriteFile(path, bytes, 0o644)
}

func readTail(path string) (tailFile, error) {
	var data, err = os.ReadFile(path)
	if err != nil {
		return tailFile{}, err
	}
	var file tailFile
	err = json.Unmarshal(data, &file)
	if err != nil {
		return tailFile{}, err
	}
	return file, nil
}

func readManifest(path string) (apiManifest, error) {
	var data, err = os.ReadFile(path)
	if err != nil {
		return apiManifest{}, err
	}
	var manifest apiManifest
	err = json.Unmarshal(data, &manifest)
	if err != nil {
		return apiManifest{}, err
	}
	return manifest, nil
}

func readParityManifest(path string) (parityManifest, error) {
	var data, err = os.ReadFile(path)
	if err != nil {
		return parityManifest{}, err
	}
	var manifest parityManifest
	err = json.Unmarshal(data, &manifest)
	if err != nil {
		return parityManifest{}, err
	}
	return manifest, nil
}

func parityKey(cppSymbol string, goTarget string) string {
	return strings.TrimSpace(cppSymbol) + "|" + strings.TrimSpace(goTarget)
}

func validateManifest(manifest apiManifest) error {
	for _, benchmark := range manifest.Benchmarks {
		if !benchmark.Enabled || benchmark.Tier != "integration" {
			continue
		}

		if benchmark.Contract == nil {
			return fmt.Errorf("integration benchmark %s is missing contract", benchmark.BenchmarkID)
		}

		var contract = benchmark.Contract
		if strings.TrimSpace(contract.TimingStart) == "" {
			return fmt.Errorf("integration benchmark %s is missing contract.timing_start", benchmark.BenchmarkID)
		}
		if strings.TrimSpace(contract.TimingEnd) == "" {
			return fmt.Errorf("integration benchmark %s is missing contract.timing_end", benchmark.BenchmarkID)
		}
		if strings.TrimSpace(contract.PayloadProfile) == "" {
			return fmt.Errorf("integration benchmark %s is missing contract.payload_profile", benchmark.BenchmarkID)
		}
		if contract.AckRequired == nil {
			return fmt.Errorf("integration benchmark %s is missing contract.ack_required", benchmark.BenchmarkID)
		}
		if contract.RetryOnDisconnect == nil {
			return fmt.Errorf("integration benchmark %s is missing contract.retry_on_disconnect", benchmark.BenchmarkID)
		}

		if strings.Contains(benchmark.BenchmarkID, "subscribe.integration") {
			if strings.TrimSpace(contract.UnsubscribeCompletion) == "" {
				return fmt.Errorf("integration benchmark %s is missing contract.unsubscribe_completion", benchmark.BenchmarkID)
			}
		}

		if benchmark.HA && strings.TrimSpace(contract.HAMode) == "" {
			return fmt.Errorf("integration benchmark %s is missing contract.ha_mode", benchmark.BenchmarkID)
		}
	}

	return nil
}

func lookupStats(file tailFile, name string) (tailBenchmarkStats, bool) {
	if name == "" {
		return tailBenchmarkStats{}, false
	}
	if stats, ok := file.Benchmarks[name]; ok {
		return stats, true
	}
	var normalized = normalizeBenchmarkName(name)
	if stats, ok := file.Benchmarks[normalized]; ok {
		return stats, true
	}
	for key, stats := range file.Benchmarks {
		if normalizeBenchmarkName(key) == normalized {
			return stats, true
		}
	}
	return tailBenchmarkStats{}, false
}

func formatMetric(stats tailBenchmarkStats, ok bool) string {
	if !ok {
		return "N/A"
	}
	return fmt.Sprintf("%.2f / %.2f / %.2f", stats.P50NSOp, stats.P95NSOp, stats.P99NSOp)
}

func winnerP95(goStats tailBenchmarkStats, hasGo bool, cStats tailBenchmarkStats, hasC bool) string {
	if !hasGo || !hasC {
		return "n/a"
	}
	if goStats.P95NSOp < cStats.P95NSOp {
		return "go"
	}
	if cStats.P95NSOp < goStats.P95NSOp {
		return "c"
	}
	if goStats.P99NSOp < cStats.P99NSOp {
		return "go"
	}
	if cStats.P99NSOp < goStats.P99NSOp {
		return "c"
	}
	return "tie"
}

func delta(goValue float64, cValue float64) (float64, float64) {
	var absolute = goValue - cValue
	if cValue == 0 {
		return absolute, 0
	}
	var percentage = (absolute / cValue) * 100.0
	return absolute, percentage
}

func commandCaptureGo(arguments []string) error {
	var flagSet = flag.NewFlagSet("capture-go", flag.ContinueOnError)
	var packagePath = flagSet.String("package", "./amps", "package path to benchmark")
	var benchPattern = flagSet.String("bench", ".", "go benchmark regex")
	var benchtime = flagSet.String("benchtime", "1s", "go benchmark benchtime")
	var extraBenchPattern = flagSet.String("extra-bench", "", "optional second go benchmark regex")
	var extraBenchtime = flagSet.String("extra-benchtime", "1x", "go benchmark benchtime for extra bench")
	var samples = flagSet.Int("samples", 20, "number of benchmark samples")
	var timeout = flagSet.Duration("timeout", defaultCaptureTimeout, "maximum capture runtime")
	var progressInterval = flagSet.Duration("progress-interval", defaultProgressInterval, "progress log interval")
	var outPath = flagSet.String("out", "tools/perf_tail_current.json", "output JSON path")
	if err := flagSet.Parse(arguments); err != nil {
		return err
	}
	if *timeout <= 0 {
		*timeout = defaultCaptureTimeout
	}
	if *progressInterval <= 0 {
		*progressInterval = defaultProgressInterval
	}

	var startedAt = time.Now()
	var deadline = startedAt.Add(*timeout)

	var remaining = time.Until(deadline)
	if remaining <= 0 {
		return fmt.Errorf("capture-go timed out before execution (%s)", *timeout)
	}

	var command = []string{"go", "test", *packagePath, "-run", "^$", "-bench", *benchPattern, "-benchmem", "-benchtime=" + *benchtime, "-count=" + strconv.Itoa(*samples)}
	var output, err = runCommand(command, remaining, *progressInterval, "capture-go primary benchmarks")
	if err != nil {
		return err
	}

	var parsed = parseGoBenchSamples(output)
	var sourceCommand = strings.Join(command, " ")

	if strings.TrimSpace(*extraBenchPattern) != "" {
		remaining = time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("capture-go timed out before extra benchmarks (%s)", *timeout)
		}
		var extraCommand = []string{"go", "test", *packagePath, "-run", "^$", "-bench", *extraBenchPattern, "-benchmem", "-benchtime=" + *extraBenchtime, "-count=" + strconv.Itoa(*samples)}
		var extraOutput, extraErr = runCommand(extraCommand, remaining, *progressInterval, "capture-go extra benchmarks")
		if extraErr != nil {
			return extraErr
		}
		var extraParsed = parseGoBenchSamples(extraOutput)
		for name, values := range extraParsed {
			parsed[name] = append(parsed[name], values...)
		}
		sourceCommand = sourceCommand + " ; " + strings.Join(extraCommand, " ")
	}

	if len(parsed) == 0 {
		return errors.New("no go benchmark samples parsed")
	}

	var file tailFile
	file.GeneratedAtUTC = time.Now().UTC().Format(time.RFC3339)
	file.SourceCommand = sourceCommand
	file.PercentileMethod = "nearest-rank"
	file.SamplesPerBench = *samples
	file.Benchmarks = map[string]tailBenchmarkStats{}
	for name, values := range parsed {
		file.Benchmarks[name] = summarizeSamples(values)
	}

	return writeJSON(*outPath, file)
}

func commandCaptureC(arguments []string) error {
	var flagSet = flag.NewFlagSet("capture-c", flag.ContinueOnError)
	var executable = flagSet.String("exe", "./official_c_parity_benchmark.exe", "C benchmark executable path")
	var extraExecutables = flagSet.String("extra-exe", "", "comma-separated extra C benchmark executables")
	var samples = flagSet.Int("samples", 20, "number of benchmark samples")
	var timeout = flagSet.Duration("timeout", defaultCaptureTimeout, "maximum capture runtime")
	var progressInterval = flagSet.Duration("progress-interval", defaultProgressInterval, "progress log interval")
	var outPath = flagSet.String("out", "tools/perf_tail_c_current.json", "output JSON path")
	if err := flagSet.Parse(arguments); err != nil {
		return err
	}
	if *timeout <= 0 {
		*timeout = defaultCaptureTimeout
	}
	if *progressInterval <= 0 {
		*progressInterval = defaultProgressInterval
	}

	var executables = []string{*executable}
	if strings.TrimSpace(*extraExecutables) != "" {
		for _, rawExecutable := range strings.Split(*extraExecutables, ",") {
			var parsedExecutable = strings.TrimSpace(rawExecutable)
			if parsedExecutable == "" {
				continue
			}
			executables = append(executables, parsedExecutable)
		}
	}

	var startedAt = time.Now()
	var deadline = startedAt.Add(*timeout)
	var nextProgress = startedAt.Add(*progressInterval)

	var values = map[string][]float64{}
	for index := 0; index < *samples; index++ {
		if time.Now().After(deadline) {
			return fmt.Errorf("capture-c timed out after %s", *timeout)
		}
		if !time.Now().Before(nextProgress) {
			var elapsed = time.Since(startedAt).Round(time.Second)
			fmt.Fprintf(os.Stderr, "[perfreport] running: capture-c sample %d/%d (elapsed=%s)\n", index+1, *samples, elapsed)
			nextProgress = time.Now().Add(*progressInterval)
		}
		for _, currentExecutable := range executables {
			var remaining = time.Until(deadline)
			if remaining <= 0 {
				return fmt.Errorf("capture-c timed out after %s", *timeout)
			}
			var label = fmt.Sprintf("capture-c sample %d/%d %s", index+1, *samples, currentExecutable)
			var output, err = runCommand([]string{currentExecutable}, remaining, *progressInterval, label)
			if err != nil {
				return err
			}
			var parsed = parseCBenchSample(output)
			if len(parsed) == 0 {
				return fmt.Errorf("no c benchmark samples parsed from %s", currentExecutable)
			}
			for name, value := range parsed {
				values[name] = append(values[name], value)
			}
		}
	}

	var file tailFile
	file.GeneratedAtUTC = time.Now().UTC().Format(time.RFC3339)
	file.SourceCommand = strings.Join(executables, " ; ")
	file.PercentileMethod = "nearest-rank"
	file.SamplesPerBench = *samples
	file.Benchmarks = map[string]tailBenchmarkStats{}
	for name, samples := range values {
		file.Benchmarks[name] = summarizeSamples(samples)
	}

	return writeJSON(*outPath, file)
}

func commandCompare(arguments []string) error {
	var flagSet = flag.NewFlagSet("compare", flag.ContinueOnError)
	var baselinePath = flagSet.String("baseline", "tools/perf_tail_baseline.json", "baseline tail file")
	var currentPath = flagSet.String("current", "tools/perf_tail_current.json", "current tail file")
	var outPath = flagSet.String("out", "tools/perf_tail_comparison.json", "output comparison JSON")
	if err := flagSet.Parse(arguments); err != nil {
		return err
	}

	var baseline, err = readTail(*baselinePath)
	if err != nil {
		return err
	}
	var current, err2 = readTail(*currentPath)
	if err2 != nil {
		return err2
	}

	var file comparisonFile
	file.BaselineGeneratedAtUTC = baseline.GeneratedAtUTC
	file.CurrentGeneratedAtUTC = current.GeneratedAtUTC
	file.Metric = "ns/op (lower is better)"
	file.PercentileMethod = "nearest-rank"
	file.Benchmarks = map[string]comparisonEntry{}

	for name, baseStats := range baseline.Benchmarks {
		var currentStats, ok = lookupStats(current, name)
		if !ok {
			continue
		}
		var p95Delta = currentStats.P95NSOp - baseStats.P95NSOp
		var p99Delta = currentStats.P99NSOp - baseStats.P99NSOp
		var p95Pct float64
		if baseStats.P95NSOp != 0 {
			p95Pct = (p95Delta / baseStats.P95NSOp) * 100.0
		}
		var p99Pct float64
		if baseStats.P99NSOp != 0 {
			p99Pct = (p99Delta / baseStats.P99NSOp) * 100.0
		}
		file.Benchmarks[name] = comparisonEntry{
			P95Before:   baseStats.P95NSOp,
			P95After:    currentStats.P95NSOp,
			P95Delta:    p95Delta,
			P95DeltaPct: p95Pct,
			P99Before:   baseStats.P99NSOp,
			P99After:    currentStats.P99NSOp,
			P99Delta:    p99Delta,
			P99DeltaPct: p99Pct,
		}
	}

	return writeJSON(*outPath, file)
}

func commandMerge(arguments []string) error {
	var flagSet = flag.NewFlagSet("merge", flag.ContinueOnError)
	var manifestPath = flagSet.String("manifest", "tools/perf_api_manifest.json", "API benchmark manifest")
	var parityPath = flagSet.String("parity-manifest", "tools/parity_manifest.json", "canonical parity manifest")
	var goPath = flagSet.String("go", "tools/perf_tail_current.json", "Go tail metrics JSON")
	var cPath = flagSet.String("c", "tools/perf_tail_c_current.json", "C tail metrics JSON")
	var outJSON = flagSet.String("out-json", "tools/perf_side_by_side_current.json", "merged output JSON")
	var outMarkdown = flagSet.String("out-md", "tools/perf_side_by_side_report.md", "merged output markdown")
	if err := flagSet.Parse(arguments); err != nil {
		return err
	}

	var manifest, err = readManifest(*manifestPath)
	if err != nil {
		return err
	}
	if err = validateManifest(manifest); err != nil {
		return err
	}
	var parityManifest, errParity = readParityManifest(*parityPath)
	if errParity != nil {
		return errParity
	}
	var goFile, err2 = readTail(*goPath)
	if err2 != nil {
		return err2
	}
	var cFile, err3 = readTail(*cPath)
	if err3 != nil {
		return err3
	}

	var output mergedFile
	output.GeneratedAtUTC = time.Now().UTC().Format(time.RFC3339)
	output.PercentileMethod = "nearest-rank"
	output.ManifestVersion = manifest.Version
	output.Rows = make([]mergedRow, 0, len(manifest.Benchmarks))

	var parityMap = map[string]struct{}{}
	for _, entry := range parityManifest.Entries {
		if strings.TrimSpace(entry.CPPSymbol) == "" || strings.TrimSpace(entry.GoTarget) == "" {
			continue
		}
		parityMap[parityKey(entry.CPPSymbol, entry.GoTarget)] = struct{}{}
	}

	var markdownLines []string
	markdownLines = append(markdownLines, "# API Parity Performance Report (C vs Go)")
	markdownLines = append(markdownLines, "")
	markdownLines = append(markdownLines, "Percentile method: `nearest-rank`")
	markdownLines = append(markdownLines, "")
	markdownLines = append(markdownLines, "| Benchmark ID | Tier | HA | C++ Symbol | Parity Mapped | Go p50/p95/p99 (ns/op) | C p50/p95/p99 (ns/op) | Winner (p95) | Delta p95 (Go-C) | Enabled |")
	markdownLines = append(markdownLines, "|---|---|---:|---|---:|---|---|---|---:|---:|")

	for _, benchmark := range manifest.Benchmarks {
		var goStats, hasGo = lookupStats(goFile, benchmark.GoBenchmark)
		var cStats, hasC = lookupStats(cFile, benchmark.CBenchmark)

		var row mergedRow
		row.BenchmarkID = benchmark.BenchmarkID
		row.Tier = benchmark.Tier
		row.HA = benchmark.HA
		row.CPPSymbol = benchmark.CPPSymbol
		row.GoTarget = benchmark.GoTarget
		_, row.ParityMapped = parityMap[parityKey(benchmark.CPPSymbol, benchmark.GoTarget)]
		row.GoBenchmark = benchmark.GoBenchmark
		row.CBenchmark = benchmark.CBenchmark
		row.Enabled = benchmark.Enabled
		row.Notes = benchmark.Notes
		row.WinnerP95 = winnerP95(goStats, hasGo, cStats, hasC)

		if hasGo {
			row.Go = mergedMetrics{P50: goStats.P50NSOp, P95: goStats.P95NSOp, P99: goStats.P99NSOp}
		}
		if hasC {
			row.C = mergedMetrics{P50: cStats.P50NSOp, P95: cStats.P95NSOp, P99: cStats.P99NSOp}
		}
		if hasGo && hasC {
			var p50NS, p50Pct = delta(goStats.P50NSOp, cStats.P50NSOp)
			var p95NS, p95Pct = delta(goStats.P95NSOp, cStats.P95NSOp)
			var p99NS, p99Pct = delta(goStats.P99NSOp, cStats.P99NSOp)
			row.Delta = &mergedDelta{P50NS: p50NS, P50Pct: p50Pct, P95NS: p95NS, P95Pct: p95Pct, P99NS: p99NS, P99Pct: p99Pct}
		}

		output.Rows = append(output.Rows, row)

		var deltaP95 = "N/A"
		if row.Delta != nil {
			deltaP95 = fmt.Sprintf("%.2f%%", row.Delta.P95Pct)
		}
		var markdownLine = fmt.Sprintf("| %s | %s | %t | `%s` | %t | %s | %s | %s | %s | %t |",
			row.BenchmarkID,
			row.Tier,
			row.HA,
			row.CPPSymbol,
			row.ParityMapped,
			formatMetric(goStats, hasGo),
			formatMetric(cStats, hasC),
			row.WinnerP95,
			deltaP95,
			row.Enabled,
		)
		markdownLines = append(markdownLines, markdownLine)

		output.Summary.Rows++
		if row.Tier == "offline" {
			output.Summary.OfflineRows++
		}
		if row.Tier == "integration" {
			output.Summary.IntegrationRows++
		}
		if row.ParityMapped {
			output.Summary.ParityMappedRows++
		} else {
			output.Summary.ParityUnmappedRows++
		}
		if hasGo && hasC {
			output.Summary.Comparable++
			switch row.WinnerP95 {
			case "go":
				output.Summary.GoWinsP95++
			case "c":
				output.Summary.CWinsP95++
			default:
				output.Summary.TiesP95++
			}
		} else {
			output.Summary.Unavailable++
		}
	}

	markdownLines = append(markdownLines, "")
	markdownLines = append(markdownLines, fmt.Sprintf("Comparable rows: **%d**", output.Summary.Comparable))
	markdownLines = append(markdownLines, fmt.Sprintf("Go wins (p95): **%d**", output.Summary.GoWinsP95))
	markdownLines = append(markdownLines, fmt.Sprintf("C wins (p95): **%d**", output.Summary.CWinsP95))
	markdownLines = append(markdownLines, fmt.Sprintf("Ties (p95): **%d**", output.Summary.TiesP95))
	markdownLines = append(markdownLines, fmt.Sprintf("Rows without both sides yet: **%d**", output.Summary.Unavailable))
	markdownLines = append(markdownLines, fmt.Sprintf("Rows mapped in parity manifest: **%d**", output.Summary.ParityMappedRows))
	markdownLines = append(markdownLines, fmt.Sprintf("Rows missing parity mapping: **%d**", output.Summary.ParityUnmappedRows))

	if err := writeJSON(*outJSON, output); err != nil {
		return err
	}
	var markdown = strings.Join(markdownLines, "\n") + "\n"
	return os.WriteFile(*outMarkdown, []byte(markdown), 0o644)
}

func commandCompareMerged(arguments []string) error {
	var flagSet = flag.NewFlagSet("compare-merged", flag.ContinueOnError)
	var baselinePath = flagSet.String("baseline", "tools/perf_side_by_side_baseline.json", "baseline merged JSON")
	var currentPath = flagSet.String("current", "tools/perf_side_by_side_current.json", "current merged JSON")
	var outPath = flagSet.String("out", "tools/perf_side_by_side_comparison.json", "output comparison JSON")
	if err := flagSet.Parse(arguments); err != nil {
		return err
	}

	var baselineData, err = os.ReadFile(*baselinePath)
	if err != nil {
		return err
	}
	var currentData, err2 = os.ReadFile(*currentPath)
	if err2 != nil {
		return err2
	}
	var baseline mergedFile
	var current mergedFile
	if err = json.Unmarshal(baselineData, &baseline); err != nil {
		return err
	}
	if err = json.Unmarshal(currentData, &current); err != nil {
		return err
	}

	type mergedComparisonEntry struct {
		BenchmarkID  string  `json:"benchmark_id"`
		Tier         string  `json:"tier"`
		HA           bool    `json:"ha"`
		GoP95Before  float64 `json:"go_p95_before,omitempty"`
		GoP95After   float64 `json:"go_p95_after,omitempty"`
		GoP95Delta   float64 `json:"go_p95_delta,omitempty"`
		CP95Before   float64 `json:"c_p95_before,omitempty"`
		CP95After    float64 `json:"c_p95_after,omitempty"`
		CP95Delta    float64 `json:"c_p95_delta,omitempty"`
		WinnerBefore string  `json:"winner_before"`
		WinnerAfter  string  `json:"winner_after"`
	}

	type mergedComparisonFile struct {
		BaselineGeneratedAtUTC string                  `json:"baseline_generated_at_utc"`
		CurrentGeneratedAtUTC  string                  `json:"current_generated_at_utc"`
		Entries                []mergedComparisonEntry `json:"entries"`
	}

	var baselineByID = map[string]mergedRow{}
	for _, row := range baseline.Rows {
		baselineByID[row.BenchmarkID] = row
	}

	var comparison mergedComparisonFile
	comparison.BaselineGeneratedAtUTC = baseline.GeneratedAtUTC
	comparison.CurrentGeneratedAtUTC = current.GeneratedAtUTC
	comparison.Entries = make([]mergedComparisonEntry, 0, len(current.Rows))

	for _, currentRow := range current.Rows {
		var entry mergedComparisonEntry
		entry.BenchmarkID = currentRow.BenchmarkID
		entry.Tier = currentRow.Tier
		entry.HA = currentRow.HA
		entry.WinnerAfter = currentRow.WinnerP95

		var baselineRow, ok = baselineByID[currentRow.BenchmarkID]
		if ok {
			entry.WinnerBefore = baselineRow.WinnerP95
			if baselineRow.Go.P95 != 0 && currentRow.Go.P95 != 0 {
				entry.GoP95Before = baselineRow.Go.P95
				entry.GoP95After = currentRow.Go.P95
				entry.GoP95Delta = currentRow.Go.P95 - baselineRow.Go.P95
			}
			if baselineRow.C.P95 != 0 && currentRow.C.P95 != 0 {
				entry.CP95Before = baselineRow.C.P95
				entry.CP95After = currentRow.C.P95
				entry.CP95Delta = currentRow.C.P95 - baselineRow.C.P95
			}
		}

		comparison.Entries = append(comparison.Entries, entry)
	}

	return writeJSON(*outPath, comparison)
}

func usage() {
	fmt.Println("Usage: perfreport <command> [flags]")
	fmt.Println("Commands:")
	fmt.Println("  capture-go      Capture Go benchmark tails into JSON")
	fmt.Println("  capture-c       Capture C benchmark tails into JSON")
	fmt.Println("  compare         Compare baseline/current tail JSON")
	fmt.Println("  merge           Merge Go/C tail JSON into side-by-side API report")
	fmt.Println("  compare-merged  Compare baseline/current side-by-side JSON")
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	var command = os.Args[1]
	var args = os.Args[2:]
	var err error

	switch command {
	case "capture-go":
		err = commandCaptureGo(args)
	case "capture-c":
		err = commandCaptureC(args)
	case "compare":
		err = commandCompare(args)
	case "merge":
		err = commandMerge(args)
	case "compare-merged":
		err = commandCompareMerged(args)
	default:
		usage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
