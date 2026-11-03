package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/Thejuampi/amps-client-go/internal/hiddenprocess"
)

const benchmarkFile = "amps/perf_benchmark_test.go"

type referenceConfig struct {
	Commit          string `json:"commit"`
	Toolchain       string `json:"toolchain"`
	BaselineSHA256  string `json:"baseline_sha256"`
	BenchmarkSHA256 string `json:"benchmark_sha256"`
}

func pinnedReference() referenceConfig {
	return referenceConfig{
		Commit: "831ab636ae5e76cd06880edafb55d3e39d3bd7dd", Toolchain: "go1.26.10",
		BaselineSHA256:  "64db5a6cdee1ad878ba73c6a2b305fc14d10c1874b93cdd3f541d0318b515539",
		BenchmarkSHA256: "aaa21566db17cfa5d09a3acf3413dc7076dd3035c714470f987ed2cee397912e",
	}
}

func loadPinnedBaseline(path string) (baselineFile, error) {
	var baseline baselineFile
	var data, err = os.ReadFile(path) // #nosec G304 -- explicit baseline; the same bytes are hashed and decoded.
	if err != nil {
		return baseline, err
	}
	var digest = sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != pinnedReference().BaselineSHA256 {
		return baseline, fmt.Errorf("baseline does not match the immutable performance budget")
	}
	err = json.Unmarshal(data, &baseline)
	return baseline, err
}

type pairedRunner func(side, name string, pair int) (benchmarkRun, error)

func preserveAllocationFailure(results []pairedResult, name string, expected benchmarkBaseline, fallback float64, baseline baselineFile, pairs []benchmarkPair, pending benchmarkPair) []pairedResult {
	var _, budget = regressionLimits(baseline, expected, fallback)
	var limit = expected.AllocsOp * (1 + budget/100)
	var observations = append(append([]benchmarkPair(nil), pairs...), pending)
	var maximum, exact float64
	for _, pair := range observations {
		if !validMetric(pair.Candidate) {
			continue
		}
		maximum = max(maximum, reportedAllocations(pair.Candidate))
		exact = max(exact, pair.Candidate.AllocsOp)
	}
	if maximum > limit {
		results = append(results, pairedResult{Name: name, Verdict: "FAIL", MaxAllocs: maximum, MaxExactAllocs: exact, LimitAllocs: limit, Pairs: observations})
	}
	return results
}

func collectPairs(runner pairedRunner, baseline baselineFile, fallback float64, count int) ([]pairedResult, benchmarkEnvironment, error) {
	var results []pairedResult
	var environment benchmarkEnvironment
	if count != requiredPairs {
		return nil, environment, fmt.Errorf("need exactly %d fixed pairs", requiredPairs)
	}
	for _, name := range benchmarkNamesForBaseline(baseline) {
		var expected = baseline.Benchmarks[name]
		var pairs = make([]benchmarkPair, 0, count)
		for index := 0; index < count; index++ {
			var order = [2]string{"reference", "candidate"}
			if index%2 != 0 {
				order[0], order[1] = order[1], order[0]
			}
			var pair benchmarkPair
			for _, side := range order {
				var run, err = runner(side, name, index)
				if err != nil {
					return preserveAllocationFailure(results, name, expected, fallback, baseline, pairs, pair), environment, fmt.Errorf("%s pair %d %s: %w", name, index+1, side, err)
				}
				if environment == (benchmarkEnvironment{}) {
					environment = run.Environment
				}
				if run.Environment != environment || environment.CPU == "" || environment.Procs != 4 || len(run.Measurements[name]) != 1 {
					return preserveAllocationFailure(results, name, expected, fallback, baseline, pairs, pair), environment, fmt.Errorf("%s pair %d %s: environment or sample count changed", name, index+1, side)
				}
				if side == "reference" {
					pair.Reference = run.Measurements[name][0]
				} else {
					pair.Candidate = run.Measurements[name][0]
				}
			}
			pairs = append(pairs, pair)
		}
		var timeBudget, allocBudget = regressionLimits(baseline, expected, fallback)
		var result, err = comparePairs(name, expected, timeBudget, allocBudget, pairs)
		if err != nil {
			return results, environment, err
		}
		results = append(results, result)
	}
	return results, environment, nil
}

func fileDigest(path string) (string, error) {
	var data, err = os.ReadFile(path) // #nosec G304 -- paths are explicit local verification inputs.
	if err != nil {
		return "", err
	}
	var digest = sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func verifyDigest(path, expected string) error {
	var actual, err = fileDigest(path)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("%s: SHA-256 %s, require %s; reference/baseline changes need explicit review", path, actual, expected)
	}
	return nil
}

func readReference(path, baselinePath, root string) (referenceConfig, error) {
	var config referenceConfig
	var data, err = os.ReadFile(path) // #nosec G304 -- explicit local configuration file.
	if err != nil {
		return config, err
	}
	var decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&config); err != nil {
		return config, err
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return config, fmt.Errorf("reference configuration has trailing JSON")
	}
	if config != pinnedReference() {
		return config, fmt.Errorf("reference manifest cannot change the pinned commit, toolchain, baseline or harness")
	}
	if err = verifyDigest(baselinePath, config.BaselineSHA256); err != nil {
		return config, err
	}
	err = verifyDigest(filepath.Join(root, benchmarkFile), config.BenchmarkSHA256)
	return config, err
}

func commandOutput(ctx context.Context, directory, executable string, args []string, env []string) ([]byte, error) {
	var executableName = filepath.Base(executable)
	var isGit = strings.EqualFold(executableName, "git") || strings.EqualFold(executableName, "git.exe")
	if isGit {
		args = append([]string{"--no-replace-objects"}, args...)
	}
	var command = exec.CommandContext(ctx, executable, args...) // #nosec G204 -- fixed git/go commands or freshly built benchmark binaries, no shell.
	hiddenprocess.HideConsole(command)
	command.Dir = directory
	command.Env = env
	if isGit {
		var stderr bytes.Buffer
		command.Stderr = &stderr
		var output, err = command.Output()
		if err != nil {
			return output, fmt.Errorf("git command failed: %w: %s", err, strings.TrimSpace(stderr.String()))
		}
		return output, nil
	}
	return command.CombinedOutput()
}

func extractReference(archive []byte, directory string) error {
	var reader = tar.NewReader(bytes.NewReader(archive))
	for {
		var header, err = reader.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		var name = strings.TrimSuffix(header.Name, "/")
		if !filepath.IsLocal(name) || strings.Contains(name, "\\") {
			return fmt.Errorf("reference archive contains nonlocal path %q", header.Name)
		}
		var target = filepath.Join(directory, filepath.FromSlash(name))
		switch header.Typeflag {
		case tar.TypeXGlobalHeader:
			continue
		case tar.TypeDir:
			if err = os.MkdirAll(target, 0700); err != nil {
				return err
			}
		case tar.TypeReg:
			if err = os.MkdirAll(filepath.Dir(target), 0700); err != nil {
				return err
			}
			var file, openErr = os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600) // #nosec G304 -- filepath.IsLocal above confines extraction to the fresh directory.
			if openErr != nil {
				return openErr
			}
			var _, copyErr = io.Copy(file, reader)
			var closeErr = file.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
		default:
			return fmt.Errorf("reference archive contains unsupported entry %q", header.Name)
		}
	}
}

func benchmarkEnv(toolchain string) []string {
	var overrides = map[string]string{
		"GOTOOLCHAIN": toolchain, "GOWORK": "off", "GOFLAGS": "", "GOMAXPROCS": "4",
		"GOOS": runtime.GOOS, "GOARCH": runtime.GOARCH, "GOAMD64": "v1", "GOEXPERIMENT": "", "GODEBUG": "", "CGO_ENABLED": "0",
	}
	var env = make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		var key, _, _ = strings.Cut(entry, "=")
		if _, replaced := overrides[strings.ToUpper(key)]; !replaced {
			env = append(env, entry)
		}
	}
	for key, value := range overrides {
		env = append(env, key+"="+value)
	}
	return env
}

func buildBenchmark(ctx context.Context, directory, binaryPath, logPath, toolchain string) (string, error) {
	var output, err = commandOutput(ctx, directory, "go", []string{"test", "-c", "-trimpath", "-mod=readonly", "-buildvcs=false", "-pgo=off", "-o", binaryPath, "./amps"}, benchmarkEnv(toolchain))
	if writeErr := os.WriteFile(logPath, output, 0600); writeErr != nil {
		return "", writeErr
	}
	if err != nil {
		return "", fmt.Errorf("benchmark build failed: %w (see %s)", err, logPath)
	}
	var info, infoErr = buildinfo.ReadFile(binaryPath)
	if err = verifyBenchmarkBuildInfo(info, infoErr, toolchain); err != nil {
		return "", err
	}
	return fileDigest(binaryPath)
}

func verifyBenchmarkBuildInfo(info *debug.BuildInfo, readErr error, toolchain string) error {
	if readErr != nil {
		return fmt.Errorf("read benchmark binary build info: %w", readErr)
	}
	if info == nil {
		return fmt.Errorf("benchmark binary has no build info")
	}
	if info.GoVersion != toolchain {
		return fmt.Errorf("benchmark binary toolchain differs: got %s, require %s", info.GoVersion, toolchain)
	}
	return nil
}

func matchingDigestError(label, expected, actual string, cause error) error {
	if cause != nil {
		return fmt.Errorf("%s: %w", label, cause)
	}
	if expected != actual {
		return fmt.Errorf("%s mismatch: expected %s, got %s", label, expected, actual)
	}
	return nil
}

func removeBenchmarkScratch(directory string) error {
	var root, rootErr = filepath.EvalSymlinks(os.TempDir())
	var resolved, pathErr = filepath.EvalSymlinks(directory)
	if rootErr != nil || pathErr != nil {
		return fmt.Errorf("cannot resolve benchmark scratch directory")
	}
	var relative, relErr = filepath.Rel(root, resolved)
	if relErr != nil || !filepath.IsLocal(relative) || !strings.HasPrefix(filepath.Base(resolved), "amps-perfgate-") {
		return fmt.Errorf("refusing to remove unexpected benchmark directory %s", resolved)
	}
	return os.RemoveAll(resolved)
}

type pairedReport struct {
	Verdict          string               `json:"verdict"`
	Error            string               `json:"error,omitempty"`
	Reference        referenceConfig      `json:"reference"`
	CandidateCommit  string               `json:"candidate_commit"`
	CandidateDirty   bool                 `json:"candidate_dirty"`
	ReferenceBinary  string               `json:"reference_binary_sha256"`
	CandidateBinary  string               `json:"candidate_binary_sha256"`
	CandidateSources string               `json:"candidate_sources_sha256"`
	BootstrapSHA256  string               `json:"bootstrap_sha256"`
	VerifierSHA256   string               `json:"verifier_sha256"`
	Benchtime        string               `json:"benchtime"`
	PairCount        int                  `json:"pair_count"`
	AllocationUnit   string               `json:"allocation_budget_unit"`
	Environment      benchmarkEnvironment `json:"environment"`
	Results          []pairedResult       `json:"results"`
}

func pairedVerdict(results []pairedResult) (string, int) {
	if len(results) == 0 {
		return "INCONCLUSIVE", 3
	}
	var inconclusive bool
	for _, result := range results {
		if result.Verdict == "FAIL" {
			return "FAIL", 2
		}
		if result.Verdict != "PASS" {
			inconclusive = true
		}
	}
	if inconclusive {
		return "INCONCLUSIVE", 3
	}
	return "PASS", 0
}

func markReportError(report *pairedReport, err error) {
	if report.Error != "" {
		report.Error += "; "
	}
	report.Error += err.Error()
	if report.Verdict != "FAIL" {
		report.Verdict = "INCONCLUSIVE"
	}
}

func finalizePairedReport(report *pairedReport, code int, cleanup func() error, persist func([]byte) error) (int, error) {
	var finalErr error
	if cleanup != nil {
		if err := cleanup(); err != nil {
			markReportError(report, err)
			finalErr = err
			code = 1
		}
	}
	data, marshalErr := json.MarshalIndent(report, "", "  ")
	if marshalErr != nil {
		markReportError(report, marshalErr)
		finalErr = errors.Join(finalErr, marshalErr)
		if report.Verdict == "FAIL" {
			return 2, finalErr
		}
		return 1, finalErr
	}
	if persistErr := persist(append(data, '\n')); persistErr != nil {
		markReportError(report, persistErr)
		finalErr = errors.Join(finalErr, persistErr)
		code = 1
		updated, retryMarshalErr := json.MarshalIndent(report, "", "  ")
		if retryMarshalErr != nil {
			finalErr = errors.Join(finalErr, retryMarshalErr)
		} else {
			_ = persist(append(updated, '\n'))
		}
	}
	if finalErr != nil && report.Verdict == "FAIL" {
		code = 2
	}
	return code, finalErr
}

func writeReportAtomic(path string, data []byte) error {
	var temporary, err = os.CreateTemp(filepath.Dir(path), ".report-*")
	if err != nil {
		return err
	}
	var temporaryPath = temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	var _, writeErr = temporary.Write(data)
	var syncErr = temporary.Sync()
	var closeErr = temporary.Close()
	if err = errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func executePaired(baselinePath, referencePath, outputPath, benchtime string, pairs int) (exitCode int) {
	var duration, err = time.ParseDuration(benchtime)
	if err != nil || duration < time.Second || pairs != requiredPairs {
		fmt.Fprintln(os.Stderr, "paired gate requires exactly six pairs and benchtime >= 1s")
		return 1
	}
	if err = os.MkdirAll(outputPath, 0700); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	var lockPath = filepath.Join(outputPath, ".lock")
	var lock, lockErr = os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600) // #nosec G304 -- explicit local output directory.
	if lockErr != nil {
		fmt.Fprintf(os.Stderr, "another performance gate owns %s; %v\n", lockPath, lockErr)
		return 1
	}
	defer func() { _ = lock.Close(); _ = os.Remove(lockPath) }()
	var artifactPath, artifactErr = os.MkdirTemp(outputPath, "run-")
	if artifactErr != nil {
		fmt.Fprintln(os.Stderr, artifactErr)
		return 1
	}
	var report = pairedReport{Verdict: "INCONCLUSIVE", PairCount: pairs, Benchtime: benchtime, AllocationUnit: "whole allocs/op (Go benchmem); exact counters and consistent fractional regressions also checked"}
	var scratch string
	defer func() {
		var cleanup func() error
		if scratch != "" {
			cleanup = func() error { return removeBenchmarkScratch(scratch) }
		}
		var finalErr error
		exitCode, finalErr = finalizePairedReport(&report, exitCode, cleanup, func(data []byte) error { return writeReportAtomic(filepath.Join(artifactPath, "report.json"), data) })
		if finalErr != nil {
			fmt.Fprintln(os.Stderr, "performance finalization failed:", finalErr)
		}
		fmt.Println("perf gate artifacts:", artifactPath)
		fmt.Println("perf gate:", report.Verdict)
	}()
	var fail = func(err error) int {
		if verdict, _ := pairedVerdict(report.Results); verdict == "FAIL" {
			report.Verdict = verdict
		}
		markReportError(&report, err)
		fmt.Fprintln(os.Stderr, "performance collection error:", err)
		if report.Verdict == "FAIL" {
			return 2
		}
		return 1
	}
	var verifier string
	verifier, err = os.Executable()
	if err != nil {
		return fail(fmt.Errorf("locate performance verifier: %w", err))
	}
	report.VerifierSHA256, err = fileDigest(verifier)
	if err != nil {
		return fail(fmt.Errorf("hash performance verifier: %w", err))
	}
	var root, rootErr = os.Getwd()
	if rootErr != nil {
		return fail(rootErr)
	}
	report.Reference, err = readReference(referencePath, baselinePath, root)
	if err != nil {
		return fail(err)
	}
	if report.Reference.Toolchain != runtime.Version() {
		return fail(fmt.Errorf("gate must run with exact toolchain %s", report.Reference.Toolchain))
	}
	var baseline baselineFile
	baseline, err = loadPinnedBaseline(baselinePath)
	if err != nil {
		return fail(err)
	}
	var scratchErr error
	scratch, scratchErr = os.MkdirTemp("", "amps-perfgate-")
	if scratchErr != nil {
		return fail(scratchErr)
	}
	var ctx, cancel = context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	var revision, revisionErr = commandOutput(ctx, root, "git", []string{"rev-parse", "HEAD"}, nil)
	if revisionErr != nil {
		return fail(revisionErr)
	}
	report.CandidateCommit = strings.TrimSpace(string(revision))
	var status, statusErr = commandOutput(ctx, root, "git", []string{"status", "--porcelain"}, nil)
	if statusErr != nil {
		return fail(statusErr)
	}
	report.CandidateDirty = len(bytes.TrimSpace(status)) > 0
	var archive, archiveErr = commandOutput(ctx, root, "git", []string{"archive", "--format=tar", report.Reference.Commit}, nil)
	if archiveErr != nil {
		return fail(fmt.Errorf("immutable reference %s unavailable; checkout must include its history: %w: %s", report.Reference.Commit, archiveErr, archive))
	}
	var referenceRoot = filepath.Join(scratch, "reference")
	if err = extractReference(archive, referenceRoot); err != nil {
		return fail(err)
	}
	if err = verifyDigest(filepath.Join(referenceRoot, benchmarkFile), report.Reference.BenchmarkSHA256); err != nil {
		return fail(err)
	}
	if err = stripReferenceTests(referenceRoot); err != nil {
		return fail(err)
	}
	var candidateRoot = filepath.Join(scratch, "candidate")
	report.CandidateSources, err = prepareCandidateSnapshot(root, candidateRoot, report.Reference.Toolchain)
	if err != nil {
		return fail(err)
	}
	var names = benchmarkNamesForBaseline(baseline)
	report.BootstrapSHA256, err = writeBenchmarkBootstrap(referenceRoot, names)
	if err != nil {
		return fail(err)
	}
	var candidateBootstrap string
	candidateBootstrap, err = writeBenchmarkBootstrap(candidateRoot, names)
	if err = matchingDigestError("benchmark bootstrap", report.BootstrapSHA256, candidateBootstrap, err); err != nil {
		return fail(err)
	}
	var suffix string
	if runtime.GOOS == "windows" {
		suffix = ".exe"
	}
	var binaries = map[string]string{"reference": filepath.Join(scratch, "reference.test"+suffix), "candidate": filepath.Join(scratch, "candidate.test"+suffix)}
	report.ReferenceBinary, err = buildBenchmark(ctx, referenceRoot, binaries["reference"], filepath.Join(artifactPath, "reference-build.log"), report.Reference.Toolchain)
	if err != nil {
		return fail(err)
	}
	report.CandidateBinary, err = buildBenchmark(ctx, candidateRoot, binaries["candidate"], filepath.Join(artifactPath, "candidate-build.log"), report.Reference.Toolchain)
	if err != nil {
		return fail(err)
	}
	var cpu string
	for _, side := range []string{"reference", "candidate"} {
		var probe, probeErr = commandOutput(ctx, root, binaries[side], []string{"-test.run=^$", "-test.bench=" + benchmarkPattern([]string{names[0]}), "-test.benchmem", "-test.count=1", "-test.benchtime=1x", "-test.cpu=4"}, benchmarkEnv(report.Reference.Toolchain))
		if writeErr := os.WriteFile(filepath.Join(artifactPath, side+"-environment.txt"), probe, 0600); writeErr != nil {
			return fail(writeErr)
		}
		if probeErr != nil {
			return fail(probeErr)
		}
		var run benchmarkRun
		run, err = parseBenchmarkRun(string(probe), []string{names[0]}, 1, 4)
		if err != nil {
			return fail(err)
		}
		if cpu != "" && cpu != run.Environment.CPU {
			return fail(fmt.Errorf("CPU changed between environment probes"))
		}
		cpu = run.Environment.CPU
	}
	var runner pairedRunner = func(side, name string, pair int) (benchmarkRun, error) {
		if pair == 0 && side == "reference" {
			fmt.Println("perf gate measuring:", name)
		}
		var sampleCtx, sampleCancel = context.WithTimeout(ctx, max(time.Minute, duration*10))
		defer sampleCancel()
		var args = []string{"-test.run=^TestPerfGateMeasure$", "-test.count=1", "-test.benchtime=" + benchtime, "-test.cpu=4", "-test.timeout=" + max(time.Minute, duration*10).String()}
		var env = append(benchmarkEnv(report.Reference.Toolchain), "PERFGATE_BENCHMARK="+name, "PERFGATE_CPU="+cpu)
		var output, commandErr = commandOutput(sampleCtx, root, binaries[side], args, env)
		var logPath = filepath.Join(artifactPath, fmt.Sprintf("%s-%02d-%s.txt", name, pair+1, side))
		if writeErr := os.WriteFile(logPath, output, 0600); writeErr != nil {
			return benchmarkRun{}, writeErr
		}
		if commandErr != nil {
			return benchmarkRun{}, fmt.Errorf("benchmark command failed: %w; see %s", commandErr, logPath)
		}
		return parseExactBenchmarkRun(string(output), name)
	}
	var fallback = 10.0
	if baseline.MaxRegression > 0 {
		fallback = baseline.MaxRegression
	}
	report.Results, report.Environment, err = collectPairs(runner, baseline, fallback, pairs)
	if err != nil {
		return fail(err)
	}
	var finalSources string
	finalSources, err = candidateSnapshotDigest(root, report.Reference.Toolchain)
	if err = matchingDigestError("candidate source inputs", report.CandidateSources, finalSources, err); err != nil {
		return fail(err)
	}
	if _, err = loadPinnedBaseline(baselinePath); err != nil {
		return fail(err)
	}
	report.Verdict, exitCode = pairedVerdict(report.Results)
	for _, result := range report.Results {
		fmt.Printf("%s: %s normalized %.4f ns/op range [%.4f, %.4f], limit %.4f; reported allocs %g, limit %g, exact peak %.9g, sustained fractional regression %v\n", result.Name, result.Verdict, result.NormalizedNS, result.MinimumNS, result.MaximumNS, result.LimitNS, result.MaxAllocs, result.LimitAllocs, result.MaxExactAllocs, result.FractionalRegression)
	}
	return exitCode
}
