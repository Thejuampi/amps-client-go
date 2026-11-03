package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/Thejuampi/amps-client-go/internal/hiddenprocess"
)

func performanceWorkflowRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate workflow test")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
}

func readPerformanceWorkflow(t *testing.T, name string) string {
	t.Helper()
	var data, err = os.ReadFile(filepath.Join(performanceWorkflowRoot(t), ".github", "workflows", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func performanceWorkflowJob(t *testing.T, workflow, name string) string {
	t.Helper()
	var pattern = regexp.MustCompile(`(?m)^  ([a-zA-Z][a-zA-Z0-9_-]*):\s*$`)
	var matches = pattern.FindAllStringSubmatchIndex(workflow, -1)
	for index, match := range matches {
		if workflow[match[2]:match[3]] == name {
			var end = len(workflow)
			if index+1 < len(matches) {
				end = matches[index+1][0]
			}
			return workflow[match[0]:end]
		}
	}
	t.Fatalf("workflow job %q not found", name)
	return ""
}

func performanceWorkflowStep(t *testing.T, job, name string) string {
	t.Helper()
	var pattern = regexp.MustCompile(`(?m)^      - name: (.+)\s*$`)
	var matches = pattern.FindAllStringSubmatchIndex(job, -1)
	for index, match := range matches {
		if strings.TrimSpace(job[match[2]:match[3]]) == name {
			var end = len(job)
			if index+1 < len(matches) {
				end = matches[index+1][0]
			}
			return job[match[0]:end]
		}
	}
	t.Fatalf("workflow step %q not found", name)
	return ""
}

func requireWorkflowText(t *testing.T, text string, fragments ...string) {
	t.Helper()
	for _, fragment := range fragments {
		if !strings.Contains(text, fragment) {
			t.Errorf("missing %q in:\n%s", fragment, text)
		}
	}
}

func makeDryRun(t *testing.T, target string) string {
	t.Helper()
	if _, err := exec.LookPath("make"); err != nil {
		t.Skip("make is not available on this runner")
	}
	var cmd = exec.Command("make", "-n", target)
	hiddenprocess.HideConsole(cmd)
	cmd.Dir = performanceWorkflowRoot(t)
	var output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("make -n %s: %v\n%s", target, err, output)
	}
	return string(output)
}

func TestPerformanceMakeTargetUsesFixedPairedGate(t *testing.T) {
	var command = makeDryRun(t, "perf-check")
	requireWorkflowText(t, command, "./tools/perfgate", "-mode paired", "-baseline tools/perf_baseline.json", "-reference tools/perf_reference.json", "-pairs 6", "-benchtime 1s", "-package ./amps")
	for _, forbidden := range []string{"-report-only", "-retry-", "-max-regression"} {
		if strings.Contains(command, forbidden) {
			t.Errorf("blocking performance gate contains %q: %s", forbidden, command)
		}
	}
}

func TestPerformanceMakeComparisonIsDiagnostic(t *testing.T) {
	var command = makeDryRun(t, "perf-compare-toolchains")
	for _, fragment := range []string{"-mode absolute", "-report-only"} {
		if strings.Count(command, fragment) != 2 {
			t.Errorf("expected two %q invocations, got:\n%s", fragment, command)
		}
	}
	requireWorkflowText(t, command, "-samples 10", "benchstat")
	for _, forbidden := range []string{"-retry-", "-max-regression"} {
		if strings.Contains(command, forbidden) {
			t.Errorf("diagnostic comparison contains %q: %s", forbidden, command)
		}
	}
}

func TestSecurityMakeTargetsKeepScratchBuildsOutsideScanScope(t *testing.T) {
	for _, target := range []string{"gosec-scan", "gosec-report"} {
		t.Run(target, func(t *testing.T) {
			var command = makeDryRun(t, target)
			requireWorkflowText(t, command, "-exclude-dir=.tmp", "./...")
			var exclusions = regexp.MustCompile(`(?:^|\s)-exclude-dir=([^\s]+)`).FindAllStringSubmatch(command, -1)
			if len(exclusions) != 1 || exclusions[0][1] != ".tmp" || strings.Contains(command, "-exclude=") || strings.Contains(command, "-no-fail") {
				t.Fatalf("security scan changes rule or source coverage: %s", command)
			}
		})
	}
}

func TestPerformanceCIBlocksPullRequestsAndPushes(t *testing.T) {
	var workflow = readPerformanceWorkflow(t, "ci.yml")
	requireWorkflowText(t, workflow, "  pull_request:", "  push:")
	var job = performanceWorkflowJob(t, workflow, "performance")
	var header, _, _ = strings.Cut(job, "\n    steps:")
	if strings.Contains(header, "\n    if:") || strings.Contains(header, "continue-on-error") {
		t.Errorf("performance job must run and block on both PR and push: %s", header)
	}
	requireWorkflowText(t, performanceWorkflowStep(t, job, "Checkout"), "uses: actions/checkout@v6", "fetch-depth: 0")
	requireWorkflowText(t, performanceWorkflowStep(t, job, "Setup Go"), "uses: actions/setup-go@v6", `go-version: "1.26.10"`)
	var gate = performanceWorkflowStep(t, job, "Performance gate")
	requireWorkflowText(t, gate, "run: make perf-check")
	if strings.Contains(gate, "continue-on-error") || strings.Contains(gate, "if:") {
		t.Errorf("performance gate must run and block CI: %s", gate)
	}
	requirePerformanceArtifact(t, job)
	var validate = performanceWorkflowJob(t, workflow, "validate")
	requireWorkflowText(t, validate, "    if: always()", "      - performance", "PERFORMANCE_RESULT: ${{ needs.performance.result }}", `test "$PERFORMANCE_RESULT" = "success"`)
	var smoke = performanceWorkflowJob(t, workflow, "preprod-smoke")
	requireWorkflowText(t, smoke, "make stress-check STRESS_COUNT=2", "make integration-live-smoke")
	if strings.Contains(smoke, "make perf-check") {
		t.Error("preprod-smoke repeats the blocking performance job")
	}
}

func requirePerformanceArtifact(t *testing.T, job string) {
	t.Helper()
	var upload = performanceWorkflowStep(t, job, "Upload performance artifacts")
	requireWorkflowText(t, upload, "if: always()", "uses: actions/upload-artifact@v7", "path: .tmp/perf/gate/")
	if strings.Index(job, upload) < strings.Index(job, "run: make ") {
		t.Error("performance artifacts must be uploaded after the gate")
	}
}

func TestPerformanceReleaseUsesOriginalBaselineAndUploadsArtifacts(t *testing.T) {
	var workflow = readPerformanceWorkflow(t, "release.yml")
	var job = performanceWorkflowJob(t, workflow, "release")
	requireWorkflowText(t, performanceWorkflowStep(t, job, "Checkout"), "git fetch origin", "--tags")
	var gate = performanceWorkflowStep(t, job, "Release gates")
	requireWorkflowText(t, gate, "make release-hosted", "FUZZTIME=2s", "STRESS_COUNT=3")
	if strings.Contains(workflow, "PERF_BASELINE=") || strings.Contains(gate, "continue-on-error") {
		t.Error("release must enforce the original baseline without an error bypass")
	}
	requirePerformanceArtifact(t, job)
}

func TestPerformanceNightlyHasHistoryAndUploadsArtifacts(t *testing.T) {
	var workflow = readPerformanceWorkflow(t, "nightly-analysis.yml")
	var job = performanceWorkflowJob(t, workflow, "preprod")
	requireWorkflowText(t, performanceWorkflowStep(t, job, "Checkout"), "fetch-depth: 0")
	requireWorkflowText(t, performanceWorkflowStep(t, job, "Setup Go"), `go-version: "1.26.10"`)
	requireWorkflowText(t, performanceWorkflowStep(t, job, "Expanded pre-production checks"), "make preprod-check")
	requirePerformanceArtifact(t, job)
}
