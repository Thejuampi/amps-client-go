package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScanRepositoryRejectsOfficialBenchmarkClaim(t *testing.T) {
	var root = t.TempDir()
	writeTestFile(t, root, "README.md", "# Client\n\nGo vs Official C Client\n")

	var violations, err = scanRepository(root)
	if err != nil || len(violations) != 1 || violations[0].Path != "README.md" || violations[0].Line != 3 {
		t.Fatalf("scanRepository() = (%+v, %v), want one README.md:3 violation", violations, err)
	}
}

func TestScanRepositoryScansNestedDocumentation(t *testing.T) {
	var root = t.TempDir()
	writeTestFile(t, root, "docs/operations/performance.md", "The client outperforms the official implementation.\n")

	var violations, err = scanRepository(root)
	if err != nil || len(violations) != 1 || violations[0].Path != "docs/operations/performance.md" {
		t.Fatalf("scanRepository() = (%+v, %v), want nested documentation violation", violations, err)
	}
}

func TestScanRepositoryScansRootReleaseNotes(t *testing.T) {
	var root = t.TempDir()
	writeTestFile(t, root, "CHANGELOG.md", "This client is faster than the official client.\n")

	var violations, err = scanRepository(root)
	if err != nil || len(violations) != 1 || violations[0].Path != "CHANGELOG.md" {
		t.Fatalf("scanRepository() = (%+v, %v), want release-note violation", violations, err)
	}
}

func TestScanRepositoryRejectsPublishedExternalResultArtifact(t *testing.T) {
	var root = t.TempDir()
	writeTestFile(t, root, "tools/perf_tail_c_current.json", "{}\n")

	var violations, err = scanRepository(root)
	if err != nil || len(violations) != 1 || violations[0].Path != "tools/perf_tail_c_current.json" {
		t.Fatalf("scanRepository() = (%+v, %v), want external result artifact violation", violations, err)
	}
}

func TestScanRepositoryRejectsEachExternalBenchmarkClaimForm(t *testing.T) {
	var cases = map[string]string{
		"direct comparison": "Go vs official C client benchmarks",
		"outperformance":    "This client outperforms the official implementation",
		"faster than":       "This client is faster than the official client",
		"official baseline": "C baseline captured from the official distribution",
		"comparison report": "Comparison against official client benchmarks",
		"short form":        "Go-vs-C benchmark results",
	}

	for name, claim := range cases {
		t.Run(name, func(t *testing.T) {
			var root = t.TempDir()
			writeTestFile(t, root, "docs/performance.md", claim+"\n")

			var violations, err = scanRepository(root)
			if err != nil || len(violations) != 1 {
				t.Fatalf("scanRepository() = (%+v, %v), want one violation", violations, err)
			}
		})
	}
}

func TestScanRepositoryRejectsEachExternalResultArtifactForm(t *testing.T) {
	var cases = []string{
		"tools/perf_tail_c_current.json",
		"tools/perf_side_by_side_current.json",
		"tools/perf_side_by_side_report.md",
	}

	for _, relativePath := range cases {
		t.Run(relativePath, func(t *testing.T) {
			var root = t.TempDir()
			writeTestFile(t, root, relativePath, "external comparison results\n")

			var violations, err = scanRepository(root)
			if err != nil || len(violations) != 1 {
				t.Fatalf("scanRepository() = (%+v, %v), want one violation", violations, err)
			}
		})
	}
}

func TestScanRepositoryRejectsRenamedExternalResultArtifact(t *testing.T) {
	var root = t.TempDir()
	writeTestFile(t, root, "benchmarks/vendor_results.csv", "implementation,p95_ns_op\nOfficial C,23.74\nGo,21.67\n")

	var violations, err = scanRepository(root)
	if err != nil || len(violations) != 1 || violations[0].Path != "benchmarks/vendor_results.csv" {
		t.Fatalf("scanRepository() = (%+v, %v), want renamed result artifact violation", violations, err)
	}
}

func TestScanRepositoryRejectsRenamedMarkdownResultArtifact(t *testing.T) {
	var root = t.TempDir()
	writeTestFile(t, root, "benchmarks/vendor_results.md", "| implementation | p95_ns_op |\n|---|---:|\n| Official C | 23 |\n| Go | 21 |\n")

	var violations, err = scanRepository(root)
	if err != nil || len(violations) != 1 || violations[0].Path != "benchmarks/vendor_results.md" {
		t.Fatalf("scanRepository() = (%+v, %v), want renamed markdown result artifact violation", violations, err)
	}
}

func TestScanRepositoryAllowsPrivateExternalResults(t *testing.T) {
	var root = t.TempDir()
	writeTestFile(t, root, ".tmp/perf/external/vendor_results.csv", "implementation,p95_ns_op\nOfficial C,23.74\nGo,21.67\n")

	var violations, err = scanRepository(root)
	if err != nil || len(violations) != 0 {
		t.Fatalf("scanRepository() = (%+v, %v), want private results ignored", violations, err)
	}
}

func TestScanRepositoryAllowsInternalRegressionBenchmarks(t *testing.T) {
	var root = t.TempDir()
	writeTestFile(t, root, "README.md", "Internal Go benchmarks detect performance regressions.\n")
	writeTestFile(t, root, "tools/perf_tail_go_api_current.json", "{}\n")

	var violations, err = scanRepository(root)
	if err != nil || len(violations) != 0 {
		t.Fatalf("scanRepository() = (%+v, %v), want no violations", violations, err)
	}
}

func TestScanRepositoryRejectsMissingRoot(t *testing.T) {
	var root = filepath.Join(t.TempDir(), "missing")

	var _, err = scanRepository(root)
	if err == nil {
		t.Fatal("scanRepository() error = nil, want missing-root error")
	}
}

func writeTestFile(t *testing.T, root string, relativePath string, content string) {
	t.Helper()

	var path = filepath.Join(root, filepath.FromSlash(relativePath))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create test directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}
}
