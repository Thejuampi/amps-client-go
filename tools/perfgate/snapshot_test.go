package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/Thejuampi/amps-client-go/internal/hiddenprocess"
)

func snapshotFixture(t *testing.T) string {
	t.Helper()
	var root = t.TempDir()
	var files = map[string]string{
		"go.mod": "module example.test/snapshot\n\ngo 1.25.0\n",
		"go.sum": "",
		"amps/client.go": `package amps
import _ "embed"
import "example.test/snapshot/internal/helper"
//go:embed testdata/payload.txt
var payload string
func Candidate() string { return helper.Value + payload }
`,
		"amps/untracked.go":         "package amps\nvar Extra = 1\n",
		"amps/testdata/payload.txt": "data",
		"internal/helper/helper.go": "package helper\nconst Value = \"fixture:\"\n",
		"amps/perf_benchmark_test.go": `package amps
import "testing"
func TestHarnessSanity(t *testing.T) { if Candidate() != "fixture:data" { t.Fatal(Candidate()) } }
func BenchmarkCandidate(b *testing.B) { for range b.N { _ = Candidate() } }
`,
		"amps/fake_test.go": `package amps
import "testing"
func TestMain(m *testing.M) { panic("foreign TestMain ran") }
`,
		"amps/unused.txt": "not selected by go list",
	}
	for name, content := range files {
		writeSnapshotFixture(t, filepath.Join(root, filepath.FromSlash(name)), content)
	}
	return root
}

func writeSnapshotFixture(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func snapshotFileNames(t *testing.T, root string) []string {
	t.Helper()
	var names []string
	var err = filepath.WalkDir(root, func(path string, info os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			var relative, relErr = filepath.Rel(root, path)
			if relErr != nil {
				return relErr
			}
			names = append(names, filepath.ToSlash(relative))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(names)
	return names
}

func TestCandidateSnapshotSelectsProductionSourcesAndFixedHarness(t *testing.T) {
	var root = snapshotFixture(t)
	var destination = filepath.Join(t.TempDir(), "candidate")
	var digest, err = prepareCandidateSnapshot(root, destination, runtime.Version())
	if err != nil {
		t.Fatal(err)
	}
	var want = []string{"amps/client.go", "amps/perf_benchmark_test.go", "amps/testdata/payload.txt", "amps/untracked.go", "go.mod", "go.sum", "internal/helper/helper.go"}
	if got := snapshotFileNames(t, destination); !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot files = %v, want %v", got, want)
	}
	var current string
	current, err = candidateSnapshotDigest(root, runtime.Version())
	if err != nil || digest == "" || current != digest {
		t.Fatalf("prepared digest=%q current=%q err=%v", digest, current, err)
	}
	var cmd = exec.Command("go", "test", "-mod=readonly", "./amps", "-run", "^TestHarnessSanity$", "-count=1", "-v")
	hiddenprocess.HideConsole(cmd)
	cmd.Dir = destination
	cmd.Env = benchmarkEnv(runtime.Version())
	var output []byte
	output, err = cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "--- PASS: TestHarnessSanity") {
		t.Fatalf("snapshot did not run real harness: %v\n%s", err, output)
	}
}

func TestCandidateSnapshotDigestDetectsEachRelevantInput(t *testing.T) {
	for _, entry := range []struct {
		name  string
		path  string
		value string
	}{
		{"production source", "amps/client.go", "package amps\nvar Modified = 1\n"},
		{"new untracked source", "amps/new_source.go", "package amps\nvar NewSource = 1\n"},
		{"embedded content", "amps/testdata/payload.txt", "changed"},
		{"transitive module source", "internal/helper/helper.go", "package helper\nconst Value = \"changed:\"\n"},
		{"module file", "go.mod", "module example.test/snapshot\n\ngo 1.25.0\n\n// changed\n"},
		{"module sums", "go.sum", "example.test/dep v1.0.0 h1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n"},
		{"benchmark harness", "amps/perf_benchmark_test.go", "package amps\n// changed\n"},
	} {
		t.Run(entry.name, func(t *testing.T) {
			var root = snapshotFixture(t)
			var original, err = candidateSnapshotDigest(root, runtime.Version())
			if err != nil {
				t.Fatal(err)
			}
			writeSnapshotFixture(t, filepath.Join(root, filepath.FromSlash(entry.path)), entry.value)
			var changed string
			changed, err = candidateSnapshotDigest(root, runtime.Version())
			if err != nil {
				t.Fatal(err)
			}
			if original == changed {
				t.Fatalf("%s did not affect snapshot digest", entry.path)
			}
		})
	}
}

func TestCandidateSnapshotRejectsExistingDestination(t *testing.T) {
	var root = snapshotFixture(t)
	var destination = t.TempDir()
	var marker = filepath.Join(destination, "existing")
	writeSnapshotFixture(t, marker, "preserve")
	if _, err := prepareCandidateSnapshot(root, destination, runtime.Version()); err == nil {
		t.Fatal("accepted an existing destination")
	}
	var data, err = os.ReadFile(marker)
	if err != nil || string(data) != "preserve" {
		t.Fatalf("existing destination was modified: %q %v", data, err)
	}
}

func TestCandidateSnapshotRejectsFailedGoList(t *testing.T) {
	var root = t.TempDir()
	if _, err := candidateSnapshotDigest(root, runtime.Version()); err == nil {
		t.Fatal("accepted a project without a module or package")
	}
}

func TestCandidateSnapshotRejectsLocalModuleReplacement(t *testing.T) {
	var root = snapshotFixture(t)
	var replacement = filepath.Join(t.TempDir(), "dependency")
	writeSnapshotFixture(t, filepath.Join(replacement, "go.mod"), "module example.test/dep\n\ngo 1.25.0\n")
	writeSnapshotFixture(t, filepath.Join(replacement, "dep.go"), "package dep\nconst Value = 1\n")
	writeSnapshotFixture(t, filepath.Join(root, "go.mod"), "module example.test/snapshot\n\ngo 1.25.0\n\nrequire example.test/dep v0.0.0\nreplace example.test/dep => "+strconv.Quote(filepath.ToSlash(replacement))+"\n")
	writeSnapshotFixture(t, filepath.Join(root, "amps", "replacement.go"), "package amps\nimport _ \"example.test/dep\"\n")
	if _, err := candidateSnapshotDigest(root, runtime.Version()); err == nil || !strings.Contains(err.Error(), "local") {
		t.Fatalf("local module replacement should be rejected, got %v", err)
	}
}

func TestSnapshotIncludesEverySelectedSourceKind(t *testing.T) {
	var root = t.TempDir()
	var directory = filepath.Join(root, "amps")
	var pkg = snapshotPackage{
		Dir: directory, GoFiles: []string{"prod.go"}, CgoFiles: []string{"cgo.go"},
		CFiles: []string{"native.c"}, CXXFiles: []string{"native.cc"}, MFiles: []string{"native.m"},
		HFiles: []string{"native.h"}, FFiles: []string{"native.f"}, SFiles: []string{"native.s"},
		SwigFiles: []string{"native.swig"}, SwigCXXFiles: []string{"native.swigcxx"},
		SysoFiles: []string{"native.syso"}, EmbedFiles: []string{"data/embed.txt", "native.h"},
	}
	var want = []string{"amps/cgo.go", "amps/data/embed.txt", "amps/native.c", "amps/native.cc", "amps/native.f", "amps/native.h", "amps/native.m", "amps/native.s", "amps/native.swig", "amps/native.swigcxx", "amps/native.syso", "amps/prod.go"}
	for _, name := range want {
		writeSnapshotFixture(t, filepath.Join(root, filepath.FromSlash(name)), name)
	}
	var names = make(map[string]bool)
	if err := addSnapshotPackageFiles(root, pkg, names); err != nil {
		t.Fatal(err)
	}
	var got []string
	for name := range names {
		got = append(got, name)
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selected files=%v, want %v", got, want)
	}
}

func TestSnapshotRelativeFileRejectsNonlocalPaths(t *testing.T) {
	var root = snapshotFixture(t)
	var packageDir = filepath.Join(root, "amps")
	for _, entry := range []struct {
		dir, name string
	}{
		{packageDir, "../outside.go"},
		{packageDir, filepath.Join(t.TempDir(), "outside.go")},
		{t.TempDir(), "outside.go"},
	} {
		if _, err := snapshotRelativeFile(root, entry.dir, entry.name); err == nil {
			t.Fatalf("accepted nonlocal source directory=%q name=%q", entry.dir, entry.name)
		}
	}
}

func TestCandidateSnapshotRejectsEscapingSymlink(t *testing.T) {
	var root = snapshotFixture(t)
	var outside = filepath.Join(t.TempDir(), "outside.go")
	writeSnapshotFixture(t, outside, "package amps\nvar Outside = 1\n")
	var link = filepath.Join(root, "amps", "outside.go")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("creating symlinks is unavailable: %v", err)
	}
	if _, err := candidateSnapshotDigest(root, runtime.Version()); err == nil {
		t.Fatal("accepted a source symlink outside the candidate root")
	}
	if _, err := captureCandidateSnapshot(root, "", []string{"amps/outside.go"}); err == nil {
		t.Fatal("capture followed a source symlink outside the candidate root")
	}
}

func TestStripReferenceTestsKeepsOnlyCanonicalHarness(t *testing.T) {
	var scratch, err = os.MkdirTemp("", "amps-perfgate-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cleanupErr := removeBenchmarkScratch(scratch); cleanupErr != nil {
			t.Error(cleanupErr)
		}
	})
	var root = filepath.Join(scratch, "reference")
	for _, name := range []string{"amps/perf_benchmark_test.go", "amps/goleak_test.go", "amps/foreign_test.go", "internal/helper/helper_test.go", "amps/client.go", "amps/test_fixture.txt"} {
		writeSnapshotFixture(t, filepath.Join(root, filepath.FromSlash(name)), name)
	}
	if err := stripReferenceTests(root); err != nil {
		t.Fatal(err)
	}
	var want = []string{"amps/client.go", "amps/perf_benchmark_test.go", "amps/test_fixture.txt"}
	if got := snapshotFileNames(t, root); !reflect.DeepEqual(got, want) {
		t.Fatalf("reference files=%v, want %v", got, want)
	}
}

func TestStripReferenceTestsRejectsUnownedDirectory(t *testing.T) {
	for _, parent := range []string{"not-owned", "amps-perfgate-imitation"} {
		t.Run(parent, func(t *testing.T) {
			var root = filepath.Join(t.TempDir(), parent, "reference")
			var marker = filepath.Join(root, "amps", "must_not_delete_test.go")
			writeSnapshotFixture(t, marker, "keep")
			if err := stripReferenceTests(root); err == nil {
				t.Fatal("accepted a directory outside gate scratch")
			}
			if _, err := os.Stat(marker); err != nil {
				t.Fatalf("unowned file was removed: %v", err)
			}
		})
	}
}
