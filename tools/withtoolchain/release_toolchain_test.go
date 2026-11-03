package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func releaseConfig(t *testing.T, path string) string {
	t.Helper()
	var data, err = os.ReadFile(filepath.Join("..", "..", path))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func hasReleaseConfigLine(config string, line string) bool {
	return regexp.MustCompile(`(?m)^[\t ]*` + regexp.QuoteMeta(line) + `\r?$`).MatchString(config)
}

func TestReleaseToolchainReachesNestedGoCommands(t *testing.T) {
	var makefile = releaseConfig(t, "Makefile")
	for _, setting := range []string{
		"RELEASE_GO_TOOLCHAIN ?= go1.26.10",
		"export GOTOOLCHAIN := $(RELEASE_GO_TOOLCHAIN)",
		"PERF_GO_TOOLCHAIN ?= $(RELEASE_GO_TOOLCHAIN)",
		"VULN_GO_TOOLCHAIN ?= $(RELEASE_GO_TOOLCHAIN)",
	} {
		if !hasReleaseConfigLine(makefile, setting) {
			t.Errorf("release does not enforce patched toolchain: missing %q", setting)
		}
	}
}

func TestDirectGoCommandsPreferPatchedToolchain(t *testing.T) {
	var module = releaseConfig(t, "go.mod")
	if !regexp.MustCompile(`(?m)^toolchain go1\.26\.10\r?$`).MatchString(module) {
		t.Fatal("direct Go commands must prefer patched Go 1.26.10")
	}
	if !regexp.MustCompile(`(?m)^go 1\.25\.0\r?$`).MatchString(module) {
		t.Fatal("toolchain update must preserve the downstream Go 1.25 language floor")
	}
}

func TestCompatibilityCompilerCannotAutoUpgrade(t *testing.T) {
	var makefile = releaseConfig(t, "Makefile")
	var minimum = regexp.MustCompile(`(?m)^MIN_GO_TOOLCHAIN \?= (\S+)\r?$`).FindStringSubmatch(makefile)
	if len(minimum) != 2 || minimum[1] != "go1.25.13" {
		t.Fatalf("compatibility and old comparison compiler must be exact Go 1.25.13, got %q", minimum)
	}
	for _, setting := range []string{
		"PERF_COMPARE_OLD_TOOLCHAIN ?= $(MIN_GO_TOOLCHAIN)",
		"-toolchain $(MIN_GO_TOOLCHAIN) -- test",
		"-toolchain $(MIN_GO_TOOLCHAIN) -- build",
	} {
		if !strings.Contains(makefile, setting) {
			t.Errorf("compatibility compiler is not exercised: missing %q", setting)
		}
	}
}

func TestSecurityAndReleaseWorkflowsUsePatchedGo(t *testing.T) {
	for _, path := range []string{
		".github/workflows/release.yml",
		".github/workflows/nightly-analysis.yml",
		".github/workflows/codeql.yml",
	} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			var workflow = releaseConfig(t, path)
			if !hasReleaseConfigLine(workflow, `go-version: "1.26.10"`) || hasReleaseConfigLine(workflow, `go-version: "1.25.13"`) {
				t.Fatal("security and release jobs must use patched Go 1.26.10")
			}
		})
	}
}

func TestCIMatrixPreservesActualCompatibilityCompiler(t *testing.T) {
	var workflow = releaseConfig(t, ".github/workflows/ci.yml")
	var jobs = strings.Split(workflow, "  test-matrix:")
	if len(jobs) != 2 {
		t.Fatal("CI compatibility matrix missing")
	}
	var matrix = strings.Split(jobs[1], "\n  validate:")[0]
	if !hasReleaseConfigLine(matrix, "GOTOOLCHAIN: local") || !hasReleaseConfigLine(matrix, `go-version: "1.25.13"`) || !hasReleaseConfigLine(matrix, `go-version: "1.26.10"`) {
		t.Fatal("CI must test both the actual Go 1.25 floor and patched release compiler without automatic upgrade")
	}
	if !hasReleaseConfigLine(matrix, "run: go test -race ./... -skip Integration") {
		t.Fatal("matrix race check must use the selected compatibility compiler")
	}
	var nonMatrix = jobs[0] + strings.Split(jobs[1], "\n  validate:")[1]
	if hasReleaseConfigLine(nonMatrix, `go-version: "1.25.13"`) || hasReleaseConfigLine(workflow, `go-version: "1.26.3"`) {
		t.Fatal("non-compatibility CI jobs must not use vulnerable compilers")
	}
}
