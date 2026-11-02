package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCIAnalysisRunsEveryBlockingScanOnce(t *testing.T) {
	var cwd, err = os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	var root, rootErr = repoRootFrom(cwd)
	if rootErr != nil {
		t.Fatal(rootErr)
	}
	var workflow, readErr = os.ReadFile(filepath.Join(root, ".github", "workflows", "ci.yml"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	var makefile, makeErr = os.ReadFile(filepath.Join(root, "Makefile"))
	if makeErr != nil {
		t.Fatal(makeErr)
	}
	var analysis = strings.SplitN(strings.SplitN(string(workflow), "\n  analysis:", 2)[1], "\n  test-matrix:", 2)[0]
	var counts = map[string]int{}
	for _, line := range strings.Split(analysis, "\n") {
		var command, found = strings.CutPrefix(strings.TrimSpace(line), "run: make ")
		if !found {
			continue
		}
		var visited = map[string]bool{}
		var visit func(string)
		visit = func(target string) {
			if visited[target] {
				return
			}
			visited[target] = true
			switch target {
			case "gosec-scan", "gosec-report":
				counts["gosec"]++
			case "static-scan", "vuln-scan", "secret-scan":
				counts[target]++
			default:
				for _, definition := range strings.Split(string(makefile), "\n") {
					if dependencies, ok := strings.CutPrefix(definition, target+":"); ok {
						for _, dependency := range strings.Fields(dependencies) {
							visit(dependency)
						}
						break
					}
				}
			}
		}
		for _, target := range strings.Fields(command) {
			if !strings.Contains(target, "=") {
				visit(target)
			}
		}
	}
	var want = map[string]int{"gosec": 1, "static-scan": 1, "vuln-scan": 1, "secret-scan": 1}
	if !reflect.DeepEqual(counts, want) {
		t.Fatalf("blocking scan executions = %v, want exactly %v", counts, want)
	}
}

func TestStrictReleaseRequiresOfficialCPPClient(t *testing.T) {
	var makefile, err = os.ReadFile(filepath.Join("..", "..", "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(makefile), "\n") {
		var dependencies, found = strings.CutPrefix(line, "release:")
		if !found {
			continue
		}
		for _, target := range strings.Fields(dependencies) {
			if target == "cpp-integration" {
				return
			}
		}
		t.Fatal("strict release must run cpp-integration against the official C++ client")
	}
	t.Fatal("strict release target is missing")
}
