package main

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Thejuampi/amps-client-go/internal/hiddenprocess"
)

func isolatedGitFixture(t *testing.T, alternate bool) (string, []string, string, string) {
	t.Helper()
	var root = t.TempDir()
	var config = filepath.Join(t.TempDir(), "empty-git-config")
	if err := os.WriteFile(config, nil, 0600); err != nil {
		t.Fatal(err)
	}
	var env []string
	for _, entry := range os.Environ() {
		var key, _, _ = strings.Cut(entry, "=")
		if strings.HasPrefix(strings.ToUpper(key), "GIT_") {
			continue
		}
		env = append(env, entry)
	}
	env = append(env, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+config, "GIT_TERMINAL_PROMPT=0")
	var git = func(args ...string) string {
		return runFixtureGit(t, root, env, args...)
	}
	git("init", "--quiet")
	git("config", "user.name", "Performance Gate Test")
	git("config", "user.email", "performance-gate@example.invalid")
	git("config", "commit.gpgsign", "false")
	git("config", "core.autocrlf", "false")
	if err := os.WriteFile(filepath.Join(root, "input.txt"), []byte("original\n"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", "input.txt")
	git("commit", "--quiet", "-m", "original")
	var original = strings.TrimSpace(git("rev-parse", "HEAD"))
	var originalTree = strings.TrimSpace(git("rev-parse", original+"^{tree}"))
	if err := os.WriteFile(filepath.Join(root, "input.txt"), []byte("replacement\n"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", "input.txt")
	git("commit", "--quiet", "-m", "replacement")
	var replacement = strings.TrimSpace(git("rev-parse", "HEAD"))
	if alternate {
		git("update-ref", "refs/perfgate-replacements/"+original, replacement)
		env = append(env, "GIT_REPLACE_REF_BASE=refs/perfgate-replacements/")
	} else {
		git("replace", original, replacement)
	}
	if got := runFixtureGit(t, root, env, "show", original+":input.txt"); got != "replacement\n" {
		t.Fatalf("replacement fixture is inactive: %q", got)
	}
	return root, env, original, originalTree
}

func runFixtureGit(t *testing.T, directory string, env []string, args ...string) string {
	t.Helper()
	var cmd = exec.Command("git", args...)
	hiddenprocess.HideConsole(cmd)
	cmd.Dir, cmd.Env = directory, env
	var output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
	return string(output)
}

func archivedInput(data []byte) (string, error) {
	var reader = tar.NewReader(bytes.NewReader(data))
	for {
		var header, err = reader.Next()
		if err == io.EOF {
			return "", fmt.Errorf("input.txt missing from archive")
		}
		if err != nil {
			return "", err
		}
		if header.Name == "input.txt" {
			var content []byte
			content, err = io.ReadAll(reader)
			return string(content), err
		}
	}
}

func TestProvenanceGitCommandsIgnoreRealReplacementRefs(t *testing.T) {
	for _, alternate := range []bool{false, true} {
		var name = "standard refs/replace"
		if alternate {
			name = "GIT_REPLACE_REF_BASE"
		}
		t.Run(name, func(t *testing.T) {
			var root, env, original, originalTree = isolatedGitFixture(t, alternate)
			var ctx = context.Background()
			var content, err = commandOutput(ctx, root, "git", []string{"show", original + ":input.txt"}, env)
			if err != nil || string(content) != "original\n" {
				t.Fatalf("show used replacement object: %q, %v", content, err)
			}
			var tree []byte
			tree, err = commandOutput(ctx, root, "git", []string{"rev-parse", original + "^{tree}"}, env)
			if err != nil || strings.TrimSpace(string(tree)) != originalTree {
				t.Fatalf("tree lookup used replacement object: %q, want %q; %v", tree, originalTree, err)
			}
			var archive []byte
			archive, err = commandOutput(ctx, root, "git", []string{"archive", "--format=tar", original, "input.txt"}, env)
			if err != nil {
				t.Fatal(err)
			}
			var archived string
			archived, err = archivedInput(archive)
			if err != nil || archived != "original\n" {
				t.Fatalf("archive used replacement object: %q, %v", archived, err)
			}
		})
	}
}

func TestProvenanceGitArchiveKeepsTraceDiagnosticsOutOfTar(t *testing.T) {
	var root, env, original, _ = isolatedGitFixture(t, false)
	env = append(env, "GIT_TRACE=1")
	var archive, err = commandOutput(context.Background(), root, "git", []string{"archive", "--format=tar", original, "input.txt"}, env)
	if err != nil {
		t.Fatal(err)
	}
	var content string
	content, err = archivedInput(archive)
	if err != nil || content != "original\n" {
		t.Fatalf("Git diagnostics corrupted reference archive: %q, %v", content, err)
	}
}

func TestProvenanceGitFailurePreservesExitStatusAndDiagnostics(t *testing.T) {
	var root, env, _, _ = isolatedGitFixture(t, false)
	var _, err = commandOutput(context.Background(), root, "git", []string{"show", "object-that-does-not-exist"}, env)
	var exitError *exec.ExitError
	if err == nil || !errors.As(err, &exitError) || !strings.Contains(err.Error(), "fatal:") {
		t.Fatalf("lost Git error status or diagnostics: %v", err)
	}
}
