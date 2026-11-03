package main

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Thejuampi/amps-client-go/internal/hiddenprocess"
)

func TestReferenceManifestCannotAdvanceOrReplaceTheBaseline(t *testing.T) {
	var root, err = filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	var data, readErr = os.ReadFile(filepath.Join(root, "tools", "perf_reference.json"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	var original referenceConfig
	if err = json.Unmarshal(data, &original); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"commit", "toolchain", "baseline", "harness"} {
		t.Run(field, func(t *testing.T) {
			var changed = original
			switch field {
			case "commit":
				changed.Commit = strings.Repeat("a", 40)
			case "toolchain":
				changed.Toolchain = "go1.25.13"
			case "baseline":
				changed.BaselineSHA256 = strings.Repeat("b", 64)
			case "harness":
				changed.BenchmarkSHA256 = strings.Repeat("c", 64)
			}
			var altered, _ = json.Marshal(changed)
			var path = filepath.Join(t.TempDir(), "reference.json")
			if err = os.WriteFile(path, altered, 0600); err != nil {
				t.Fatal(err)
			}
			var _, configErr = readReference(path, filepath.Join(root, "tools", "perf_baseline.json"), root)
			if configErr == nil {
				t.Fatal("alternate reference was accepted")
			}
		})
	}
}

func TestExtractReferenceRejectsNonlocalAndLinkEntries(t *testing.T) {
	for _, entry := range []*tar.Header{
		{Name: "../escape", Mode: 0600, Typeflag: tar.TypeReg},
		{Name: "escape\\name", Mode: 0600, Typeflag: tar.TypeReg},
		{Name: "symlink", Linkname: "../escape", Typeflag: tar.TypeSymlink},
	} {
		var archive bytes.Buffer
		var writer = tar.NewWriter(&archive)
		if err := writer.WriteHeader(entry); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		if err := extractReference(archive.Bytes(), t.TempDir()); err == nil {
			t.Fatalf("accepted %q", entry.Name)
		}
	}
}

func TestExtractReferenceAcceptsActualPinnedGitArchive(t *testing.T) {
	var command = exec.Command("git", "--no-replace-objects", "archive", "--format=tar", "831ab636ae5e76cd06880edafb55d3e39d3bd7dd", benchmarkFile)
	hiddenprocess.HideConsole(command)
	command.Dir = filepath.Join("..", "..")
	var archive, err = command.Output()
	if err != nil {
		t.Skipf("pinned history unavailable in this checkout: %v", err)
	}
	var destination = t.TempDir()
	if err = extractReference(archive, destination); err != nil {
		t.Fatal(err)
	}
	if err = verifyDigest(filepath.Join(destination, benchmarkFile), "aaa21566db17cfa5d09a3acf3413dc7076dd3035c714470f987ed2cee397912e"); err != nil {
		t.Fatal(err)
	}
}
