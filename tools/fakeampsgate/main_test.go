package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFakeAMPSURIUsesJSONPath(t *testing.T) {
	var got = fakeAMPSURI("127.0.0.1:19000")
	if got != "tcp://127.0.0.1:19000/amps/json" {
		t.Fatalf("fakeAMPSURI() = %q", got)
	}
}

func TestRepoRootFromFindsAncestorGoMod(t *testing.T) {
	var root = t.TempDir()
	var err = os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/test\n"), 0600)
	if err != nil {
		t.Fatalf("write go.mod: %v", err)
	}

	var nested = filepath.Join(root, "a", "b", "c")
	err = os.MkdirAll(nested, 0755)
	if err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}

	var got string
	got, err = repoRootFrom(nested)
	if err != nil || got != root {
		t.Fatalf("repoRootFrom() = (%q, %v)", got, err)
	}
}

func TestRepoRootFromErrorsWithoutGoMod(t *testing.T) {
	var _, err = repoRootFrom(t.TempDir())
	if err == nil {
		t.Fatalf("expected missing go.mod error")
	}
}
