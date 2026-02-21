package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadWriteCoverage(t *testing.T) {
	if err := Write("", []byte("data")); err == nil {
		t.Fatalf("expected empty path write error")
	}
	if _, err := Read(""); err == nil {
		t.Fatalf("expected empty path read error")
	}

	path := filepath.Join(t.TempDir(), "wal.bin")
	input := []byte("wal-data")
	if err := Write(path, input); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	output, err := Read(path)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(output) != string(input) {
		t.Fatalf("unexpected wal output: %q", string(output))
	}

	// ReadFile error branch.
	if _, err := Read(filepath.Join(t.TempDir(), "missing.bin")); err == nil {
		t.Fatalf("expected missing file read error")
	}

	// WriteFile error branch (directory path).
	dirPath := filepath.Join(t.TempDir(), "subdir")
	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	if err := Write(dirPath, []byte("x")); err == nil {
		t.Fatalf("expected directory write error")
	}
}

