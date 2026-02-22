//go:build !windows

package amps

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMMapUnixWriteReadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_roundtrip.dat")
	payload := []byte("hello mmap")

	if err := mmapWriteFile(path, payload, 0o600, 4096); err != nil {
		t.Fatalf("mmapWriteFile failed: %v", err)
	}

	readBack, err := mmapReadFile(path)
	if err != nil {
		t.Fatalf("mmapReadFile failed: %v", err)
	}
	if string(readBack) != string(payload) {
		t.Fatalf("unexpected payload: got %q want %q", string(readBack), string(payload))
	}
}

func TestMMapUnixReadEmptyAndMissing(t *testing.T) {
	tempDir := t.TempDir()

	emptyPath := filepath.Join(tempDir, "empty.dat")
	if err := os.WriteFile(emptyPath, []byte{}, 0o600); err != nil {
		t.Fatalf("write empty file failed: %v", err)
	}

	data, err := mmapReadFile(emptyPath)
	if err != nil {
		t.Fatalf("mmapReadFile empty failed: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("expected empty payload, got %d bytes", len(data))
	}

	if _, err = mmapReadFile(filepath.Join(tempDir, "missing.dat")); err == nil {
		t.Fatalf("expected missing file error")
	}
}

func TestMMapUnixWriteValidationErrors(t *testing.T) {
	if err := mmapWriteFile("", []byte("x"), 0o600, 1); err == nil {
		t.Fatalf("expected error for empty path")
	}
}
