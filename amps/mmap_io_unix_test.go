//go:build !windows

package amps

import (
	"io"
	"os"
	"path/filepath"
	"syscall"
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

func TestNormalizeMMapPath(t *testing.T) {
	var got = normalizeMMapPath("./mmap/../store.dat")

	if got != "store.dat" {
		t.Fatalf("normalizeMMapPath() = %q, want store.dat", got)
	}
}

func TestOpenMMapReadAndWriteFileHelpers(t *testing.T) {
	var path = filepath.Join(t.TempDir(), "helpers.dat")

	file, err := openMMapWriteFile(path, 0o600)
	if err != nil {
		t.Fatalf("openMMapWriteFile() error = %v", err)
	}
	if _, err = file.Write([]byte("payload")); err != nil {
		_ = file.Close()
		t.Fatalf("Write() error = %v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	file, err = openMMapReadFile(path)
	if err != nil {
		t.Fatalf("openMMapReadFile() error = %v", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(data) != "payload" {
		t.Fatalf("ReadAll() = %q, want payload", string(data))
	}
}

func TestMSyncMappedBytes(t *testing.T) {
	if errno := msyncMappedBytes(nil); errno != 0 {
		t.Fatalf("msyncMappedBytes(nil) = %v, want 0", errno)
	}

	var path = filepath.Join(t.TempDir(), "mapped.dat")
	file, err := openMMapWriteFile(path, 0o600)
	if err != nil {
		t.Fatalf("openMMapWriteFile() error = %v", err)
	}
	defer file.Close()

	if err = file.Truncate(4096); err != nil {
		t.Fatalf("Truncate() error = %v", err)
	}

	mapped, err := syscall.Mmap(int(file.Fd()), 0, 4096, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		t.Fatalf("Mmap() error = %v", err)
	}
	defer syscall.Munmap(mapped)

	copy(mapped, []byte("payload"))
	if errno := msyncMappedBytes(mapped); errno != 0 {
		t.Fatalf("msyncMappedBytes(mapped) = %v, want 0", errno)
	}
}
