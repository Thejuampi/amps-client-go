package amps

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestMMapWriteSyncsParentDirectory(t *testing.T) {
	originalSync := mmapSyncDirectory
	defer func() {
		mmapSyncDirectory = originalSync
	}()

	called := 0
	mmapSyncDirectory = func(path string) error {
		called++
		if path == "" {
			t.Fatalf("expected non-empty directory path")
		}
		return nil
	}

	path := filepath.Join(t.TempDir(), "mmap_sync.dat")
	if err := mmapWriteFile(path, []byte("payload"), 0o600, 64); err != nil {
		t.Fatalf("mmapWriteFile failed: %v", err)
	}
	if called != 1 {
		t.Fatalf("expected one parent directory sync, got %d", called)
	}
}

func TestMMapWritePropagatesDirectorySyncError(t *testing.T) {
	originalSync := mmapSyncDirectory
	defer func() {
		mmapSyncDirectory = originalSync
	}()

	mmapSyncDirectory = func(string) error { return errors.New("sync failed") }

	path := filepath.Join(t.TempDir(), "mmap_sync_error.dat")
	if err := mmapWriteFile(path, []byte("payload"), 0o600, 64); err == nil {
		t.Fatalf("expected mmapWriteFile to surface directory sync failure")
	}
}
