package amps

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileBookmarkStoreMutationsPropagateWALErrors(t *testing.T) {
	tempDir := t.TempDir()
	walDir := filepath.Join(tempDir, "waldir")
	if err := os.MkdirAll(walDir, 0o700); err != nil {
		t.Fatalf("MkdirAll walDir: %v", err)
	}

	store := &FileBookmarkStore{
		MemoryBookmarkStore: NewMemoryBookmarkStore(),
		path:                filepath.Join(tempDir, "checkpoint.json"),
		walPath:             walDir,
		options: FileStoreOptions{
			UseWAL:             true,
			SyncOnWrite:        false,
			CheckpointInterval: 100,
		},
	}

	message := bookmarkMessage("sub-1", "10|1|")
	seqNo, err := store.LogWithError(message)
	if err == nil {
		t.Fatalf("expected Log to propagate WAL append failure")
	}
	if seqNo == 0 {
		t.Fatalf("expected Log to return a non-zero sequence even on WAL failure")
	}

	if err = store.DiscardWithError("sub-1", seqNo); err == nil {
		t.Fatalf("expected Discard to propagate WAL append failure")
	}
	if err = store.DiscardMessageWithError(message); err == nil {
		t.Fatalf("expected DiscardMessage to propagate WAL append failure")
	}
	if _, err = store.PersistedWithError("sub-1", "10|1|"); err == nil {
		t.Fatalf("expected Persisted to propagate WAL append failure")
	}
	if err = store.PurgeWithError("sub-1"); err == nil {
		t.Fatalf("expected Purge to propagate WAL append failure")
	}
	if err = store.SetServerVersionWithError("6.0.0.0"); err == nil {
		t.Fatalf("expected SetServerVersion to propagate WAL append failure")
	}
}
