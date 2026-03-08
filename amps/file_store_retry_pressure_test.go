package amps

import (
	"path/filepath"
	"testing"
)

func TestFileBookmarkStoreCheckpointFailureRetainsRetryPressure(t *testing.T) {
	path := t.TempDir()
	store := &FileBookmarkStore{
		MemoryBookmarkStore: NewMemoryBookmarkStore(),
		path:                path,
		walPath:             filepath.Join(path, "bookmark_store.json.wal"),
		options: FileStoreOptions{
			UseWAL:             true,
			SyncOnWrite:        false,
			CheckpointInterval: 1,
		},
	}

	err := store.bumpMutationAndMaybeCheckpoint()
	if err == nil {
		t.Fatalf("expected checkpoint failure when path is a directory")
	}
	if store.opsSinceCheckpoint != 1 {
		t.Fatalf("expected retry pressure to remain at 1 after checkpoint failure, got %d", store.opsSinceCheckpoint)
	}
}

func TestFilePublishStoreCheckpointFailureRetainsRetryPressure(t *testing.T) {
	path := t.TempDir()
	store := &FilePublishStore{
		MemoryPublishStore: NewMemoryPublishStore(),
		path:               path,
		walPath:            filepath.Join(path, "publish_store.json.wal"),
		options: FileStoreOptions{
			UseWAL:             true,
			SyncOnWrite:        false,
			CheckpointInterval: 1,
		},
	}

	err := store.bumpMutationAndMaybeCheckpoint()
	if err == nil {
		t.Fatalf("expected checkpoint failure when path is a directory")
	}
	if store.opsSinceCheckpoint != 1 {
		t.Fatalf("expected retry pressure to remain at 1 after checkpoint failure, got %d", store.opsSinceCheckpoint)
	}
}
