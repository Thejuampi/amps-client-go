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
	if seqNo != 0 {
		t.Fatalf("expected Log to return zero sequence on WAL failure, got %d", seqNo)
	}
	if oldest := store.GetOldestBookmarkSeq("sub-1"); oldest != 0 {
		t.Fatalf("expected WAL failure to leave bookmark state unchanged, got oldest %d", oldest)
	}

	if err = store.DiscardWithError("sub-1", seqNo); err == nil {
		t.Fatalf("expected Discard to propagate WAL append failure")
	}
	store.MemoryBookmarkStore.Log(message)
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

func TestNewFileBookmarkStoreWithOptionsRetainsLoadError(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "bad_checkpoint.json")
	if err := os.WriteFile(path, []byte("{bad json"), 0o600); err != nil {
		t.Fatalf("write malformed checkpoint: %v", err)
	}

	store := NewFileBookmarkStoreWithOptions(path, defaultFileStoreOptions())
	if err := store.LoadError(); err == nil {
		t.Fatalf("expected constructor to retain load error")
	}

	seqNo, err := store.LogWithError(bookmarkMessage("sub-1", "10|1|"))
	if err == nil {
		t.Fatalf("expected LogWithError to surface retained load error")
	}
	if seqNo != 0 {
		t.Fatalf("expected zero sequence when load failed, got %d", seqNo)
	}
}

func TestMemoryBookmarkStoreErrorWrappers(t *testing.T) {
	store := NewMemoryBookmarkStore()
	message := bookmarkMessage("sub-wrap", "11|1|")

	seqNo, err := store.LogWithError(message)
	if err != nil || seqNo == 0 {
		t.Fatalf("LogWithError failed: seq=%d err=%v", seqNo, err)
	}

	if err := store.DiscardWithError("sub-wrap", seqNo); err != nil {
		t.Fatalf("DiscardWithError failed: %v", err)
	}
	if err := store.DiscardMessageWithError(message); err != nil {
		t.Fatalf("DiscardMessageWithError failed: %v", err)
	}
	if persisted, err := store.PersistedWithError("sub-wrap", "11|1|"); err != nil || persisted != "11|1|" {
		t.Fatalf("PersistedWithError failed: persisted=%q err=%v", persisted, err)
	}
	if err := store.SetServerVersionWithError("6.0.0.1"); err != nil {
		t.Fatalf("SetServerVersionWithError failed: %v", err)
	}
	if err := store.PurgeWithError("sub-wrap"); err != nil {
		t.Fatalf("PurgeWithError failed: %v", err)
	}
	if mostRecent := store.GetMostRecent("sub-wrap"); mostRecent != "" {
		t.Fatalf("expected wrapped purge to clear sub-wrap state, got %q", mostRecent)
	}
}

func TestFileBookmarkStoreAppendUpsertForWritesExistingRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bookmark_store_append_upsert.json")
	options := FileStoreOptions{
		UseWAL:             true,
		SyncOnWrite:        false,
		CheckpointInterval: 100,
	}

	store := NewFileBookmarkStoreWithOptions(path, options)
	message := bookmarkMessage("sub-upsert", "12|1|")
	if seqNo := store.MemoryBookmarkStore.Log(message); seqNo == 0 {
		t.Fatalf("expected seed bookmark record")
	}

	if err := store.appendUpsertFor("sub-upsert", "12|1|"); err != nil {
		t.Fatalf("appendUpsertFor failed: %v", err)
	}

	reloaded := NewFileBookmarkStoreWithOptions(path, options)
	if mostRecent := reloaded.GetMostRecent("sub-upsert"); mostRecent != "12|1|" {
		t.Fatalf("expected WAL replay to recover bookmark, got %q", mostRecent)
	}
}
