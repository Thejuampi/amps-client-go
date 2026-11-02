//go:build windows

package amps

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestMMapCheckpointSyncFailurePreservesWALAndOldCheckpoint(t *testing.T) {
	var path = filepath.Join(t.TempDir(), "publish")
	var options = FileStoreOptions{UseWAL: true, SyncOnWrite: true, CheckpointInterval: 1, MMap: MMapOptions{Enabled: true}}
	var store = NewFilePublishStoreWithOptions(path, options)
	if _, err := store.Store(NewCommand("publish").SetTopic("first")); err != nil {
		t.Fatal(err)
	}
	var before, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var original = mmapSyncFile
	var failure = errors.New("injected mmap sync failure")
	mmapSyncFile = func(*os.File) error { return failure }
	var _, storeErr = store.Store(NewCommand("publish").SetTopic("second"))
	mmapSyncFile = original
	if !errors.Is(storeErr, failure) {
		t.Fatalf("store error = %v", storeErr)
	}
	var after, readErr = os.ReadFile(path)
	if readErr != nil || string(after) != string(before) {
		t.Fatalf("checkpoint changed after failed sync: %v", readErr)
	}
	var wal, walErr = os.ReadFile(path + ".wal")
	if walErr != nil || len(wal) == 0 {
		t.Fatalf("WAL lost after failed sync: %v", walErr)
	}
	var recovered = NewFilePublishStoreWithOptions(path, options)
	if recovered.LoadError() != nil || recovered.UnpersistedCount() != 2 {
		t.Fatalf("recovery: error=%v count=%d", recovered.LoadError(), recovered.UnpersistedCount())
	}
}
