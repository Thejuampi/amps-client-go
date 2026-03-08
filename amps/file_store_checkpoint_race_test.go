package amps

import (
	"path/filepath"
	"testing"
	"time"
)

func TestFileBookmarkStoreCheckpointPreservesConcurrentMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bookmark_store_checkpoint_race.json")
	options := FileStoreOptions{
		UseWAL:             true,
		SyncOnWrite:        true,
		CheckpointInterval: 100,
	}
	store := NewFileBookmarkStoreWithOptions(path, options)
	store.Log(bookmarkMessage("sub-1", "10|1|"))

	checkpointReady := make(chan struct{})
	releaseCheckpoint := make(chan struct{})
	mutationAppend := make(chan struct{}, 1)

	currentFileStoreTestHooks.bookmarkCheckpointBeforeCommit = func() {
		close(checkpointReady)
		<-releaseCheckpoint
	}
	currentFileStoreTestHooks.bookmarkWalBeforeAppend = func() {
		select {
		case mutationAppend <- struct{}{}:
		default:
		}
	}
	t.Cleanup(func() {
		currentFileStoreTestHooks = fileStoreTestHooks{}
	})

	checkpointResult := make(chan error, 1)
	go func() {
		checkpointResult <- store.saveCheckpoint()
	}()

	select {
	case <-checkpointReady:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for bookmark checkpoint snapshot")
	}

	mutationResult := make(chan uint64, 1)
	go func() {
		mutationResult <- store.Log(bookmarkMessage("sub-1", "10|2|"))
	}()

	select {
	case <-mutationAppend:
	case <-time.After(250 * time.Millisecond):
	}

	close(releaseCheckpoint)

	select {
	case err := <-checkpointResult:
		if err != nil {
			t.Fatalf("bookmark checkpoint failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for bookmark checkpoint completion")
	}

	select {
	case seqNo := <-mutationResult:
		if seqNo == 0 {
			t.Fatalf("expected concurrent bookmark mutation to return a sequence")
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for bookmark mutation completion")
	}

	reloaded := NewFileBookmarkStoreWithOptions(path, options)
	if got := reloaded.GetMostRecent("sub-1"); got != "10|2|" {
		t.Fatalf("expected reloaded store to retain concurrent bookmark mutation, got %q", got)
	}
}

func TestFilePublishStoreCheckpointPreservesConcurrentMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "publish_store_checkpoint_race.json")
	options := FileStoreOptions{
		UseWAL:             true,
		SyncOnWrite:        true,
		CheckpointInterval: 100,
	}
	store := NewFilePublishStoreWithOptions(path, options)

	firstSequence, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":1}`)))
	if err != nil {
		t.Fatalf("seed store failed: %v", err)
	}
	if firstSequence == 0 {
		t.Fatalf("expected non-zero seed publish sequence")
	}

	checkpointReady := make(chan struct{})
	releaseCheckpoint := make(chan struct{})
	mutationAppend := make(chan struct{}, 1)

	currentFileStoreTestHooks.publishCheckpointBeforeCommit = func() {
		close(checkpointReady)
		<-releaseCheckpoint
	}
	currentFileStoreTestHooks.publishWalBeforeAppend = func() {
		select {
		case mutationAppend <- struct{}{}:
		default:
		}
	}
	t.Cleanup(func() {
		currentFileStoreTestHooks = fileStoreTestHooks{}
	})

	checkpointResult := make(chan error, 1)
	go func() {
		checkpointResult <- store.saveCheckpoint()
	}()

	select {
	case <-checkpointReady:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for publish checkpoint snapshot")
	}

	mutationResult := make(chan struct {
		sequence uint64
		err      error
	}, 1)
	go func() {
		sequence, storeErr := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":2}`)))
		mutationResult <- struct {
			sequence uint64
			err      error
		}{sequence: sequence, err: storeErr}
	}()

	select {
	case <-mutationAppend:
	case <-time.After(250 * time.Millisecond):
	}

	close(releaseCheckpoint)

	select {
	case err = <-checkpointResult:
		if err != nil {
			t.Fatalf("publish checkpoint failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for publish checkpoint completion")
	}

	select {
	case result := <-mutationResult:
		if result.err != nil {
			t.Fatalf("concurrent publish mutation failed: %v", result.err)
		}
		if result.sequence <= firstSequence {
			t.Fatalf("expected concurrent publish mutation to allocate a later sequence, got %d", result.sequence)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for publish mutation completion")
	}

	reloaded := NewFilePublishStoreWithOptions(path, options)
	if count := reloaded.UnpersistedCount(); count != 2 {
		t.Fatalf("expected reloaded publish store to retain both commands, got %d", count)
	}
}
