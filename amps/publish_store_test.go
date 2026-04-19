package amps

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Thejuampi/amps-client-go/amps/internal/wal"
)

func isPublishStoreSignalClosed(signal <-chan struct{}) bool {
	select {
	case <-signal:
		return true
	default:
		return false
	}
}

func TestMemoryPublishStoreReplayAndDiscard(t *testing.T) {
	store := NewMemoryPublishStore()
	first := NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":1}`))
	second := NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":2}`))

	firstSeq, err := store.Store(first)
	if err != nil {
		t.Fatalf("store first failed: %v", err)
	}
	secondSeq, err := store.Store(second)
	if err != nil {
		t.Fatalf("store second failed: %v", err)
	}
	if firstSeq == 0 || secondSeq <= firstSeq {
		t.Fatalf("unexpected sequence values: %d, %d", firstSeq, secondSeq)
	}

	var replayed []uint64
	err = store.Replay(func(command *Command) error {
		sequence, hasSequence := command.SequenceID()
		if !hasSequence {
			t.Fatalf("replayed command missing sequence ID")
		}
		replayed = append(replayed, sequence)
		return nil
	})
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	if len(replayed) != 2 || replayed[0] != firstSeq || replayed[1] != secondSeq {
		t.Fatalf("unexpected replay order: %+v", replayed)
	}

	if err := store.DiscardUpTo(firstSeq); err != nil {
		t.Fatalf("discard failed: %v", err)
	}
	if store.UnpersistedCount() != 1 {
		t.Fatalf("expected one unpersisted command after discard, got %d", store.UnpersistedCount())
	}
}

func TestFilePublishStoreRoundTrip(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "publish_store.json")

	store := NewFilePublishStore(path)
	sequence, err := store.Store(NewCommand("publish").SetTopic("topic").SetData([]byte("payload")))
	if err != nil {
		t.Fatalf("store failed: %v", err)
	}
	if sequence == 0 {
		t.Fatalf("expected non-zero sequence")
	}

	reloaded := NewFilePublishStore(path)
	if reloaded.UnpersistedCount() != 1 {
		t.Fatalf("expected one recovered entry, got %d", reloaded.UnpersistedCount())
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected persisted file at %s: %v", path, err)
	}
}

func TestMemoryPublishStoreAdditionalCoverage(t *testing.T) {
	store := NewMemoryPublishStore()
	command := NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":10}`))
	sequence, err := store.Store(command)
	if err != nil || sequence == 0 {
		t.Fatalf("store failed: seq=%d err=%v", sequence, err)
	}

	found, err := store.ReplaySingle(func(replayed *Command) error {
		if replayed == nil {
			t.Fatalf("expected replayed command")
		}
		return nil
	}, sequence)
	if err != nil || !found {
		t.Fatalf("expected ReplaySingle hit: found=%v err=%v", found, err)
	}
	found, err = store.ReplaySingle(func(*Command) error { return nil }, sequence+100)
	if err != nil || found {
		t.Fatalf("expected ReplaySingle miss")
	}

	if lowest := store.GetLowestUnpersisted(); lowest != sequence {
		t.Fatalf("unexpected lowest unpersisted: %d", lowest)
	}
	if persisted := store.GetLastPersisted(); persisted != 0 {
		t.Fatalf("unexpected last persisted before discard: %d", persisted)
	}

	store.SetErrorOnPublishGap(true)
	if !store.ErrorOnPublishGap() {
		t.Fatalf("expected error-on-gap enabled")
	}
	if err := store.DiscardUpTo(sequence); err != nil {
		t.Fatalf("discard up to sequence failed: %v", err)
	}
	if err := store.DiscardUpTo(sequence - 1); err == nil {
		t.Fatalf("expected publish gap error")
	}

	if err := store.Flush(20 * time.Millisecond); err != nil {
		t.Fatalf("flush should succeed when store empty: %v", err)
	}

	store.SetErrorOnPublishGap(false)
	seq2, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":11}`)))
	if err != nil || seq2 == 0 {
		t.Fatalf("second store failed: seq=%d err=%v", seq2, err)
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = store.DiscardUpTo(seq2)
	}()
	if err := store.Flush(0); err != nil {
		t.Fatalf("zero-timeout flush should block until empty: %v", err)
	}

	_, _ = store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":12}`)))
	if err := store.Flush(5 * time.Millisecond); err == nil {
		t.Fatalf("expected timed flush error when entries remain")
	}
}

func TestMemoryPublishStoreDrainSignalLifecycle(t *testing.T) {
	var store = NewMemoryPublishStore()
	if !isPublishStoreSignalClosed(store.drainedCh) {
		t.Fatalf("expected initial drain signal to be closed for empty store")
	}

	var initialSignal = store.drainedCh
	var sequence, err = store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":21}`)))
	if err != nil {
		t.Fatalf("store failed: %v", err)
	}
	if isPublishStoreSignalClosed(store.drainedCh) {
		t.Fatalf("expected drain signal to remain open while entries are pending")
	}
	if store.drainedCh == initialSignal {
		t.Fatalf("expected non-empty transition to replace the closed drain signal")
	}

	err = store.DiscardUpTo(sequence)
	if err != nil {
		t.Fatalf("discard failed: %v", err)
	}
	if !isPublishStoreSignalClosed(store.drainedCh) {
		t.Fatalf("expected drain signal to close when store becomes empty")
	}

	var drainedSignal = store.drainedCh
	_, err = store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":22}`)))
	if err != nil {
		t.Fatalf("second store failed: %v", err)
	}
	if store.drainedCh == drainedSignal {
		t.Fatalf("expected refill to reopen the drain signal")
	}
	if isPublishStoreSignalClosed(store.drainedCh) {
		t.Fatalf("expected reopened drain signal to be open until discard")
	}
}

func TestMemoryPublishStoreFlushReleasesConcurrentWaiters(t *testing.T) {
	var store = NewMemoryPublishStore()
	var sequence, err = store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":23}`)))
	if err != nil {
		t.Fatalf("store failed: %v", err)
	}

	var results = make(chan error, 2)
	go func() {
		results <- store.Flush(250 * time.Millisecond)
	}()
	go func() {
		results <- store.Flush(250 * time.Millisecond)
	}()

	time.Sleep(20 * time.Millisecond)
	err = store.DiscardUpTo(sequence)
	if err != nil {
		t.Fatalf("discard failed: %v", err)
	}

	select {
	case err = <-results:
		if err != nil {
			t.Fatalf("first waiter failed: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("timed out waiting for first flush waiter")
	}

	select {
	case err = <-results:
		if err != nil {
			t.Fatalf("second waiter failed: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("timed out waiting for second flush waiter")
	}
}

func TestFilePublishStoreDrainSignalLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file_publish_store_drain.json")
	store := NewFilePublishStore(path)
	if !isPublishStoreSignalClosed(store.drainedCh) {
		t.Fatalf("expected initial drain signal to be closed for empty file store")
	}

	initialSignal := store.drainedCh
	sequence, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":24}`)))
	if err != nil {
		t.Fatalf("store failed: %v", err)
	}
	if isPublishStoreSignalClosed(store.drainedCh) {
		t.Fatalf("expected drain signal to remain open while file store has pending entries")
	}
	if store.drainedCh == initialSignal {
		t.Fatalf("expected file store to replace the closed drain signal on first store")
	}

	if err := store.DiscardUpTo(sequence); err != nil {
		t.Fatalf("discard failed: %v", err)
	}
	if !isPublishStoreSignalClosed(store.drainedCh) {
		t.Fatalf("expected drain signal to close when file store becomes empty")
	}

	drainedSignal := store.drainedCh
	if _, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":25}`))); err != nil {
		t.Fatalf("second store failed: %v", err)
	}
	if store.drainedCh == drainedSignal {
		t.Fatalf("expected refill to reopen the file-store drain signal")
	}
	if isPublishStoreSignalClosed(store.drainedCh) {
		t.Fatalf("expected reopened file-store drain signal to remain open until discard")
	}
}

func TestFilePublishStoreAdditionalCoverage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "publish_store_wrappers.json")
	store := NewFilePublishStore(path)
	sequence, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte("payload")))
	if err != nil || sequence == 0 {
		t.Fatalf("store failed: seq=%d err=%v", sequence, err)
	}
	store.SetErrorOnPublishGap(true)
	if err := store.DiscardUpTo(sequence); err != nil {
		t.Fatalf("discard failed: %v", err)
	}

	reloaded := NewFilePublishStore(path)
	if !reloaded.ErrorOnPublishGap() {
		t.Fatalf("expected persisted error-on-gap setting")
	}
}

func TestFilePublishStoreWithOptionsCoverage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "publish_store_options.json")
	store := NewFilePublishStoreWithOptions(path, FileStoreOptions{
		UseWAL:             true,
		SyncOnWrite:        true,
		CheckpointInterval: 2,
		MMap: MMapOptions{
			Enabled:     true,
			InitialSize: 1024,
		},
	})

	firstSeq, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":1}`)))
	if err != nil || firstSeq == 0 {
		t.Fatalf("first store failed: seq=%d err=%v", firstSeq, err)
	}
	secondSeq, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":2}`)))
	if err != nil || secondSeq == 0 || secondSeq <= firstSeq {
		t.Fatalf("second store failed: seq=%d err=%v", secondSeq, err)
	}

	reloaded := NewFilePublishStoreWithOptions(path, FileStoreOptions{
		UseWAL:             true,
		SyncOnWrite:        true,
		CheckpointInterval: 2,
		MMap: MMapOptions{
			Enabled:     true,
			InitialSize: 1024,
		},
	})
	if reloaded.UnpersistedCount() != 2 {
		t.Fatalf("expected two recovered entries with options, got %d", reloaded.UnpersistedCount())
	}
}

func TestPublishStoreNilAndErrorCoverage(t *testing.T) {
	var nilMemory *MemoryPublishStore
	if _, err := nilMemory.Store(nil); err == nil {
		t.Fatalf("expected nil memory store error")
	}
	if err := nilMemory.DiscardUpTo(1); err == nil {
		t.Fatalf("expected nil memory discard error")
	}
	if err := nilMemory.Replay(nil); err == nil {
		t.Fatalf("expected nil memory replay error")
	}
	if _, err := nilMemory.ReplaySingle(nil, 1); err == nil {
		t.Fatalf("expected nil memory replay single error")
	}
	if err := nilMemory.Flush(time.Millisecond); err == nil {
		t.Fatalf("expected nil memory flush error")
	}
	if nilMemory.UnpersistedCount() != 0 || nilMemory.GetLowestUnpersisted() != 0 || nilMemory.GetLastPersisted() != 0 || nilMemory.ErrorOnPublishGap() {
		t.Fatalf("unexpected nil memory helper values")
	}
	nilMemory.SetErrorOnPublishGap(true)

	store := NewMemoryPublishStore()
	if _, err := store.Store(nil); err == nil {
		t.Fatalf("expected nil command store error")
	}
	sequence, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":1}`)))
	if err != nil || sequence == 0 {
		t.Fatalf("expected seeded command for replay error coverage: seq=%d err=%v", sequence, err)
	}
	if err := store.Replay(func(*Command) error { return errors.New("stop") }); err == nil {
		t.Fatalf("expected replay callback error")
	}
	if _, err := store.ReplaySingle(func(*Command) error { return errors.New("stop") }, sequence); err == nil {
		t.Fatalf("expected replay single callback error")
	}
	if found, err := store.ReplaySingle(nil, sequence); err != nil || found {
		t.Fatalf("expected nil replay single callback noop, found=%v err=%v", found, err)
	}
}

func TestFilePublishStoreWALReplayAndApplyCoverage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "publish_store_wal.json")
	options := FileStoreOptions{
		UseWAL:             true,
		SyncOnWrite:        true,
		CheckpointInterval: 100,
		MMap: MMapOptions{
			Enabled:     false,
			InitialSize: 1024,
		},
	}
	store := NewFilePublishStoreWithOptions(path, options)

	seq1, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":1}`)))
	if err != nil || seq1 == 0 {
		t.Fatalf("first wal store failed: seq=%d err=%v", seq1, err)
	}
	seq2, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":2}`)))
	if err != nil || seq2 == 0 || seq2 <= seq1 {
		t.Fatalf("second wal store failed: seq=%d err=%v", seq2, err)
	}
	if err := store.DiscardUpTo(seq1); err != nil {
		t.Fatalf("wal discard failed: %v", err)
	}
	store.SetErrorOnPublishGap(true)

	reloaded := NewFilePublishStoreWithOptions(path, options)
	if !reloaded.ErrorOnPublishGap() {
		t.Fatalf("expected reloaded error-on-gap flag")
	}
	if reloaded.GetLastPersisted() != seq1 {
		t.Fatalf("expected reloaded last persisted %d, got %d", seq1, reloaded.GetLastPersisted())
	}
	if reloaded.UnpersistedCount() != 1 {
		t.Fatalf("expected one unpersisted entry after replay, got %d", reloaded.UnpersistedCount())
	}

	var replayed []uint64
	if err := reloaded.Replay(func(command *Command) error {
		sequence, ok := command.SequenceID()
		if !ok {
			t.Fatalf("expected replayed command sequence")
		}
		replayed = append(replayed, sequence)
		return nil
	}); err != nil {
		t.Fatalf("reloaded replay failed: %v", err)
	}
	if len(replayed) != 1 || replayed[0] != seq2 {
		t.Fatalf("unexpected replayed sequences: %+v", replayed)
	}

	manual := &FilePublishStore{
		MemoryPublishStore: NewMemoryPublishStore(),
		options:            defaultFileStoreOptions(),
	}
	manual.applyWalRecord(publishStoreWalRecord{
		Type: "store",
		Command: func() commandSnapshot {
			cmd := NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":3}`))
			seq := uint64(3)
			cmd.SetSequenceID(seq)
			snapshot := snapshotFromCommand(cmd)
			return snapshot
		}(),
	})
	if manual.UnpersistedCount() != 1 {
		t.Fatalf("expected manual wal store apply to create entry")
	}

	manual.lastPersisted = 10
	manual.errorOnPublishGap = true
	manual.applyWalRecord(publishStoreWalRecord{Type: "discard", Sequence: 5})
	if manual.GetLastPersisted() != 10 {
		t.Fatalf("expected discard gap to be ignored when error-on-gap enabled")
	}

	manual.errorOnPublishGap = false
	manual.applyWalRecord(publishStoreWalRecord{Type: "discard", Sequence: 20})
	if manual.GetLastPersisted() != 20 {
		t.Fatalf("expected discard apply to update last persisted")
	}

	manual.applyWalRecord(publishStoreWalRecord{Type: "store"})
	if manual.UnpersistedCount() != 0 {
		t.Fatalf("expected zero-sequence store wal record to no-op")
	}

	flag := true
	manual.applyWalRecord(publishStoreWalRecord{Type: "error_on_gap", ErrorOnGap: &flag})
	if !manual.ErrorOnPublishGap() {
		t.Fatalf("expected wal error_on_gap apply true")
	}
	manual.applyWalRecord(publishStoreWalRecord{Type: "error_on_gap"})
	if !manual.ErrorOnPublishGap() {
		t.Fatalf("expected nil error_on_gap pointer to no-op")
	}
}

func TestFilePublishStoreLoadAndReplayErrorCoverage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "publish_store_load_error.json")
	store := NewFilePublishStoreWithOptions(path, FileStoreOptions{
		UseWAL:             true,
		SyncOnWrite:        true,
		CheckpointInterval: 10,
	})

	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatalf("write malformed checkpoint failed: %v", err)
	}
	if err := store.loadCheckpoint(); err == nil {
		t.Fatalf("expected malformed checkpoint parse error")
	}

	if err := wal.WriteAtomic(path, []byte(`{"next_sequence":0,"records":[]}`), 0o600); err != nil {
		t.Fatalf("write valid checkpoint failed: %v", err)
	}
	if err := store.loadCheckpoint(); err != nil {
		t.Fatalf("expected checkpoint load success: %v", err)
	}
	if store.nextSequence != 1 {
		t.Fatalf("expected next sequence normalization to 1, got %d", store.nextSequence)
	}

	if err := os.WriteFile(store.walPath, []byte("not-json\n"), 0o600); err != nil {
		t.Fatalf("write malformed wal failed: %v", err)
	}
	if err := store.replayWal(); err == nil {
		t.Fatalf("expected malformed wal replay error")
	}

	validRecord := publishStoreWalRecord{Type: "discard", Sequence: 1}
	recordBytes, err := json.Marshal(validRecord)
	if err != nil {
		t.Fatalf("marshal wal record failed: %v", err)
	}
	if err = os.WriteFile(store.walPath, append(recordBytes, '\n'), 0o600); err != nil {
		t.Fatalf("write valid wal failed: %v", err)
	}
	if err = store.replayWal(); err != nil {
		t.Fatalf("expected valid wal replay success: %v", err)
	}

	noWal := NewFilePublishStoreWithOptions(path, FileStoreOptions{
		UseWAL:             false,
		SyncOnWrite:        false,
		CheckpointInterval: 1,
	})
	if err := noWal.replayWal(); err != nil {
		t.Fatalf("expected replayWal noop when WAL disabled: %v", err)
	}
	if err := noWal.appendWalNoLock(publishStoreWalRecord{Type: "store"}); err != nil {
		t.Fatalf("expected appendWalNoLock noop when WAL disabled: %v", err)
	}
	if err := noWal.bumpMutationAndMaybeCheckpoint(); err != nil {
		t.Fatalf("expected checkpoint path when WAL disabled: %v", err)
	}
}

func TestFilePublishStoreAppendWalDoesNotDeadlockWhileLockHeld(t *testing.T) {
	// appendWalNoLock no longer acquires store.lock internally (BUG-08 fix), so
	// calling it while a caller holds the lock externally must NOT deadlock.
	path := filepath.Join(t.TempDir(), "publish_store_locking.json")
	store := NewFilePublishStoreWithOptions(path, FileStoreOptions{
		UseWAL:             true,
		SyncOnWrite:        true,
		CheckpointInterval: 100,
	})

	done := make(chan error, 1)
	store.lock.Lock()
	go func() {
		// This must NOT block forever — appendWalNoLock should succeed without
		// trying to re-acquire store.lock.
		done <- store.appendWalNoLock(publishStoreWalRecord{Type: "discard", Sequence: 1})
	}()

	select {
	case err := <-done:
		// Returned promptly — no deadlock.
		if err != nil {
			t.Fatalf("expected appendWalNoLock to succeed while lock held externally, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("appendWalNoLock deadlocked while store.lock was held externally")
	}

	store.lock.Unlock()
}

func TestFilePublishStoreSaveCheckpointErrorPreservesMutationCounter(t *testing.T) {
	path := t.TempDir()
	store := &FilePublishStore{
		MemoryPublishStore: NewMemoryPublishStore(),
		path:               path,
		walPath:            filepath.Join(path, "publish_store.json.wal"),
		options: FileStoreOptions{
			UseWAL:             false,
			SyncOnWrite:        false,
			CheckpointInterval: 1,
		},
		opsSinceCheckpoint: 7,
	}

	command := NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":1}`))
	command.SetSequenceID(1)
	store.entries[1] = command
	store.nextSequence = 2

	if err := store.saveCheckpoint(); err == nil {
		t.Fatalf("expected saveCheckpoint write failure when path is a directory")
	}
	if store.opsSinceCheckpoint != 7 {
		t.Fatalf("expected mutation counter to remain unchanged on save failure, got %d", store.opsSinceCheckpoint)
	}
}

func TestFilePublishStoreWALFailurePaths(t *testing.T) {
	// Use a directory as the WAL path so all wal.Append calls fail,
	// exercising the error-return branches in Store, DiscardUpTo, and
	// SetErrorOnPublishGap that are otherwise unreachable.
	tempDir := t.TempDir()
	walDir := filepath.Join(tempDir, "waldir") // will be created as a directory below
	if err := os.MkdirAll(walDir, 0o700); err != nil {
		t.Fatalf("MkdirAll walDir: %v", err)
	}

	// Construct a store whose walPath is a directory — guaranteed write failure.
	store := &FilePublishStore{
		MemoryPublishStore: NewMemoryPublishStore(),
		path:               filepath.Join(tempDir, "checkpoint.json"),
		walPath:            walDir, // directory, not a file → append will fail
		options: FileStoreOptions{
			UseWAL:             true,
			SyncOnWrite:        false,
			CheckpointInterval: 100,
		},
	}

	// Store: appendWalNoLock fails → error is returned and no in-memory mutation occurs.
	seq, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":1}`)))
	if err == nil {
		t.Fatalf("expected Store to propagate WAL-append error")
	}
	if seq != 0 {
		t.Fatalf("expected zero sequence on WAL failure, got %d", seq)
	}
	if count := store.UnpersistedCount(); count != 0 {
		t.Fatalf("expected Store WAL failure to leave store empty, got %d entries", count)
	}

	// DiscardUpTo: appendWalNoLock fails → error is returned.
	// Seed a real entry first via the in-memory layer directly.
	seq2, _ := store.MemoryPublishStore.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":2}`)))
	if err2 := store.DiscardUpTo(seq2); err2 == nil {
		t.Fatalf("expected DiscardUpTo to propagate WAL-append error")
	}

	// SetErrorOnPublishGap: appendWalNoLock fails and the in-memory flag must not change.
	store.SetErrorOnPublishGap(true)
	if store.ErrorOnPublishGap() {
		t.Fatalf("expected WAL failure to leave error-on-gap disabled in memory")
	}
}

func TestMemoryPublishStorePreservesCustomCommandEnum(t *testing.T) {
	store := NewMemoryPublishStore()
	command := NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":11}`))
	const customCommand = 123456
	command.SetCommandEnum(customCommand)

	sequence, err := store.Store(command)
	if err != nil || sequence == 0 {
		t.Fatalf("store failed: seq=%d err=%v", sequence, err)
	}

	err = store.Replay(func(replayed *Command) error {
		if replayed.GetCommandEnum() != customCommand {
			t.Fatalf("expected replayed command enum %d, got %d", customCommand, replayed.GetCommandEnum())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
}

func TestFilePublishStoreLoadPropagatesCheckpointError(t *testing.T) {
	// load() calls loadCheckpoint() first; a parse failure there should be
	// returned by load() without calling replayWal.
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "bad_checkpoint.json")

	// Write an intentionally malformed JSON checkpoint file.
	if err := os.WriteFile(path, []byte("{bad json"), 0o600); err != nil {
		t.Fatalf("write malformed checkpoint: %v", err)
	}

	store := &FilePublishStore{
		MemoryPublishStore: NewMemoryPublishStore(),
		path:               path,
		walPath:            path + ".wal",
		options:            defaultFileStoreOptions(),
	}
	if err := store.load(); err == nil {
		t.Fatalf("expected load() to propagate checkpoint parse error")
	}
}

func TestNewFilePublishStoreWithOptionsRetainsLoadError(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "bad_checkpoint.json")
	if err := os.WriteFile(path, []byte("{bad json"), 0o600); err != nil {
		t.Fatalf("write malformed checkpoint: %v", err)
	}

	store := NewFilePublishStoreWithOptions(path, defaultFileStoreOptions())
	if err := store.LoadError(); err == nil {
		t.Fatalf("expected constructor to retain load error")
	}

	seq, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":1}`)))
	if err == nil {
		t.Fatalf("expected Store to surface retained load error")
	}
	if seq != 0 {
		t.Fatalf("expected zero sequence when load failed, got %d", seq)
	}
	if err = store.Flush(10 * time.Millisecond); err == nil {
		t.Fatalf("expected Flush to surface retained load error")
	}
}

func TestNewFilePublishStoreWithOptionsHidesPartialStateAfterReplayError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "publish_partial_state.json")
	options := FileStoreOptions{
		UseWAL:             true,
		SyncOnWrite:        false,
		CheckpointInterval: 1,
	}

	store := NewFilePublishStoreWithOptions(path, options)
	sequence, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":1}`)))
	if err != nil || sequence == 0 {
		t.Fatalf("seed publish store failed: seq=%d err=%v", sequence, err)
	}
	if err := os.WriteFile(path+".wal", []byte("not-json\n"), 0o600); err != nil {
		t.Fatalf("write malformed wal: %v", err)
	}

	reloaded := NewFilePublishStoreWithOptions(path, options)
	if err := reloaded.LoadError(); err == nil {
		t.Fatalf("expected retained replay error")
	}
	if count := reloaded.UnpersistedCount(); count != 0 {
		t.Fatalf("expected load error to hide partial publish state, got %d", count)
	}
	if lowest := reloaded.GetLowestUnpersisted(); lowest != 0 {
		t.Fatalf("expected load error to hide partial publish low watermark, got %d", lowest)
	}
	if err := reloaded.Replay(func(*Command) error { return nil }); err == nil {
		t.Fatalf("expected Replay to surface retained load error")
	}
}

func TestFilePublishStoreLoadCheckpointDerivesNextSequenceFromEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "publish_store_next_seq.json")
	state := publishStoreFileState{
		NextSequence: 0,
		Records: []publishStoreRecord{{
			Sequence: 9,
			Command:  snapshotFromCommand(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":1}`))),
		}},
	}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal checkpoint failed: %v", err)
	}
	if err = os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write checkpoint failed: %v", err)
	}

	store := NewFilePublishStoreWithOptions(path, FileStoreOptions{UseWAL: false, SyncOnWrite: false, CheckpointInterval: 100})
	sequence, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":2}`)))
	if err != nil {
		t.Fatalf("store after reload failed: %v", err)
	}
	if sequence != 10 {
		t.Fatalf("expected derived next sequence 10, got %d", sequence)
	}
}

func TestFilePublishStoreRuntimeSequenceExhaustionRejectsNewCommand(t *testing.T) {
	path := filepath.Join(t.TempDir(), "publish_store_sequence_exhaustion.json")
	store := NewFilePublishStoreWithOptions(path, FileStoreOptions{UseWAL: false, SyncOnWrite: false, CheckpointInterval: 100})
	store.nextSequence = math.MaxUint64

	sequence, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":1}`)))
	if err != nil || sequence != math.MaxUint64 {
		t.Fatalf("expected first max sequence store to succeed, got seq=%d err=%v", sequence, err)
	}

	sequence, err = store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":2}`)))
	if err == nil {
		t.Fatalf("expected new command after max sequence to fail")
	}
	if sequence != 0 {
		t.Fatalf("expected failed overflow store to return zero sequence, got %d", sequence)
	}
}

func TestFilePublishStoreLoadCheckpointMaxSequenceRejectsNewCommand(t *testing.T) {
	path := filepath.Join(t.TempDir(), "publish_store_max_checkpoint.json")
	state := publishStoreFileState{
		NextSequence: 0,
		Records: []publishStoreRecord{{
			Sequence: math.MaxUint64,
			Command:  snapshotFromCommand(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":1}`))),
		}},
	}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal checkpoint failed: %v", err)
	}
	if err = os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write checkpoint failed: %v", err)
	}

	store := NewFilePublishStoreWithOptions(path, FileStoreOptions{UseWAL: false, SyncOnWrite: false, CheckpointInterval: 100})
	if count := store.UnpersistedCount(); count != 1 {
		t.Fatalf("expected checkpoint data to remain readable, got %d entries", count)
	}
	sequence, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":2}`)))
	if err == nil {
		t.Fatalf("expected exhausted checkpoint sequence to reject new command")
	}
	if sequence != 0 {
		t.Fatalf("expected exhausted checkpoint store to return zero sequence, got %d", sequence)
	}
}

func TestMemoryPublishStoreSetInitialSequence(t *testing.T) {
	store := NewMemoryPublishStore()

	seq, err := store.Store(NewCommand("publish").SetTopic("t").SetData([]byte("a")))
	if err != nil || seq != 1 {
		t.Fatalf("expected first sequence 1, got %d err=%v", seq, err)
	}

	store.SetInitialSequence(100)

	nextSeq, nextErr := store.Store(NewCommand("publish").SetTopic("t").SetData([]byte("b")))
	if nextErr != nil || nextSeq != 101 {
		t.Fatalf("expected sequence 101 after SetInitialSequence(100), got %d err=%v", nextSeq, nextErr)
	}
}

func TestMemoryPublishStoreSetInitialSequenceNoOpIfSmaller(t *testing.T) {
	store := NewMemoryPublishStore()

	store.SetInitialSequence(50)

	seq, err := store.Store(NewCommand("publish").SetTopic("t").SetData([]byte("a")))
	if err != nil || seq != 51 {
		t.Fatalf("expected sequence 51, got %d err=%v", seq, err)
	}

	store.SetInitialSequence(30)

	nextSeq, nextErr := store.Store(NewCommand("publish").SetTopic("t").SetData([]byte("b")))
	if nextErr != nil || nextSeq != 52 {
		t.Fatalf("expected sequence 52 (SetInitialSequence(30) should be no-op), got %d err=%v", nextSeq, nextErr)
	}
}

func TestMemoryPublishStoreSetInitialSequenceNilReceiver(t *testing.T) {
	var store *MemoryPublishStore
	store.SetInitialSequence(100)
}

func TestFilePublishStoreSetInitialSequence(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "store_initial_seq.json")

	store := NewFilePublishStoreWithOptions(path, FileStoreOptions{UseWAL: false, SyncOnWrite: false, CheckpointInterval: 100})
	seq, err := store.Store(NewCommand("publish").SetTopic("t").SetData([]byte("a")))
	if err != nil || seq != 1 {
		t.Fatalf("expected first sequence 1, got %d err=%v", seq, err)
	}

	store.SetInitialSequence(200)

	nextSeq, nextErr := store.Store(NewCommand("publish").SetTopic("t").SetData([]byte("b")))
	if nextErr != nil || nextSeq != 201 {
		t.Fatalf("expected sequence 201 after SetInitialSequence(200), got %d err=%v", nextSeq, nextErr)
	}
}

func TestFilePublishStoreSetInitialSequencePersistsAcrossReload(t *testing.T) {
	cases := []struct {
		name    string
		options FileStoreOptions
	}{
		{
			name: "no-wal",
			options: FileStoreOptions{
				UseWAL:             false,
				SyncOnWrite:        false,
				CheckpointInterval: 100,
			},
		},
		{
			name: "wal",
			options: FileStoreOptions{
				UseWAL:             true,
				SyncOnWrite:        false,
				CheckpointInterval: 100,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store_initial_seq_"+tc.name+".json")
			store := NewFilePublishStoreWithOptions(path, tc.options)

			firstSeq, firstErr := store.Store(NewCommand("publish").SetTopic("t").SetData([]byte("a")))
			if firstErr != nil || firstSeq != 1 {
				t.Fatalf("expected first sequence 1, got %d err=%v", firstSeq, firstErr)
			}

			store.SetInitialSequence(200)

			reloaded := NewFilePublishStoreWithOptions(path, tc.options)
			nextSeq, nextErr := reloaded.Store(NewCommand("publish").SetTopic("t").SetData([]byte("b")))
			if nextErr != nil || nextSeq != 201 {
				t.Fatalf("expected persisted initial sequence to survive reload, got %d err=%v", nextSeq, nextErr)
			}
		})
	}
}

func TestFilePublishStoreReplaySingleCoverage(t *testing.T) {
	var nilStore *FilePublishStore
	if found, err := nilStore.ReplaySingle(nil, 1); err == nil || found {
		t.Fatalf("expected nil FilePublishStore ReplaySingle error, found=%v err=%v", found, err)
	}

	loadErrStore := &FilePublishStore{
		MemoryPublishStore: NewMemoryPublishStore(),
		loadErr:            errors.New("load failed"),
	}
	if found, err := loadErrStore.ReplaySingle(nil, 1); err == nil || err.Error() != "load failed" || found {
		t.Fatalf("expected load error from FilePublishStore ReplaySingle, found=%v err=%v", found, err)
	}

	path := filepath.Join(t.TempDir(), "store_replay_single.json")
	store := NewFilePublishStoreWithOptions(path, FileStoreOptions{UseWAL: false, SyncOnWrite: false, CheckpointInterval: 100})
	sequence, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte("payload")))
	if err != nil {
		t.Fatalf("Store() error = %v", err)
	}

	found, err := store.ReplaySingle(func(command *Command) error {
		topic, _ := command.Topic()
		if topic != "orders" {
			t.Fatalf("unexpected replayed topic: %q", topic)
		}
		return nil
	}, sequence)
	if err != nil || !found {
		t.Fatalf("expected ReplaySingle hit, found=%v err=%v", found, err)
	}
}
