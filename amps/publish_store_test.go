package amps

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
