package amps

import (
	"os"
	"path/filepath"
	"testing"
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

	// Re-open file-backed store and verify state recovery.
	reloaded := NewFilePublishStore(path)
	if reloaded.UnpersistedCount() != 1 {
		t.Fatalf("expected one recovered entry, got %d", reloaded.UnpersistedCount())
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected persisted file at %s: %v", path, err)
	}
}
