package amps

import (
	"path/filepath"
	"testing"
)

func bookmarkMessage(subID string, bookmark string) *Message {
	return &Message{
		header: &_Header{
			command:  CommandPublish,
			subID:    []byte(subID),
			topic:    []byte("orders"),
			bookmark: []byte(bookmark),
		},
		data: []byte(`{"id":1}`),
	}
}

func TestMemoryBookmarkStoreDuplicateAndMostRecent(t *testing.T) {
	store := NewMemoryBookmarkStore()
	message := bookmarkMessage("sub-1", "10|1|")

	seq1 := store.Log(message)
	if seq1 == 0 {
		t.Fatalf("expected non-zero bookmark seq")
	}
	if store.IsDiscarded(message) {
		t.Fatalf("first delivery should not be considered discarded")
	}

	seq2 := store.Log(message)
	if seq1 != seq2 {
		t.Fatalf("duplicate bookmark should keep same sequence; got %d and %d", seq1, seq2)
	}
	if !store.IsDiscarded(message) {
		t.Fatalf("second delivery should be considered duplicate/discarded")
	}

	if recent := store.GetMostRecent("sub-1"); recent != "10|1|" {
		t.Fatalf("expected most recent bookmark to be 10|1|, got %q", recent)
	}

	store.Discard("sub-1", seq1)
	if oldest := store.GetOldestBookmarkSeq("sub-1"); oldest != 0 {
		t.Fatalf("expected no remaining non-discarded bookmarks, got %d", oldest)
	}
}

func TestFileBookmarkStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bookmark_store.json")
	store := NewFileBookmarkStore(path)

	message := bookmarkMessage("sub-file", "11|2|")
	store.Log(message)
	store.Persisted("sub-file", "11|2|")

	reloaded := NewFileBookmarkStore(path)
	if mostRecent := reloaded.GetMostRecent("sub-file"); mostRecent != "11|2|" {
		t.Fatalf("expected persisted bookmark, got %q", mostRecent)
	}
}
