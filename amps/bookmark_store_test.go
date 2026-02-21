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

func TestBookmarkStoreAdditionalCoverage(t *testing.T) {
	store := NewMemoryBookmarkStore()
	queryMessage := &Message{
		header: &_Header{
			command:  CommandPublish,
			queryID:  []byte("query-1"),
			bookmark: []byte("20|1|"),
		},
	}
	subIDsMessage := &Message{
		header: &_Header{
			command:  CommandPublish,
			subIDs:   []byte("sub-a,sub-b"),
			bookmark: []byte("21|1|"),
		},
	}
	defaultMessage := &Message{
		header: &_Header{
			command:  CommandPublish,
			bookmark: []byte("22|1|"),
		},
	}

	seqQuery := store.Log(queryMessage)
	seqSubIDs := store.Log(subIDsMessage)
	seqDefault := store.Log(defaultMessage)
	if seqQuery == 0 || seqSubIDs == 0 || seqDefault == 0 {
		t.Fatalf("expected sequence IDs for all bookmark key paths")
	}

	if recent := store.GetMostRecent("query-1"); recent != "20|1|" {
		t.Fatalf("unexpected most recent bookmark for query route: %q", recent)
	}

	store.DiscardMessage(queryMessage)
	if !store.IsDiscarded(queryMessage) {
		t.Fatalf("expected DiscardMessage to mark bookmark as discarded")
	}

	store.Persisted("query-1", "20|2|")
	store.SetServerVersion("5.3.5.1")
	if oldest := store.GetOldestBookmarkSeq("query-1"); oldest != 0 {
		t.Fatalf("expected no oldest sequence after persisted discard, got %d", oldest)
	}

	store.Purge("query-1")
	if recent := store.GetMostRecent("query-1"); recent != "" {
		t.Fatalf("expected purge-by-subid to clear most recent")
	}
	store.Purge()
	if recent := store.GetMostRecent("sub-a,sub-b"); recent != "" {
		t.Fatalf("expected purge-all to clear store")
	}

	if NewRingBookmarkStore() == nil {
		t.Fatalf("expected ring bookmark store")
	}
}

func TestFileBookmarkStoreWrapperCoverage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bookmark_store_wrappers.json")
	store := NewFileBookmarkStore(path)
	message := bookmarkMessage("sub-wrap", "30|1|")
	seq := store.Log(message)
	if seq == 0 {
		t.Fatalf("expected sequence from file store log")
	}
	store.Discard("sub-wrap", seq)
	store.DiscardMessage(message)
	store.Purge("sub-wrap")
	store.SetServerVersion("6.0.0.0")

	reloaded := NewFileBookmarkStore(path)
	if reloaded.serverVersion != "6.0.0.0" {
		t.Fatalf("expected persisted server version, got %q", reloaded.serverVersion)
	}

	mmapStore := NewMMapBookmarkStore(path)
	if mmapStore == nil || mmapStore.FileBookmarkStore == nil {
		t.Fatalf("expected mmap bookmark store wrapper")
	}
}
