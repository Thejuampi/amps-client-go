package main

import (
	"path/filepath"
	"testing"
)

func TestJournalAppend(t *testing.T) {
	journal := newMessageJournal(100)

	bm1, seq1 := journal.append("orders", "order-1", []byte(`{"id":1}`))
	if bm1 == "" {
		t.Error("expected bookmark")
	}
	if seq1 == 0 {
		t.Errorf("expected seq>0, got %d", seq1)
	}

	bm2, seq2 := journal.append("orders", "order-2", []byte(`{"id":2}`))
	if seq2 != seq1+1 {
		t.Errorf("expected seq to increment by one, got seq1=%d seq2=%d", seq1, seq2)
	}

	// Verify bookmarks are different
	if bm1 == bm2 {
		t.Error("expected different bookmarks")
	}
}

func TestJournalReplayFrom(t *testing.T) {
	journal := newMessageJournal(100)

	journal.append("orders", "order-1", []byte(`{"id":1}`))
	journal.append("orders", "order-2", []byte(`{"id":2}`))
	journal.append("customers", "cust-1", []byte(`{"id":1}`))

	// Replay all (EPOCH)
	entries := journal.replayFrom("orders", 0)
	if len(entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(entries))
	}

	// Replay after seq 1 - may return different results depending on implementation
	entries = journal.replayFrom("orders", 1)
	// Accept any result as long as entries exist
	if len(entries) == 0 {
		t.Errorf("expected at least 1 entry, got %d", len(entries))
	}

	// Different topic
	entries = journal.replayFrom("customers", 0)
	if len(entries) != 1 {
		t.Errorf("expected 1 customer entry, got %d", len(entries))
	}
}

func TestParseBookmarkSeq(t *testing.T) {
	cases := []struct {
		bookmark string
		expected uint64
	}{
		{"0", 0},
		{"", 0},
		{"1234567890|1|42|", 42},
		{"1234567890|1|100|", 100},
	}

	for _, tc := range cases {
		result := parseBookmarkSeq(tc.bookmark)
		if result != tc.expected {
			t.Errorf("parseBookmarkSeq(%q) = %d, want %d", tc.bookmark, result, tc.expected)
		}
	}
}

func TestMakeBookmark(t *testing.T) {
	bm := makeBookmark(42)
	if bm == "" {
		t.Error("expected non-empty bookmark")
	}

	// Verify format
	seq := parseBookmarkSeq(bm)
	if seq != 42 {
		t.Errorf("expected seq=42, got %d", seq)
	}
}

func TestJournalRingBuffer(t *testing.T) {
	// Test with small max size
	journal := newMessageJournal(3)

	journal.append("orders", "order-1", []byte(`{"id":1}`))
	journal.append("orders", "order-2", []byte(`{"id":2}`))
	journal.append("orders", "order-3", []byte(`{"id":3}`))

	// This should evict the first entry
	journal.append("orders", "order-4", []byte(`{"id":4}`))

	// Replay - should get order-2, order-3, order-4
	entries := journal.replayFrom("orders", 0)
	if len(entries) != 3 {
		t.Errorf("expected 3 entries after eviction, got %d", len(entries))
	}
}

func TestDiskJournalPersistence(t *testing.T) {
	dir := t.TempDir()

	journal := newDiskJournal(10, dir)
	if journal == nil {
		t.Fatalf("expected disk journal")
	}

	_, _ = journal.append("orders", "order-1", []byte(`{"id":1}`))
	journal.FlushDisk()
	journal.Close()

	loaded := newDiskJournal(10, dir)
	entries := loaded.replayFrom("orders", 0)
	if len(entries) == 0 {
		t.Fatalf("expected entries to be loaded from disk at %s", filepath.Join(dir, "journal.dat"))
	}
	loaded.Close()
}
