package amps

import (
	"fmt"
	"testing"
)

// Store state in both tests is built ONLY through the exported Log/Discard API.
// Nothing writes to unexported fields, so neither test can manufacture a state
// the store cannot reach on its own.

// A replayed bookmark whose individual record was dropped by pruning must still
// be reported as discarded, or the application is handed the message twice.
func TestMemoryBookmarkStoreReportsDuplicateAfterPrune(t *testing.T) {
	var store = NewMemoryBookmarkStore()
	for sequence := 1; sequence <= memoryBookmarkPruneThreshold; sequence++ {
		store.Discard("sub", store.Log(bookmarkMessage("sub", fmt.Sprintf("1|%d|", sequence))))
	}

	var replayed = bookmarkMessage("sub", "1|5|")
	store.Log(replayed)

	if !store.IsDiscarded(replayed) {
		t.Fatalf("IsDiscarded(1|5|) = false, want true: bookmark was discarded, then pruned, then redelivered")
	}
}

// Negative control. Pruning must not collapse duplicate detection into a range
// check: a bookmark that was never discarded must still be delivered, even when
// its publisher sequence falls below one that was. Without this, a fix for the
// test above silently drops live messages instead of duplicating them, which is
// strictly worse.
func TestMemoryBookmarkStoreDeliversBookmarkItNeverDiscarded(t *testing.T) {
	var store = NewMemoryBookmarkStore()
	for sequence := 2000; sequence > 2000-memoryBookmarkPruneThreshold; sequence-- {
		store.Discard("sub", store.Log(bookmarkMessage("sub", fmt.Sprintf("1|%d|", sequence))))
	}

	// Sequence 500 is below everything discarded above, but was never seen.
	var neverSeen = bookmarkMessage("sub", "1|500|")
	store.Log(neverSeen)

	if store.IsDiscarded(neverSeen) {
		t.Fatalf("IsDiscarded(1|500|) = true, want false: this bookmark was never discarded and must be delivered")
	}
}
