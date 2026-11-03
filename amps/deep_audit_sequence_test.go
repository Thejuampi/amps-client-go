package amps

import "testing"

func TestDeepAuditNextPublishSequenceReportsFloorAndExhaustion(t *testing.T) {
	var missing *MemoryPublishStore
	if sequence, available := missing.NextPublishSequence(); sequence != 0 || available {
		t.Fatalf("nil store next sequence=%d available=%v", sequence, available)
	}
	var store = newMemoryPublishStore(10)
	if sequence, available := store.NextPublishSequence(); sequence != 10 || !available {
		t.Fatalf("fresh store next sequence=%d available=%v", sequence, available)
	}
	if sequence, err := store.Store(NewCommand("publish").SetTopic("orders")); sequence != 10 || err != nil {
		t.Fatalf("stored sequence=%d error=%v", sequence, err)
	}
	if sequence, available := store.NextPublishSequence(); sequence != 11 || !available {
		t.Fatalf("next sequence after store=%d available=%v", sequence, available)
	}
	store.SetInitialSequence(^uint64(0))
	if sequence, available := store.NextPublishSequence(); sequence != ^uint64(0) || available {
		t.Fatalf("exhausted sequence=%d available=%v", sequence, available)
	}
}
