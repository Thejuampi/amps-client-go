package main

import "testing"

func TestReplicationNoPeersHelpers(t *testing.T) {
	initReplication("", "instance-test")
	replicatePublish("orders", []byte(`{"id":1}`), "json", "k1")
	stopReplication()

	if isReplicatedMessage(headerFields{}) {
		t.Fatalf("expected isReplicatedMessage to be false")
	}
}

func TestApplyReplicatedPublish(t *testing.T) {
	oldSow := sow
	oldJournal := journal
	sow = newSOWCache()
	journal = newMessageJournal(100)
	defer func() {
		sow = oldSow
		journal = oldJournal
	}()

	h := headerFields{c: "publish", t: "orders", mt: "json", k: "k1"}
	applyReplicatedPublish(h, []byte(`{"id":1}`))

	result := sow.query("orders", "", -1, "")
	if result.totalCount != 1 {
		t.Fatalf("expected replicated publish to upsert sow record, got count=%d", result.totalCount)
	}

	entries := journal.replayFrom("orders", 0)
	if len(entries) != 1 {
		t.Fatalf("expected replicated publish to append journal entry, got %d", len(entries))
	}
}
