package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFollowupPublisherDedupSurvivesDiskJournalReload(t *testing.T) {
	var previousSOW, previousJournal = sow, journal
	var directory = t.TempDir()
	sow, journal = newSOWCache(), newDiskJournal(20, directory)
	defer func() {
		journal.Close()
		sow, journal = previousSOW, previousJournal
	}()
	var client, closeSession = deepAuditFakeSession(t)
	deepAuditFakePublish(t, client, `{"c":"publish","cid":"first","t":"deep-audit","s":10,"a":"persisted"}`)
	closeSession()
	journal.Close()
	journal = newDiskJournal(20, directory)
	client, closeSession = deepAuditFakeSession(t)
	deepAuditFakePublish(t, client, `{"c":"publish","cid":"retry","t":"deep-audit","s":10,"a":"persisted"}`)
	closeSession()
	journal.mu.RLock()
	var applied = journal.count
	journal.mu.RUnlock()
	if applied != 1 {
		t.Fatalf("persisted replay applied twice after disk journal reload: got %d entries, want 1", applied)
	}
}

func TestFollowupCommandIDDoesNotSuppressDistinctSOWDeletes(t *testing.T) {
	var previousSOW, previousJournal = sow, journal
	sow, journal = newSOWCache(), newMessageJournal(20)
	defer func() { sow, journal = previousSOW, previousJournal }()
	var client, closeSession = deepAuditFakeSession(t)
	defer closeSession()
	for _, key := range []string{"first", "second"} {
		sendFrame(t, client, buildCommandFrame(fmt.Sprintf(`{"c":"publish","cid":%q,"t":"delete-audit","k":%q,"a":"persisted"}`, key, key), []byte(`{"id":1}`)))
		_ = readFrameBody(t, client, time.Second)
	}
	for _, key := range []string{"first", "second"} {
		sendFrame(t, client, buildCommandFrame(fmt.Sprintf(`{"c":"sow_delete","cid":"shared-delete","t":"delete-audit","k":%q,"a":"processed"}`, key), nil))
		_ = readFrameBody(t, client, time.Second)
	}
	if count := sow.count("delete-audit"); count != 0 {
		t.Fatalf("command id suppressed a distinct SOW delete: %d rows remain", count)
	}
}

func TestFollowupDiskJournalTornTailCannotHideLaterAcceptedPublish(t *testing.T) {
	var previousSOW, previousJournal = sow, journal
	var directory = t.TempDir()
	sow, journal = newSOWCache(), newDiskJournal(20, directory)
	defer func() {
		journal.Close()
		sow, journal = previousSOW, previousJournal
	}()
	var client, closeSession = deepAuditFakeSession(t)
	deepAuditFakePublish(t, client, `{"c":"publish","cid":"first","t":"deep-audit","s":10,"a":"persisted"}`)
	closeSession()
	journal.Close()
	var file, err = os.OpenFile(filepath.Join(directory, "journal.dat"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	// The tail advertises 1 MiB, but a crash left only two bytes of its body.
	if _, err = file.Write([]byte{0, 16, 0, 0, 1, 2}); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	journal = newDiskJournal(20, directory)
	client, closeSession = deepAuditFakeSession(t)
	deepAuditFakePublish(t, client, `{"c":"publish","cid":"second","t":"deep-audit","s":11,"a":"persisted"}`)
	closeSession()
	journal.Close()
	journal = newDiskJournal(20, directory)
	journal.mu.RLock()
	var applied = journal.count
	journal.mu.RUnlock()
	if applied != 2 {
		t.Fatalf("torn journal tail hid a later accepted publication: got %d entries, want 2", applied)
	}
}

func TestFollowupSequencedDeleteSurvivesDiskRestart(t *testing.T) {
	var previousSOW, previousJournal = sow, journal
	var directory = t.TempDir()
	sow, journal = newDiskSOWCache(directory, 20, evictionNone), newDiskJournal(20, directory)
	defer func() {
		journal.Close()
		sow, journal = previousSOW, previousJournal
	}()
	var client, closeSession = deepAuditFakeSession(t)
	var publish = `{"c":"publish","cid":"publish","t":"delete-restart","k":"order-1","a":"persisted"}`
	deepAuditFakePublish(t, client, publish)
	var deletion = `{"c":"sow_delete","cid":"delete","t":"delete-restart","k":"order-1","s":10,"a":"persisted"}`
	sendFrame(t, client, buildCommandFrame(deletion, nil))
	if ack := readFrameBody(t, client, time.Second); !strings.Contains(ack, `"status":"success"`) {
		t.Fatalf("initial delete failed: %q", ack)
	}
	if count := sow.count("delete-restart"); count != 0 {
		t.Fatalf("initial delete left %d rows", count)
	}
	deepAuditFakePublish(t, client, publish)
	closeSession()
	journal.Close()
	sow, journal = newDiskSOWCache(directory, 20, evictionNone), newDiskJournal(20, directory)
	if count := sow.count("delete-restart"); count != 1 {
		t.Fatalf("restart did not restore newer row: %d", count)
	}
	client, closeSession = deepAuditFakeSession(t)
	defer closeSession()
	sendFrame(t, client, buildCommandFrame(deletion, nil))
	if ack := readFrameBody(t, client, time.Second); !strings.Contains(ack, `"status":"success"`) {
		t.Fatalf("duplicate delete failed: %q", ack)
	}
	if count := sow.count("delete-restart"); count != 1 {
		t.Fatalf("replayed delete removed newer row after restart: remaining=%d", count)
	}
	if entries := journal.replayAll(0); len(entries) != 2 {
		t.Fatalf("delete identity leaked into publish replay: %d entries", len(entries))
	}
}
