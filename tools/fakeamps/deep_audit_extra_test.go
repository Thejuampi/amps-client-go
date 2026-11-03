package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestDeepAuditUnavailableJournalRejectsWirePublishAndReleasesPublisher(t *testing.T) {
	var previousSOW, previousJournal = sow, journal
	var path = filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(path, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	sow, journal = newSOWCache(), newDiskJournal(20, path)
	defer func() { journal.Close(); sow, journal = previousSOW, previousJournal }()
	var client, closeSession = deepAuditFakeSession(t)
	defer closeSession()
	var header = `{"c":"publish","cid":"retry","t":"orders","k":"order-1","s":10,"a":"persisted"}`
	for attempt := 0; attempt < 2; attempt++ {
		sendFrame(t, client, buildCommandFrame(header, []byte(`{"id":1}`)))
		var ack = readFrameBody(t, client, time.Second)
		if !strings.Contains(ack, `"a":"persisted"`) || !strings.Contains(ack, `"status":"failure"`) {
			t.Fatalf("attempt %d acknowledged unavailable journal: %q", attempt, ack)
		}
		if count := sow.query("orders", "", -1, "").totalCount; count != 0 || journal.count != 0 {
			t.Fatalf("attempt %d applied rejected publication: rows=%d journal=%d", attempt, count, journal.count)
		}
		journal.publishers.mu.Lock()
		var publisher = journal.publishers.publishers[publisherIdentity{name: "deep-audit-publisher"}]
		journal.publishers.mu.Unlock()
		if publisher == nil || !publisher.mu.TryLock() {
			t.Fatal("journal failure left publisher locked")
		}
		var accepted = publisher.accepted
		publisher.mu.Unlock()
		if accepted != 0 {
			t.Fatalf("rejected publication advanced sequence to %d", accepted)
		}
	}
}

func TestDeepAuditMalformedDiskPublisherSuffixBlocksBothReopens(t *testing.T) {
	var directory = t.TempDir()
	var path = filepath.Join(directory, "journal.dat")
	var journal = newDiskJournal(20, directory)
	if _, _, err := journal.appendPublish("orders", "key", []byte("payload"), publisherIdentity{name: "publisher", user: "user"}, 10, true); err != nil {
		t.Fatal(err)
	}
	journal.Close()
	var data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var suffix = bytes.LastIndex(data, []byte("PUB1"))
	if suffix < 0 || suffix+6 > len(data) {
		t.Fatal("publisher suffix missing")
	}
	binary.BigEndian.PutUint16(data[suffix+4:suffix+6], 0xffff)
	if err = os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		journal = newDiskJournal(20, directory)
		if journal.diskErr == nil {
			journal.Close()
			t.Fatal("malformed complete publisher suffix accepted")
		}
		if _, _, err = journal.appendPublish("orders", "", nil, publisherIdentity{name: "publisher"}, 11, true); err == nil {
			journal.Close()
			t.Fatal("publication accepted after malformed committed record")
		}
		journal.Close()
		var after, readErr = os.ReadFile(path)
		if readErr != nil || !bytes.Equal(after, data) {
			t.Fatalf("corrupt record changed during reopen %d: %v", attempt, readErr)
		}
	}
}

func TestDeepAuditDiskJournalRepairsPartialPrefixAndReadsLegacyRecords(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		var name = "partial-length-prefix"
		if legacy {
			name = "legacy-record"
		}
		t.Run(name, func(t *testing.T) {
			var directory = t.TempDir()
			var path = filepath.Join(directory, "journal.dat")
			var journal = newDiskJournal(20, directory)
			if legacy {
				journal.append("orders", "old", []byte("old payload"))
			} else if _, _, err := journal.appendPublish("orders", "old", []byte("old payload"), publisherIdentity{name: "publisher"}, 10, true); err != nil {
				t.Fatal(err)
			}
			journal.Close()
			var original, err = os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if legacy && bytes.Contains(original, []byte("PUB1")) {
				t.Fatal("legacy test record unexpectedly contains publisher suffix")
			}
			if !legacy {
				var file *os.File
				file, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = file.Write([]byte{0, 0}); err != nil {
					_ = file.Close()
					t.Fatal(err)
				}
				if err = file.Close(); err != nil {
					t.Fatal(err)
				}
			}
			journal = newDiskJournal(20, directory)
			if journal.diskErr != nil || journal.count != 1 {
				t.Fatalf("first reopen count=%d error=%v", journal.count, journal.diskErr)
			}
			var repaired, readErr = os.ReadFile(path)
			if readErr != nil || !bytes.Equal(repaired, original) {
				t.Fatalf("original record not preserved: %v", readErr)
			}
			if _, _, err = journal.appendPublish("orders", "new", []byte("new payload"), publisherIdentity{name: "publisher"}, 11, true); err != nil {
				t.Fatal(err)
			}
			journal.Close()
			journal = newDiskJournal(20, directory)
			defer journal.Close()
			var entries = journal.replayAll(0)
			if journal.diskErr != nil || len(entries) != 2 || string(entries[0].payload) != "old payload" || string(entries[1].payload) != "new payload" {
				t.Fatalf("second reopen entries=%v error=%v", entries, journal.diskErr)
			}
		})
	}
}

func TestDeepAuditEmptySequencedDeleteSurvivesReloadAndHigherSequenceApplies(t *testing.T) {
	var previousSOW, previousJournal = sow, journal
	var directory = t.TempDir()
	sow, journal = newSOWCache(), newDiskJournal(20, directory)
	defer func() { journal.Close(); sow, journal = previousSOW, previousJournal }()
	var client, closeSession = deepAuditFakeSession(t)
	var delete10 = `{"c":"sow_delete","cid":"delete-10","t":"orders","k":"order-1","s":10,"a":"persisted"}`
	sendFrame(t, client, buildCommandFrame(delete10, nil))
	if ack := readFrameBody(t, client, time.Second); !strings.Contains(ack, `"status":"success"`) {
		t.Fatalf("empty delete failed: %q", ack)
	}
	deepAuditFakePublish(t, client, `{"c":"publish","cid":"new","t":"orders","k":"order-1","a":"persisted"}`)
	closeSession()
	journal.Close()
	journal = newDiskJournal(20, directory)
	client, closeSession = deepAuditFakeSession(t)
	defer closeSession()
	sendFrame(t, client, buildCommandFrame(delete10, nil))
	_ = readFrameBody(t, client, time.Second)
	if count := sow.count("orders"); count != 1 {
		t.Fatalf("replayed empty delete removed newer row: %d", count)
	}
	sendFrame(t, client, buildCommandFrame(`{"c":"sow_delete","cid":"delete-11","t":"orders","k":"order-1","s":11,"a":"persisted"}`, nil))
	if ack := readFrameBody(t, client, time.Second); !strings.Contains(ack, `"status":"success"`) {
		t.Fatalf("higher sequence delete failed: %q", ack)
	}
	if count := sow.count("orders"); count != 0 {
		t.Fatalf("higher sequence delete did not apply: %d", count)
	}
	if entries := journal.replayAll(0); len(entries) != 1 || string(entries[0].payload) != `{"id":1}` {
		t.Fatalf("delete metadata appeared in publish replay: %v", entries)
	}
}

func TestDeepAuditDeleteJournalFailureDoesNotApplyOrLockPublisher(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		var name = "rollback-succeeds"
		if uncertain {
			name = "rollback-fails"
		}
		t.Run(name, func(t *testing.T) {
			var previousSOW, previousJournal = sow, journal
			sow, journal = newSOWCache(), newDiskJournal(20, t.TempDir())
			defer func() { journal.Close(); sow, journal = previousSOW, previousJournal }()
			var client, closeSession = deepAuditFakeSession(t)
			defer closeSession()
			deepAuditFakePublish(t, client, `{"c":"publish","cid":"seed","t":"orders","k":"order-1","a":"persisted"}`)
			var original = journalSyncFile
			t.Cleanup(func() { journalSyncFile = original })
			var calls int
			journalSyncFile = func(file *os.File) error {
				calls++
				if calls == 1 || uncertain {
					return errors.New("injected delete sync failure")
				}
				return file.Sync()
			}
			var deletion = `{"c":"sow_delete","cid":"delete","t":"orders","k":"order-1","s":10,"a":"processed,stats,persisted"}`
			sendFrame(t, client, buildCommandFrame(deletion, nil))
			var ack string
			for range 3 {
				ack = readFrameBody(t, client, time.Second)
				if !strings.Contains(ack, `"status":"failure"`) {
					t.Fatalf("failed journal returned a successful acknowledgement: %q", ack)
				}
			}
			journalSyncFile = original
			if !strings.Contains(ack, `"status":"failure"`) || sow.count("orders") != 1 {
				t.Fatalf("failed delete ack=%q rows=%d", ack, sow.count("orders"))
			}
			journal.publishers.mu.Lock()
			var publisher = journal.publishers.publishers[publisherIdentity{name: "deep-audit-publisher"}]
			journal.publishers.mu.Unlock()
			if publisher == nil || !publisher.mu.TryLock() {
				t.Fatal("failed journal left publisher locked")
			}
			var accepted = publisher.accepted
			publisher.mu.Unlock()
			if accepted != 0 {
				t.Fatalf("failed delete advanced sequence to %d", accepted)
			}
			sendFrame(t, client, buildCommandFrame(deletion, nil))
			for range 3 {
				ack = readFrameBody(t, client, time.Second)
				if uncertain && !strings.Contains(ack, `"status":"failure"`) || !uncertain && !strings.Contains(ack, `"status":"success"`) {
					t.Fatalf("unexpected retry acknowledgement: %q", ack)
				}
			}
			if uncertain {
				if !strings.Contains(ack, `"status":"failure"`) || sow.count("orders") != 1 {
					t.Fatalf("uncertain journal accepted retry: ack=%q rows=%d", ack, sow.count("orders"))
				}
			} else if !strings.Contains(ack, `"status":"success"`) || sow.count("orders") != 0 {
				t.Fatalf("safe rollback did not allow retry: ack=%q rows=%d", ack, sow.count("orders"))
			}
		})
	}
}

func TestDeepAuditMalformedDeleteRecordBlocksReopen(t *testing.T) {
	var directory = t.TempDir()
	var path = filepath.Join(directory, "journal.dat")
	var journal = newDiskJournal(20, directory)
	if err := journal.appendDelete(publisherIdentity{name: "publisher", user: "user"}, 10); err != nil {
		t.Fatal(err)
	}
	journal.Close()
	var data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var marker = bytes.Index(data, []byte("DEL1"))
	if marker < 0 {
		t.Fatal("typed delete marker missing")
	}
	data[marker] = 'X'
	if err = os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		journal = newDiskJournal(20, directory)
		if journal.diskErr == nil {
			journal.Close()
			t.Fatal("malformed complete typed record accepted")
		}
		if err = journal.appendDelete(publisherIdentity{name: "publisher"}, 11); err == nil {
			journal.Close()
			t.Fatal("delete accepted after malformed typed record")
		}
		journal.Close()
		var after, readErr = os.ReadFile(path)
		if readErr != nil || !bytes.Equal(data, after) {
			t.Fatalf("malformed record changed after reopen %d: %v", attempt, readErr)
		}
	}
}

func TestDeepAuditTornDeleteRecordIsRepairedBeforeLaterAppend(t *testing.T) {
	var directory = t.TempDir()
	var path = filepath.Join(directory, "journal.dat")
	var journal = newDiskJournal(20, directory)
	var identity = publisherIdentity{name: "publisher", user: "user"}
	if _, _, err := journal.appendPublish("orders", "key", []byte("payload"), identity, 1, true); err != nil {
		t.Fatal(err)
	}
	if err := journal.appendDelete(identity, 10); err != nil {
		t.Fatal(err)
	}
	journal.Close()
	var data, err = os.ReadFile(path)
	if err != nil || len(data) < 2 {
		t.Fatalf("read journal: %v", err)
	}
	if err = os.WriteFile(path, data[:len(data)-2], 0o600); err != nil {
		t.Fatal(err)
	}
	journal = newDiskJournal(20, directory)
	if journal.diskErr != nil || journal.count != 1 {
		t.Fatalf("first reopen count=%d error=%v", journal.count, journal.diskErr)
	}
	if err = journal.appendDelete(identity, 11); err != nil {
		t.Fatal(err)
	}
	journal.Close()
	journal = newDiskJournal(20, directory)
	defer journal.Close()
	if journal.diskErr != nil || journal.count != 1 {
		t.Fatalf("second reopen count=%d error=%v", journal.count, journal.diskErr)
	}
	var state, _, duplicate, persisted = journal.publishers.begin(identity, "11")
	if state != nil {
		state.abort()
	}
	if !duplicate || !persisted {
		t.Fatal("complete later delete sequence was not restored")
	}
}

func TestDeepAuditDiskPublisherIdentityLengthBoundaries(t *testing.T) {
	for _, kind := range []string{"publish", "delete"} {
		for _, field := range []string{"name", "user"} {
			for _, size := range []int{65535, 65536} {
				var name = kind + "/" + field + "/" + strconv.Itoa(size)
				t.Run(name, func(t *testing.T) {
					var directory = t.TempDir()
					var journal = newDiskJournal(20, directory)
					var identity = publisherIdentity{name: "publisher", user: "user"}
					if field == "name" {
						identity.name = strings.Repeat("n", size)
					} else {
						identity.user = strings.Repeat("u", size)
					}
					var err error
					if kind == "publish" {
						_, _, err = journal.appendPublish("orders", "key", []byte("payload"), identity, 42, true)
					} else {
						err = journal.appendDelete(identity, 42)
					}
					if (err != nil) != (size == 65536) {
						journal.Close()
						t.Fatalf("identity size=%d error=%v", size, err)
					}
					if size == 65536 && journal.count != 0 {
						journal.Close()
						t.Fatalf("rejected identity advanced journal count to %d", journal.count)
					}
					journal.Close()
					var disk, readErr = os.ReadFile(filepath.Join(directory, "journal.dat"))
					if readErr != nil || size == 65536 && len(disk) != 0 {
						t.Fatalf("rejected identity wrote journal bytes=%d error=%v", len(disk), readErr)
					}
					journal = newDiskJournal(20, directory)
					defer journal.Close()
					if journal.diskErr != nil {
						t.Fatal(journal.diskErr)
					}
					if size == 65536 {
						if len(journal.publishers.publishers) != 0 {
							t.Fatal("rejected identity restored a publisher watermark")
						}
						return
					}
					var state, _, duplicate, persisted = journal.publishers.begin(identity, "42")
					if state != nil {
						state.abort()
					}
					if !duplicate || !persisted {
						t.Fatal("maximum-length identity did not round-trip exactly")
					}
				})
			}
		}
	}
}
