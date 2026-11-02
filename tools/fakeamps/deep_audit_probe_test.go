package main

import (
	"errors"
	"github.com/Thejuampi/amps-client-go/amps"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func deepAuditFakeSession(t *testing.T) (net.Conn, func()) {
	t.Helper()
	var server, client = net.Pipe()
	var done = make(chan struct{})
	go func() { defer close(done); handleConnection(server) }()
	var once sync.Once
	var cleanup = func() { once.Do(func() { _ = client.Close(); <-done }) }
	t.Cleanup(cleanup)
	sendFrame(t, client, buildCommandFrame(`{"c":"logon","cid":"logon","a":"processed","client_name":"deep-audit-publisher"}`, nil))
	_ = readFrameBody(t, client, time.Second)
	return client, cleanup
}

type deepAuditReplayStore struct {
	*amps.MemoryPublishStore
	entered chan struct{}
	resume  chan struct{}
}

func (store *deepAuditReplayStore) Replay(replayer func(*amps.Command) error) error {
	var first = true
	return store.MemoryPublishStore.Replay(func(command *amps.Command) error {
		if first {
			first = false
			close(store.entered)
			<-store.resume
		}
		return replayer(command)
	})
}

func TestDeepAuditIntegrationLivePublishDuringReplayLosesBacklog(t *testing.T) {
	var previousSOW, previousJournal = sow, journal
	sow, journal = newSOWCache(), newMessageJournal(20)
	defer func() { sow, journal = previousSOW, previousJournal }()
	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var serverDone = make(chan struct{})
	go func() {
		defer close(serverDone)
		var conn, acceptErr = listener.Accept()
		if acceptErr == nil {
			handleConnection(conn)
		}
	}()
	var store = &deepAuditReplayStore{MemoryPublishStore: amps.NewMemoryPublishStore(), entered: make(chan struct{}), resume: make(chan struct{})}
	var client = amps.NewClient("deep-audit-loss").SetPublishStore(store)
	client.SetHeartbeat(0)
	defer func() { _ = client.Close(); _ = listener.Close(); <-serverDone }()
	for _, data := range []string{`{"id":1}`, `{"id":2}`} {
		if _, err = client.ExecuteAsync(amps.NewCommand("publish").SetTopic("deep-audit-loss").SetData([]byte(data)).SetAckType(amps.AckTypePersisted), nil); err != nil {
			t.Fatal(err)
		}
	}
	if err = client.Connect("tcp://" + listener.Addr().String() + "/amps/json"); err != nil {
		t.Fatal(err)
	}
	var loggedOn = make(chan error, 1)
	go func() { loggedOn <- client.Logon() }()
	<-store.entered
	var published = make(chan error, 1)
	go func() { published <- client.Publish("deep-audit-loss", `{"id":3}`) }()
	var completed bool
	select {
	case err = <-published:
		completed = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
	}
	if completed {
		if err = store.Flush(time.Second); err != nil {
			t.Fatal(err)
		}
	}
	close(store.resume)
	if err = <-loggedOn; err != nil {
		t.Fatal(err)
	}
	if !completed {
		if err = <-published; err != nil {
			t.Fatal(err)
		}
	}
	// A control command is processed after every preceding publish on this socket.
	if _, err = client.ExecuteAsync(amps.NewCommand("flush"), nil); err != nil {
		t.Fatal(err)
	}
	journal.mu.RLock()
	var applied = journal.count
	journal.mu.RUnlock()
	if applied != 3 {
		t.Fatalf("two retained publishes lost: journal=%d, store pending=%d, all ExecuteAsync/Publish/Logon calls returned success", applied, store.UnpersistedCount())
	}
}

func deepAuditFakePublish(t *testing.T, conn net.Conn, header string) {
	t.Helper()
	sendFrame(t, conn, buildCommandFrame(header, []byte(`{"id":1}`)))
	_ = readFrameBody(t, conn, time.Second)
}

func TestDeepAuditFakePublisherDedupSurvivesReconnect(t *testing.T) {
	var previousSOW, previousJournal = sow, journal
	sow, journal = newSOWCache(), newMessageJournal(20)
	defer func() { sow, journal = previousSOW, previousJournal }()
	for index := 0; index < 2; index++ {
		var client, closeSession = deepAuditFakeSession(t)
		deepAuditFakePublish(t, client, `{"c":"publish","cid":"replay","t":"deep-audit","s":10,"a":"persisted"}`)
		closeSession()
	}
	journal.mu.RLock()
	var applied = journal.count
	journal.mu.RUnlock()
	if applied != 1 {
		t.Fatalf("same publisher+sequence applied twice after reconnect: journal entries=%d", applied)
	}
}

func TestDeepAuditFakeCommandIDIsNotPublishIdentity(t *testing.T) {
	for _, sequences := range []bool{true, false} {
		var name = "unsequenced"
		if sequences {
			name = "sequenced"
		}
		t.Run(name, func(t *testing.T) {
			var previousSOW, previousJournal = sow, journal
			sow, journal = newSOWCache(), newMessageJournal(20)
			defer func() { sow, journal = previousSOW, previousJournal }()
			var client, closeSession = deepAuditFakeSession(t)
			if sequences {
				deepAuditFakePublish(t, client, `{"c":"publish","cid":"shared","t":"deep-audit","s":10,"a":"persisted"}`)
				deepAuditFakePublish(t, client, `{"c":"publish","cid":"shared","t":"deep-audit","s":11,"a":"persisted"}`)
			} else {
				for index := 0; index < 2; index++ {
					deepAuditFakePublish(t, client, `{"c":"publish","cid":"shared","t":"deep-audit","a":"persisted"}`)
				}
			}
			closeSession()
			journal.mu.RLock()
			var applied = journal.count
			journal.mu.RUnlock()
			if applied != 2 {
				t.Fatalf("command id suppressed a distinct publish: journal entries=%d", applied)
			}
		})
	}
}

func TestDeepAuditFakePublisherIdentitySeparatesUsersAndAnonymousSessions(t *testing.T) {
	var previousSOW, previousJournal = sow, journal
	sow, journal = newSOWCache(), newMessageJournal(20)
	defer func() { sow, journal = previousSOW, previousJournal }()
	var logons = []string{
		`{"c":"logon","cid":"logon","a":"processed","client_name":"same","user_id":"left"}`,
		`{"c":"logon","cid":"logon","a":"processed","client_name":"same","user_id":"right"}`,
		`{"c":"logon","cid":"logon","a":"processed","client_name":"a","user_id":"bc"}`,
		`{"c":"logon","cid":"logon","a":"processed","client_name":"ab","user_id":"c"}`,
		`{"c":"logon","cid":"logon","a":"processed"}`,
		`{"c":"logon","cid":"logon","a":"processed"}`,
		`{"c":"logon","cid":"logon","a":"processed","client_name":"same","user_id":"left"}`,
	}
	for _, logon := range logons {
		var server, client = net.Pipe()
		var done = make(chan struct{})
		go func() { defer close(done); handleConnection(server) }()
		sendFrame(t, client, buildCommandFrame(logon, nil))
		_ = readFrameBody(t, client, time.Second)
		deepAuditFakePublish(t, client, `{"c":"publish","cid":"same","t":"deep-audit","s":10,"a":"persisted"}`)
		_ = client.Close()
		<-done
	}
	journal.mu.RLock()
	var count = journal.count
	journal.mu.RUnlock()
	if count != 6 {
		t.Fatalf("publisher identities produced %d entries, want 6", count)
	}
}

func TestDeepAuditFakeReplayDoesNotConvertFailedPersistenceIntoSuccess(t *testing.T) {
	var previousSOW, previousJournal, previousPeers = sow, journal, replicaPeers
	sow, journal = newSOWCache(), newMessageJournal(20)
	var local, remote = net.Pipe()
	_ = remote.Close()
	replicaPeers = []*peerConn{{addr: "pipe", conn: local, alive: true}}
	defer func() { _ = local.Close(); sow, journal, replicaPeers = previousSOW, previousJournal, previousPeers }()
	for index := 0; index < 2; index++ {
		var client, closeSession = deepAuditFakeSession(t)
		sendFrame(t, client, buildCommandFrame(`{"c":"publish","cid":"failed","t":"deep-audit","s":10,"a":"persisted,sync"}`, []byte(`{"id":1}`)))
		var ack = readFrameBody(t, client, time.Second)
		if !strings.Contains(ack, `"status":"failure"`) {
			t.Fatalf("failed persisted acknowledgement became success: %q", ack)
		}
		closeSession()
	}
	journal.mu.RLock()
	var count = journal.count
	journal.mu.RUnlock()
	if count != 1 {
		t.Fatalf("failed persistence replay applied %d times", count)
	}
}

func TestDeepAuditFakeRepeatedSequencedDeleteDoesNotDeleteNewData(t *testing.T) {
	var previousSOW, previousJournal = sow, journal
	sow, journal = newSOWCache(), newMessageJournal(20)
	defer func() { sow, journal = previousSOW, previousJournal }()
	var client, closeSession = deepAuditFakeSession(t)
	defer closeSession()
	var publish = `{"c":"publish","cid":"publish","t":"orders","k":"order-1","a":"persisted"}`
	deepAuditFakePublish(t, client, publish)
	sendFrame(t, client, buildCommandFrame(`{"c":"sow_delete","cid":"delete-1","t":"orders","k":"order-1","s":10,"a":"processed"}`, nil))
	_ = readFrameBody(t, client, time.Second)
	deepAuditFakePublish(t, client, publish)
	sendFrame(t, client, buildCommandFrame(`{"c":"sow_delete","cid":"delete-2","t":"orders","k":"order-1","s":10,"a":"processed"}`, nil))
	_ = readFrameBody(t, client, time.Second)
	if count := sow.query("orders", "", -1, "").totalCount; count != 1 {
		t.Fatalf("replayed delete removed newer row: remaining=%d", count)
	}
}

func TestDeepAuditDiskJournalSyncFailureRollsBackOrBlocks(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		var name = "rollback-succeeds"
		if uncertain {
			name = "rollback-uncertain"
		}
		t.Run(name, func(t *testing.T) {
			var directory = t.TempDir()
			var journal = newDiskJournal(20, directory)
			var identity = publisherIdentity{name: "publisher", user: "user"}
			if _, _, err := journal.appendPublish("orders", "", []byte("first"), identity, 10, true); err != nil {
				t.Fatal(err)
			}
			var original = journalSyncFile
			var failure = errors.New("injected journal sync failure")
			var calls int
			journalSyncFile = func(file *os.File) error {
				calls++
				if calls == 1 || uncertain {
					return failure
				}
				return file.Sync()
			}
			var _, _, err = journal.appendPublish("orders", "", []byte("second"), identity, 11, true)
			journalSyncFile = original
			if !errors.Is(err, failure) {
				t.Fatalf("failed append error = %v", err)
			}
			var _, _, retryErr = journal.appendPublish("orders", "", []byte("second"), identity, 11, true)
			if uncertain != (retryErr != nil) {
				t.Fatalf("retry error = %v, uncertain=%v", retryErr, uncertain)
			}
			journal.Close()
			journal = newDiskJournal(20, directory)
			defer journal.Close()
			var expected = 2
			if uncertain {
				expected = 1
			}
			if journal.diskErr != nil || journal.count != expected {
				t.Fatalf("reopened count=%d error=%v", journal.count, journal.diskErr)
			}
		})
	}
}

func TestDeepAuditDiskJournalRejectsMalformedCompleteRecord(t *testing.T) {
	var directory = t.TempDir()
	var path = filepath.Join(directory, "journal.dat")
	if err := os.WriteFile(path, []byte{0, 0, 0, 1, 0}, 0o600); err != nil {
		t.Fatal(err)
	}
	var journal = newDiskJournal(20, directory)
	defer journal.Close()
	if _, _, err := journal.appendPublish("orders", "", nil, publisherIdentity{name: "test"}, 1, true); err == nil {
		t.Fatal("complete malformed record was silently accepted")
	}
	var data, err = os.ReadFile(path)
	if err != nil || len(data) != 5 {
		t.Fatalf("malformed record changed: %x, %v", data, err)
	}
}

func TestDeepAuditUnavailableDiskJournalRejectsPublish(t *testing.T) {
	var path = filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(path, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	var journal = newDiskJournal(20, path)
	defer journal.Close()
	if _, _, err := journal.appendPublish("orders", "", nil, publisherIdentity{name: "test"}, 1, true); err == nil {
		t.Fatal("unavailable disk journal accepted publication")
	}
}
