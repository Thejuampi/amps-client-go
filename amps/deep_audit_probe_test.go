package amps

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Thejuampi/amps-client-go/amps/internal/wal"
)

func TestDeepAuditReplayPreservesPersistedAck(t *testing.T) {
	for _, kind := range []string{"memory", "file"} {
		for _, api := range []string{"publish", "delta", "execute"} {
			t.Run(kind+"/"+api, func(t *testing.T) {
				var client = NewClient("deep-audit")
				defer client.Close()
				var store PublishStore = NewMemoryPublishStore()
				if kind == "file" {
					store = NewFilePublishStore(filepath.Join(t.TempDir(), "publish"))
				}
				client.SetPublishStore(store)
				var err error
				switch api {
				case "publish":
					err = client.Publish("orders", "payload")
				case "delta":
					err = client.DeltaPublish("orders", "payload")
				case "execute":
					_, err = client.ExecuteAsync(NewCommand("publish").SetTopic("orders").SetData([]byte("payload")).SetAckType(AckTypeProcessed), nil)
				}
				if err != nil {
					t.Fatal(err)
				}
				var count int
				err = store.Replay(func(command *Command) error {
					count++
					var ack, _ = command.AckType()
					if ack&AckTypePersisted == 0 {
						t.Errorf("replay lost persisted acknowledgement: ack=%d", ack)
					}
					if api == "execute" && ack&AckTypeProcessed == 0 {
						t.Error("replay lost user's processed acknowledgement")
					}
					return nil
				})
				if err != nil || count != 1 {
					t.Fatalf("replay count=%d error=%v", count, err)
				}
			})
		}
	}
}

func deepAuditAppendTornTail(t *testing.T, path string) {
	t.Helper()
	var file, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteString(`{"type":`); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDeepAuditPublishWALTornTailThenAppendThenRestart(t *testing.T) {
	var path = filepath.Join(t.TempDir(), "publish")
	var options = FileStoreOptions{UseWAL: true, SyncOnWrite: true, CheckpointInterval: 1000}
	var store = NewFilePublishStoreWithOptions(path, options)
	if _, err := store.Store(NewCommand("publish").SetTopic("orders").SetSequenceID(100)); err != nil {
		t.Fatal(err)
	}
	deepAuditAppendTornTail(t, path+".wal")
	store = NewFilePublishStoreWithOptions(path, options)
	if err := store.LoadError(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Store(NewCommand("publish").SetTopic("orders").SetSequenceID(101)); err != nil {
		t.Fatal(err)
	}
	store = NewFilePublishStoreWithOptions(path, options)
	if err := store.LoadError(); err != nil {
		t.Fatalf("second restart cannot recover successfully stored publishes: %v", err)
	}
	var sequences []uint64
	if err := store.Replay(func(command *Command) error {
		var seq, _ = command.SequenceID()
		sequences = append(sequences, seq)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sequences, []uint64{100, 101}) {
		t.Fatalf("recovered sequences: %v", sequences)
	}
}

func TestDeepAuditBookmarkWALTornTailThenAppendThenRestart(t *testing.T) {
	var path = filepath.Join(t.TempDir(), "bookmarks")
	var options = FileStoreOptions{UseWAL: true, SyncOnWrite: true, CheckpointInterval: 1000}
	var store = NewFileBookmarkStoreWithOptions(path, options)
	var first = auditBookmarkMessage("1|1|")
	if _, err := store.LogWithError(first); err != nil {
		t.Fatal(err)
	}
	deepAuditAppendTornTail(t, path+".wal")
	store = NewFileBookmarkStoreWithOptions(path, options)
	if err := store.LoadError(); err != nil {
		t.Fatal(err)
	}
	if err := store.DiscardMessageWithError(first); err != nil {
		t.Fatal(err)
	}
	store = NewFileBookmarkStoreWithOptions(path, options)
	if err := store.LoadError(); err != nil {
		t.Fatalf("second restart lost a successful discard: %v", err)
	}
	if !store.IsDiscarded(first) {
		t.Fatal("successful discard forgotten")
	}
}

func TestDeepAuditPublishCheckpointFailureIsNotRejectedAfterRetention(t *testing.T) {
	var path = filepath.Join(t.TempDir(), "publish")
	var store = NewFilePublishStore(path)
	// Only the checkpoint path is unavailable; the WAL remains writable.
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	var client = NewClient("deep-audit")
	defer client.Close()
	client.SetPublishStore(store)
	var publishErr = client.Publish("orders", "must-not-be-retried")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	var recovered = NewFilePublishStore(path)
	if err := recovered.LoadError(); err != nil {
		t.Fatal(err)
	}
	if publishErr != nil && recovered.UnpersistedCount() != 0 {
		t.Fatalf("Publish reports rejection but message is durably retained: err=%v retained=%d", publishErr, recovered.UnpersistedCount())
	}
}

type deepAuditRejectingPublishStore struct {
	*MemoryPublishStore
	err error
}

func (store *deepAuditRejectingPublishStore) Store(*Command) (uint64, error) {
	return 99, store.err
}

func TestDeepAuditUnmarkedStoreErrorStillRejectsPublish(t *testing.T) {
	var failure = errors.New("store rejected publish")
	var client = NewClient("deep-audit")
	defer client.Close()
	client.SetPublishStore(&deepAuditRejectingPublishStore{MemoryPublishStore: NewMemoryPublishStore(), err: failure})
	if err := client.Publish("orders", "payload"); !errors.Is(err, failure) {
		t.Fatalf("unmarked store error = %v, want %v", err, failure)
	}
}

func TestDeepAuditCheckpointFailureReportsMaintenanceAfterAcceptance(t *testing.T) {
	var path = filepath.Join(t.TempDir(), "publish")
	var store = NewFilePublishStore(path)
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	var client = NewClient("deep-audit")
	defer client.Close()
	client.SetPublishStore(store)
	var reported error
	client.SetExceptionListener(ExceptionListenerFunc(func(err error) {
		reported = err
		_ = client.PublishStore()
	}))
	if err := client.Publish("orders", "payload"); err != nil {
		t.Fatalf("accepted publish rejected: %v", err)
	}
	var accepted *PublishStoreAcceptedError
	if !errors.As(reported, &accepted) || store.UnpersistedCount() != 1 {
		t.Fatalf("maintenance error = %v, retained = %d", reported, store.UnpersistedCount())
	}
}

func TestDeepAuditWALAppendFailureRejectsWithoutRetention(t *testing.T) {
	var path = filepath.Join(t.TempDir(), "publish")
	var store = NewFilePublishStore(path)
	if err := os.Mkdir(path+".wal", 0o700); err != nil {
		t.Fatal(err)
	}
	var client = NewClient("deep-audit")
	defer client.Close()
	client.SetPublishStore(store)
	if err := client.Publish("orders", "payload"); err == nil || store.UnpersistedCount() != 0 {
		t.Fatalf("WAL failure error = %v, retained = %d", err, store.UnpersistedCount())
	}
}

func TestDeepAuditUncertainWALAppendBlocksFurtherWritesAndCheckpoint(t *testing.T) {
	var path = filepath.Join(t.TempDir(), "publish")
	var options = FileStoreOptions{UseWAL: true, SyncOnWrite: true, CheckpointInterval: 1000}
	var store = NewFilePublishStoreWithOptions(path, options)
	if _, err := store.Store(NewCommand("publish").SetTopic("first").SetSequenceID(100)); err != nil {
		t.Fatal(err)
	}
	var before, readErr = os.ReadFile(path + ".wal")
	if readErr != nil {
		t.Fatal(readErr)
	}
	var failure = errors.New("append sync failed")
	var rollbackFailure = errors.New("rollback sync failed")
	currentFileStoreTestHooks.publishWalAppendError = func() error { return &wal.AppendOutcomeError{Err: failure, RollbackErr: rollbackFailure} }
	var sequence, err = store.Store(NewCommand("publish").SetTopic("uncertain"))
	currentFileStoreTestHooks.publishWalAppendError = nil
	var uncertain *PublishStoreUncertainError
	if !errors.As(err, &uncertain) || !errors.Is(err, failure) || !errors.Is(err, rollbackFailure) || uncertain.Sequence != sequence || sequence == 0 || sequence == 100 {
		t.Fatalf("uncertain outcome: sequence=%d error=%v", sequence, err)
	}
	if _, err = store.Store(NewCommand("publish").SetTopic("blocked")); !errors.As(err, &uncertain) {
		t.Fatalf("store accepted new write after uncertainty: %v", err)
	}
	if err = store.saveCheckpoint(); !errors.As(err, &uncertain) {
		t.Fatalf("checkpoint erased uncertain WAL: %v", err)
	}
	var after, afterErr = os.ReadFile(path + ".wal")
	if afterErr != nil || !bytes.Equal(before, after) {
		t.Fatalf("WAL changed after uncertainty: %v", afterErr)
	}
}

func TestDeepAuditUncertainDiscardBlocksPublishStoreMutations(t *testing.T) {
	var path = filepath.Join(t.TempDir(), "publish")
	var store = NewFilePublishStoreWithOptions(path, FileStoreOptions{UseWAL: true, SyncOnWrite: true, CheckpointInterval: 1000})
	var sequence, err = store.Store(NewCommand("publish").SetTopic("orders"))
	if err != nil {
		t.Fatal(err)
	}
	var before, _ = os.ReadFile(path + ".wal")
	var failure = errors.New("uncertain discard sync")
	currentFileStoreTestHooks.publishWalAppendError = func() error { return &wal.AppendOutcomeError{Err: failure, RollbackErr: failure} }
	err = store.DiscardUpTo(sequence)
	currentFileStoreTestHooks.publishWalAppendError = nil
	if !errors.Is(err, failure) || !errors.Is(store.LoadError(), failure) {
		t.Fatalf("discard error=%v load error=%v", err, store.LoadError())
	}
	if _, err = store.Store(NewCommand("publish").SetTopic("next")); !errors.Is(err, failure) {
		t.Fatalf("publish after uncertain discard = %v", err)
	}
	if err = store.saveCheckpoint(); !errors.Is(err, failure) {
		t.Fatalf("checkpoint after uncertain discard = %v", err)
	}
	var after, readErr = os.ReadFile(path + ".wal")
	if readErr != nil || !bytes.Equal(before, after) {
		t.Fatalf("uncertain discard WAL changed: %v", readErr)
	}
}

func TestDeepAuditUncertainBookmarkDiscardBlocksMutations(t *testing.T) {
	var path = filepath.Join(t.TempDir(), "bookmarks")
	var store = NewFileBookmarkStoreWithOptions(path, FileStoreOptions{UseWAL: true, SyncOnWrite: true, CheckpointInterval: 1000})
	var message = auditBookmarkMessage("1|1|")
	if _, err := store.LogWithError(message); err != nil {
		t.Fatal(err)
	}
	var before, _ = os.ReadFile(path + ".wal")
	var failure = errors.New("uncertain bookmark sync")
	currentFileStoreTestHooks.bookmarkWalAppendError = func() error { return &wal.AppendOutcomeError{Err: failure, RollbackErr: failure} }
	var err = store.DiscardMessageWithError(message)
	currentFileStoreTestHooks.bookmarkWalAppendError = nil
	if !errors.Is(err, failure) || !errors.Is(store.LoadError(), failure) {
		t.Fatalf("discard error=%v load error=%v", err, store.LoadError())
	}
	if err = store.PurgeWithError(); !errors.Is(err, failure) {
		t.Fatalf("purge after uncertain discard = %v", err)
	}
	if err = store.saveCheckpoint(); !errors.Is(err, failure) {
		t.Fatalf("checkpoint after uncertain discard = %v", err)
	}
	var after, readErr = os.ReadFile(path + ".wal")
	if readErr != nil || !bytes.Equal(before, after) {
		t.Fatalf("uncertain bookmark WAL changed: %v", readErr)
	}
}

type deepAuditReplayBarrier struct {
	*MemoryPublishStore
	entered chan struct{}
	resume  chan struct{}
}

func (store *deepAuditReplayBarrier) Replay(replayer func(*Command) error) error {
	var first = true
	return store.MemoryPublishStore.Replay(func(command *Command) error {
		if first {
			first = false
			close(store.entered)
			<-store.resume
		}
		return replayer(command)
	})
}

func deepAuditWrittenSequences(t *testing.T, raw []byte) []uint64 {
	t.Helper()
	var sequences []uint64
	for len(raw) > 0 {
		if len(raw) < 4 {
			t.Fatal("partial frame length")
		}
		var length = int(binary.BigEndian.Uint32(raw[:4]))
		if length > len(raw)-4 {
			t.Fatal("partial frame body")
		}
		var header struct {
			Sequence uint64 `json:"s"`
		}
		if err := json.Unmarshal(raw[4:4+length], &header); err != nil {
			t.Fatal(err)
		}
		sequences = append(sequences, header.Sequence)
		raw = raw[4+length:]
	}
	return sequences
}

func TestDeepAuditLivePublishCannotOvertakeRecovery(t *testing.T) {
	var client, conn = auditConnectedClient(t)
	var store = &deepAuditReplayBarrier{MemoryPublishStore: newMemoryPublishStore(1), entered: make(chan struct{}), resume: make(chan struct{})}
	for index := 0; index < 2; index++ {
		if _, err := store.Store(NewCommand("publish").SetTopic("orders").SetAckType(AckTypePersisted)); err != nil {
			t.Fatal(err)
		}
	}
	client.SetPublishStore(store)
	var recovered = make(chan error, 1)
	go func() { recovered <- client.postLogonRecovery() }()
	<-store.entered
	var published = make(chan error, 1)
	go func() { published <- client.Publish("orders", "") }()
	var completed bool
	select {
	case err := <-published:
		if err != nil {
			t.Fatal(err)
		}
		completed = true
	case <-time.After(100 * time.Millisecond):
	}
	close(store.resume)
	if err := <-recovered; err != nil {
		t.Fatal(err)
	}
	if !completed {
		if err := <-published; err != nil {
			t.Fatal(err)
		}
	}
	var sequences = deepAuditWrittenSequences(t, conn.WrittenBytes())
	if !reflect.DeepEqual(sequences, []uint64{1, 2, 3}) {
		t.Fatalf("new publish overtook stored messages: wire sequence=%v", sequences)
	}
}

func TestDeepAuditUnstoredPublishDoesNotWaitForReplayGate(t *testing.T) {
	for _, commandType := range []string{"publish", "delta_publish", "sow_delete"} {
		t.Run(commandType, func(t *testing.T) {
			var client, _ = auditConnectedClient(t)
			client.publishLock.Lock()
			var result = make(chan error, 1)
			go func() {
				_, err := client.ExecuteAsync(NewCommand(commandType).SetTopic("orders"), nil)
				result <- err
			}()
			select {
			case err := <-result:
				client.publishLock.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(500 * time.Millisecond):
				client.publishLock.Unlock()
				<-result
				t.Fatal("publication without a store waited for replay gate")
			}
		})
	}
}

func TestDeepAuditPublishWithoutStoreProceedsWhileStoreInstallationWaits(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	client.publishLock.Lock()
	var installed = make(chan struct{})
	go func() { client.SetPublishStore(newMemoryPublishStore(1)); close(installed) }()
	var published = make(chan error, 1)
	go func() { published <- client.Publish("orders", "") }()
	select {
	case err := <-published:
		if err != nil {
			client.publishLock.Unlock()
			t.Fatal(err)
		}
	case <-time.After(500 * time.Millisecond):
		client.publishLock.Unlock()
		<-published
		t.Fatal("unstored publish blocked by store installation")
	}
	select {
	case <-installed:
		client.publishLock.Unlock()
		t.Fatal("store installation bypassed replay gate")
	default:
	}
	client.publishLock.Unlock()
	select {
	case <-installed:
	case <-time.After(time.Second):
		t.Fatal("store installation deadlocked")
	}
	if err := client.Publish("orders", ""); err != nil {
		t.Fatal(err)
	}
	if count := client.PublishStore().UnpersistedCount(); count != 1 {
		t.Fatalf("installed store retained %d publications, want 1", count)
	}
}

func TestDeepAuditStoreReplacementWaitsForReplay(t *testing.T) {
	var client, conn = auditConnectedClient(t)
	var oldStore = &deepAuditReplayBarrier{MemoryPublishStore: newMemoryPublishStore(1), entered: make(chan struct{}), resume: make(chan struct{})}
	for range 2 {
		if _, err := oldStore.Store(NewCommand("publish").SetTopic("orders")); err != nil {
			t.Fatal(err)
		}
	}
	client.SetPublishStore(oldStore)
	var recovered = make(chan error, 1)
	go func() { recovered <- client.postLogonRecovery() }()
	<-oldStore.entered
	var newStore = newMemoryPublishStore(3)
	var replaced = make(chan struct{})
	go func() { client.SetPublishStore(newStore); close(replaced) }()
	select {
	case <-replaced:
		close(oldStore.resume)
		<-recovered
		t.Fatal("store replacement bypassed replay gate")
	case <-time.After(50 * time.Millisecond):
	}
	close(oldStore.resume)
	if err := <-recovered; err != nil {
		t.Fatal(err)
	}
	select {
	case <-replaced:
	case <-time.After(time.Second):
		t.Fatal("store replacement deadlocked")
	}
	if err := client.Publish("orders", ""); err != nil {
		t.Fatal(err)
	}
	if got := deepAuditWrittenSequences(t, conn.WrittenBytes()); !reflect.DeepEqual(got, []uint64{1, 2, 3}) {
		t.Fatalf("wire order during store replacement = %v", got)
	}
}

type deepAuditReplayEndBarrier struct {
	*MemoryPublishStore
	entered chan struct{}
	resume  chan struct{}
}

func (store *deepAuditReplayEndBarrier) Replay(replayer func(*Command) error) error {
	var err = store.MemoryPublishStore.Replay(replayer)
	close(store.entered)
	<-store.resume
	return err
}

func TestDeepAuditReplayCannotCompleteAfterConnectionChanges(t *testing.T) {
	for _, count := range []int{0, 1} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			var client, _ = auditConnectedClient(t)
			var store = &deepAuditReplayEndBarrier{MemoryPublishStore: newMemoryPublishStore(1), entered: make(chan struct{}), resume: make(chan struct{})}
			if count != 0 {
				if _, err := store.Store(NewCommand("publish").SetTopic("orders")); err != nil {
					t.Fatal(err)
				}
			}
			client.SetPublishStore(store)
			var done = make(chan error, 1)
			go func() { done <- client.postLogonRecovery() }()
			<-store.entered
			client.lock.Lock()
			client.generation.Add(1)
			client.awaitingReplay = true
			client.lock.Unlock()
			close(store.resume)
			if err := <-done; !IsErrorKind(err, DisconnectedError) {
				t.Fatalf("stale recovery result = %v", err)
			}
			client.lock.Lock()
			var awaiting = client.awaitingReplay
			client.lock.Unlock()
			if !awaiting {
				t.Fatal("stale recovery cleared the newer generation's replay gate")
			}
		})
	}
}

func TestDeepAuditHATimeoutIncludesLogon(t *testing.T) {
	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var accepted = make(chan net.Conn, 1)
	go func() {
		var conn, acceptErr = listener.Accept()
		if acceptErr == nil {
			accepted <- conn
			_, _ = io.Copy(io.Discard, conn)
		}
	}()
	var ha = NewHAClient("deep-audit-timeout").SetTimeout(40 * time.Millisecond)
	ha.Client().SetHeartbeat(0)
	ha.SetServerChooser(NewDefaultServerChooser().Add("tcp://" + listener.Addr().String() + "/amps/json"))
	var result = make(chan error, 1)
	go func() { result <- ha.ConnectAndLogon() }()
	var conn = <-accepted
	defer conn.Close()
	select {
	case err := <-result:
		_ = ha.Disconnect()
		if !IsErrorKind(err, TimedOutError) {
			t.Fatalf("wrong timeout result: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		_ = ha.Disconnect()
		<-result
		t.Fatal("40ms HA timeout did not bound a logon with no response after 300ms")
	}
}

func TestDeepAuditHADisconnectCancelsPendingLogon(t *testing.T) {
	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var accepted = make(chan net.Conn, 1)
	go func() {
		var conn, acceptErr = listener.Accept()
		if acceptErr == nil {
			accepted <- conn
			_, _ = io.Copy(io.Discard, conn)
		}
	}()
	var ha = NewHAClient("deep-audit-cancel").SetTimeout(0)
	ha.Client().SetHeartbeat(0)
	ha.SetServerChooser(NewDefaultServerChooser().Add("tcp://" + listener.Addr().String() + "/amps/json"))
	var result = make(chan error, 1)
	go func() { result <- ha.ConnectAndLogon() }()
	var conn = <-accepted
	defer conn.Close()
	_ = ha.Disconnect()
	select {
	case err = <-result:
		if !IsErrorKind(err, DisconnectedError) {
			t.Fatalf("cancelled logon error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Disconnect did not cancel pending logon")
	}
}

func TestDeepAuditLogonSendFailureRemovesAckRoute(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	client.url, _ = url.Parse("tcp://localhost:9000/amps/json")
	client.SetTransportFilter(func(TransportFilterDirection, []byte) []byte { return []byte{1} })
	if err := client.Logon(); err == nil {
		t.Fatal("invalid filtered logon was accepted")
	}
	var routes int
	client.routes.Range(func(_, _ any) bool { routes++; return true })
	if routes != 0 {
		t.Fatalf("failed logon retained %d acknowledgement routes", routes)
	}
}

func deepAuditRecoveryServer(t *testing.T, acknowledgeRecovery bool) (string, <-chan struct{}, <-chan struct{}) {
	t.Helper()
	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var observed, done = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		defer listener.Close()
		var conn, acceptErr = listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		var length [4]byte
		for index := 0; index < 2; index++ {
			if _, readErr := io.ReadFull(conn, length[:]); readErr != nil {
				return
			}
			var body = make([]byte, binary.BigEndian.Uint32(length[:]))
			if _, readErr := io.ReadFull(conn, body); readErr != nil {
				return
			}
			var command struct {
				CommandID string `json:"cid"`
			}
			if json.Unmarshal(body, &command) != nil {
				return
			}
			if index == 1 {
				close(observed)
				if !acknowledgeRecovery {
					break
				}
			}
			var ack, _ = json.Marshal(map[string]string{"c": "ack", "a": "processed", "cid": command.CommandID, "status": "success"})
			binary.BigEndian.PutUint32(length[:], uint32(len(ack)))
			if _, writeErr := conn.Write(append(length[:], ack...)); writeErr != nil {
				return
			}
		}
		_, _ = io.Copy(io.Discard, conn)
	}()
	return "tcp://" + listener.Addr().String() + "/amps/json", observed, done
}

func TestDeepAuditHARecoveryObservesCancellationAndSuccess(t *testing.T) {
	for _, success := range []bool{false, true} {
		var name = "cancel"
		if success {
			name = "success"
		}
		t.Run(name, func(t *testing.T) {
			var uri, observed, done = deepAuditRecoveryServer(t, success)
			var ha = NewHAClient("recovery-context")
			ha.Client().SetHeartbeat(0)
			var manager = NewDefaultSubscriptionManager()
			manager.Subscribe(nil, NewCommand("subscribe").SetTopic("orders").SetSubID("sub-1"), AckTypeNone)
			ha.Client().SetSubscriptionManager(manager)
			ha.SetServerChooser(NewDefaultServerChooser().Add(uri))
			var ctx, cancel = context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var result = make(chan error, 1)
			go func() { result <- ha.connectAndLogon(ctx) }()
			select {
			case <-observed:
			case <-time.After(time.Second):
				cancel()
				t.Fatal("recovery subscription was not sent")
			}
			if !success {
				cancel()
			}
			select {
			case err := <-result:
				if success && err != nil || !success && !IsErrorKind(err, DisconnectedError) {
					t.Fatalf("recovery result = %v, success=%v", err, success)
				}
			case <-time.After(time.Second):
				t.Fatal("recovery did not finish")
			}
			_ = ha.Disconnect()
			<-done
		})
	}
}

func TestDeepAuditLogonContextDoesNotCloseNewGeneration(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	client.url, _ = url.Parse("tcp://localhost:9000/amps/json")
	var store = &deepAuditReplayEndBarrier{MemoryPublishStore: NewMemoryPublishStore(), entered: make(chan struct{}), resume: make(chan struct{})}
	client.SetPublishStore(store)
	var ctx, cancel = context.WithCancel(context.Background())
	var result = make(chan error, 1)
	go func() { result <- client.logonWithContext(ctx) }()
	var id, _ = waitForAnyRouteHandler(t, client)
	var ack = commandToMessage(NewCommand("ack").SetCommandID(id).SetAckType(AckTypeProcessed))
	ack.header.status = []byte("success")
	if err := client.onMessage(ack); err != nil {
		t.Fatal(err)
	}
	<-store.entered
	var newer = newTestConn()
	client.lock.Lock()
	client.connectionStateLock.Lock()
	client.connection = newer
	client.generation.Add(1)
	client.awaitingReplay = true
	client.connectionStateLock.Unlock()
	client.lock.Unlock()
	cancel()
	close(store.resume)
	if err := <-result; !IsErrorKind(err, DisconnectedError) {
		t.Fatalf("cancelled old logon = %v", err)
	}
	if _, err := newer.Write([]byte("new connection remains open")); err != nil {
		t.Fatalf("old watcher closed new connection: %v", err)
	}
}

func TestDeepAuditLoggedOnCallbackUsesServerSequenceFloor(t *testing.T) {
	var client, conn = auditConnectedClient(t)
	var store = newMemoryPublishStore(1)
	client.SetPublishStore(store)
	var published = make(chan error, 1)
	client.AddConnectionStateListener(ConnectionStateListenerFunc(func(state ConnectionState) {
		if state == ConnectionStateLoggedOn {
			published <- client.Publish("orders", "")
		}
	}))
	var ack = commandToMessage(NewCommand("ack").SetAckType(AckTypeProcessed).SetSequenceID(100))
	ack.header.status = []byte("success")
	if err := auditLogon(t, client, LogonParams{Timeout: 500}, ack); err != nil {
		t.Fatal(err)
	}
	if err := <-published; err != nil {
		t.Fatal(err)
	}
	var sequences = deepAuditWrittenSequences(t, conn.WrittenBytes())
	if !reflect.DeepEqual(sequences, []uint64{0, 101}) {
		t.Fatalf("logon callback used sequence before server floor: %v", sequences)
	}
}

func TestDeepAuditReceivedHeaderUnescapesText(t *testing.T) {
	for _, raw := range []string{`{"t":"orders\\archive"}`, `{"c":"p","t":"orders\\archive","sub_id":"sub-1"}`, `{"c":"p","t":"orders\\archive","sub_id":"sub\"1","bm":"1|1|"}`} {
		var expected map[string]string
		if err := json.Unmarshal([]byte(raw), &expected); err != nil {
			t.Fatal(err)
		}
		var msg Message
		if _, err := parseHeader(&msg, true, []byte(raw)); err != nil {
			t.Fatal(err)
		}
		var topic, _ = msg.Topic()
		var subID, _ = msg.SubID()
		if topic != expected["t"] || subID != expected["sub_id"] {
			t.Errorf("header=%s got topic=%q sub_id=%q; want topic=%q sub_id=%q", raw, topic, subID, expected["t"], expected["sub_id"])
		}
	}
}

func TestDeepAuditEscapedSubscriptionIDRoutes(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	var deliveries int
	client.routes.Store(`sub"1`, func(*Message) error { deliveries++; return nil })
	var msg Message
	if _, err := parseHeader(&msg, true, []byte(`{"c":"p","t":"orders","sub_id":"sub\"1"}`)); err != nil {
		t.Fatal(err)
	}
	if err := client.onMessage(&msg); err != nil {
		t.Fatal(err)
	}
	if deliveries != 1 {
		t.Fatalf("escaped subscription id lost delivery: calls=%d", deliveries)
	}
}

func TestDeepAuditHeaderRejectsInvalidEscapedKeysAndControls(t *testing.T) {
	var cases = []struct {
		name   string
		header []byte
	}{
		{"single-command-escape", []byte(`{"c":"\","t":"orders","sub_id":"sub-1"}`)},
		{"bad-key-string", []byte(`{"bad\q":"value"}`)},
		{"bad-key-number-before-comma", []byte(`{"bad\q":1,"t":"orders"}`)},
		{"bad-key-number-at-end", []byte(`{"bad\q":1}`)},
		{"unescaped-control", []byte("{\"t\":\"order\x01s\"}")},
	}
	for _, entry := range cases {
		t.Run(entry.name, func(t *testing.T) {
			var message Message
			if _, err := parseHeader(&message, true, entry.header); err == nil {
				t.Fatalf("accepted invalid header %q", entry.header)
			}
		})
	}
}

func TestDeepAuditHeaderSlowPathDecodesEscapedCommand(t *testing.T) {
	var message Message
	if _, err := parseHeader(&message, true, []byte(`{"c":"pub\u006cish","t":"orders","sub_id":"sub-1"}`)); err != nil {
		t.Fatal(err)
	}
	if message.header.command != CommandPublish {
		t.Fatalf("escaped command decoded to %d", message.header.command)
	}
}

func TestDeepAuditFailedQueueHandlerCancelsLease(t *testing.T) {
	var client, conn = auditConnectedClient(t)
	client.SetAutoAck(true).SetAckBatchSize(1).SetAckTimeout(0)
	var failed = errors.New("application could not process message")
	client.routes.Store("sub-1", func(*Message) error { return failed })
	var message = auditBookmarkMessage("1|1|")
	message.header.textExtras = &_HeaderTextExtras{leasePeriod: []byte("30000")}
	if err := client.onMessage(message); !errors.Is(err, failed) {
		t.Fatalf("error=%v", err)
	}
	if wire := conn.WrittenPayload(); !strings.Contains(wire, `"opts":"cancel"`) || !strings.Contains(wire, `"bm":"1|1|"`) {
		t.Fatalf("failed queue handler must cancel its lease, got wire=%q", wire)
	}
}

func TestDeepAuditFailedQueueHandlerHonorsIgnoreAutoAck(t *testing.T) {
	var client, conn = auditConnectedClient(t)
	client.SetAutoAck(true).SetAckBatchSize(1).SetAckTimeout(0)
	var failure = errors.New("handler failed")
	client.routes.Store("sub-1", func(message *Message) error {
		message.SetIgnoreAutoAck(true)
		return failure
	})
	var message = auditBookmarkMessage("1|1|")
	message.header.textExtras = &_HeaderTextExtras{leasePeriod: []byte("30000")}
	if err := client.onMessage(message); !errors.Is(err, failure) || len(conn.WrittenBytes()) != 0 {
		t.Fatalf("ignored cancellation error=%v wire=%q", err, conn.WrittenPayload())
	}
}

func TestDeepAuditFailedQueueHandlerReportsCancelFailure(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	client.SetAutoAck(true).SetAckBatchSize(1).SetAckTimeout(0)
	var failure = errors.New("handler failed")
	var reported error
	client.SetExceptionListener(ExceptionListenerFunc(func(err error) { reported = err }))
	client.routes.Store("sub-1", func(*Message) error {
		client.connected.Store(false)
		return failure
	})
	var message = auditBookmarkMessage("1|1|")
	message.header.textExtras = &_HeaderTextExtras{leasePeriod: []byte("30000")}
	var err = client.onMessage(message)
	if !errors.Is(err, failure) || !IsErrorKind(reported, DisconnectedError) {
		t.Fatalf("handler error=%v cancellation error=%v", err, reported)
	}
}

func TestDeepAuditMessageAckDefersToAutoAck(t *testing.T) {
	var client, conn = auditConnectedClient(t)
	client.SetAutoAck(true).SetAckBatchSize(1).SetAckTimeout(0)
	var message = auditBookmarkMessage("1|1|")
	message.client = client
	if err := message.Ack(); err != nil {
		t.Fatal(err)
	}
	if wire := conn.WrittenBytes(); len(wire) != 0 {
		t.Fatalf("Message.Ack sent an extra acknowledgement with AutoAck enabled: %s", wire[4:])
	}
}

func TestDeepAuditMessageAckRemainsManualWhenAutoAckDisabled(t *testing.T) {
	var client, conn = auditConnectedClient(t)
	client.SetAutoAck(false).SetAckBatchSize(1).SetAckTimeout(0)
	var message = auditBookmarkMessage("1|1|")
	message.client = client
	if err := message.Ack(); err != nil || !strings.Contains(conn.WrittenPayload(), `"bm":"1|1|"`) {
		t.Fatalf("manual ack error=%v wire=%q", err, conn.WrittenPayload())
	}
}

func TestDeepAuditStreamAckFailureCannotBeBypassedByNextCall(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	client.SetAutoAck(true).SetAckBatchSize(1).SetAckTimeout(0)
	var stream = newMessageStream(client).SetSubscription("sub-1", "sub-1")
	client.routes.Store("sub-1", stream.messageHandler)
	for _, bookmark := range []string{"1|1|", "1|2|"} {
		var message = auditBookmarkMessage(bookmark)
		message.header.textExtras = &_HeaderTextExtras{leasePeriod: []byte("30000")}
		if err := client.onMessage(message); err != nil {
			t.Fatal(err)
		}
	}
	if stream.Next() == nil {
		t.Fatal("missing first delivery")
	}
	client.connected.Store(false)
	if stream.HasNext() {
		t.Fatal("advanced on first failed acknowledgement")
	}
	if stream.HasNext() || stream.Depth() != 1 {
		t.Fatal("second HasNext bypassed the failed acknowledgement and advanced")
	}
}

func TestDeepAuditStreamAckRetriesQueuedBatchWithoutDuplicating(t *testing.T) {
	var client, oldConn = auditConnectedClient(t)
	client.SetAutoAck(true).SetAckBatchSize(1).SetAckTimeout(0)
	client.SetErrorHandler(func(error) {})
	var stream = newMessageStream(client).SetSubscription("sub-1", "sub-1")
	client.routes.Store("sub-1", stream.messageHandler)
	for _, bookmark := range []string{"1|1|", "1|2|"} {
		var message = auditBookmarkMessage(bookmark)
		message.header.textExtras = &_HeaderTextExtras{leasePeriod: []byte("30000")}
		if err := client.onMessage(message); err != nil {
			t.Fatal(err)
		}
	}
	if stream.Next() == nil {
		t.Fatal("missing first message")
	}
	_ = oldConn.Close()
	var firstTry = stream.HasNext()
	var secondTry = stream.HasNext()
	if firstTry || secondTry || stream.Depth() != 1 {
		t.Fatal("ack failure allowed stream to advance")
	}
	_ = client.Disconnect()
	var newConn = newTestConn()
	client.connectionStateLock.Lock()
	client.connection = newConn
	client.connected.Store(true)
	client.stopped.Store(false)
	client.connectionStateLock.Unlock()
	if !stream.HasNext() || stream.Depth() != 0 {
		t.Fatal("stream did not resume after retry")
	}
	var wire = newConn.WrittenPayload()
	if strings.Count(wire, `"bm":"1|1|"`) != 1 || strings.Contains(wire, "1|1|,1|1|") {
		t.Fatalf("retry duplicated ack: %q", wire)
	}
}

func TestDeepAuditRecoveryFlushesPendingAckWithoutNewTraffic(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	client.SetAckBatchSize(10).SetAckTimeout(0)
	if err := client.Ack("orders", "1|1|"); err != nil {
		t.Fatal(err)
	}
	if err := client.Disconnect(); err != nil {
		t.Fatal(err)
	}
	var conn = newTestConn()
	client.connectionStateLock.Lock()
	client.connection = conn
	client.connected.Store(true)
	client.stopped.Store(false)
	client.connectionStateLock.Unlock()
	if err := client.postLogonRecovery(); err != nil {
		t.Fatal(err)
	}
	if wire := conn.WrittenPayload(); !strings.Contains(wire, `"bm":"1|1|"`) {
		t.Fatalf("recovery did not flush retained ack: %q", wire)
	}
}

type deepAuditBlockingBookmarkStore struct {
	*MemoryBookmarkStore
	entered chan struct{}
	resume  chan struct{}
}

func (store *deepAuditBlockingBookmarkStore) GetMostRecent(string) string {
	close(store.entered)
	<-store.resume
	return "1|1|"
}

func TestDeepAuditUnsubscribeDuringRecoveryIsNotResurrected(t *testing.T) {
	var client, conn = auditConnectedClient(t)
	client.SetRetryOnDisconnect(true)
	var manager = NewDefaultSubscriptionManager()
	client.SetSubscriptionManager(manager)
	var store = &deepAuditBlockingBookmarkStore{MemoryBookmarkStore: NewMemoryBookmarkStore(), entered: make(chan struct{}), resume: make(chan struct{})}
	client.SetBookmarkStore(store)
	manager.Subscribe(func(*Message) error { return nil }, NewCommand("subscribe").SetTopic("orders").SetSubID("sub-1").SetBookmark(BookmarksRECENT).SetAckType(AckTypeProcessed), AckTypeProcessed)
	var done = make(chan error, 1)
	go func() { done <- manager.Resubscribe(client) }()
	<-store.entered
	var unsubscribed = make(chan error, 1)
	go func() { unsubscribed <- client.Unsubscribe("sub-1") }()
	var completed bool
	select {
	case err := <-unsubscribed:
		if err != nil {
			t.Fatal(err)
		}
		completed = true
	case <-time.After(100 * time.Millisecond):
	}
	close(store.resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !completed {
		if err := <-unsubscribed; err != nil {
			t.Fatal(err)
		}
	}
	var wire = conn.WrittenBytes()
	var subscribe = bytes.LastIndex(wire, []byte(`"c":"subscribe"`))
	var unsubscribe = bytes.LastIndex(wire, []byte(`"c":"unsubscribe"`))
	if unsubscribe < 0 || subscribe > unsubscribe {
		t.Fatalf("recovery resurrected a subscription after successful Unsubscribe: %s", wire)
	}
	if _, exists := client.routes.Load("sub-1"); exists {
		t.Fatal("route retained after successful Unsubscribe")
	}
}

type deepAuditOnceBlockingBookmarkStore struct {
	*MemoryBookmarkStore
	once    sync.Once
	entered chan struct{}
	resume  chan struct{}
}

func (store *deepAuditOnceBlockingBookmarkStore) GetMostRecent(string) string {
	store.once.Do(func() { close(store.entered); <-store.resume })
	return "1|1|"
}

type deepAuditResumeConn struct {
	*testConn
	once sync.Once
	sent chan struct{}
}

func (conn *deepAuditResumeConn) Write(data []byte) (int, error) {
	var count, err = conn.testConn.Write(data)
	if bytes.Contains(data, []byte(`"opts":"resume"`)) {
		conn.once.Do(func() { close(conn.sent) })
	}
	return count, err
}

func TestDeepAuditPartialResumeCancellationDoesNotResurrectMember(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	var conn = &deepAuditResumeConn{testConn: newTestConn(), sent: make(chan struct{})}
	client.connectionStateLock.Lock()
	client.connection = conn
	client.connectionStateLock.Unlock()
	client.SetRetryOnDisconnect(true)
	var manager = NewDefaultSubscriptionManager()
	client.SetSubscriptionManager(manager)
	var store = &deepAuditOnceBlockingBookmarkStore{MemoryBookmarkStore: NewMemoryBookmarkStore(), entered: make(chan struct{}), resume: make(chan struct{})}
	client.SetBookmarkStore(store)
	manager.Subscribe(nil, NewCommand("subscribe").SetTopic("orders").SetSubID("sub-a,sub-b").SetBookmark("1|1|").SetOptions("resume"), AckTypeProcessed)
	var done = make(chan error, 1)
	go func() { done <- manager.Resubscribe(client) }()
	<-store.entered
	if err := client.Unsubscribe("sub-a"); err != nil {
		t.Fatal(err)
	}
	close(store.resume)
	select {
	case <-conn.sent:
	case <-time.After(time.Second):
		t.Fatal("remaining resume member was not replayed")
	}
	var ack = commandToMessage(NewCommand("ack").SetCommandID("1").SetAckType(AckTypeProcessed))
	ack.header.status = []byte("success")
	if err := client.onMessage(ack); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var wire = conn.WrittenPayload()
	if !strings.Contains(wire, `"sub_id":"sub-b"`) || strings.Contains(wire, `"sub_id":"sub-a,sub-b"`) {
		t.Fatalf("cancelled member reappeared: %q", wire)
	}
}

func TestDeepAuditOfflineExecutePreservesAckHandler(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	client.connected.Store(false)
	client.SetPublishStore(NewMemoryPublishStore())
	var calls int
	var command = NewCommand("publish").SetTopic("orders").SetAckType(AckTypeProcessed | AckTypePersisted)
	var id, err = client.ExecuteAsync(command, func(*Message) error { calls++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	client.connected.Store(true)
	if err = client.postLogonRecovery(); err != nil {
		t.Fatal(err)
	}
	var ack = commandToMessage(NewCommand("ack").SetCommandID(id).SetAckType(AckTypeProcessed))
	ack.header.status = []byte("success")
	if err = client.onMessage(ack); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("successfully retained ExecuteAsync lost callback: calls=%d command_id=%q", calls, id)
	}
}

func TestDeepAuditOfflineSOWDeletePreservesSequenceAndHandler(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	client.connected.Store(false)
	var store = NewMemoryPublishStore()
	client.SetPublishStore(store)
	var calls int
	var id, err = client.ExecuteAsync(NewCommand("sow_delete").SetTopic("orders").SetAckType(AckTypeProcessed), func(*Message) error { calls++; return nil })
	if err != nil || store.UnpersistedCount() != 1 {
		t.Fatalf("offline delete error=%v retained=%d", err, store.UnpersistedCount())
	}
	client.connected.Store(true)
	if err = client.postLogonRecovery(); err != nil {
		t.Fatal(err)
	}
	var ack = commandToMessage(NewCommand("ack").SetCommandID(id).SetAckType(AckTypeProcessed))
	ack.header.status = []byte("success")
	if err = client.onMessage(ack); err != nil || calls != 1 {
		t.Fatalf("delete callback calls=%d error=%v", calls, err)
	}
}

func TestDeepAuditRejectedOfflineExecuteCleansAckRoute(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	client.connected.Store(false)
	var failure = errors.New("publish rejected")
	client.SetPublishStore(&deepAuditRejectingPublishStore{MemoryPublishStore: NewMemoryPublishStore(), err: failure})
	var id, err = client.ExecuteAsync(NewCommand("publish").SetTopic("orders").SetAckType(AckTypeProcessed), func(*Message) error { return nil })
	if !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}
	if _, exists := client.routes.Load(id); exists {
		t.Fatal("rejected publish left its acknowledgement route behind")
	}
}

func TestDeepAuditRejectedOnlineExecuteCleansAckRoute(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	client.SetFailedWriteHandler(FailedWriteHandlerFunc(func(*Message, string) {}))
	client.SetPublishStore(NewMemoryPublishStore())
	var priorID, priorErr = client.ExecuteAsync(NewCommand("publish").SetTopic("orders").SetAckType(AckTypeProcessed), func(*Message) error { return nil })
	if priorErr != nil {
		t.Fatal(priorErr)
	}
	var failure = errors.New("publish rejected")
	client.SetPublishStore(&deepAuditRejectingPublishStore{MemoryPublishStore: NewMemoryPublishStore(), err: failure})
	var id, err = client.ExecuteAsync(NewCommand("publish").SetTopic("orders").SetAckType(AckTypeProcessed), func(*Message) error { return nil })
	if !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}
	if _, exists := client.routes.Load(id); exists {
		t.Fatal("rejected online publish left its acknowledgement route behind")
	}
	if _, exists := client.routes.Load(priorID); !exists {
		t.Fatal("rejecting another publish removed an existing retained route")
	}
	var state = ensureClientState(client)
	state.lock.Lock()
	var _, rejectedRetained = state.pendingPublishByCmdID[id]
	var _, priorRetained = state.pendingPublishByCmdID[priorID]
	state.lock.Unlock()
	if rejectedRetained || !priorRetained {
		t.Fatalf("failed-write tracking rejected=%v prior=%v", rejectedRetained, priorRetained)
	}
}

func TestDeepAuditRetainedAckHandlerSurvivesDisconnectAndCleansUp(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	client.connected.Store(false)
	client.SetPublishStore(NewMemoryPublishStore())
	var calls []int
	var id, err = client.ExecuteAsync(NewCommand("publish").SetTopic("orders").SetAckType(AckTypeProcessed|AckTypePersisted), func(message *Message) error {
		var ack, _ = message.AckType()
		calls = append(calls, ack)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = client.clearRoutes(); err != nil {
		t.Fatal(err)
	}
	client.connected.Store(true)
	if err = client.postLogonRecovery(); err != nil {
		t.Fatal(err)
	}
	for _, ackType := range []int{AckTypeReceived, AckTypeProcessed, AckTypeProcessed, AckTypePersisted, AckTypePersisted} {
		var ack = commandToMessage(NewCommand("ack").SetCommandID(id).SetAckType(ackType))
		ack.header.status = []byte("success")
		if err = client.onMessage(ack); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(calls, []int{AckTypeProcessed, AckTypePersisted}) {
		t.Fatalf("callback acknowledgement types = %v", calls)
	}
	if _, exists := client.routes.Load(id); exists {
		t.Fatal("completed publish route was retained")
	}
}

func TestDeepAuditJSONBuilderProducesValidJSONForControls(t *testing.T) {
	for code := 0; code < 32; code++ {
		var value = string(rune(code))
		var builder = NewJSONBuilder()
		builder.Append("value", value)
		var decoded map[string]string
		var data = builder.Bytes()
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Errorf("control U+%04X produces invalid JSON %s: %v", code, data, err)
			continue
		}
		if decoded["value"] != value {
			t.Errorf("control U+%04X corrupted in JSON round trip", code)
		}
	}
}

func TestDeepAuditJSONBuilderRoundTripsKeysAndUnicode(t *testing.T) {
	var values = []string{"\x00", "\x1f", "\\\"", "café", "🙂", "\u2028", "\n\r\t\b\f"}
	for _, value := range values {
		var builder JSONMessageBuilder
		builder.Append(value, value)
		var decoded map[string]string
		if err := json.Unmarshal(builder.Bytes(), &decoded); err != nil || len(decoded) != 1 || decoded[value] != value {
			t.Fatalf("JSON round trip for %q: %v, %v", value, decoded, err)
		}
	}
}

func BenchmarkJSONBuilderEscaping(b *testing.B) {
	var builder JSONMessageBuilder
	builder.Append("topic", "orders")
	builder.Append("note\"", "line\n\x00世界")
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		_ = builder.Bytes()
	}
}

func TestDeepAuditLogonMetadataConcurrentReaders(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	var stop, started, exited = make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(exited)
		close(started)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = client.ServerVersion()
			_ = client.GetNameHash()
			_ = client.GetNameHashValue()
		}
	}()
	<-started
	defer func() { close(stop); <-exited }()
	for index := 0; index < 3; index++ {
		var ack = commandToMessage(NewCommand("ack").SetAckType(AckTypeProcessed).SetBookmark("123|42|"))
		ack.header.status = []byte("success")
		ack.header.version = []byte("5.3.5.0")
		if err := auditLogon(t, client, LogonParams{}, ack); err != nil {
			t.Fatal(err)
		}
	}
	if client.ServerVersion() != "5.3.5.0" || client.GetNameHash() != "123" || client.GetNameHashValue() != 123 {
		t.Fatal("logon failed to publish the expected final metadata")
	}
}
