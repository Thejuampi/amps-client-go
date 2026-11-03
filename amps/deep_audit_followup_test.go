package amps

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func followupPublishSequences(t *testing.T, wire []byte) []uint64 {
	t.Helper()
	var sequences []uint64
	for len(wire) > 0 {
		if len(wire) < 4 {
			t.Fatal("missing frame length")
		}
		var size = int(binary.BigEndian.Uint32(wire[:4]))
		wire = wire[4:]
		if size > len(wire) {
			t.Fatal("incomplete frame")
		}
		var header struct {
			Sequence uint64 `json:"s"`
		}
		if err := json.Unmarshal(wire[:size], &header); err != nil {
			t.Fatal(err)
		}
		if header.Sequence != 0 {
			sequences = append(sequences, header.Sequence)
		}
		wire = wire[size:]
	}
	return sequences
}

func TestFollowupLoggedOnCallbackCannotOvertakeReplay(t *testing.T) {
	var client, connection = auditConnectedClient(t)
	var store = newMemoryPublishStore(1)
	for range 2 {
		if _, err := store.Store(NewCommand("publish").SetTopic("orders").SetAckType(AckTypePersisted)); err != nil {
			t.Fatal(err)
		}
	}
	client.SetPublishStore(store)
	var published = make(chan error, 1)
	client.AddConnectionStateListener(ConnectionStateListenerFunc(func(state ConnectionState) {
		if state == ConnectionStateLoggedOn {
			published <- client.Publish("orders", "")
		}
	}))
	var ack = commandToMessage(NewCommand("ack").SetAckType(AckTypeProcessed))
	ack.header.status = []byte("success")
	if err := auditLogon(t, client, LogonParams{Timeout: 500}, ack); err != nil {
		t.Fatal(err)
	}
	if err := <-published; err != nil {
		t.Fatal(err)
	}
	if got := followupPublishSequences(t, connection.WrittenBytes()); !reflect.DeepEqual(got, []uint64{1, 2, 3}) {
		t.Fatalf("logon callback overtook or duplicated backlog: got %v, want [1 2 3]", got)
	}
}

type followupBlockingSequenceFloor struct {
	*MemoryPublishStore
	entered chan struct{}
	resume  chan struct{}
}

func (store *followupBlockingSequenceFloor) SetInitialSequence(sequence uint64) {
	close(store.entered)
	<-store.resume
	store.MemoryPublishStore.SetInitialSequence(sequence)
}

func TestFollowupConcurrentPublishWaitsForServerSequenceFloor(t *testing.T) {
	var client, connection = auditConnectedClient(t)
	var store = &followupBlockingSequenceFloor{
		MemoryPublishStore: newMemoryPublishStore(1),
		entered:            make(chan struct{}),
		resume:             make(chan struct{}),
	}
	client.SetPublishStore(store)
	var ack = commandToMessage(NewCommand("ack").SetAckType(AckTypeProcessed).SetSequenceID(100))
	ack.header.status = []byte("success")
	var loggedOn = make(chan error, 1)
	var released bool
	defer func() {
		if !released {
			close(store.resume)
		}
	}()
	go func() { loggedOn <- auditLogon(t, client, LogonParams{Timeout: 500}, ack) }()
	select {
	case <-store.entered:
	case err := <-loggedOn:
		t.Fatalf("logon finished before installing server sequence floor: %v", err)
	case <-time.After(time.Second):
		t.Fatal("logon did not attempt to install server sequence floor")
	}
	var published = make(chan error, 1)
	go func() { published <- client.Publish("orders", "") }()
	var completed bool
	var publishErr error
	select {
	case publishErr = <-published:
		completed = true
	case <-time.After(30 * time.Millisecond):
	}
	close(store.resume)
	released = true
	if err := <-loggedOn; err != nil {
		t.Fatal(err)
	}
	if !completed {
		publishErr = <-published
	}
	if publishErr != nil {
		t.Fatal(publishErr)
	}
	if got := followupPublishSequences(t, connection.WrittenBytes()); !reflect.DeepEqual(got, []uint64{101}) {
		t.Fatalf("concurrent publication used sequence below server floor: got %v, want [101]", got)
	}
}

func TestFollowupLegacyPublishBacklogRequestsPersistedAck(t *testing.T) {
	for _, kind := range []string{"memory", "wal", "checkpoint"} {
		for _, commandType := range []string{"publish", "delta_publish", "sow_delete"} {
			for _, requested := range []int{AckTypeNone, AckTypeProcessed} {
				t.Run(fmt.Sprintf("%s/%s/acks-%d", kind, commandType, requested), func(t *testing.T) {
					var store PublishStore = newMemoryPublishStore(1)
					var path = filepath.Join(t.TempDir(), "legacy-publish")
					var options = FileStoreOptions{UseWAL: kind == "wal", SyncOnWrite: true, CheckpointInterval: 1}
					if kind == "wal" {
						options.CheckpointInterval = 1000
					}
					if kind != "memory" {
						store = NewFilePublishStoreWithOptions(path, options)
					}
					// Previous clients retained the snapshot before adding persisted.
					if _, err := store.Store(NewCommand(commandType).SetTopic("legacy").SetSequenceID(100).SetAckType(requested).SetData([]byte("legacy-payload"))); err != nil {
						t.Fatal(err)
					}
					if kind != "memory" {
						var reopened = NewFilePublishStoreWithOptions(path, options)
						if err := reopened.LoadError(); err != nil {
							t.Fatal(err)
						}
						store = reopened
					}
					var client, connection = auditConnectedClient(t)
					client.SetPublishStore(store)
					if _, err := client.replayPublishStore(); err != nil {
						t.Fatal(err)
					}
					var wire = connection.WrittenBytes()
					if len(wire) < 4 {
						t.Fatal("legacy backlog was not replayed")
					}
					var message Message
					var payload, parseErr = parseHeader(&message, true, wire[4:])
					if parseErr != nil {
						t.Fatal(parseErr)
					}
					if sequence, _ := message.SequenceID(); sequence != 100 || !bytes.Equal(payload, []byte("legacy-payload")) {
						t.Fatalf("legacy replay changed identity or data: sequence=%d payload=%q", sequence, payload)
					}
					if ack, _ := message.AckType(); ack != requested|AckTypePersisted {
						t.Fatalf("legacy replay acknowledgements=%d, want %d", ack, requested|AckTypePersisted)
					}
					var ack = commandToMessage(NewCommand("ack").SetAckType(AckTypePersisted).SetSequenceID(100))
					ack.header.status = []byte("success")
					if err := client.onMessage(ack); err != nil || store.UnpersistedCount() != 0 {
						t.Fatalf("replay-only acknowledgement did not drain legacy backlog: count=%d error=%v", store.UnpersistedCount(), err)
					}
				})
			}
		}
	}
}

func TestFollowupHTTPPreflightObservesConnectCancellation(t *testing.T) {
	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var accepted = make(chan net.Conn, 1)
	var serverDone = make(chan struct{})
	go func() {
		defer close(serverDone)
		var peer, acceptErr = listener.Accept()
		if acceptErr == nil {
			accepted <- peer
			_, _ = io.Copy(io.Discard, peer)
		}
	}()
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	var client = NewClient("followup-preflight")
	defer func() {
		cancel()
		_ = listener.Close()
		_ = client.Close()
	}()
	var result = make(chan error, 1)
	go func() {
		result <- client.connectWithContext(ctx, "tcp://"+listener.Addr().String()+"/amps/json?http_preflight=1")
	}()
	var peer net.Conn
	select {
	case peer = <-accepted:
	case <-time.After(time.Second):
		t.Fatal("connect did not reach the local listener")
	}
	defer func() { _ = peer.Close(); <-serverDone }()
	cancel()
	select {
	case err = <-result:
		if err == nil || client.connected.Load() {
			t.Fatalf("cancelled connect unexpectedly succeeded: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		_ = peer.Close()
		<-result
		t.Fatal("cancelled connect remained blocked in HTTP preflight")
	}
}

type followupSequenceErrorStore struct {
	*MemoryPublishStore
}

func (store followupSequenceErrorStore) Store(*Command) (uint64, error) {
	return 42, errors.New("custom store did not accept the publication")
}

func TestFollowupNonzeroSequenceDoesNotHideStoreFailure(t *testing.T) {
	var client, connection = auditConnectedClient(t)
	client.SetPublishStore(followupSequenceErrorStore{NewMemoryPublishStore()})
	if err := client.Publish("orders", ""); err == nil {
		t.Fatal("nonzero sequence was incorrectly treated as durable acceptance")
	}
	if len(connection.WrittenBytes()) != 0 {
		t.Fatal("rejected publication reached the connection")
	}
}

func TestFollowupCheckpointOnlyRejectionDoesNotRetainRetryDuplicate(t *testing.T) {
	var path = filepath.Join(t.TempDir(), "publish")
	var options = FileStoreOptions{UseWAL: false, SyncOnWrite: true, CheckpointInterval: 1}
	var store = NewFilePublishStoreWithOptions(path, options)
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	var client = NewClient("followup-checkpoint-only").SetPublishStore(store)
	defer client.Close()
	if err := client.Publish("orders", "one operation"); err == nil {
		t.Fatal("checkpoint-only store reported success without writing its only durable copy")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := client.Publish("orders", "one operation"); err != nil {
		t.Fatal(err)
	}
	var recovered = NewFilePublishStoreWithOptions(path, options)
	if err := recovered.LoadError(); err != nil {
		t.Fatal(err)
	}
	if count := recovered.UnpersistedCount(); count != 1 {
		t.Fatalf("rejected publication survived and duplicated its retry: got %d records, want 1", count)
	}
}

func TestFollowupDecodedHeaderCopyPreservesPayloadAndText(t *testing.T) {
	var values = []string{"plain", "quote\"slash\\", "line\nnext", "\x00\x1f", "café-日本-🙂", "before\u2028after"}
	for index, value := range values {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			var encoded, err = json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			var wire = []byte(fmt.Sprintf(`{"c":"p","t":%s,"sub_id":%s}`, encoded, encoded))
			var payload = []byte("{\"body\":\"still quoted\\\"\"}\x00")
			wire = append(wire, payload...)
			var message Message
			var remaining, parseErr = parseHeader(&message, true, wire)
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			if !bytes.Equal(remaining, payload) {
				t.Fatalf("header parsing changed payload: got %q, want %q", remaining, payload)
			}
			message.SetData(remaining)
			var copied = message.Copy()
			clear(wire)
			var topic, _ = copied.Topic()
			var subID, _ = copied.SubID()
			if topic != value || subID != value || !bytes.Equal(copied.Data(), payload) {
				t.Fatalf("copy did not retain decoded message: topic=%q sub_id=%q data=%q", topic, subID, copied.Data())
			}
		})
	}
}

func TestFollowupHeadersRejectInvalidStringEscapes(t *testing.T) {
	for _, header := range []string{`{"t":"\x00"}`, `{"c":"p","t":"bad\q","sub_id":"sub-1"}`, `{"c":"p","t":"bad\u00xz","sub_id":"sub-1","bm":"1|1|"}`} {
		var message Message
		if _, err := parseHeader(&message, true, []byte(header)); err == nil {
			t.Errorf("invalid JSON escape was accepted: %s", header)
		}
	}
}

func TestFollowupSOWDeleteCannotFallBehindReplaySnapshot(t *testing.T) {
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
	var deleted = make(chan error, 1)
	go func() {
		_, err := client.ExecuteAsync(NewCommand("sow_delete").SetTopic("orders").SetFilter("/id = 1"), nil)
		deleted <- err
	}()
	var completed bool
	var deleteErr error
	select {
	case deleteErr = <-deleted:
		completed = true
	case <-time.After(100 * time.Millisecond):
	}
	close(store.resume)
	if err := <-recovered; err != nil {
		t.Fatal(err)
	}
	if !completed {
		deleteErr = <-deleted
	}
	if deleteErr != nil {
		t.Fatal(deleteErr)
	}
	if err := client.Publish("orders", ""); err != nil {
		t.Fatal(err)
	}
	var sequences = followupPublishSequences(t, conn.WrittenBytes())
	if !reflect.DeepEqual(sequences, []uint64{1, 2, 3, 4}) {
		t.Fatalf("stored SOW delete was skipped by recovery: wire sequence=%v", sequences)
	}
}

func TestFollowupHATimeoutIncludesResubscriptionAck(t *testing.T) {
	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var serverDone = make(chan struct{})
	go func() {
		defer close(serverDone)
		var conn, acceptErr = listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		var prefix [4]byte
		if _, readErr := io.ReadFull(conn, prefix[:]); readErr != nil {
			return
		}
		var body = make([]byte, binary.BigEndian.Uint32(prefix[:]))
		if _, readErr := io.ReadFull(conn, body); readErr != nil {
			return
		}
		var header struct {
			CommandID string `json:"cid"`
		}
		if json.Unmarshal(body, &header) != nil {
			return
		}
		var ack = []byte(fmt.Sprintf(`{"c":"ack","a":"processed","cid":%q,"status":"success","version":"5.3.5"}`, header.CommandID))
		binary.BigEndian.PutUint32(prefix[:], uint32(len(ack)))
		if _, writeErr := conn.Write(append(prefix[:], ack...)); writeErr != nil {
			return
		}
		_, _ = io.Copy(io.Discard, conn)
	}()
	var ha = NewHAClient("followup-ha-recovery-timeout").SetTimeout(40 * time.Millisecond)
	ha.Client().SetHeartbeat(0)
	var manager = NewDefaultSubscriptionManager()
	manager.Subscribe(func(*Message) error { return nil }, NewCommand("subscribe").SetTopic("orders").SetSubID("retained-sub"), AckTypeNone)
	ha.Client().SetSubscriptionManager(manager)
	ha.SetServerChooser(NewDefaultServerChooser().Add("tcp://" + listener.Addr().String() + "/amps/json"))
	defer func() {
		_ = ha.Disconnect()
		_ = listener.Close()
		<-serverDone
	}()
	var result = make(chan error, 1)
	go func() { result <- ha.ConnectAndLogon() }()
	select {
	case err = <-result:
		if !IsErrorKind(err, TimedOutError) {
			t.Fatalf("recovery timeout error = %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		_ = ha.Disconnect()
		<-result
		t.Fatal("40ms HA timeout did not bound recovery waiting for subscription ack after 300ms")
	}
}

func TestFollowupHATerminalConnectFailureRejectsLateDisconnect(t *testing.T) {
	var ha = NewHAClient("followup-ha-terminal-failure")
	var reconnecting = make(chan struct{}, 1)
	ha.Client().AddConnectionStateListener(ConnectionStateListenerFunc(func(state ConnectionState) {
		if state == ConnectionStateReconnecting {
			select {
			case reconnecting <- struct{}{}:
			default:
			}
		}
	}))
	t.Cleanup(func() {
		_ = ha.Disconnect()
		var deadline = time.Now().Add(time.Second)
		for ha.reconnecting.Load() && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if ha.reconnecting.Load() {
			t.Error("reconnect worker did not stop during cleanup")
		}
	})
	if err := ha.ConnectAndLogon(); !IsErrorKind(err, ConnectionError) {
		t.Fatalf("empty chooser result = %v, want ConnectionError", err)
	}
	ha.handleDisconnect(NewError(ConnectionError, "late disconnect from failed attempt"))
	select {
	case <-reconnecting:
		t.Fatal("terminal ConnectAndLogon failure allowed a late callback to restart recovery")
	default:
	}
}
