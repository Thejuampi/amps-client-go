package amps

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type failingAuditAuthenticator struct {
	calls atomic.Int32
	err   error
}

func (auth *failingAuditAuthenticator) Authenticate(string, string) (string, error) {
	auth.calls.Add(1)
	return "", auth.err
}

func (*failingAuditAuthenticator) Retry(string, string) (string, error) { return "", nil }
func (*failingAuditAuthenticator) Completed(string, string, string)     {}

func TestLogonAuthenticatorFailureReleasesClientLock(t *testing.T) {
	client := NewClient("auth-lock")
	client.connected.Store(true)
	client.url = &url.URL{User: url.User("user")}
	authErr := errors.New("denied")

	err := client.Logon(LogonParams{Authenticator: &failingAuditAuthenticator{err: authErr}})
	if !errors.Is(err, authErr) {
		t.Fatalf("Logon() error = %v, want wrapped authenticator error", err)
	}

	locked := make(chan struct{})
	go func() {
		client.lock.Lock()
		_ = client.connected.Load()
		client.lock.Unlock()
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("client lock remained held after authenticator failure")
	}
}

func TestLogonBeforeConnectReturnsDisconnectedError(t *testing.T) {
	client := NewClient("logon-before-connect")

	err := client.Logon()
	if !IsErrorKind(err, DisconnectedError) {
		t.Fatalf("Logon() error = %v, want DisconnectedError", err)
	}
}

func TestLogonWithMissingConnectionReturnsDisconnectedError(t *testing.T) {
	client := NewClient("logon-missing-connection")
	client.connected.Store(true)
	client.url, _ = url.Parse("tcp://user:pass@localhost:9007/amps/json")

	err := client.Logon()
	if !IsErrorKind(err, DisconnectedError) {
		t.Fatalf("Logon() error = %v, want DisconnectedError", err)
	}
}

func TestLogonAuthenticatorRunsWithoutURIUserInfo(t *testing.T) {
	client := NewClient("auth-empty-user")
	client.connected.Store(true)
	client.url = &url.URL{}
	auth := &failingAuditAuthenticator{err: errors.New("called")}

	_ = client.Logon(LogonParams{Authenticator: auth})
	if calls := auth.calls.Load(); calls != 1 {
		t.Fatalf("Authenticate() calls = %d, want 1", calls)
	}
}

func TestMessageStreamTimeoutDoesNotCompleteLiveStream(t *testing.T) {
	stream := newMessageStream(nil)
	stream.setState(messageStreamStateReading)
	stream.SetTimeout(1)

	if !stream.HasNext() {
		t.Fatal("HasNext() = false for a live stream timeout")
	}
	if !stream.timedOut.Load() {
		t.Fatal("HasNext() timeout did not set timedOut")
	}
	if state := atomic.LoadInt32(&stream.state); state == messageStreamStateComplete {
		t.Fatal("timeout completed a live stream")
	}

	message := &Message{header: newHeader()}
	stream.queue.enqueue(message)
	if stream.Next() != nil {
		t.Fatal("first Next() after timeout must consume the timeout marker")
	}
	if got := stream.Next(); got != message {
		t.Fatalf("second Next() = %p, want queued message %p", got, message)
	}
}

func TestMessageStreamHasNextPreservesPendingTimeoutMarker(t *testing.T) {
	stream := newMessageStream(nil)
	stream.setState(messageStreamStateReading)
	stream.SetTimeout(1)

	if !stream.HasNext() {
		t.Fatal("HasNext() = false for a live stream timeout")
	}
	message := &Message{header: newHeader()}
	stream.queue.enqueue(message)
	if !stream.HasNext() {
		t.Fatal("HasNext() = false with pending timeout marker")
	}
	if stream.Next() != nil {
		t.Fatal("Next() after repeated HasNext must consume the timeout marker")
	}
	if got := stream.Next(); got != message {
		t.Fatalf("Next() after timeout marker = %p, want queued message %p", got, message)
	}
}

func TestConnectTLSUsesCurrentHostWithoutMutatingBaseConfig(t *testing.T) {
	originalDial := clientTLSDialContext
	defer func() { clientTLSDialContext = originalDial }()

	var serverNames []string
	clientTLSDialContext = func(_ context.Context, _, _ string, config *tls.Config) (net.Conn, error) {
		serverNames = append(serverNames, config.ServerName)
		return newTestConn(), nil
	}

	client := NewClient("tls-host")
	client.SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12})
	if err := client.Connect("tcps://first.example:9007/amps/json"); err != nil {
		t.Fatalf("first Connect() error = %v", err)
	}
	_ = client.Disconnect()
	if err := client.Connect("tcps://second.example:9007/amps/json"); err != nil {
		t.Fatalf("second Connect() error = %v", err)
	}

	if got := strings.Join(serverNames, ","); got != "first.example,second.example" {
		t.Fatalf("TLS ServerName sequence = %q", got)
	}
	if client.tlsConfig.ServerName != "" {
		t.Fatalf("base TLS config ServerName mutated to %q", client.tlsConfig.ServerName)
	}
}

func TestStaleReadErrorDoesNotDisconnectReplacementConnection(t *testing.T) {
	client := NewClient("stale-read-error")
	oldConnection := newTestConn()
	replacement := newTestConn()
	client.connection = replacement
	client.connected.Store(true)
	client.stopped.Store(false)

	client.onConnectionErrorForConnection(NewError(ConnectionError, "stale read"), oldConnection)

	client.connectionStateLock.Lock()
	current := client.connection
	client.connectionStateLock.Unlock()
	if !client.connected.Load() || current != replacement {
		t.Fatalf("stale read replaced connection state: connected=%v current=%p want=%p", client.connected.Load(), current, replacement)
	}
}

func TestAMPSErrorSupportsKindAndCauseInspection(t *testing.T) {
	cause := context.DeadlineExceeded
	err := NewError(ConnectionError, cause)

	if !IsErrorKind(err, ConnectionError) {
		t.Fatalf("IsErrorKind(%v, ConnectionError) = false", err)
	}
	if !errors.Is(err, &AMPSError{Kind: ConnectionError}) {
		t.Fatalf("errors.Is(%v, ConnectionError target) = false", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("errors.Is(%v, DeadlineExceeded) = false", err)
	}
}

func TestNilAMPSErrorMethods(t *testing.T) {
	var err *AMPSError
	if err.Error() != "<nil>" {
		t.Fatalf("nil AMPSError text = %q", err.Error())
	}
	if err.Unwrap() != nil {
		t.Fatal("nil AMPSError returned a cause")
	}
}

func TestMessageStreamDetachedClientHelpers(t *testing.T) {
	var stream *MessageStream
	if stream.getClient() != nil {
		t.Fatal("nil stream returned a client")
	}
	stream.detachClient()

	stream = newMessageStream(nil)
	stream.SetAcksOnly("detached")
	if stream.FromExistingHandler(func(*Message) error { return nil }) != stream {
		t.Fatal("FromExistingHandler did not preserve detached stream")
	}
	stream.detachClient()
}

func TestSyncCommandAllowsReadPathAutoAckBeforeProcessedAck(t *testing.T) {
	client := NewClient("sync-auto-ack")
	client.connected.Store(true)
	client.connection = newTestConn()
	client.SetAutoAck(true).SetAckBatchSize(1).SetAckTimeout(time.Second)

	command := NewCommand("flush")
	executeDone := make(chan error, 1)
	go func() {
		_, err := client.ExecuteAsync(command, nil)
		executeDone <- err
	}()

	commandID, _ := waitForAnyRouteHandler(t, client)

	readPathDone := make(chan error, 1)
	go func() {
		if err := client.onMessage(makeAutoAckMessage("10|1|")); err != nil {
			readPathDone <- err
			return
		}
		ack := &Message{header: &_Header{
			command:   CommandAck,
			commandID: []byte(commandID),
			ackType:   intPointer(AckTypeProcessed),
			status:    []byte("success"),
		}}
		readPathDone <- client.onMessage(ack)
	}()

	select {
	case err := <-readPathDone:
		if err != nil {
			t.Fatalf("read path error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("read path deadlocked while sending auto-ack")
	}
	select {
	case err := <-executeDone:
		if err != nil {
			t.Fatalf("ExecuteAsync() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ExecuteAsync did not receive processed ack")
	}
}

func TestLogonAllowsReadPathAutoAckBeforeProcessedAck(t *testing.T) {
	client := NewClient("logon-auto-ack")
	client.connected.Store(true)
	client.resetDisconnectSignal()
	client.connection = newTestConn()
	client.url, _ = url.Parse("tcp://user:pass@localhost:9007/amps/json")
	client.SetAutoAck(true).SetAckBatchSize(1).SetAckTimeout(time.Second)

	logonDone := make(chan error, 1)
	go func() {
		logonDone <- client.Logon()
	}()

	commandID, _ := waitForAnyRouteHandler(t, client)

	readPathDone := make(chan error, 1)
	go func() {
		if err := client.onMessage(makeAutoAckMessage("11|1|")); err != nil {
			readPathDone <- err
			return
		}
		ack := &Message{header: &_Header{
			command:    CommandAck,
			commandID:  []byte(commandID),
			ackType:    intPointer(AckTypeProcessed),
			status:     []byte("success"),
			version:    []byte("5.3.5.1"),
			clientName: []byte("12345"),
		}}
		readPathDone <- client.onMessage(ack)
	}()

	select {
	case err := <-readPathDone:
		if err != nil {
			t.Fatalf("read path error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("read path deadlocked while sending auto-ack during logon")
	}
	select {
	case err := <-logonDone:
		if err != nil {
			t.Fatalf("Logon() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Logon did not receive processed ack")
	}
}

func intPointer(value int) *int { return &value }

func TestHTTPPreflightConsumesOnlyCompleteFragmentedHeaders(t *testing.T) {
	conn := newTestConn()
	conn.enqueueRead([]byte("HTTP/1."))
	conn.enqueueRead([]byte("1 200 OK\r\nX-Test: value\r\n\r\nAMPS"))

	if err := performHTTPPreflight(conn, "broker.example", []string{"X-Client: test"}); err != nil {
		t.Fatalf("performHTTPPreflight() error = %v", err)
	}
	var remaining = make([]byte, 4)
	count, err := conn.Read(remaining)
	if err != nil || count != 4 || string(remaining) != "AMPS" {
		t.Fatalf("remaining read = (%q, %d, %v), want AMPS", remaining[:count], count, err)
	}
}

func TestHTTPPreflightRejectsHeaderInjectionBeforeWrite(t *testing.T) {
	conn := newTestConn()

	err := performHTTPPreflight(conn, "broker.example", []string{"X-Test: safe\r\nInjected: true"})
	if !IsErrorKind(err, ProtocolError) {
		t.Fatalf("performHTTPPreflight() error = %v, want ProtocolError", err)
	}
	if written := conn.WrittenBytes(); len(written) != 0 {
		t.Fatalf("preflight wrote %d bytes before rejecting injected header", len(written))
	}
}

func TestSOWBatchRecordFieldsDoNotBleedIntoNextRecord(t *testing.T) {
	client := NewClient("sow-record-reset")
	conn := newTestConn()
	client.connected.Store(true)
	client.connection = conn
	client.SetErrorHandler(func(error) {})

	var delivered []*Message
	client.routes.Store("sub-sow-reset", func(message *Message) error {
		delivered = append(delivered, message.Copy())
		return nil
	})
	frame := buildRawFrame(
		`{"c":"sow"}`,
		[]byte(`{"c":"p","sub_id":"sub-sow-reset","k":"first","bm":"1|1|","ts":"1","l":1}a{"c":"p","sub_id":"sub-sow-reset","l":1}b`),
	)
	conn.enqueueRead(frame)
	client.readRoutine()

	if len(delivered) != 2 {
		t.Fatalf("delivered records = %d, want 2", len(delivered))
	}
	secondKey, hasSecondKey := delivered[1].SowKey()
	secondBookmark, hasSecondBookmark := delivered[1].Bookmark()
	secondTimestamp, hasSecondTimestamp := delivered[1].Timestamp()
	if hasSecondKey || secondKey != "" || hasSecondBookmark || secondBookmark != "" || hasSecondTimestamp || secondTimestamp != "" {
		t.Fatalf("second record inherited fields key=(%q,%v) bookmark=(%q,%v) timestamp=(%q,%v)", secondKey, hasSecondKey, secondBookmark, hasSecondBookmark, secondTimestamp, hasSecondTimestamp)
	}
}

func TestReadRoutineReleasesOversizedReceiveBuffer(t *testing.T) {
	client := NewClient("receive-buffer-shrink")
	conn := newTestConn()
	client.connected.Store(true)
	client.connection = conn
	client.SetErrorHandler(func(error) {})

	command := NewCommand("publish").SetTopic("orders").SetData(make([]byte, initialReceiveBufferSize+1))
	conn.enqueueRead(buildFrameFromCommand(t, command))
	client.readRoutine()

	if size := len(client.receiveBuffer); size != initialReceiveBufferSize {
		t.Fatalf("receive buffer size = %d, want %d", size, initialReceiveBufferSize)
	}
}

func TestEmptyHeaderWriteRespectsExistingFramePrefix(t *testing.T) {
	var buffer = bytes.NewBufferString("    ")
	if err := newHeader().write(buffer); err != nil {
		t.Fatalf("write() error = %v", err)
	}
	if got := buffer.String(); got != "    {}" {
		t.Fatalf("empty framed header = %q, want %q", got, "    {}")
	}
}

func TestMemoryBookmarkStorePrunesSupersededDiscardedRecords(t *testing.T) {
	store := NewMemoryBookmarkStore()
	for sequence := 1; sequence <= memoryBookmarkPruneThreshold; sequence++ {
		message := &Message{header: &_Header{
			subID:    []byte("sub-prune"),
			bookmark: []byte("1|" + fmt.Sprint(sequence) + "|"),
		}}
		seqNo := store.Log(message)
		store.Discard("sub-prune", seqNo)
	}

	if count := len(store.records["sub-prune"]); count != 1 {
		t.Fatalf("retained discarded records = %d, want 1 latest publisher record", count)
	}
	var expected = fmt.Sprintf("1|%d|", memoryBookmarkPruneThreshold)
	if recent := store.GetMostRecent("sub-prune"); recent != expected {
		t.Fatalf("most recent bookmark = %q, want %q", recent, expected)
	}
}
