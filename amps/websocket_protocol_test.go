package amps

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWebSocketProtocolConnectAndLogon(t *testing.T) {
	var results = make(chan error, 1)
	var upgrader = websocket.Upgrader{}
	var server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var connection, err = upgrader.Upgrade(writer, request, nil)
		if err != nil {
			results <- err
			return
		}
		defer connection.Close()
		_ = connection.SetReadDeadline(time.Now().Add(2 * time.Second))
		var kind int
		var payload []byte
		kind, payload, err = connection.ReadMessage()
		if err != nil || kind != websocket.TextMessage || string(payload) != "text" {
			results <- fmt.Errorf("preface type=%d payload=%q error=%v", kind, payload, err)
			return
		}
		kind, payload, err = connection.ReadMessage()
		if err != nil || kind != websocket.BinaryMessage || len(payload) != 4 || binary.LittleEndian.Uint32(payload) != 1 {
			results <- fmt.Errorf("part count type=%d payload=%x error=%v", kind, payload, err)
			return
		}
		kind, payload, err = connection.ReadMessage()
		var header struct {
			Command string `json:"c"`
			ID      string `json:"cid"`
		}
		if err != nil || kind != websocket.TextMessage {
			results <- fmt.Errorf("logon header type=%d payload=%q error=%v", kind, payload, err)
			return
		}
		if err = json.Unmarshal(payload, &header); err != nil || header.Command != "logon" || header.ID == "" {
			results <- fmt.Errorf("logon header=%q parsed=%+v error=%v", payload, header, err)
			return
		}
		var response, _ = json.Marshal(map[string]string{
			"c": "ack", "a": "processed", "status": "success", "cid": header.ID, "version": "5.3.5.60",
		})
		if err = connection.WriteMessage(websocket.TextMessage, response); err != nil {
			results <- err
			return
		}
		results <- nil
		_, _, _ = connection.ReadMessage()
	}))
	defer server.Close()

	var client = NewClient("strict-websocket-logon")
	defer client.Close()
	var uri = websocketTestURL(server.URL, "ws")
	uri.Path = "/amps/json"
	if err := client.Connect(uri.String()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	var logonErr = client.Logon(LogonParams{Timeout: 1000})
	var serverErr = <-results
	if serverErr != nil {
		t.Fatalf("strict server: %v (logon: %v)", serverErr, logonErr)
	}
	if logonErr != nil {
		t.Fatalf("Logon: %v", logonErr)
	}
	if got := client.ServerVersion(); got != "5.3.5.60" {
		t.Fatalf("server version = %q", got)
	}
}

// websocketProtocolPair uses only Gorilla and protocol literals on the peer.
func websocketProtocolPair(t *testing.T, messageType string) (netConn *websocketNetConn, peer *websocket.Conn) {
	t.Helper()
	var peers = make(chan *websocket.Conn, 1)
	var done = make(chan struct{})
	var upgrader = websocket.Upgrader{}
	var server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var connection, err = upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		peers <- connection
		<-done
	}))
	t.Cleanup(func() { close(done); server.Close() })
	var uri = websocketTestURL(server.URL, "ws")
	uri.Path = "/amps/" + messageType
	var client = NewClient("protocol-pair")
	var connection, err = client.dialWebSocket(context.Background(), uri)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	peer = <-peers
	t.Cleanup(func() { _ = peer.Close() })
	_ = peer.SetReadDeadline(time.Now().Add(3 * time.Second))
	_ = connection.SetReadDeadline(time.Now().Add(3 * time.Second))
	var kind, preface, readErr = peer.ReadMessage()
	var expected = "binary"
	if messageType == "json" {
		expected = "text"
	}
	if readErr != nil || kind != websocket.TextMessage || string(preface) != expected {
		t.Fatalf("preface = type %d, %q, %v; want text %q", kind, preface, readErr, expected)
	}
	return connection.(*websocketNetConn), peer
}

func websocketTCPFrame(header string, body []byte) []byte {
	var frame = make([]byte, 4, 4+len(header)+len(body))
	binary.BigEndian.PutUint32(frame, uint32(len(header)+len(body)))
	frame = append(frame, header...)
	return append(frame, body...)
}

func websocketReadTCPFrame(t *testing.T, connection *websocketNetConn) []byte {
	t.Helper()
	var prefix [4]byte
	if _, err := io.ReadFull(connection, prefix[:]); err != nil {
		t.Fatalf("read prefix: %v", err)
	}
	var size = binary.BigEndian.Uint32(prefix[:])
	if size > 1024*1024 {
		t.Fatalf("unexpected test frame length: %d", size)
	}
	var frame = make([]byte, size)
	if _, err := io.ReadFull(connection, frame); err != nil {
		t.Fatalf("read frame: %v", err)
	}
	return frame
}

func websocketExpectMessage(t *testing.T, peer *websocket.Conn, expectedKind int, expected []byte) {
	t.Helper()
	var kind, payload, err = peer.ReadMessage()
	if err != nil || kind != expectedKind || !bytes.Equal(payload, expected) {
		t.Fatalf("message = type %d, %q, %v; want type %d, %q", kind, payload, err, expectedKind, expected)
	}
}

func TestWebSocketProtocolWriteBatchAndPartialFrames(t *testing.T) {
	var connection, peer = websocketProtocolPair(t, "binary")
	var firstHeader = `{"c":"p","t":"quoted\\\"topic"}`
	var firstPayload = []byte{0xff, 0, '{', '}', '\\', 0x80}
	var first = websocketTCPFrame(firstHeader, firstPayload)
	var secondHeader = `{"c":"flush"}`
	var second = websocketTCPFrame(secondHeader, nil)
	var combined = append(first, second...)
	for _, part := range [][]byte{combined[:2], combined[2:7], combined[7:]} {
		if n, err := connection.Write(part); err != nil || n != len(part) {
			t.Fatalf("Write(%d) = %d, %v", len(part), n, err)
		}
	}
	websocketExpectMessage(t, peer, websocket.BinaryMessage, []byte{2, 0, 0, 0})
	websocketExpectMessage(t, peer, websocket.TextMessage, []byte(firstHeader))
	websocketExpectMessage(t, peer, websocket.BinaryMessage, firstPayload)
	websocketExpectMessage(t, peer, websocket.BinaryMessage, []byte{1, 0, 0, 0})
	websocketExpectMessage(t, peer, websocket.TextMessage, []byte(secondHeader))
}

func TestWebSocketProtocolWriteTextPayloadAndErrorCount(t *testing.T) {
	var connection, peer = websocketProtocolPair(t, "json")
	var header = `{"c":"p","t":"orders"}`
	var payload = []byte(`{"value":"é"}`)
	var good = websocketTCPFrame(header, payload)
	var malformed = websocketTCPFrame(`{"c":`, nil)
	var input = append(append([]byte(nil), good...), malformed...)
	var n, err = connection.Write(input)
	if !IsErrorKind(err, ProtocolError) || n != len(good) {
		t.Fatalf("Write valid + malformed = %d, %v; want %d and ProtocolError", n, err, len(good))
	}
	websocketExpectMessage(t, peer, websocket.BinaryMessage, []byte{2, 0, 0, 0})
	websocketExpectMessage(t, peer, websocket.TextMessage, []byte(header))
	websocketExpectMessage(t, peer, websocket.TextMessage, payload)
}

func TestWebSocketProtocolReadPublishEmptyAndFollowingAck(t *testing.T) {
	var connection, peer = websocketProtocolPair(t, "json")
	var raw = []byte{0xff, 0, '{', '}', '\\', 0x80}
	var publish = `{"c":"p","sids":"a","l":6}`
	var empty = `{"c":"publish","sids":"a","l":0}`
	var ack = `{"c":"ack","cid":"after"}`
	for _, part := range []struct {
		kind int
		data []byte
	}{
		{websocket.TextMessage, []byte(publish)}, {websocket.BinaryMessage, raw},
		{websocket.TextMessage, []byte(empty)}, {websocket.TextMessage, []byte(ack)},
	} {
		if err := peer.WriteMessage(part.kind, part.data); err != nil {
			t.Fatal(err)
		}
	}
	if got := websocketReadTCPFrame(t, connection); !bytes.Equal(got, append([]byte(publish), raw...)) {
		t.Fatalf("publish frame = %q", got)
	}
	if got := websocketReadTCPFrame(t, connection); string(got) != empty {
		t.Fatalf("empty publish = %q", got)
	}
	if got := websocketReadTCPFrame(t, connection); string(got) != ack {
		t.Fatalf("next ack = %q", got)
	}
	if n, err := connection.Read(nil); n != 0 || err != nil {
		t.Fatalf("Read(nil) = %d, %v", n, err)
	}
}

func TestWebSocketProtocolReadOofWithoutLengthConsumesBody(t *testing.T) {
	var connection, peer = websocketProtocolPair(t, "json")
	var header = `{"c":"oof","sids":"a"}`
	var body = `{"c":"ack","cid":"looks-like-a-header"}`
	var next = `{"c":"ack","cid":"actual"}`
	for _, part := range []string{header, body, next} {
		if err := peer.WriteMessage(websocket.TextMessage, []byte(part)); err != nil {
			t.Fatal(err)
		}
	}
	if got := websocketReadTCPFrame(t, connection); string(got) != header+body {
		t.Fatalf("OOF frame = %q", got)
	}
	if got := websocketReadTCPFrame(t, connection); string(got) != next {
		t.Fatalf("following ack = %q", got)
	}
}

func TestWebSocketProtocolRejectsTruncatedSOWAndStaysFailed(t *testing.T) {
	var connection, peer = websocketProtocolPair(t, "json")
	if err := peer.WriteMessage(websocket.TextMessage, []byte(`{"c":"sow","bs":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := peer.WriteMessage(websocket.TextMessage, []byte(`{"k":"unfinished"}`)); err != nil {
		t.Fatal(err)
	}
	_ = peer.Close()
	var buffer [16]byte
	var n, firstErr = connection.Read(buffer[:])
	if n != 0 || firstErr == nil || IsErrorKind(firstErr, ProtocolError) {
		t.Fatalf("truncated SOW = %d, %v", n, firstErr)
	}
	if n, err := connection.Read(buffer[:]); n != 0 || err == nil || err.Error() != firstErr.Error() {
		t.Fatalf("Read after truncated SOW = %d, %v; want same error %v", n, err, firstErr)
	}
}

func TestWebSocketProtocolRejectsMalformedHeaderAndStaysFailed(t *testing.T) {
	var connection, peer = websocketProtocolPair(t, "json")
	for _, header := range []string{`{"c":`, `{"c":"ack","cid":"must-not-be-delivered"}`} {
		if err := peer.WriteMessage(websocket.TextMessage, []byte(header)); err != nil {
			t.Fatal(err)
		}
	}
	var buffer [32]byte
	var n, firstErr = connection.Read(buffer[:])
	if n != 0 || !IsErrorKind(firstErr, ProtocolError) {
		t.Fatalf("malformed header = %d, %v", n, firstErr)
	}
	if n, err := connection.Read(buffer[:]); n != 0 || err == nil || err.Error() != firstErr.Error() {
		t.Fatalf("Read after malformed header = %d, %v; want same error %v", n, err, firstErr)
	}
}

func TestWebSocketProtocolPreservesNetworkReadTimeout(t *testing.T) {
	var connection, _ = websocketProtocolPair(t, "json")
	if err := connection.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	var buffer [1]byte
	var n, err = connection.Read(buffer[:])
	var networkErr net.Error
	if n != 0 || !errors.As(err, &networkErr) || !networkErr.Timeout() || IsErrorKind(err, ProtocolError) {
		t.Fatalf("read timeout = %d, %v; want original network timeout", n, err)
	}
}

func TestWebSocketProtocolSOWEmptyRecordAndLengthMismatch(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		var connection, peer = websocketProtocolPair(t, "json")
		for _, part := range []string{`{"c":"sow","bs":1}`, `{ }`, ""} {
			if err := peer.WriteMessage(websocket.TextMessage, []byte(part)); err != nil {
				t.Fatal(err)
			}
		}
		var frame = websocketReadTCPFrame(t, connection)
		var message = &Message{}
		var remaining, err = parseHeader(message, true, frame)
		if err != nil {
			t.Fatal(err)
		}
		remaining, err = parseHeader(message, false, remaining)
		if err != nil || message.header.messageLength == nil || *message.header.messageLength != 0 || len(remaining) != 0 {
			t.Fatalf("empty SOW record: length=%v remaining=%q error=%v", message.header.messageLength, remaining, err)
		}
	})
	t.Run("mismatched length", func(t *testing.T) {
		var connection, peer = websocketProtocolPair(t, "json")
		for _, part := range []string{`{"c":"sow","bs":1}`, `{"l":7}`, "short"} {
			if err := peer.WriteMessage(websocket.TextMessage, []byte(part)); err != nil {
				t.Fatal(err)
			}
		}
		var buffer [16]byte
		if n, err := connection.Read(buffer[:]); n != 0 || !IsErrorKind(err, ProtocolError) {
			t.Fatalf("mismatched SOW length = %d, %v", n, err)
		}
	})
}

func TestWebSocketProtocolReadFragmentedHeaderWithPing(t *testing.T) {
	var connection, peer = websocketProtocolPair(t, "json")
	var header = `{"c":"ack","reason":"` + strings.Repeat("x", 8192) + `"}`
	var writer, err = peer.NextWriter(websocket.TextMessage)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Write([]byte(header[:4096])); err != nil {
		t.Fatal(err)
	}
	if err = peer.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Write([]byte(header[4096:])); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if got := websocketReadTCPFrame(t, connection); string(got) != header {
		t.Fatalf("fragmented header mismatch: got %d bytes", len(got))
	}
}

func TestWebSocketProtocolCancelLogonWaitingForAck(t *testing.T) {
	var requestReceived = make(chan struct{})
	var done = make(chan struct{})
	var upgrader = websocket.Upgrader{}
	var server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var connection, err = upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		for index := 0; index < 3; index++ {
			if _, _, err = connection.ReadMessage(); err != nil {
				return
			}
		}
		close(requestReceived)
		<-done
	}))
	t.Cleanup(func() { close(done); server.Close() })
	var client = NewClient("websocket-cancel")
	t.Cleanup(func() { _ = client.Close() })
	var uri = websocketTestURL(server.URL, "ws")
	uri.Path = "/amps/json"
	if err := client.Connect(uri.String()); err != nil {
		t.Fatal(err)
	}
	var ctx, cancel = context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var result = make(chan error, 1)
	go func() { result <- client.logonWithContext(ctx) }()
	select {
	case <-requestReceived:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not receive logon")
	}
	cancel()
	select {
	case err := <-result:
		if !IsErrorKind(err, DisconnectedError) {
			t.Fatalf("cancelled logon = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled logon did not return")
	}
}

func TestWebSocketProtocolReadSOWBatch(t *testing.T) {
	var connection, peer = websocketProtocolPair(t, "json")
	var parts = []struct {
		kind int
		data string
	}{
		{websocket.TextMessage, `{"c":"sow","query_id":"q","bs":2}`},
		{websocket.TextMessage, `{"k":"a\\\"b","x":"first"}`},
		{websocket.BinaryMessage, `one`},
		{websocket.TextMessage, `{"k":"last","l":4}`},
		{websocket.TextMessage, `two!`},
	}
	for _, part := range parts {
		if err := peer.WriteMessage(part.kind, []byte(part.data)); err != nil {
			t.Fatal(err)
		}
	}
	for index, want := range []struct{ key, body string }{{`a\"b`, "one"}, {"last", "two!"}} {
		var frame = websocketReadTCPFrame(t, connection)
		var message = &Message{}
		var remaining, err = parseHeader(message, true, frame)
		if err != nil || message.header.command != CommandSOW {
			t.Fatalf("outer SOW header: %v", err)
		}
		remaining, err = parseHeader(message, false, remaining)
		if err != nil || message.header.messageLength == nil {
			t.Fatalf("record %d header: %v", index, err)
		}
		var size = int(*message.header.messageLength)
		if size > len(remaining) || string(message.header.sowKey) != want.key || string(remaining[:size]) != want.body {
			t.Fatalf("record %d key=%q body=%q size=%d", index, message.header.sowKey, remaining, size)
		}
		remaining = remaining[size:]
		if len(remaining) != 0 {
			t.Fatalf("trailing SOW bytes = %q", remaining)
		}
	}
}

func websocketStartSOWDispatch(t *testing.T, connection *websocketNetConn) (<-chan string, <-chan error, <-chan struct{}) {
	t.Helper()
	var client = NewClient("websocket-sow-dispatch")
	var received = make(chan string, 4)
	var failures = make(chan error, 1)
	var done = make(chan struct{})
	client.SetErrorHandler(func(err error) { failures <- err })
	client.routes.Store("q", func(message *Message) error {
		received <- string(message.Data())
		return nil
	})
	client.connectionStateLock.Lock()
	client.connection = connection
	client.connected.Store(true)
	client.stopped.Store(false)
	client.connectionStateLock.Unlock()
	client.resetDisconnectSignal()
	go func() { defer close(done); client.readRoutineForConnection(connection) }()
	t.Cleanup(func() {
		_ = client.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("SOW read routine did not stop")
		}
	})
	return received, failures, done
}

func TestWebSocketProtocolSOWDeliversBeforeLaterRecord(t *testing.T) {
	var connection, peer = websocketProtocolPair(t, "json")
	var received, _, _ = websocketStartSOWDispatch(t, connection)
	for _, part := range []string{`{"c":"sow","query_id":"q","bs":2}`, `{"k":"first"}`, `one`} {
		if err := peer.WriteMessage(websocket.TextMessage, []byte(part)); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case got := <-received:
		if got != "one" {
			t.Fatalf("first callback = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("first SOW record was not delivered before the second record")
	}
	for _, part := range []string{`{"k":"second"}`, `two`} {
		if err := peer.WriteMessage(websocket.TextMessage, []byte(part)); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case got := <-received:
		if got != "two" {
			t.Fatalf("second callback = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("second SOW record was not delivered")
	}
}

func TestWebSocketProtocolSOWTruncationPreservesCompletedCallback(t *testing.T) {
	var connection, peer = websocketProtocolPair(t, "json")
	var received, failures, done = websocketStartSOWDispatch(t, connection)
	for _, part := range []string{`{"c":"sow","query_id":"q","bs":2}`, `{"k":"first"}`, `one`, `{"k":"second"}`} {
		if err := peer.WriteMessage(websocket.TextMessage, []byte(part)); err != nil {
			t.Fatal(err)
		}
	}
	_ = peer.Close()
	select {
	case err := <-failures:
		if err == nil {
			t.Fatal("expected connection error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("missing error for truncated second record")
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("receive loop did not stop")
	}
	select {
	case got := <-received:
		if got != "one" {
			t.Fatalf("callback = %q", got)
		}
	default:
		t.Fatal("completed first record was lost")
	}
	select {
	case got := <-received:
		t.Fatalf("unexpected extra callback = %q", got)
	default:
	}
}

// The HTTP handshake uses the real socket. Once armed, writes block against
// an unread net.Pipe and are released by its deadline or Close.
type websocketBlockingTestConn struct {
	net.Conn
	blocked      net.Conn
	unread       net.Conn
	armed        atomic.Bool
	armAfterHTTP bool
	response     []byte
	entered      chan struct{}
	enteredOnce  sync.Once
}

func (connection *websocketBlockingTestConn) Read(buffer []byte) (int, error) {
	var n, err = connection.Conn.Read(buffer)
	if connection.armAfterHTTP && !connection.armed.Load() {
		connection.response = append(connection.response, buffer[:n]...)
		if bytes.Contains(connection.response, []byte("\r\n\r\n")) {
			connection.armed.Store(true)
			connection.response = nil
		}
	}
	return n, err
}

func (connection *websocketBlockingTestConn) Write(buffer []byte) (int, error) {
	if connection.armed.Load() {
		connection.enteredOnce.Do(func() { close(connection.entered) })
		return connection.blocked.Write(buffer)
	}
	return connection.Conn.Write(buffer)
}

func (connection *websocketBlockingTestConn) SetWriteDeadline(deadline time.Time) error {
	var err = connection.Conn.SetWriteDeadline(deadline)
	var blockedErr = connection.blocked.SetWriteDeadline(deadline)
	if err != nil {
		return err
	}
	return blockedErr
}

func (connection *websocketBlockingTestConn) SetDeadline(deadline time.Time) error {
	var err = connection.Conn.SetDeadline(deadline)
	var blockedErr = connection.blocked.SetDeadline(deadline)
	if err != nil {
		return err
	}
	return blockedErr
}

func (connection *websocketBlockingTestConn) Close() error {
	_ = connection.blocked.Close()
	_ = connection.unread.Close()
	return connection.Conn.Close()
}

func websocketInstallBlockingDial(t *testing.T, armAfterHTTP bool) <-chan *websocketBlockingTestConn {
	t.Helper()
	var previous = websocket.DefaultDialer.NetDialContext
	var previousClientDial = clientNetDialContext
	var connections = make(chan *websocketBlockingTestConn, 1)
	var blockingDial = func(ctx context.Context, network, address string) (net.Conn, error) {
		var dialer net.Dialer
		var raw, err = dialer.DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		var blocked, unread = net.Pipe()
		var wrapped = &websocketBlockingTestConn{Conn: raw, blocked: blocked, unread: unread, armAfterHTTP: armAfterHTTP, entered: make(chan struct{})}
		connections <- wrapped
		return wrapped, nil
	}
	websocket.DefaultDialer.NetDialContext = blockingDial
	clientNetDialContext = blockingDial
	t.Cleanup(func() {
		websocket.DefaultDialer.NetDialContext = previous
		clientNetDialContext = previousClientDial
	})
	return connections
}

func TestWebSocketProtocolConcurrentWriteDeadlineUnblocksWriter(t *testing.T) {
	for _, useBoth := range []bool{false, true} {
		t.Run(fmt.Sprintf("set-deadline-%t", useBoth), func(t *testing.T) {
			testWebSocketProtocolConcurrentWriteDeadline(t, useBoth)
		})
	}
}

func testWebSocketProtocolConcurrentWriteDeadline(t *testing.T, useBoth bool) {
	var wrappedConnections = websocketInstallBlockingDial(t, false)
	var connection, _ = websocketProtocolPair(t, "binary")
	var wrapped = <-wrappedConnections
	wrapped.armed.Store(true)
	var finished = make(chan struct{})
	t.Cleanup(func() {
		_ = wrapped.Close()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("writer did not stop")
		}
	})
	var result = make(chan error, 1)
	var frame = websocketTCPFrame(`{"c":"p"}`, bytes.Repeat([]byte{'x'}, 2<<20))
	go func() {
		defer close(finished)
		_, err := connection.Write(frame)
		result <- err
	}()
	select {
	case <-wrapped.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("write did not block")
	}
	var deadline = time.Now().Add(50 * time.Millisecond)
	var err error
	if useBoth {
		err = connection.SetDeadline(deadline)
	} else {
		err = connection.SetWriteDeadline(deadline)
	}
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		var networkErr net.Error
		if !errors.As(err, &networkErr) || !networkErr.Timeout() {
			t.Fatalf("write error = %v, want timeout", err)
		}
	case <-time.After(time.Second):
		t.Fatal("deadline did not unblock an ongoing write")
	}
}

type websocketDeadlineCaptureConn struct {
	net.Conn
	last time.Time
}

func (connection *websocketDeadlineCaptureConn) SetWriteDeadline(deadline time.Time) error {
	connection.last = deadline
	return connection.Conn.SetWriteDeadline(deadline)
}

func TestWebSocketProtocolDeadlinePrecedenceAndClearing(t *testing.T) {
	var local, peer = net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = peer.Close() })
	var capture = &websocketDeadlineCaptureConn{Conn: local}
	var connection = &websocketDeadlineConn{Conn: capture}
	var past = time.Now().Add(-time.Second)
	var future = time.Now().Add(time.Hour)
	for _, deadlines := range []struct{ internal, external time.Time }{{future, past}, {past, future}} {
		if err := connection.SetWriteDeadline(deadlines.internal); err != nil {
			t.Fatal(err)
		}
		if err := connection.setExternalWriteDeadline(deadlines.external); err != nil {
			t.Fatal(err)
		}
		if !capture.last.Equal(past) {
			t.Fatalf("effective deadline = %v, want %v", capture.last, past)
		}
		var _, err = connection.Write([]byte("x"))
		var networkErr net.Error
		if !errors.As(err, &networkErr) || !networkErr.Timeout() {
			t.Fatalf("deadline error = %v", err)
		}
	}
	if err := connection.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := connection.setExternalWriteDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	var received = make(chan byte, 1)
	go func() { var b [1]byte; _, _ = peer.Read(b[:]); received <- b[0] }()
	if _, err := connection.Write([]byte("y")); err != nil {
		t.Fatalf("write after clearing deadlines: %v", err)
	}
	if got := <-received; got != 'y' {
		t.Fatalf("received %q", got)
	}
}

func TestWebSocketProtocolWSSDeadlineUnblocksWriter(t *testing.T) {
	var wrappedConnections = websocketInstallBlockingDial(t, false)
	var done = make(chan struct{})
	var upgrader = websocket.Upgrader{}
	var server = httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var connection, err = upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		_, _, _ = connection.ReadMessage()
		<-done
	}))
	t.Cleanup(func() { close(done); server.Close() })
	var client = NewClient("wss-deadline")
	client.tlsConfig = &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}
	var uri = websocketTestURL(server.URL, "wss")
	uri.Path = "/amps/binary"
	var raw, err = client.dialWebSocket(context.Background(), uri, socketOptions{ipPreference: "ipv4"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	var connection = raw.(*websocketNetConn)
	var wrapped = <-wrappedConnections
	wrapped.armed.Store(true)
	var finished = make(chan struct{})
	var result = make(chan error, 1)
	t.Cleanup(func() {
		_ = wrapped.Close()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("WSS writer did not stop")
		}
	})
	go func() {
		defer close(finished)
		_, writeErr := connection.Write(websocketTCPFrame(`{"c":"p"}`, []byte("data")))
		result <- writeErr
	}()
	select {
	case <-wrapped.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("WSS write did not block")
	}
	if err = connection.SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-result:
		var networkErr net.Error
		if !errors.As(err, &networkErr) || !networkErr.Timeout() {
			t.Fatalf("WSS write error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("deadline did not unblock WSS write")
	}
}

func TestWebSocketProtocolCancelBlockedPreface(t *testing.T) {
	var wrappedConnections = websocketInstallBlockingDial(t, true)
	var done = make(chan struct{})
	var upgrader = websocket.Upgrader{}
	var server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var connection, err = upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		<-done
	}))
	t.Cleanup(func() { close(done); server.Close() })
	var client = NewClient("cancel-preface")
	var uri = websocketTestURL(server.URL, "ws")
	uri.Path = "/amps/json"
	var ctx, cancel = context.WithCancel(context.Background())
	var finished = make(chan struct{})
	var result = make(chan error, 1)
	go func() {
		defer close(finished)
		var connection, err = client.dialWebSocket(ctx, uri)
		if connection != nil {
			_ = connection.Close()
		}
		result <- err
	}()
	var wrapped = <-wrappedConnections
	t.Cleanup(func() {
		cancel()
		_ = wrapped.Close()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("dial did not stop")
		}
	})
	select {
	case <-wrapped.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("preface write did not block")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled preface = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled context did not unblock preface write")
	}
}

type websocketTrackedContext struct {
	lock      sync.Mutex
	done      chan struct{}
	cancelled bool
	callbacks map[int]func()
	next      int
}

func (ctx *websocketTrackedContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (ctx *websocketTrackedContext) Done() <-chan struct{}       { return ctx.done }
func (ctx *websocketTrackedContext) Value(any) any               { return nil }
func (ctx *websocketTrackedContext) Err() error {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if ctx.cancelled {
		return context.Canceled
	}
	return nil
}
func (ctx *websocketTrackedContext) AfterFunc(f func()) func() bool {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if ctx.cancelled {
		go f()
		return func() bool { return false }
	}
	var id = ctx.next
	ctx.next++
	ctx.callbacks[id] = f
	return func() bool {
		ctx.lock.Lock()
		defer ctx.lock.Unlock()
		_, ok := ctx.callbacks[id]
		delete(ctx.callbacks, id)
		return ok
	}
}
func (ctx *websocketTrackedContext) cancel() {
	ctx.lock.Lock()
	ctx.cancelled = true
	close(ctx.done)
	var callbacks = ctx.callbacks
	ctx.callbacks = make(map[int]func())
	ctx.lock.Unlock()
	for _, callback := range callbacks {
		go callback()
	}
}

func TestWebSocketProtocolSuccessDetachesPrefaceWatcher(t *testing.T) {
	var peers = make(chan *websocket.Conn, 1)
	var done = make(chan struct{})
	var upgrader = websocket.Upgrader{}
	var server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var connection, err = upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		peers <- connection
		<-done
	}))
	t.Cleanup(func() { close(done); server.Close() })
	var client = NewClient("watcher-detached")
	var uri = websocketTestURL(server.URL, "ws")
	uri.Path = "/amps/json"
	var ctx = &websocketTrackedContext{done: make(chan struct{}), callbacks: make(map[int]func())}
	var raw, err = client.dialWebSocket(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	var peer = <-peers
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	websocketExpectMessage(t, peer, websocket.TextMessage, []byte("text"))
	ctx.lock.Lock()
	var pending = len(ctx.callbacks)
	ctx.lock.Unlock()
	if pending != 0 {
		t.Fatalf("successful dial retained %d cancellation callbacks", pending)
	}
	ctx.cancel()
	var header = `{"c":"ack","cid":"after-cancel"}`
	if err = peer.WriteMessage(websocket.TextMessage, []byte(header)); err != nil {
		t.Fatal(err)
	}
	if got := websocketReadTCPFrame(t, raw.(*websocketNetConn)); string(got) != header {
		t.Fatalf("message after context cancellation = %q", got)
	}
}

func TestWebSocketProtocolRejectsMalformedInboundAndHugeBatch(t *testing.T) {
	for _, header := range []string{`{"c":`, `{"c":"sow","bs":4294967295}`} {
		t.Run(strings.ReplaceAll(header, `"`, ""), func(t *testing.T) {
			var connection, peer = websocketProtocolPair(t, "json")
			if err := peer.WriteMessage(websocket.TextMessage, []byte(header)); err != nil {
				t.Fatal(err)
			}
			var buffer [16]byte
			if n, err := connection.Read(buffer[:]); !IsErrorKind(err, ProtocolError) || n != 0 {
				t.Fatalf("Read malformed header = %d, %v", n, err)
			}
		})
	}
}
