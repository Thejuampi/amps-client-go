package amps

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Bug: Disconnect zeroed the configured heartbeat interval and timeout, not
// just the runtime timestamp/timer. HAClient configures a heartbeat once in
// NewHAClient and calls Disconnect between failover attempts, so a single
// failed logon permanently disabled heartbeat liveness detection for every
// later reconnect. Config retention is asserted by
// TestClientDisconnectKeepsHeartbeatConfig; this covers the HA default pairing.
func TestHAClientKeepsHeartbeatConfigurationAcrossFailedAttempt(t *testing.T) {
	// Drive a real failed attempt through the public API. The connect must
	// SUCCEED and the logon must FAIL, because only that path reaches
	// Client.Disconnect (connectAndLogonOnceWithContext returns early when
	// connect itself fails). The server therefore accepts and immediately
	// closes. No state is fabricated; only the assertion reads an unexported
	// field, because SetHeartbeat has no getter.
	var listener, listenErr = net.Listen("tcp", "127.0.0.1:0")
	if listenErr != nil {
		t.Fatalf("Listen() error = %v", listenErr)
	}
	defer listener.Close()

	var accepting sync.WaitGroup
	accepting.Add(1)
	go func() {
		defer accepting.Done()
		for {
			var conn, acceptErr = listener.Accept()
			if acceptErr != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	var rejectingURI = "tcp://" + listener.Addr().String() + "/amps/json"

	var ha = NewHAClient("ha-heartbeat-config")
	// HAClient installs an internal disconnect handler that spawns a background
	// reconnect goroutine. Without an explicit Disconnect that goroutine can
	// outlive the test by up to the configured timeout, leaving work in flight
	// for goleak and for whatever runs next. Disconnect sets stopped and cancels
	// it deterministically. It runs after the assertion below, so it cannot mask
	// the bug under test.
	defer func() { _ = ha.Disconnect() }()
	ha.SetServerChooser(NewDefaultServerChooser(rejectingURI))
	ha.SetReconnectDelay(time.Millisecond)
	ha.SetTimeout(150 * time.Millisecond)

	if err := ha.ConnectAndLogon(); err == nil {
		t.Fatalf("ConnectAndLogon() to a closed port unexpectedly succeeded")
	}

	_ = listener.Close()
	accepting.Wait()

	if interval := ha.Client().heartbeatInterval.Load(); interval != 30 {
		t.Fatalf("HA heartbeat interval after failed attempt = %d, want 30", interval)
	}
}

// The runtime heartbeat state must still be cleared by Disconnect so a stale
// timestamp cannot trigger a spurious absence error on the next connection.
func TestDisconnectClearsHeartbeatRuntimeState(t *testing.T) {
	var client = NewClient("heartbeat-runtime")
	client.SetHeartbeat(30)
	client.heartbeatTimestamp.Store(12345)

	_ = client.Disconnect()

	if timestamp := client.heartbeatTimestamp.Load(); timestamp != 0 {
		t.Fatalf("heartbeat timestamp after Disconnect = %d, want 0", timestamp)
	}
}

// Bug: MessageStream.SetMaxDepth/SetTimeout wrote plain struct fields while the
// receive goroutine reads ms.depth on every enqueue, so tuning a live stream
// raced with delivery.
//
// This covers ONLY that pair of fields. MessageStream as a whole is not
// race-free: commandID/queryID/unsubscribeID are still written unsynchronised
// by SetSubscription while Close/Next write them under lifecycleLock, and
// Close can read a torn commandID/unsubscribeID pair and unsubscribe the wrong
// route. Those remain open.
func TestMessageStreamDepthTuningDoesNotRaceWithDelivery(t *testing.T) {
	var stream = newMessageStream(nil)
	stream.setRunning()

	var waitGroup sync.WaitGroup
	waitGroup.Add(2)

	go func() {
		defer waitGroup.Done()
		for index := 0; index < 2000; index++ {
			_ = stream.messageHandler(&Message{header: &_Header{command: CommandPublish}})
		}
	}()
	go func() {
		defer waitGroup.Done()
		for index := 0; index < 2000; index++ {
			stream.SetMaxDepth(uint64(index%16) + 1)
		}
	}()

	waitGroup.Wait()

	if depth := stream.MaxDepth(); depth == 0 || depth > 16 {
		t.Fatalf("MaxDepth() = %d, want a value stored by SetMaxDepth (1..16)", depth)
	}
}

// noProgressConn always reports a successful read of zero bytes, the behaviour a
// WebSocket peer produces by emitting zero-length frames.
type noProgressConn struct {
	reads int
}

func (connection *noProgressConn) Read(buffer []byte) (int, error) {
	connection.reads++
	if connection.reads > 10*maxConsecutiveEmptyReads {
		return 0, io.EOF
	}
	return 0, nil
}

func (connection *noProgressConn) Write(buffer []byte) (int, error) { return len(buffer), nil }
func (connection *noProgressConn) Close() error                     { return nil }
func (connection *noProgressConn) LocalAddr() net.Addr              { return dummyAddr{value: "local"} }
func (connection *noProgressConn) RemoteAddr() net.Addr             { return dummyAddr{value: "remote"} }
func (connection *noProgressConn) SetDeadline(time.Time) error      { return nil }
func (connection *noProgressConn) SetReadDeadline(time.Time) error  { return nil }
func (connection *noProgressConn) SetWriteDeadline(time.Time) error { return nil }

// Bug: the receive loop treated a (0, nil) read as "keep reading", so a
// transport that reports success without data spun the receive goroutine
// forever. That goroutine also never re-checks client.stopped inside the inner
// read loop, so the spin was unbreakable. A WebSocket peer sending zero-length
// frames reaches this through websocketNetConn.Read.
func TestReceiveLoopFailsOnTransportWithoutReadProgress(t *testing.T) {
	var client = NewClient("no-read-progress")
	var connection = &noProgressConn{}
	client.connectionStateLock.Lock()
	client.connection = connection
	client.connected.Store(true)
	client.stopped.Store(false)
	client.connectionStateLock.Unlock()
	client.resetDisconnectSignal()

	var done = make(chan struct{})
	go func() {
		defer close(done)
		client.readRoutineForConnection(connection)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("receive loop spun on a transport returning (0, nil) instead of failing")
	}

	if connection.reads > maxConsecutiveEmptyReads+1 {
		t.Fatalf("read attempts = %d, want the loop to stop at %d", connection.reads, maxConsecutiveEmptyReads)
	}
}

// Black-box companion to TestReceiveLoopFailsOnTransportWithoutReadProgress:
// a real WebSocket peer emitting only zero-length frames, reached through the
// public Connect path, must terminate the connection rather than pin the
// receive goroutine. websocketNetConn.Read returns (0, nil) for such frames.
func TestConnectSurvivesWebSocketPeerSendingOnlyEmptyFrames(t *testing.T) {
	var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var handlerDone = make(chan struct{})
	var server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer close(handlerDone)
		var conn, upgradeErr = upgrader.Upgrade(writer, request, nil)
		if upgradeErr != nil {
			return
		}
		defer conn.Close()
		for index := 0; index < 10*maxConsecutiveEmptyReads; index++ {
			if writeErr := conn.WriteMessage(websocket.BinaryMessage, nil); writeErr != nil {
				return
			}
		}
		<-request.Context().Done()
	}))
	defer server.Close()

	var client = NewClient("ws-empty-frames")
	var disconnected = make(chan struct{})
	var once sync.Once
	client.AddConnectionStateListener(ConnectionStateListenerFunc(func(state ConnectionState) {
		if state == ConnectionStateDisconnected {
			once.Do(func() { close(disconnected) })
		}
	}))

	var parsed = websocketTestURL(server.URL, "ws")
	parsed.Path = "/amps/json"
	if err := client.Connect(parsed.String()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer client.Close()

	select {
	case <-disconnected:
	case <-time.After(10 * time.Second):
		t.Fatalf("receive loop never terminated against a peer sending only empty frames")
	}
}
