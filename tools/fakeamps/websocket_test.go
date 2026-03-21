package main

import (
	"bufio"
	"bytes"
	"net"
	"sync"
	"testing"
	"time"
)

type recordingWebsocketConn struct {
	mu        sync.Mutex
	writes    bytes.Buffer
	deadlines []time.Time
	closed    bool
}

func (c *recordingWebsocketConn) Read(_ []byte) (int, error) {
	return 0, net.ErrClosed
}

func (c *recordingWebsocketConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	return c.writes.Write(p)
}

func (c *recordingWebsocketConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *recordingWebsocketConn) LocalAddr() net.Addr {
	return &net.TCPAddr{}
}

func (c *recordingWebsocketConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{}
}

func (c *recordingWebsocketConn) SetDeadline(_ time.Time) error {
	return nil
}

func (c *recordingWebsocketConn) SetReadDeadline(_ time.Time) error {
	return nil
}

func (c *recordingWebsocketConn) SetWriteDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadlines = append(c.deadlines, deadline)
	return nil
}

func (c *recordingWebsocketConn) Bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.writes.Bytes()...)
}

func (c *recordingWebsocketConn) DeadlineCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.deadlines)
}

type failingWorkspaceConn struct {
	writeStarted chan struct{}
	closeOnce    sync.Once
}

func newFailingWorkspaceConn() *failingWorkspaceConn {
	return &failingWorkspaceConn{
		writeStarted: make(chan struct{}),
	}
}

func (c *failingWorkspaceConn) Read(_ []byte) (int, error) {
	return 0, net.ErrClosed
}

func (c *failingWorkspaceConn) Write(_ []byte) (int, error) {
	select {
	case <-c.writeStarted:
	default:
		close(c.writeStarted)
	}
	return 0, net.ErrClosed
}

func (c *failingWorkspaceConn) Close() error {
	c.closeOnce.Do(func() {})
	return nil
}

func (c *failingWorkspaceConn) LocalAddr() net.Addr {
	return &net.TCPAddr{}
}

func (c *failingWorkspaceConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{}
}

func (c *failingWorkspaceConn) SetDeadline(_ time.Time) error {
	return nil
}

func (c *failingWorkspaceConn) SetReadDeadline(_ time.Time) error {
	return nil
}

func (c *failingWorkspaceConn) SetWriteDeadline(_ time.Time) error {
	return nil
}

func TestWebsocketQueueTextWritesAsyncFrame(t *testing.T) {
	var conn = &recordingWebsocketConn{}
	var ws = &websocketConn{
		Conn:   conn,
		reader: bufio.NewReader(bytes.NewReader(nil)),
	}
	defer func() {
		_ = ws.Close()
	}()

	if err := ws.QueueText([]byte("ok")); err != nil {
		t.Fatalf("QueueText error = %v", err)
	}

	var frame []byte
	var deadline = time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		frame = conn.Bytes()
		if len(frame) >= 4 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if len(frame) < 4 {
		t.Fatalf("expected async websocket frame to be written")
	}
	if frame[0] != 0x81 || frame[1] != 2 || string(frame[2:]) != "ok" {
		t.Fatalf("unexpected async websocket frame: %v", frame)
	}
	if conn.DeadlineCount() < 2 {
		t.Fatalf("expected write deadline to be set and cleared")
	}
}

func TestWebsocketQueueTextQueueFullClosesConnection(t *testing.T) {
	var conn = newBlockingWorkspaceConn()
	var ws = &websocketConn{
		Conn:   conn,
		reader: bufio.NewReader(bytes.NewReader(nil)),
	}
	defer func() {
		_ = ws.Close()
	}()

	ws.workspaceQueue = make(chan websocketQueuedFrame, 1)
	ws.workspaceDone = make(chan struct{})
	close(ws.workspaceDone)
	ws.workspaceQueue <- websocketQueuedFrame{opcode: 0x1, payload: []byte("full")}

	if err := ws.QueueText([]byte("overflow")); err == nil {
		t.Fatalf("expected QueueText to fail when the async queue is full")
	}

	select {
	case <-conn.release:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected QueueText overflow to close websocket connection")
	}
}

func TestWebsocketQueueTextRejectsFramesAfterAsyncWriterFailure(t *testing.T) {
	var conn = newFailingWorkspaceConn()
	var ws = &websocketConn{
		Conn:   conn,
		reader: bufio.NewReader(bytes.NewReader(nil)),
	}
	defer func() {
		_ = ws.Close()
	}()

	if err := ws.QueueText([]byte("first")); err != nil {
		t.Fatalf("QueueText first error = %v", err)
	}

	select {
	case <-conn.writeStarted:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected async writer to attempt the first frame")
	}

	select {
	case <-ws.workspaceDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected async writer to stop after write failure")
	}

	if err := ws.QueueText([]byte("second")); err == nil {
		t.Fatalf("expected QueueText to reject frames after async writer failure")
	}
}

func TestWebsocketWriteControlFrameWritesPong(t *testing.T) {
	var conn = &recordingWebsocketConn{}
	var ws = &websocketConn{
		Conn:   conn,
		reader: bufio.NewReader(bytes.NewReader(nil)),
	}

	if err := ws.writeControlFrame(0xA, []byte("hi")); err != nil {
		t.Fatalf("writeControlFrame error = %v", err)
	}

	var frame = conn.Bytes()
	if len(frame) != 4 {
		t.Fatalf("unexpected control frame length: %d", len(frame))
	}
	if frame[0] != 0x8A || frame[1] != 2 || string(frame[2:]) != "hi" {
		t.Fatalf("unexpected control frame: %v", frame)
	}
}

func TestWebsocketReadFrameRejectsOversizedPayload(t *testing.T) {
	var ws = &websocketConn{
		Conn:   &recordingWebsocketConn{},
		reader: bufio.NewReader(bytes.NewReader([]byte{0x81, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})),
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("readFrame should reject oversized payloads without panicking: %v", recovered)
		}
	}()

	if _, _, err := ws.readFrame(); err == nil {
		t.Fatalf("expected oversized websocket frame to return an error")
	}
}
