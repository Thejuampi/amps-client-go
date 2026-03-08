package main

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type blockingWorkspaceConn struct {
	closeOnce sync.Once
	release   chan struct{}
}

func newBlockingWorkspaceConn() *blockingWorkspaceConn {
	return &blockingWorkspaceConn{
		release: make(chan struct{}),
	}
}

func (c *blockingWorkspaceConn) Read(_ []byte) (int, error) {
	return 0, io.EOF
}

func (c *blockingWorkspaceConn) Write(_ []byte) (int, error) {
	<-c.release
	return 0, net.ErrClosed
}

func (c *blockingWorkspaceConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.release)
	})
	return nil
}

func (c *blockingWorkspaceConn) LocalAddr() net.Addr {
	return &net.TCPAddr{}
}

func (c *blockingWorkspaceConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{}
}

func (c *blockingWorkspaceConn) SetDeadline(_ time.Time) error {
	return nil
}

func (c *blockingWorkspaceConn) SetReadDeadline(_ time.Time) error {
	return nil
}

func (c *blockingWorkspaceConn) SetWriteDeadline(_ time.Time) error {
	return nil
}

func TestWorkspaceNotifyRowDoesNotBlockOnSlowWebSocket(t *testing.T) {
	resetWorkspaceSessionsForTest()
	defer resetWorkspaceSessionsForTest()

	var conn = newBlockingWorkspaceConn()
	var ws = &websocketConn{
		Conn:   conn,
		reader: bufio.NewReader(bytes.NewReader(nil)),
	}
	defer func() {
		_ = ws.Close()
	}()

	workspaceSessions.Set(ws, workspaceLiveQuery{
		RequestID: "workspace-1",
		Mode:      "subscribe",
		Topic:     "orders",
	})

	var done = make(chan struct{})
	go func() {
		workspaceSessions.NotifyRow("orders", nil, []byte(`{"id":1}`), "1|1|1|", "ts-1", "order-1")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected workspace notification to avoid blocking publish path")
	}
}
