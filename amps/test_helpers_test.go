package amps

import (
	"bytes"
	"io"
	"net"
	"sync"
	"time"
)

type dummyAddr struct {
	value string
}

func (addr dummyAddr) Network() string { return "tcp" }
func (addr dummyAddr) String() string  { return addr.value }

type testConn struct {
	lock      sync.Mutex
	readQueue [][]byte
	writeBuf  bytes.Buffer
	closed    bool
}

func newTestConn() *testConn {
	return &testConn{}
}

func (connection *testConn) enqueueRead(frame []byte) {
	connection.lock.Lock()
	connection.readQueue = append(connection.readQueue, append([]byte(nil), frame...))
	connection.lock.Unlock()
}

func (connection *testConn) Read(buffer []byte) (int, error) {
	connection.lock.Lock()
	defer connection.lock.Unlock()

	if connection.closed {
		return 0, io.EOF
	}
	if len(connection.readQueue) == 0 {
		return 0, io.EOF
	}
	frame := connection.readQueue[0]
	if len(frame) <= len(buffer) {
		copy(buffer, frame)
		connection.readQueue = connection.readQueue[1:]
		return len(frame), nil
	}
	copy(buffer, frame[:len(buffer)])
	connection.readQueue[0] = frame[len(buffer):]
	return len(buffer), nil
}

func (connection *testConn) Write(buffer []byte) (int, error) {
	connection.lock.Lock()
	defer connection.lock.Unlock()
	if connection.closed {
		return 0, io.EOF
	}
	return connection.writeBuf.Write(buffer)
}

func (connection *testConn) Close() error {
	connection.lock.Lock()
	connection.closed = true
	connection.lock.Unlock()
	return nil
}

func (connection *testConn) LocalAddr() net.Addr  { return dummyAddr{value: "127.0.0.1:9000"} }
func (connection *testConn) RemoteAddr() net.Addr { return dummyAddr{value: "127.0.0.1:9100"} }
func (connection *testConn) SetDeadline(time.Time) error      { return nil }
func (connection *testConn) SetReadDeadline(time.Time) error  { return nil }
func (connection *testConn) SetWriteDeadline(time.Time) error { return nil }

func (connection *testConn) WrittenBytes() []byte {
	connection.lock.Lock()
	defer connection.lock.Unlock()
	return append([]byte(nil), connection.writeBuf.Bytes()...)
}

func (connection *testConn) WrittenPayload() string {
	buffer := connection.WrittenBytes()
	if len(buffer) <= 4 {
		return ""
	}
	return string(buffer[4:])
}
