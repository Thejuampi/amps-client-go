package amps

import (
	"io"
	"testing"
)

type disconnectDuringReadConn struct {
	*testConn
	client   *Client
	chunks   [][]byte
	readStep int
}

func newDisconnectDuringReadConn(client *Client, chunks ...[]byte) *disconnectDuringReadConn {
	return &disconnectDuringReadConn{
		testConn: newTestConn(),
		client:   client,
		chunks:   chunks,
	}
}

func (connection *disconnectDuringReadConn) Read(buffer []byte) (int, error) {
	connection.lock.Lock()
	defer connection.lock.Unlock()

	if connection.closed {
		return 0, io.EOF
	}
	if connection.readStep >= len(connection.chunks) {
		return 0, io.EOF
	}

	var chunk = connection.chunks[connection.readStep]
	connection.readStep++
	copy(buffer, chunk)

	if connection.readStep == 1 && connection.client != nil {
		connection.client.connectionStateLock.Lock()
		connection.client.connection = nil
		connection.client.connectionStateLock.Unlock()
	}

	return len(chunk), nil
}

func TestClientReadRoutineUsesSnapshotConnectionAcrossTeardown(t *testing.T) {
	var client = NewClient("read-routine-snapshot")
	var delivered int
	client.connected.Store(true)
	client.stopped.Store(false)
	client.SetErrorHandler(func(error) {})
	client.routes.Store("sub-1", func(*Message) error {
		delivered++
		return nil
	})

	var command = NewCommand("publish").SetSubID("sub-1").SetTopic("orders").SetData([]byte("payload"))
	var frame = buildFrameFromCommand(t, command)
	var connection = newDisconnectDuringReadConn(client, frame[:4], frame[4:])
	client.connection = connection

	defer func() {
		var recovered = recover()
		if recovered != nil {
			t.Fatalf("readRoutine panicked after connection teardown: %v", recovered)
		}
	}()

	client.readRoutine()

	if delivered != 1 {
		t.Fatalf("expected one delivered message, got %d", delivered)
	}
}

func TestClientAddRouteReplaceWithoutExistingHandlerDoesNotPanic(t *testing.T) {
	var client = NewClient("route-replace-missing")

	defer func() {
		var recovered = recover()
		if recovered != nil {
			t.Fatalf("replace route panicked without existing handler: %v", recovered)
		}
	}()

	var err = client.addRoute("route-new", nil, AckTypeProcessed, AckTypeNone, true, true, false)
	if err != nil {
		t.Fatalf("expected replace route without existing handler to succeed: %v", err)
	}
}
