package amps

import (
	"testing"
	"time"
)

func TestClientDisconnectClosesSynchronousSubscribeStream(t *testing.T) {
	client, _, stream := executeSyncStreamWithProcessedAck(
		t,
		"execute-sync-disconnect-close",
		NewCommand("subscribe").SetTopic("orders").SetSubID("sync-disconnect-sub"),
		"sync-disconnect-sub",
	)
	t.Cleanup(func() {
		if stream != nil && stream.queue != nil {
			stream.queue.close()
		}
	})

	hasNextCh := make(chan bool, 1)
	go func() {
		hasNextCh <- stream.HasNext()
	}()

	select {
	case hasNext := <-hasNextCh:
		t.Fatalf("HasNext returned before disconnect: %v", hasNext)
	case <-time.After(20 * time.Millisecond):
	}

	if err := client.Disconnect(); err != nil {
		t.Fatalf("disconnect failed: %v", err)
	}

	select {
	case hasNext := <-hasNextCh:
		if hasNext {
			t.Fatalf("expected disconnect to close synchronous subscribe stream")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for synchronous subscribe stream to close on disconnect")
	}
}

func TestClientExecuteRegistersSynchronousSubscribeStreamForCleanup(t *testing.T) {
	client, _, stream := executeSyncStreamWithProcessedAck(
		t,
		"execute-sync-register",
		NewCommand("subscribe").SetTopic("orders").SetSubID("sync-registered-sub"),
		"sync-registered-sub",
	)

	if _, exists := client.messageStreams.Load(stream.commandID); !exists {
		t.Fatalf("expected synchronous subscribe stream to be registered for cleanup")
	}
}

func TestClientExecuteRegistersSynchronousSubscribeStreamBeforeProcessedAck(t *testing.T) {
	var client = NewClient("execute-sync-register-before-ack")
	var conn = newTestConn()
	client.connected.Store(true)
	client.connection = conn

	type executeResult struct {
		stream *MessageStream
		err    error
	}

	var resultCh = make(chan executeResult, 1)
	go func() {
		var stream, err = client.Execute(
			NewCommand("subscribe").SetTopic("orders").SetSubID("sync-register-before-ack"),
		)
		resultCh <- executeResult{stream: stream, err: err}
	}()

	var handler = waitForRouteHandler(t, client, "sync-register-before-ack")
	if _, exists := client.messageStreams.Load("sync-register-before-ack"); !exists {
		t.Fatalf("expected synchronous subscribe stream to be registered before processed ack")
	}

	var ack = AckTypeProcessed
	_ = handler(&Message{header: &_Header{
		command: CommandAck,
		ackType: &ack,
		status:  []byte("success"),
	}})

	var result = <-resultCh
	if result.err != nil || result.stream == nil {
		t.Fatalf("expected execute success, stream=%v err=%v", result.stream, result.err)
	}
}
