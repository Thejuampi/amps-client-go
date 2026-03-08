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
