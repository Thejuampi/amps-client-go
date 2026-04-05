package amps

import (
	"strings"
	"testing"
	"time"
)

func makeAutoAckMessage(bookmark string) *Message {
	return &Message{
		header: &_Header{
			command:     CommandPublish,
			topic:       []byte("queue://orders"),
			subID:       []byte("sub-ack"),
			leasePeriod: []byte("2026-01-01T00:00:00.000000Z"),
			bookmark:    []byte(bookmark),
		},
	}
}

func TestAutoAckBatchFlush(t *testing.T) {
	client := NewClient("auto-ack-batch")
	conn := newTestConn()
	client.connected.Store(true)
	client.connection = conn
	client.SetAutoAck(true).SetAckBatchSize(2).SetAckTimeout(5 * time.Second)

	client.maybeAutoAck(makeAutoAckMessage("1|1|"))
	if payload := conn.WrittenPayload(); payload != "" {
		t.Fatalf("did not expect ack before batch threshold, got payload=%q", payload)
	}

	client.maybeAutoAck(makeAutoAckMessage("1|2|"))
	payload := conn.WrittenPayload()
	if !strings.Contains(payload, `"c":"ack"`) {
		t.Fatalf("expected ack command payload, got %q", payload)
	}
	if !strings.Contains(payload, `1|1|,1|2|`) {
		t.Fatalf("expected batched bookmarks in payload, got %q", payload)
	}
}

func TestAutoAckTimeoutFlush(t *testing.T) {
	client := NewClient("auto-ack-timeout")
	conn := newTestConn()
	client.connected.Store(true)
	client.connection = conn
	client.SetAutoAck(true).SetAckBatchSize(100).SetAckTimeout(25 * time.Millisecond)

	client.maybeAutoAck(makeAutoAckMessage("2|1|"))
	time.Sleep(80 * time.Millisecond)

	payload := conn.WrittenPayload()
	if !strings.Contains(payload, `"c":"ack"`) {
		t.Fatalf("expected timeout ack payload, got %q", payload)
	}
	if !strings.Contains(payload, `2|1|`) {
		t.Fatalf("expected bookmark in timeout ack payload, got %q", payload)
	}
}

func TestOnMessageRouteDispatchDoesNotAllocateWhenAutoAckDoesNotApply(t *testing.T) {
	client := NewClient("route-hot-no-auto-ack")
	client.routes.Store("sub-1", func(message *Message) error { return nil })

	message := &Message{
		header: &_Header{
			command: CommandPublish,
			subID:   []byte("sub-1"),
			topic:   []byte("orders"),
		},
		data: []byte(`{"id":1}`),
	}

	allocs := testing.AllocsPerRun(1000, func() {
		if err := client.onMessage(message); err != nil {
			t.Fatalf("onMessage failed: %v", err)
		}
	})

	if allocs != 0 {
		t.Fatalf("allocs per route dispatch = %v, want 0", allocs)
	}
}

func TestPublishBytesDoesNotAllocateOnSteadyStateDirectPath(t *testing.T) {
	client := NewClient("publish-direct-no-alloc")
	conn := newTestConn()
	client.connected.Store(true)
	client.connection = conn

	payload := []byte(`{"id":1}`)
	resetWrites := func() {
		conn.lock.Lock()
		conn.writeBuf.Reset()
		conn.lock.Unlock()
	}

	for warmup := 0; warmup < 32; warmup++ {
		if err := client.PublishBytes("orders", payload); err != nil {
			t.Fatalf("publish warmup failed: %v", err)
		}
		resetWrites()
	}

	allocs := testing.AllocsPerRun(1000, func() {
		if err := client.PublishBytes("orders", payload); err != nil {
			t.Fatalf("publish failed: %v", err)
		}
		resetWrites()
	})

	if allocs != 0 {
		t.Fatalf("allocs per direct publish = %v, want 0", allocs)
	}
}
