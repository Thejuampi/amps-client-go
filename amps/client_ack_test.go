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
	client.connected = true
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
	client.connected = true
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
