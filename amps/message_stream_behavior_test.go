package amps

import (
	"testing"
	"time"
)

func TestMessageStreamTimeoutPath(t *testing.T) {
	stream := newMessageStream(nil)
	stream.setRunning()
	stream.SetTimeout(10)

	start := time.Now()
	if !stream.HasNext() {
		t.Fatalf("expected timeout signal")
	}
	elapsed := time.Since(start)
	if elapsed < 8*time.Millisecond {
		t.Fatalf("timeout returned too early: %v", elapsed)
	}
	if elapsed > 300*time.Millisecond {
		t.Fatalf("timeout returned too late: %v", elapsed)
	}

	if next := stream.Next(); next != nil {
		t.Fatalf("expected nil message on timeout")
	}
}

func TestMessageStreamStatsOnlyCompletion(t *testing.T) {
	stream := newMessageStream(nil)
	stream.setStatsOnly()
	ack := AckTypeStats
	stream.current = &Message{
		header: &_Header{
			command: CommandAck,
			ackType: &ack,
		},
	}

	message := stream.Next()
	if message == nil {
		t.Fatalf("expected stats ack message")
	}
	if stream.state != messageStreamStateComplete {
		t.Fatalf("expected stream completion after stats ack, got state=%d", stream.state)
	}
}
