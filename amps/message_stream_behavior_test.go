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

func TestMessageStreamStatsOnlyCompletionCleansBothRouteIDs(t *testing.T) {
	client := NewClient("stats-route-cleanup")
	stream := newMessageStream(client)
	stream.SetStatsOnly("cid-stats", "qid-stats")
	client.routes.Store("cid-stats", func(*Message) error { return nil })
	client.routes.Store("qid-stats", func(*Message) error { return nil })

	ack := AckTypeStats
	stream.current = &Message{
		header: &_Header{
			command: CommandAck,
			ackType: &ack,
		},
	}

	if message := stream.Next(); message == nil {
		t.Fatalf("expected stats ack message")
	}
	if stream.commandID != "" {
		t.Fatalf("expected stats completion to clear command id, got %q", stream.commandID)
	}
	if stream.queryID != "" {
		t.Fatalf("expected stats completion to clear query id, got %q", stream.queryID)
	}
	if _, exists := client.routes.Load("cid-stats"); exists {
		t.Fatalf("expected stats completion to remove command route")
	}
	if _, exists := client.routes.Load("qid-stats"); exists {
		t.Fatalf("expected stats completion to remove query route")
	}
}

func TestMessageStreamCloseIsIdempotentForSubscribedQueryStreams(t *testing.T) {
	client := NewClient("stream-close-idempotent")
	client.SetRetryOnDisconnect(true)

	stream := newMessageStream(client)
	stream.SetSubscription("route-1", "sub-1", "qid-1")
	client.routes.Store("qid-1", func(*Message) error { return nil })
	client.messageStreams.Store("route-1", stream)
	client.messageStreams.Store("qid-1", stream)

	if err := stream.Close(); err != nil {
		t.Fatalf("expected first close to queue unsubscribe without error, got %v", err)
	}
	if stream.queryID != "" {
		t.Fatalf("expected first close to clear query id, got %q", stream.queryID)
	}

	state := ensureClientState(client)
	state.lock.Lock()
	firstPending := len(state.pendingRetry)
	state.lock.Unlock()
	if firstPending != 1 {
		t.Fatalf("expected one queued unsubscribe after first close, got %d", firstPending)
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("expected second close to be idempotent, got %v", err)
	}

	state.lock.Lock()
	secondPending := len(state.pendingRetry)
	state.lock.Unlock()
	if secondPending != 1 {
		t.Fatalf("expected idempotent close to avoid duplicate unsubscribe, got %d queued commands", secondPending)
	}
}
