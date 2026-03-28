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

func TestMessageStreamSOWOnlyCompletionCleansBothRouteIDs(t *testing.T) {
	client := NewClient("sow-route-cleanup")
	stream := newMessageStream(client)
	stream.SetSOWOnly("cid-sow", "qid-sow")
	client.routes.Store("cid-sow", func(*Message) error { return nil })
	client.routes.Store("qid-sow", func(*Message) error { return nil })

	stream.current = &Message{header: &_Header{command: CommandGroupEnd}}
	if message := stream.Next(); message == nil {
		t.Fatalf("expected group-end message")
	}
	if _, exists := client.routes.Load("cid-sow"); exists {
		t.Fatalf("expected SOW completion to remove command route")
	}
	if _, exists := client.routes.Load("qid-sow"); exists {
		t.Fatalf("expected SOW completion to remove query route")
	}
}

func TestMessageStreamReconfigurationClearsStaleIDs(t *testing.T) {
	stream := newMessageStream(NewClient("stream-reconfigure"))
	stream.SetSubscription("cid-1", "sub-1", "qid-1")
	stream.SetStatsOnly("cid-2")
	if stream.queryID != "" || stream.unsubscribeID != "" {
		t.Fatalf("expected stats-only reconfigure to clear stale ids, got query=%q unsubscribe=%q", stream.queryID, stream.unsubscribeID)
	}

	stream.SetSOWOnly("cid-3", "qid-3")
	stream.SetAcksOnly("cid-4")
	if stream.queryID != "" || stream.unsubscribeID != "" {
		t.Fatalf("expected ack-only reconfigure to clear stale ids, got query=%q unsubscribe=%q", stream.queryID, stream.unsubscribeID)
	}
}

func TestMessageStreamReconfigurationClearsTransientState(t *testing.T) {
	stream := newMessageStream(NewClient("stream-reconfigure-state"))
	stream.current = &Message{data: []byte("stale")}
	stream.queue.enqueue(&Message{data: []byte("queued")})
	stream.timedOut.Store(true)
	stream.setSubID("sub-reconfigure")
	stream.Conflate()
	stream.sowKeyMap["k1"] = &Message{data: []byte("conflated")}

	stream.SetAcksOnly("cid-reset")

	if stream.current != nil {
		t.Fatalf("expected reconfigure to clear current message")
	}
	if stream.Depth() != 0 {
		t.Fatalf("expected reconfigure to clear queued messages, got depth %d", stream.Depth())
	}
	if stream.timedOut.Load() {
		t.Fatalf("expected reconfigure to clear timeout state")
	}
	if stream.sowKeyMap != nil {
		t.Fatalf("expected reconfigure to clear conflate state")
	}
}

func TestMessageStreamResetForConfigurationNilAndZeroValue(t *testing.T) {
	var nilStream *MessageStream
	nilStream.resetForConfiguration()

	var zeroStream MessageStream
	zeroStream.resetForConfiguration()
	if zeroStream.current != nil || zeroStream.timedOut.Load() {
		t.Fatalf("expected zero-value resetForConfiguration to be a noop cleanup")
	}
}
