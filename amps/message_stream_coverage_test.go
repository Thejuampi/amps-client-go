package amps

import (
	"testing"
	"time"
)

func TestMessageStreamGeneralCoverage(t *testing.T) {
	stream := newMessageStream(nil)
	if stream.IsValid() {
		t.Fatalf("unset stream should be invalid")
	}
	if stream.Begin() == nil || stream.End() == nil {
		t.Fatalf("expected iterators")
	}

	if stream.FromExistingHandler(nil) != stream {
		t.Fatalf("expected same stream from existing handler with nil input")
	}
	if stream.SetAcksOnly("cid") != stream {
		t.Fatalf("expected SetAcksOnly fluent return")
	}
	if stream.SetSOWOnly("cid", "qid") != stream {
		t.Fatalf("expected SetSOWOnly fluent return")
	}
	if stream.SetStatsOnly("cid") != stream {
		t.Fatalf("expected SetStatsOnly fluent return")
	}
	if stream.SetSubscription("cid", "sub", "qid") != stream {
		t.Fatalf("expected SetSubscription fluent return")
	}
	stream.SetTimeout(123).SetMaxDepth(9)
	if stream.Timeout() != 123 || stream.MaxDepth() != 9 {
		t.Fatalf("unexpected timeout/max depth")
	}
	stream.setState(messageStreamStateDisconnected)
	if stream.state == messageStreamStateDisconnected {
		t.Fatalf("setState should ignore disconnected sentinel")
	}

	var nilStream *MessageStream
	if nilStream.SetAcksOnly("x") != nil || nilStream.SetSOWOnly("x", "y") != nil || nilStream.SetStatsOnly("x") != nil || nilStream.SetSubscription("x", "y") != nil {
		t.Fatalf("nil stream fluent setters should return nil")
	}

	stream.setSubID("sid-1")
	if stream.commandID != "sid-1" || stream.state != messageStreamStateSubscribed {
		t.Fatalf("setSubID should set command id and subscribed state")
	}
	stream.setQueryID("qid-1")
	if stream.queryID != "qid-1" {
		t.Fatalf("setQueryID should set query id")
	}
}

func TestMessageStreamNextAndCloseCoverage(t *testing.T) {
	client := NewClient("stream-close")
	stream := newMessageStream(client)

	stream.Conflate()
	if !stream.isConflating() {
		t.Fatalf("expected conflating stream")
	}
	stream.sowKeyMap["k1"] = &Message{header: &_Header{sowKey: []byte("k1")}}
	stream.current = &Message{header: &_Header{sowKey: []byte("k1")}}
	stream.consumeConflateState()
	if _, exists := stream.sowKeyMap["k1"]; exists {
		t.Fatalf("expected sow key removal after consume")
	}
	stream.consumeConflateState()
	stream.current = nil
	stream.consumeConflateState()

	stream.setSowOnly()
	stream.queryID = "qid-1"
	client.routes.Store("qid-1", func(*Message) error { return nil })
	stream.current = &Message{header: &_Header{command: CommandGroupEnd}}
	if got := stream.Next(); got == nil {
		t.Fatalf("expected group end message")
	}
	if stream.state != messageStreamStateComplete {
		t.Fatalf("expected sow-only completion")
	}

	stream = newMessageStream(client)
	stream.setStatsOnly()
	stream.commandID = "cmd-1"
	client.routes.Store("cmd-1", func(*Message) error { return nil })
	ackStats := AckTypeStats
	stream.current = &Message{header: &_Header{command: CommandAck, ackType: &ackStats}}
	if got := stream.Next(); got == nil {
		t.Fatalf("expected stats ack message")
	}
	if stream.state != messageStreamStateComplete {
		t.Fatalf("expected stats-only completion")
	}

	stream = newMessageStream(nil)
	stream.commandID = "cid"
	stream.current = &Message{header: &_Header{command: CommandPublish}}
	if got := stream.Next(); got == nil {
		t.Fatalf("expected non-nil current message")
	}

	stream = newMessageStream(nil)
	stream.commandID = "cid"
	stream.queryID = "qid"
	if err := stream.Close(); err != nil {
		t.Fatalf("nil-client close should not fail: %v", err)
	}
	if stream.commandID != "" || stream.queryID != "" {
		t.Fatalf("expected nil-client close to clear ids")
	}

	stream = newMessageStream(client)
	stream.commandID = "sub-1"
	stream.setState(messageStreamStateSubscribed)
	if err := stream.Close(); err == nil {
		t.Fatalf("expected unsubscribe error while disconnected")
	}

	stream = newMessageStream(client)
	stream.queryID = "qid-2"
	stream.setState(messageStreamStateUnset)
	client.routes.Store("qid-2", func(*Message) error { return nil })
	if err := stream.Close(); err != nil {
		t.Fatalf("expected route-delete close success: %v", err)
	}

	stream = newMessageStream(client)
	stream.queryID = "qid-3"
	stream.setState(messageStreamStateComplete)
	if err := stream.Close(); err == nil {
		t.Fatalf("expected unsubscribe branch error for complete query stream")
	}

	if empty := emptyMessageStream(); empty == nil {
		t.Fatalf("expected shared empty stream")
	}
}

func TestMessageStreamHasNextAndMessageHandlerCoverage(t *testing.T) {
	stream := newMessageStream(nil)

	if stream.HasNext() {
		t.Fatalf("unset stream should not have next")
	}
	stream.setRunning()
	stream.SetTimeout(5)
	if !stream.HasNext() {
		t.Fatalf("timeout branch should report has-next")
	}
	if stream.Next() != nil {
		t.Fatalf("timed out next should be nil")
	}
	stream.setState(messageStreamStateComplete)
	if stream.HasNext() {
		t.Fatalf("complete stream should not have next")
	}

	stream = newMessageStream(nil)
	stream.setRunning()
	message := &Message{header: &_Header{command: CommandPublish}, data: []byte("x")}
	if err := stream.messageHandler(nil); err != nil {
		t.Fatalf("nil message handler should be no-op: %v", err)
	}
	if err := stream.messageHandler(message); err != nil {
		t.Fatalf("message handler enqueue failed: %v", err)
	}
	if stream.Depth() != 1 {
		t.Fatalf("expected depth 1 after enqueue, got %d", stream.Depth())
	}
	if !stream.HasNext() {
		t.Fatalf("expected has-next after enqueue")
	}
	next := stream.Next()
	if next == nil || string(next.Data()) != "x" {
		t.Fatalf("unexpected dequeued message")
	}
}

func TestMessageQueueCoverage(t *testing.T) {
	queue := newQueue(1)
	if queue.length() != 0 {
		t.Fatalf("expected empty queue")
	}

	message1 := &Message{header: &_Header{command: CommandPublish}, data: []byte("1")}
	message2 := &Message{header: &_Header{command: CommandPublish}, data: []byte("2")}
	queue.enqueue(message1)
	queue.enqueue(message2)
	if queue.capacity < 2 {
		t.Fatalf("expected queue resize")
	}

	if message, err := queue.dequeue(); err != nil || string(message.Data()) != "1" {
		t.Fatalf("unexpected dequeue result: msg=%v err=%v", message, err)
	}
	if message, ok := queue.tryDequeue(); !ok || string(message.Data()) != "2" {
		t.Fatalf("unexpected tryDequeue result: msg=%v ok=%v", message, ok)
	}
	if _, err := queue.dequeue(); err == nil {
		t.Fatalf("expected dequeue error on empty queue")
	}
	if _, ok := queue.tryDequeue(); ok {
		t.Fatalf("expected tryDequeue miss on empty queue")
	}

	// waitDequeue
	go func() {
		time.Sleep(10 * time.Millisecond)
		queue.enqueue(&Message{header: &_Header{command: CommandPublish}, data: []byte("3")})
	}()
	if message, ok := queue.waitDequeue(); !ok || string(message.Data()) != "3" {
		t.Fatalf("unexpected waitDequeue result")
	}

	// waitDequeueTimeout with timeout<=0 uses waitDequeue path.
	go func() {
		time.Sleep(10 * time.Millisecond)
		queue.enqueue(&Message{header: &_Header{command: CommandPublish}, data: []byte("4")})
	}()
	if message, ok := queue.waitDequeueTimeout(0); !ok || string(message.Data()) != "4" {
		t.Fatalf("unexpected waitDequeueTimeout(0) result")
	}

	// waitDequeueTimeout expires.
	if message, ok := queue.waitDequeueTimeout(5 * time.Millisecond); ok || message != nil {
		t.Fatalf("expected waitDequeueTimeout expiration")
	}

	// waitDequeueTimeout returns message before timeout.
	go func() {
		time.Sleep(10 * time.Millisecond)
		queue.enqueue(&Message{header: &_Header{command: CommandPublish}, data: []byte("5")})
	}()
	if message, ok := queue.waitDequeueTimeout(50 * time.Millisecond); !ok || string(message.Data()) != "5" {
		t.Fatalf("unexpected waitDequeueTimeout message result")
	}

	// enqueueWithDepth blocking path.
	queue.enqueue(&Message{header: &_Header{command: CommandPublish}, data: []byte("a")})
	queue.enqueue(&Message{header: &_Header{command: CommandPublish}, data: []byte("b")})
	done := make(chan struct{})
	go func() {
		queue.enqueueWithDepth(&Message{header: &_Header{command: CommandPublish}, data: []byte("c")}, 1)
		close(done)
	}()
	time.Sleep(10 * time.Millisecond)
	if _, err := queue.dequeue(); err != nil {
		t.Fatalf("unexpected dequeue while unblocking depth wait: %v", err)
	}
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("enqueueWithDepth did not unblock")
	}

	queue.clear()
	if queue.length() != 0 {
		t.Fatalf("expected clear to reset queue")
	}

	emptyResize := newQueue(2)
	emptyResize.resize()
	if emptyResize.capacity != 4 {
		t.Fatalf("expected resize to double empty queue capacity")
	}
	if emptyResize.first != 0 || emptyResize.last != 0 {
		t.Fatalf("expected empty resize to keep first/last at zero")
	}
}

func TestMessageStreamCloseUnblocksHasNext(t *testing.T) {
	stream := newMessageStream(nil)
	stream.setRunning()

	done := make(chan bool, 1)
	go func() {
		done <- stream.HasNext()
	}()

	time.Sleep(10 * time.Millisecond)
	if err := stream.Close(); err != nil {
		t.Fatalf("close should not fail for nil client: %v", err)
	}

	select {
	case hasNext := <-done:
		if hasNext {
			t.Fatalf("expected blocked HasNext to stop after Close")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("Close did not unblock waiting HasNext")
	}
}

func TestMessageStreamCompletionAndClosedQueueCoverage(t *testing.T) {
	stream := newMessageStream(nil)
	stream.setRunning()
	stream.SetTimeout(200)

	done := make(chan bool, 1)
	go func() {
		done <- stream.HasNext()
	}()

	time.Sleep(10 * time.Millisecond)
	stream.setState(messageStreamStateComplete)
	select {
	case hasNext := <-done:
		if hasNext {
			t.Fatalf("expected HasNext to return false after completion while waiting")
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("HasNext did not unblock after completion")
	}

	queue := newQueue(2)
	queue.close()
	queue.enqueueWithDepth(&Message{header: &_Header{command: CommandPublish}, data: []byte("x")}, 1)
	if queue.length() != 0 {
		t.Fatalf("expected closed queue to reject enqueue immediately")
	}

	queue = newQueue(2)
	queue.enqueue(&Message{header: &_Header{command: CommandPublish}, data: []byte("a")})
	queue.enqueue(&Message{header: &_Header{command: CommandPublish}, data: []byte("b")})
	blocked := make(chan struct{}, 1)
	go func() {
		queue.enqueueWithDepth(&Message{header: &_Header{command: CommandPublish}, data: []byte("c")}, 1)
		blocked <- struct{}{}
	}()
	time.Sleep(10 * time.Millisecond)
	queue.close()
	select {
	case <-blocked:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("enqueueWithDepth did not unblock after queue close")
	}
	if queue.length() != 2 {
		t.Fatalf("expected blocked enqueue to exit without appending when closed")
	}
}

func TestMessageStreamIteratorCoverage(t *testing.T) {
	var nilIterator *MessageStreamIterator
	if message, ok := nilIterator.Next(); ok || message != nil {
		t.Fatalf("nil iterator should terminate")
	}

	iterator := &MessageStreamIterator{}
	if message, ok := iterator.Next(); ok || message != nil {
		t.Fatalf("iterator with nil stream should terminate")
	}

	stream := newMessageStream(nil)
	stream.setState(messageStreamStateComplete)
	iterator = &MessageStreamIterator{stream: stream}
	if message, ok := iterator.Next(); ok || message != nil {
		t.Fatalf("iterator should stop on complete stream")
	}

	stream = newMessageStream(nil)
	stream.setState(messageStreamStateReading)
	stream.current = &Message{header: &_Header{command: CommandPublish}, data: []byte("iter")}
	iterator = &MessageStreamIterator{stream: stream}
	if message, ok := iterator.Next(); !ok || message == nil || string(message.Data()) != "iter" {
		t.Fatalf("expected iterator to return queued message")
	}
}

func TestMessageStreamAdditionalBranchCoverage(t *testing.T) {
	stream := newMessageStream(nil)
	stream.setState(messageStreamStateReading)
	go func() {
		time.Sleep(5 * time.Millisecond)
		stream.queue.enqueue(&Message{header: &_Header{command: CommandPublish}, data: []byte("wait")})
	}()
	if !stream.HasNext() {
		t.Fatalf("expected HasNext to dequeue from blocking wait path")
	}
	if message := stream.Next(); message == nil || string(message.Data()) != "wait" {
		t.Fatalf("unexpected message from blocking wait path")
	}

	stream = newMessageStream(nil)
	stream.setState(messageStreamStateUnset)
	if stream.HasNext() {
		t.Fatalf("expected non-reading stream without queued messages to have no next")
	}
	stream.current = &Message{header: &_Header{command: CommandPublish}}
	if !stream.HasNext() {
		t.Fatalf("expected HasNext true when current message is already set")
	}
	stream.current = nil
	stream.queue.enqueue(&Message{header: &_Header{command: CommandPublish}, data: []byte("queued")})
	if !stream.HasNext() {
		t.Fatalf("expected HasNext true when non-reading queue has data")
	}
	if message := stream.Next(); message == nil || string(message.Data()) != "queued" {
		t.Fatalf("unexpected queued message from non-reading path")
	}

	stream = newMessageStream(nil)
	stream.setState(messageStreamStateReading)
	stream.SetTimeout(20)
	go func() {
		time.Sleep(5 * time.Millisecond)
		stream.queue.enqueue(&Message{header: &_Header{command: CommandPublish}, data: []byte("timed")})
	}()
	if !stream.HasNext() {
		t.Fatalf("expected timeout HasNext to receive queued message")
	}
	if message := stream.Next(); message == nil || string(message.Data()) != "timed" {
		t.Fatalf("unexpected message from timeout path")
	}

	stream = newMessageStream(nil)
	stream.setState(messageStreamStateSOWOnly)
	stream.current = &Message{header: &_Header{command: CommandGroupEnd}}
	if message := stream.Next(); message == nil {
		t.Fatalf("expected sow-only group-end message")
	}
	if stream.commandID != "" || stream.queryID != "" {
		t.Fatalf("expected nil-client sow-only completion to clear IDs")
	}

	stream = newMessageStream(nil)
	stream.Conflate()
	stream.current = &Message{header: &_Header{command: CommandPublish}}
	stream.consumeConflateState()

	client := NewClient("stream-close-complete")
	stream = newMessageStream(client)
	stream.queryID = "q-complete"
	stream.setState(messageStreamStateComplete)
	client.routes.Store("q-complete", func(*Message) error { return nil })
	_ = stream.Close()

	connected := NewClient("stream-close-connected")
	stream = newMessageStream(connected)
	stream.commandID = "sub-close"
	stream.setState(messageStreamStateUnset)
	connected.routes.Store("sub-close", func(*Message) error { return nil })
	if err := stream.Close(); err != nil {
		t.Fatalf("expected connected close success on route-delete path, got %v", err)
	}

	stream = newMessageStream(nil)
	stream.setState(messageStreamStateComplete)
	if message := stream.Next(); message != nil {
		t.Fatalf("expected Next nil on complete stream without current message")
	}

	statsClient := NewClient("stream-stats-query")
	stream = newMessageStream(statsClient)
	stream.setStatsOnly()
	stream.commandID = ""
	stream.queryID = "qid-stats"
	statsClient.routes.Store("qid-stats", func(*Message) error { return nil })
	ackStats := AckTypeStats
	stream.current = &Message{header: &_Header{command: CommandAck, ackType: &ackStats}}
	if message := stream.Next(); message == nil {
		t.Fatalf("expected stats message")
	}
	if stream.queryID != "" {
		t.Fatalf("expected stats query route cleanup")
	}

	timeoutStream := newMessageStream(nil)
	timeoutStream.setRunning()
	timeoutStream.SetTimeout(1)
	if message, ok := (&MessageStreamIterator{stream: timeoutStream}).Next(); ok || message != nil {
		t.Fatalf("expected iterator timeout nil result")
	}
}
