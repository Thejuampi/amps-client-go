package amps

import (
	"sync"
	"testing"
	"time"
)

// TestMessageStreamConflateDataRace guards the conflation path against
// mutating a queued message while the consumer is reading it. It fails under
// the race detector when conflate replacement and consumption are not guarded
// by the same stream lock.
func TestMessageStreamConflateDataRace(t *testing.T) {
	var stream = newMessageStream(nil)
	stream.SetSubscription("sub-race", "sub-race")
	stream.SetTimeout(1)
	stream.Conflate()

	var stop = make(chan struct{})
	var wg sync.WaitGroup

	// Producer: repeatedly hand the SAME sow key to the stream so the
	// conflation branch takes existingMessage.Replace(...).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			var msg = &Message{
				header: &_Header{command: CommandSOW, sowKey: []byte("K")},
				data:   []byte("payload-from-producer"),
			}
			_ = stream.messageHandler(msg)
		}
	}()

	// Consumer: read the conflated message and touch its bytes.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if stream.HasNext() {
				var got = stream.Next()
				if got != nil {
					_ = len(got.Data())
				}
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestMessageStreamSlowConsumerDoesNotStallReceivePath guards against
// head-of-line blocking from a full bounded stream queue. messageHandler runs
// on the shared receive goroutine, so bounded queues must evict instead of
// blocking indefinitely.
func TestMessageStreamSlowConsumerDoesNotStallReceivePath(t *testing.T) {
	var stream = newMessageStream(nil)
	stream.SetSubscription("sub-slow", "sub-slow")
	stream.SetMaxDepth(1)

	// Fill the bounded queue to capacity. This call returns.
	var first = &Message{header: &_Header{command: CommandSOW}, data: []byte("1")}
	_ = stream.messageHandler(first)

	// The next delivery used to block because the consumer never drained.
	var returned = make(chan struct{})
	go func() {
		var second = &Message{header: &_Header{command: CommandSOW}, data: []byte("2")}
		_ = stream.messageHandler(second)
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("messageHandler blocked behind a full bounded queue")
	}

	var next = stream.Next()
	if next == nil || string(next.Data()) != "2" {
		t.Fatalf("expected bounded queue to keep newest message, got %#v", next)
	}
}

func TestMessageStreamConflateEvictionRemovesStaleSowKey(t *testing.T) {
	var stream = newMessageStream(nil)
	stream.SetSubscription("sub-evict", "sub-evict")
	stream.SetMaxDepth(1)
	stream.Conflate()

	var first = &Message{header: &_Header{command: CommandSOW, sowKey: []byte("K1")}, data: []byte("first")}
	var second = &Message{header: &_Header{command: CommandSOW, sowKey: []byte("K2")}, data: []byte("second")}
	var third = &Message{header: &_Header{command: CommandSOW, sowKey: []byte("K1")}, data: []byte("third")}

	_ = stream.messageHandler(first)
	_ = stream.messageHandler(second)
	_ = stream.messageHandler(third)

	var next = stream.Next()
	if next == nil || string(next.Data()) != "third" {
		t.Fatalf("expected evicted SOW key to be enqueueable again, got %#v", next)
	}
}

// TestMessageStreamTimeoutDoesNotAdvertiseNilNext guards the HasNext/Next
// contract: a timeout without a message must not report HasNext as true.
func TestMessageStreamTimeoutDoesNotAdvertiseNilNext(t *testing.T) {
	var stream = newMessageStream(nil)
	stream.SetAcksOnly("cid-timeout")
	stream.setRunning()
	stream.SetTimeout(20) // 20ms

	if stream.HasNext() {
		t.Fatalf("HasNext()=true after timeout with no message")
	}
}
