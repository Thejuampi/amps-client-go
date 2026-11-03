package amps

import (
	"testing"
	"time"
)

func conflateFastPathMessage(key, data string) *Message {
	return &Message{header: &_Header{command: CommandPublish, sowKey: []byte(key)}, data: []byte(data)}
}

func TestMessageStreamNeverConflatedConsumeDoesNotWaitForLock(t *testing.T) {
	for _, callConflate := range []bool{false, true} {
		var stream = newMessageStream(nil)
		if callConflate {
			stream.Conflate() // The unset state cannot enable conflation.
		}
		stream.current = conflateFastPathMessage("k", "value")
		stream.lock.Lock()
		var started = make(chan struct{})
		var done = make(chan struct{})
		go func() {
			close(started)
			stream.consumeConflateState()
			close(done)
		}()
		<-started
		var completedWhileLocked bool
		select {
		case <-done:
			completedWhileLocked = true
		case <-time.After(time.Second):
		}
		stream.lock.Unlock()
		if !completedWhileLocked {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("consumer did not stop after unlocking")
			}
			t.Fatalf("consume waited for the lock; attempted inactive Conflate=%t", callConflate)
		}
	}
}

func TestMessageStreamConflationKeepsLatestAndCleansOnClosedDrain(t *testing.T) {
	var stream = newMessageStream(nil).SetSubscription("sub", "sub")
	stream.Conflate()
	for _, value := range []string{"A", "B"} {
		if err := stream.messageHandler(conflateFastPathMessage("same", value)); err != nil {
			t.Fatal(err)
		}
	}
	if got := stream.Depth(); got != 1 {
		t.Fatalf("conflated depth = %d, want 1", got)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	var message = stream.Next()
	if message == nil || string(message.Data()) != "B" {
		t.Fatalf("closed stream drained %v, want B", message)
	}
	stream.lock.Lock()
	var _, retained = stream.sowKeyMap["same"]
	stream.lock.Unlock()
	if retained {
		t.Fatal("draining a closed stream left the consumed key indexed")
	}
	if stream.Next() != nil {
		t.Fatal("closed stream delivered an extra message")
	}
}

func TestMessageStreamReconfigureRetainsConflationHistory(t *testing.T) {
	var stream = newMessageStream(nil).SetSubscription("first", "first")
	stream.Conflate()
	stream.SetAcksOnly("acks")
	stream.current = conflateFastPathMessage("old", "old")
	stream.lock.Lock()
	var started = make(chan struct{})
	var done = make(chan struct{})
	go func() {
		close(started)
		stream.consumeConflateState()
		close(done)
	}()
	<-started
	var returnedWhileLocked bool
	select {
	case <-done:
		returnedWhileLocked = true
	case <-time.After(50 * time.Millisecond):
	}
	stream.lock.Unlock()
	if !returnedWhileLocked {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("consumer did not stop after unlocking")
		}
	}
	if returnedWhileLocked {
		t.Fatal("reconfiguration forgot prior conflation and bypassed synchronization")
	}
	stream.SetSubscription("second", "second")
	stream.Conflate()
	for _, value := range []string{"A", "B"} {
		if err := stream.messageHandler(conflateFastPathMessage("key", value)); err != nil {
			t.Fatal(err)
		}
	}
	var message = stream.Next()
	if message == nil || string(message.Data()) != "B" {
		t.Fatalf("reconfigured stream returned %v, want B", message)
	}
	stream.lock.Lock()
	var _, retained = stream.sowKeyMap["key"]
	stream.lock.Unlock()
	if retained {
		t.Fatal("reconfigured stream retained consumed key")
	}
}

func TestMessageStreamConflationWaitsForPendingIndex(t *testing.T) {
	var stream = newMessageStream(nil).SetSubscription("sub", "sub")
	stream.Conflate()
	stream.lock.Lock()
	var copied, accepted = stream.queue.enqueueCopyWithDepth(conflateFastPathMessage("before-index", "value"), 0)
	if !accepted {
		stream.lock.Unlock()
		t.Fatal("enqueue failed")
	}
	var message, ok = stream.queue.tryDequeue()
	if !ok || message != copied {
		stream.lock.Unlock()
		t.Fatalf("dequeue = %p, %t; want copied message %p", message, ok, copied)
	}
	stream.current = message
	var started = make(chan struct{})
	var done = make(chan struct{})
	go func() {
		close(started)
		stream.consumeConflateState()
		close(done)
	}()
	<-started
	var returnedBeforeIndex bool
	select {
	case <-done:
		returnedBeforeIndex = true
	case <-time.After(50 * time.Millisecond):
	}
	copied.header.sowKey = []byte("key")
	stream.sowKeyMap["key"] = copied
	stream.lock.Unlock()
	if !returnedBeforeIndex {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("consumer did not stop after index was published")
		}
	}
	if returnedBeforeIndex {
		t.Fatal("consumer did not wait for the producer to publish the index")
	}
	stream.lock.Lock()
	var _, retained = stream.sowKeyMap["key"]
	stream.lock.Unlock()
	if retained {
		t.Fatal("consumer did not remove the newly published matching index")
	}
}

func TestMessageStreamFirstConflateDoesNotRemoveNewerCopy(t *testing.T) {
	var stream = newMessageStream(nil).SetSubscription("sub", "sub")
	if err := stream.messageHandler(conflateFastPathMessage("key", "old")); err != nil {
		t.Fatal(err)
	}
	var old, ok = stream.queue.tryDequeue()
	if !ok {
		t.Fatal("pre-conflation copy was not queued")
	}
	stream.current = old
	var publish = make(chan struct{})
	var published = make(chan error, 1)
	go func() {
		<-publish
		stream.Conflate()
		published <- stream.messageHandler(conflateFastPathMessage("key", "new"))
	}()
	close(publish)
	select {
	case err := <-published:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("new conflated copy was not published")
	}
	stream.lock.Lock()
	var newer = stream.sowKeyMap["key"]
	stream.lock.Unlock()
	if newer == nil || newer == old {
		t.Fatalf("new index = %p, old copy = %p", newer, old)
	}
	stream.consumeConflateState()
	stream.lock.Lock()
	var retained = stream.sowKeyMap["key"]
	stream.lock.Unlock()
	if retained != newer {
		t.Fatalf("consuming old copy replaced or removed newer index: %p, want %p", retained, newer)
	}
	stream.current = nil
	var got = stream.Next()
	if got != newer || string(got.Data()) != "new" {
		t.Fatalf("new copy = %p (%v), want %p", got, got, newer)
	}
	stream.lock.Lock()
	_, ok = stream.sowKeyMap["key"]
	stream.lock.Unlock()
	if ok {
		t.Fatal("newer copy was not removed after consumption")
	}
}
