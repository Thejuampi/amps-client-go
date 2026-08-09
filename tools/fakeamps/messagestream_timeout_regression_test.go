package main

import (
	"testing"
	"time"
)

// HasNext() reads the stream timeout, sees a non-zero value and dispatches to
// the bounded wait; waitForNextWithTimeout then RE-READS the same field. If
// SetTimeout(0) lands between the two reads, the bounded wait degrades into an
// unbounded one and HasNext never returns, even though the caller's timeout was
// non-zero when the call was made.
//
// Only exported API is used: this file is outside package amps.
func TestHasNextHonoursTimeoutObservedAtCallTime(t *testing.T) {
	var harness = startDeliveryHarness(t)
	var subscriber = harness.newClient(t, "stream-timeout-toctou")

	// Nothing publishes to this topic, so the queue stays empty and every
	// HasNext must come back via its timeout rather than via a message.
	var stream, err = subscriber.Subscribe("quiet-topic")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	var stopFlipper = make(chan struct{})
	var flipperDone = make(chan struct{})
	go func() {
		defer close(flipperDone)
		for {
			select {
			case <-stopFlipper:
				return
			default:
			}
			stream.SetTimeout(0)
			stream.SetTimeout(50)
		}
	}()
	defer func() {
		close(stopFlipper)
		<-flipperDone
	}()

	for attempt := 0; attempt < 400; attempt++ {
		stream.SetTimeout(50)

		var returned = make(chan struct{})
		go func() {
			defer close(returned)
			stream.HasNext()
		}()

		select {
		case <-returned:
		case <-time.After(3 * time.Second):
			t.Fatalf("HasNext() did not return on attempt %d: a concurrent SetTimeout(0) turned a bounded wait into an unbounded one", attempt)
		}
	}
}
