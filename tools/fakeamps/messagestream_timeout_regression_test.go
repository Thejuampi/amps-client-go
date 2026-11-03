package main

import (
	"testing"
	"time"
)

// HasNext() must remain bounded while a live caller adjusts the configured
// timeout between positive values.
//
// Only exported API is used: this file is outside package amps.
func TestHasNextRemainsBoundedDuringConcurrentTimeoutUpdates(t *testing.T) {
	if raceEnabled {
		t.Skip("covered deterministically in the amps package; skip slow integration stress under -race")
	}

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
			stream.SetTimeout(25)
			stream.SetTimeout(50)
		}
	}()
	defer func() {
		close(stopFlipper)
		<-flipperDone
	}()

	for attempt := 0; attempt < 40; attempt++ {
		stream.SetTimeout(50)

		var returned = make(chan struct{})
		go func() {
			defer close(returned)
			stream.HasNext()
		}()

		select {
		case <-returned:
		case <-time.After(3 * time.Second):
			t.Fatalf("HasNext() did not return during bounded timeout update on attempt %d", attempt)
		}
		if message := stream.Next(); message != nil {
			t.Fatalf("Next() returned a message on quiet topic during attempt %d", attempt)
		}
	}
}
