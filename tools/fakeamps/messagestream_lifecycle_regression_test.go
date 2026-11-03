package main

import (
	"sync"
	"testing"
)

// SetSubscription writes commandID/unsubscribeID/queryID with no lock, while
// Close() reads and writes the same three fields under lifecycleLock. Close()
// reads commandID and unsubscribeID as separate unsynchronised loads and then
// unsubscribes using them, so a torn pair unsubscribes one route while blanking
// another - the correct route is then never unsubscribed and leaks.
//
// The client reaches Close() on its own teardown (clearRoutes -> deleteRoute),
// so an application reconfiguring a stream while the connection drops hits this.
//
// Run under -race. Only exported API is used: this file is outside package amps.
func TestStreamReconfigureAndCloseAreSafeConcurrently(t *testing.T) {
	var harness = startDeliveryHarness(t)
	var subscriber = harness.newClient(t, "stream-lifecycle-race")

	var stream, err = subscriber.Subscribe("orders")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	var waitGroup sync.WaitGroup
	waitGroup.Add(2)

	go func() {
		defer waitGroup.Done()
		for attempt := 0; attempt < 300; attempt++ {
			stream.SetSubscription("orders-route", "orders-unsub")
		}
	}()
	go func() {
		defer waitGroup.Done()
		for attempt := 0; attempt < 300; attempt++ {
			_ = stream.Close()
		}
	}()

	waitGroup.Wait()
}
