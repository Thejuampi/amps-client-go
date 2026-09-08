package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

// A conflating MessageStream is reconfigured through its exported API while the
// subscription is still delivering messages.
//
// isConflating() takes and RELEASES the stream lock before messageHandler
// re-acquires it to write ms.sowKeyMap[sowKey], and every exported mutator
// (SetSubscription/SetSOWOnly/SetStatsOnly/SetAcksOnly) nils that map via
// resetForConfiguration. The write therefore landed on a nil map and panicked
// on the client receive goroutine, which has no recover, taking down the whole
// process.
//
// Only exported API is used: this file is outside package amps.
func TestConflatingStreamSurvivesReconfigurationDuringDelivery(t *testing.T) {
	var oldSow = sow
	var oldJournal = journal
	sow = newSOWCache()
	journal = newMessageJournal(1000)
	t.Cleanup(func() {
		sow = oldSow
		journal = oldJournal
	})

	var harness = startDeliveryHarness(t)
	var subscriber = harness.newClient(t, "conflate-subscriber")
	var publisher = harness.newClient(t, "conflate-publisher")

	// Seed the SOW so delivered messages carry a sow key, which is what selects
	// the conflation branch in messageHandler.
	for index := 0; index < 8; index++ {
		if err := publisher.Publish("orders", fmt.Sprintf(`{"id":%d,"v":0}`, index)); err != nil {
			t.Fatalf("seed publish failed: %v", err)
		}
	}
	time.Sleep(200 * time.Millisecond)

	var stream, err = subscriber.Execute(
		amps.NewCommand("sow_and_subscribe").SetTopic("orders").SetOptions("conflation=none"),
	)
	if err != nil {
		t.Fatalf("sow_and_subscribe failed: %v", err)
	}
	stream.SetTimeout(250)

	// PRECONDITION: this test is worthless unless delivered messages actually
	// carry a sow key. Fail loudly rather than pass vacuously.
	var sawSowKey bool
	var deadline = time.Now().Add(5 * time.Second)
	for !sawSowKey && time.Now().Before(deadline) {
		if !stream.HasNext() {
			continue
		}
		var message = stream.Next()
		if message == nil {
			continue
		}
		if key, ok := message.SowKey(); ok && key != "" {
			sawSowKey = true
		}
	}
	if !sawSowKey {
		t.Fatalf("no delivered message carried a sow key: the conflation path would never be exercised")
	}

	stream.Conflate()

	var waitGroup sync.WaitGroup
	var stop = make(chan struct{})
	waitGroup.Add(2)

	go func() {
		defer waitGroup.Done()
		for index := 0; ; index++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = publisher.Publish("orders", fmt.Sprintf(`{"id":%d,"v":%d}`, index%8, index))
		}
	}()
	go func() {
		defer waitGroup.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			stream.SetSubscription("orders-route", "orders-unsub")
			stream.Conflate()
		}
	}()

	time.Sleep(3 * time.Second)
	close(stop)
	waitGroup.Wait()
}
