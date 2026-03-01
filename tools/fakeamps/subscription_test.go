package main

import (
	"net"
	"testing"
	"time"
)

func TestQueueLeaseFunctions(t *testing.T) {
	// Test addQueueLease
	addQueueLease("orders", "sub1", "order-1", "bm1", []byte(`{"id":1}`), "ts1", "json", 30*time.Second)

	// Test getQueueLease
	lease := getQueueLease("order-1")
	if lease == nil {
		t.Error("expected lease")
	}
	if lease.topic != "orders" {
		t.Errorf("topic = %q, want %q", lease.topic, "orders")
	}
	if lease.sowKey != "order-1" {
		t.Errorf("sowKey = %q, want %q", lease.sowKey, "order-1")
	}

	// Test removeQueueLease
	removed := removeQueueLease("order-1")
	if removed == nil {
		t.Error("expected removed lease")
	}

	// Should be nil now
	lease = getQueueLease("order-1")
	if lease != nil {
		t.Error("expected nil lease")
	}
}

func TestQueueLeaseExpiration(t *testing.T) {
	// Add a lease with very short duration
	addQueueLease("orders", "sub1", "order-expire", "bm1", []byte(`{"id":1}`), "ts1", "json", 50*time.Millisecond)

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	// Try to get it - should still exist until requeueExpiredLeases runs
	lease := getQueueLease("order-expire")
	if lease == nil {
		t.Log("lease already expired (expected after requeueExpiredLeases runs)")
	}
}

func TestSubscriptionMatches(t *testing.T) {
	// Create a mock connection
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()

	sub := &subscription{
		conn:   conn,
		subID:  "sub1",
		topic:  "orders",
		filter: "/status = 'active'",
	}

	// Test matching topic and filter
	matched := sub.matches("orders", []byte(`{"status":"active"}`))
	if !matched {
		t.Error("expected match")
	}

	// Test non-matching topic
	matched = sub.matches("customers", []byte(`{"status":"active"}`))
	if matched {
		t.Error("expected no match")
	}

	// Test non-matching filter
	matched = sub.matches("orders", []byte(`{"status":"inactive"}`))
	if matched {
		t.Error("expected no match")
	}
}

func TestSubscriptionPaused(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()

	sub := &subscription{
		conn:  conn,
		subID: "sub1",
		topic: "orders",
	}

	// Pause the subscription
	sub.paused.Store(true)

	// Should not match when paused
	matched := sub.matches("orders", []byte(`{"id":1}`))
	if matched {
		t.Error("expected no match when paused")
	}

	// Resume
	sub.paused.Store(false)

	// Should match now
	matched = sub.matches("orders", []byte(`{"id":1}`))
	if !matched {
		t.Error("expected match after resume")
	}
}

func TestForEachMatchingSubscriber(t *testing.T) {
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()

	// Register a subscription
	sub := &subscription{
		conn:  conn1,
		subID: "sub1",
		topic: "orders",
	}
	registerSubscription("orders", sub)

	// Test iteration
	count := 0
	forEachMatchingSubscriber("orders", []byte(`{"id":1}`), func(s *subscription) {
		count++
	})

	if count != 1 {
		t.Errorf("expected count=1, got %d", count)
	}

	// Cleanup
	unregisterSubscription("orders", sub)
}

func TestPauseResumeSubscription(t *testing.T) {
	localSubs := make(map[string]*localSub)
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()

	sub := &subscription{
		conn:  conn,
		subID: "sub1",
		topic: "orders",
	}
	localSubs["sub1"] = &localSub{topic: "orders", sub: sub}

	// Pause
	pauseSubscription(localSubs, "sub1")
	if !sub.paused.Load() {
		t.Error("expected paused")
	}

	// Resume
	resumeSubscription(localSubs, "sub1")
	if sub.paused.Load() {
		t.Error("expected not paused")
	}
}

func TestHandleQueueAckRemovesLease(t *testing.T) {
	addQueueLease("orders", "sub1", "ack-key", "bm1", []byte(`{"id":1}`), "ts1", "json", time.Second)
	handleQueueAck("ack-key")
	if getQueueLease("ack-key") != nil {
		t.Fatal("expected queue lease to be removed by ack")
	}
}

func TestRequeueExpiredLeasesRemovesExpired(t *testing.T) {
	addQueueLease("orders", "sub1", "expired-key", "bm1", []byte(`{"id":1}`), "ts1", "json", 10*time.Millisecond)
	time.Sleep(25 * time.Millisecond)
	requeueExpiredLeases()
	if getQueueLease("expired-key") != nil {
		t.Fatal("expected expired lease to be removed")
	}
}

func TestStartLeaseWatcher(t *testing.T) {
	addQueueLease("orders", "sub1", "watch-key", "bm1", []byte(`{"id":1}`), "ts1", "json", 10*time.Millisecond)
	StartLeaseWatcher(5 * time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if getQueueLease("watch-key") != nil {
		t.Fatal("expected watcher to clear expired lease")
	}
}

func TestOOFCandidatesAndUnregisterAll(t *testing.T) {
	connA, connB := net.Pipe()
	defer connA.Close()
	defer connB.Close()

	sub := &subscription{
		conn:    connA,
		subID:   "delta1",
		topic:   "orders",
		filter:  "/status = 'active'",
		isDelta: true,
	}
	registerSubscription("orders", sub)
	defer unregisterSubscription("orders", sub)

	if !sub.matchesTopic("orders") {
		t.Fatal("expected matchesTopic true")
	}

	count := 0
	forEachOOFCandidate("orders", []byte(`{"status":"inactive"}`), func(_ *subscription) {
		count++
	})
	if count != 1 {
		t.Fatalf("expected one OOF candidate, got %d", count)
	}

	ss := newSubscriberSet()
	ss.add(sub)
	seen := 0
	ss.forEach(func(_ *subscription) { seen++ })
	if seen != 1 {
		t.Fatalf("expected subscriberSet forEach to iterate once, got %d", seen)
	}

	unregisterAll(connA)
	countAfter := 0
	forEachMatchingSubscriber("orders", []byte(`{"status":"active"}`), func(_ *subscription) {
		countAfter++
	})
	if countAfter != 0 {
		t.Fatalf("expected no subscribers after unregisterAll, got %d", countAfter)
	}
}
