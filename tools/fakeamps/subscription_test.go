package main

import (
	"encoding/binary"
	"io"
	"net"
	"strings"
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

func TestHandleQueueAckByBookmarkRemovesLease(t *testing.T) {
	addQueueLease("orders", "sub1", "ack-bm-key", "bm-ack", []byte(`{"id":1}`), "ts1", "json", time.Second)
	handleQueueAckByBookmark("bm-ack")
	if getQueueLease("ack-bm-key") != nil {
		t.Fatal("expected queue lease to be removed by bookmark ack")
	}
}

func TestRequeueMessagePrefersDifferentQueueSubscriber(t *testing.T) {
	var topic = "queue://orders.requeue"

	var connA, peerA = net.Pipe()
	var connB, peerB = net.Pipe()
	defer connA.Close()
	defer peerA.Close()
	defer connB.Close()
	defer peerB.Close()

	var stats connStats
	var writerA = newConnWriter(connA, &stats)
	var writerB = newConnWriter(connB, &stats)
	defer writerA.close()
	defer writerB.close()

	var subA = &subscription{subID: "subA", topic: topic, writer: writerA, isQueue: true}
	var subB = &subscription{subID: "subB", topic: topic, writer: writerB, isQueue: true}
	registerSubscription(topic, subA)
	registerSubscription(topic, subB)
	defer unregisterSubscription(topic, subA)
	defer unregisterSubscription(topic, subB)

	var lease = &queueLease{
		topic:       topic,
		subID:       "subA",
		sowKey:      "requeue-key",
		bookmark:    "bm1",
		payload:     []byte(`{"id":1}`),
		timestamp:   "ts1",
		messageType: "json",
		leasePeriod: time.Second,
	}
	requeueMessage(lease)

	var body = readFrameBody(t, peerB, 500*time.Millisecond)
	if !strings.Contains(body, `"c":"p"`) || !strings.Contains(body, `"sub_id":"subB"`) {
		t.Fatalf("expected requeued delivery to alternate queue subscriber, got %s", body)
	}

	var tracked = getQueueLease("requeue-key")
	if tracked == nil || tracked.subID != "subB" {
		t.Fatalf("expected lease to be tracked against alternate subscriber")
	}

	removeQueueLease("requeue-key")
}

func TestRequeueMessageRoundRobinAcrossQueueSubscribers(t *testing.T) {
	var topic = "queue://orders.roundrobin"

	var connA, peerA = net.Pipe()
	var connB, peerB = net.Pipe()
	var connC, peerC = net.Pipe()
	defer connA.Close()
	defer peerA.Close()
	defer connB.Close()
	defer peerB.Close()
	defer connC.Close()
	defer peerC.Close()

	var stats connStats
	var writerA = newConnWriter(connA, &stats)
	var writerB = newConnWriter(connB, &stats)
	var writerC = newConnWriter(connC, &stats)
	defer writerA.close()
	defer writerB.close()
	defer writerC.close()

	var subA = &subscription{subID: "subA", topic: topic, writer: writerA, isQueue: true}
	var subB = &subscription{subID: "subB", topic: topic, writer: writerB, isQueue: true}
	var subC = &subscription{subID: "subC", topic: topic, writer: writerC, isQueue: true}
	registerSubscription(topic, subA)
	registerSubscription(topic, subB)
	registerSubscription(topic, subC)
	defer unregisterSubscription(topic, subA)
	defer unregisterSubscription(topic, subB)
	defer unregisterSubscription(topic, subC)

	var leaseOne = &queueLease{
		topic:       topic,
		subID:       "subA",
		sowKey:      "rr-key-1",
		bookmark:    "bm-1",
		payload:     []byte(`{"id":1}`),
		timestamp:   "ts1",
		messageType: "json",
		leasePeriod: time.Second,
	}

	requeueMessage(leaseOne)
	var firstBody, okFirst = tryReadFrameBodyFromConn(peerB, 100*time.Millisecond)
	if !okFirst {
		firstBody, okFirst = tryReadFrameBodyFromConn(peerC, 200*time.Millisecond)
	}
	if !okFirst {
		t.Fatal("expected first round-robin redelivery")
	}
	if !strings.Contains(firstBody, `"sub_id":"subB"`) {
		t.Fatalf("expected first redelivery on subB, got %s", firstBody)
	}
	removeQueueLease("rr-key-1")

	var leaseTwo = &queueLease{
		topic:       topic,
		subID:       "subA",
		sowKey:      "rr-key-2",
		bookmark:    "bm-2",
		payload:     []byte(`{"id":2}`),
		timestamp:   "ts2",
		messageType: "json",
		leasePeriod: time.Second,
	}

	requeueMessage(leaseTwo)
	var secondBody, okSecond = tryReadFrameBodyFromConn(peerB, 100*time.Millisecond)
	if !okSecond {
		secondBody, okSecond = tryReadFrameBodyFromConn(peerC, 200*time.Millisecond)
	}
	if !okSecond {
		t.Fatal("expected second round-robin redelivery")
	}
	if !strings.Contains(secondBody, `"sub_id":"subC"`) {
		t.Fatalf("expected second redelivery on subC, got %s", secondBody)
	}
	removeQueueLease("rr-key-2")
}

func tryReadFrameBodyFromConn(conn net.Conn, timeout time.Duration) (string, bool) {
	var err = conn.SetReadDeadline(time.Now().Add(timeout))
	if err != nil {
		return "", false
	}

	var lenBuf [4]byte
	_, err = io.ReadFull(conn, lenBuf[:])
	if err != nil {
		_ = conn.SetReadDeadline(time.Time{})
		return "", false
	}

	var frameLen = binary.BigEndian.Uint32(lenBuf[:])
	var frame = make([]byte, frameLen)
	_, err = io.ReadFull(conn, frame)
	if err != nil {
		_ = conn.SetReadDeadline(time.Time{})
		return "", false
	}

	_ = conn.SetReadDeadline(time.Time{})
	return string(frame), true
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
