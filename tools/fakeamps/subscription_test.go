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
		t.Fatal("expected lease")
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

func TestHandleQueueAckBatchRemovesMultipleLeases(t *testing.T) {
	addQueueLease("orders", "sub1", "ack-batch-1", "bm-batch-1", []byte(`{"id":1}`), "ts1", "json", time.Second)
	addQueueLease("orders", "sub1", "ack-batch-2", "bm-batch-2", []byte(`{"id":2}`), "ts2", "json", time.Second)

	var released = handleQueueAckBatch("ack-batch-1, ack-batch-2")
	if len(released) != 2 {
		t.Fatalf("expected two released leases, got %d", len(released))
	}
	if getQueueLease("ack-batch-1") != nil || getQueueLease("ack-batch-2") != nil {
		t.Fatal("expected queue leases to be removed by batched key ack")
	}
}

func TestHandleQueueAckByBookmarkBatchRemovesMultipleLeases(t *testing.T) {
	addQueueLease("orders", "sub1", "ack-bm-batch-1", "bm-batch-a", []byte(`{"id":1}`), "ts1", "json", time.Second)
	addQueueLease("orders", "sub1", "ack-bm-batch-2", "bm-batch-b", []byte(`{"id":2}`), "ts2", "json", time.Second)

	var released = handleQueueAckByBookmarkBatch("bm-batch-a, bm-batch-b")
	if len(released) != 2 {
		t.Fatalf("expected two released leases, got %d", len(released))
	}
	if getQueueLease("ack-bm-batch-1") != nil || getQueueLease("ack-bm-batch-2") != nil {
		t.Fatal("expected queue leases to be removed by batched bookmark ack")
	}
}

func TestReleaseQueueLeasesForSubscriptionsMatchesTopicAndSubID(t *testing.T) {
	var connA, peerA = net.Pipe()
	defer connA.Close()
	defer peerA.Close()

	var topicA = "queue://orders.a"
	var topicB = "queue://orders.b"
	var localSubs = map[string]*localSub{
		"shared-sub": {
			topic: topicA,
			sub: &subscription{
				conn:    connA,
				subID:   "shared-sub",
				topic:   topicA,
				isQueue: true,
			},
		},
	}

	addQueueLease(topicA, "shared-sub", "release-a", "bm-a", []byte(`{"id":1}`), "ts-a", "json", time.Second)
	addQueueLease(topicB, "shared-sub", "keep-b", "bm-b", []byte(`{"id":2}`), "ts-b", "json", time.Second)
	defer removeQueueLease("release-a")
	defer removeQueueLease("keep-b")

	var released = releaseQueueLeasesForSubscriptions(localSubs)
	if len(released) != 1 || released[0].sowKey != "release-a" {
		t.Fatalf("expected only matching topic/sub lease to be released, got %#v", released)
	}
	if getQueueLease("release-a") != nil {
		t.Fatalf("expected matching lease to be removed from tracking")
	}
	if getQueueLease("keep-b") == nil {
		t.Fatalf("expected non-matching topic lease to remain tracked")
	}
}

func TestReleaseQueueLeasesForSubscriptionsMatchesConnectionIdentity(t *testing.T) {
	var connA, peerA = net.Pipe()
	var connB, peerB = net.Pipe()
	defer connA.Close()
	defer peerA.Close()
	defer connB.Close()
	defer peerB.Close()

	var topic = "queue://orders.shared"
	var localSubs = map[string]*localSub{
		"shared-sub": {
			topic: topic,
			sub: &subscription{
				conn:    connA,
				subID:   "shared-sub",
				topic:   topic,
				isQueue: true,
			},
		},
	}

	addQueueLease(topic, "shared-sub", "release-a", "bm-a", []byte(`{"id":1}`), "ts-a", "json", time.Second)
	addQueueLease(topic, "shared-sub", "keep-b", "bm-b", []byte(`{"id":2}`), "ts-b", "json", time.Second)

	var lease = getQueueLease("keep-b")
	if lease == nil {
		t.Fatalf("expected second lease to exist")
	}
	lease.ownerConn = connB

	defer removeQueueLease("release-a")
	defer removeQueueLease("keep-b")

	var released = releaseQueueLeasesForSubscriptions(localSubs)
	if len(released) != 1 || released[0].sowKey != "release-a" {
		t.Fatalf("expected only same-connection lease to be released, got %#v", released)
	}
	if getQueueLease("keep-b") == nil {
		t.Fatalf("expected different-connection lease to remain tracked")
	}
	if getQueueLease("release-a") != nil {
		t.Fatalf("expected released lease to be removed from tracking")
	}
	_ = connB
	_ = peerB
}

func TestQueueSubCanAcceptIgnoresOtherConnectionWithSameSubID(t *testing.T) {
	var connA, peerA = net.Pipe()
	var connB, peerB = net.Pipe()
	defer connA.Close()
	defer peerA.Close()
	defer connB.Close()
	defer peerB.Close()

	var subA = &subscription{conn: connA, subID: "shared-sub", topic: "queue://orders.shared", isQueue: true, maxBacklog: 1}
	var subB = &subscription{conn: connB, subID: "shared-sub", topic: "queue://orders.shared", isQueue: true, maxBacklog: 1}

	addQueueLease(subA.topic, subA.subID, "shared-key", "bm-shared", []byte(`{"id":1}`), "ts1", "json", time.Second)
	defer removeQueueLease("shared-key")

	var lease = getQueueLease("shared-key")
	if lease == nil {
		t.Fatalf("expected shared lease to exist")
	}
	lease.ownerConn = connA

	if queueSubCanAccept(subB) == false {
		t.Fatalf("expected backlog accounting to ignore lease owned by another connection with same sub_id")
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

func TestRequeueMessageSkipsSubscribersWhoseFiltersRejectPayload(t *testing.T) {
	var topic = "queue://orders.requeue.filter"

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
	var subB = &subscription{subID: "subB", topic: topic, filter: `/kind = 'match'`, writer: writerB, isQueue: true}
	registerSubscription(topic, subA)
	registerSubscription(topic, subB)
	defer unregisterSubscription(topic, subA)
	defer unregisterSubscription(topic, subB)
	defer removeQueueLease("requeue-filter-key")
	defer func() {
		queuePendingMu.Lock()
		delete(queuePending, topic)
		queuePendingMu.Unlock()
		queueRedeliveryMu.Lock()
		delete(queueRedeliveryCursor, topic)
		queueRedeliveryMu.Unlock()
	}()

	var lease = &queueLease{
		topic:       topic,
		subID:       "subA",
		sowKey:      "requeue-filter-key",
		bookmark:    "bm-filter",
		payload:     []byte(`{"kind":"different"}`),
		timestamp:   "ts1",
		messageType: "json",
		leasePeriod: time.Second,
	}

	requeueMessage(lease)

	if body, ok := tryReadFrameBodyFromConn(peerB, 100*time.Millisecond); ok {
		t.Fatalf("expected filter-mismatched subscriber to receive no redelivery, got %s", body)
	}

	var pending = popPendingQueueMessage(topic)
	if pending == nil || pending.sowKey != "requeue-filter-key" {
		t.Fatalf("expected rejected redelivery to remain pending, got %#v", pending)
	}
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

func TestRequeueMessageQueuesPendingWhenNoSubscriberAvailable(t *testing.T) {
	var topic = "queue://orders.pending"
	defer func() {
		queuePendingMu.Lock()
		delete(queuePending, topic)
		queuePendingMu.Unlock()
	}()

	var lease = &queueLease{
		topic:       topic,
		subID:       "subA",
		sowKey:      "pending-key",
		bookmark:    "bm-pending",
		payload:     []byte(`{"id":1}`),
		timestamp:   "ts1",
		messageType: "json",
		leasePeriod: time.Second,
	}

	requeueMessage(lease)

	var pending = popPendingQueueMessage(topic)
	if pending == nil || pending.sowKey != "pending-key" {
		t.Fatalf("expected expired lease to be preserved as pending queue message")
	}
	if pending.bookmark != "bm-pending" || pending.messageType != "json" || string(pending.payload) != `{"id":1}` {
		t.Fatalf("expected pending queue message to preserve lease delivery metadata")
	}
}

func TestPendingRequeueDoesNotApplyOnDeliverTwice(t *testing.T) {
	resetActionsForTest()
	defer resetActionsForTest()

	registerAction(actionDef{
		trigger:    triggerOnDeliver,
		topicMatch: "queue://orders.pending.transform",
		action:     actionTransform,
		target:     "add_timestamp",
	})

	var topic = "queue://orders.pending.transform"
	defer func() {
		queuePendingMu.Lock()
		delete(queuePending, topic)
		queuePendingMu.Unlock()
		removeQueueLease("transform-key")
	}()

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

	var delivered = deliverQueueMessageToSubscription(&queuePendingMessage{
		topic:       topic,
		sowKey:      "transform-key",
		bookmark:    "bm-transform",
		payload:     []byte(`{"id":1}`),
		timestamp:   "ts1",
		messageType: "json",
	}, subA)
	if !delivered {
		t.Fatalf("expected initial queue delivery to succeed")
	}

	var initialBody = readFrameBody(t, peerA, 500*time.Millisecond)
	if strings.Count(initialBody, `"_delivered_at"`) != 1 {
		t.Fatalf("expected initial queue delivery to apply on-deliver once, got %s", initialBody)
	}

	var lease = removeQueueLease("transform-key")
	if lease == nil {
		t.Fatalf("expected initial delivery to create queue lease")
	}

	requeueMessage(lease)
	registerSubscription(topic, subB)
	defer unregisterSubscription(topic, subB)
	dispatchPendingQueueTopic(topic)

	var requeuedBody = readFrameBody(t, peerB, 500*time.Millisecond)
	if strings.Count(requeuedBody, `"_delivered_at"`) != 1 {
		t.Fatalf("expected pending requeue to preserve single on-deliver transform, got %s", requeuedBody)
	}
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
