package main

import (
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// subscription — represents a client subscription registration.
// ---------------------------------------------------------------------------

type subscription struct {
	conn       net.Conn
	subID      string
	topic      string // the subscription pattern (may include wildcards)
	filter     string // content filter expression (e.g. "/id > 10")
	writer     *connWriter
	isQueue    bool
	isBookmark bool // options contained "bookmark"
	isDelta    bool // delta_subscribe or sow_and_delta_subscribe
	paused     atomic.Bool
	maxBacklog int
	pullMode   bool

	// Queue lease tracking
	leaseDuration time.Duration // lease period for queue messages
}

// ---------------------------------------------------------------------------
// Queue lease tracking — tracks delivered but unacknowledged messages.
// ---------------------------------------------------------------------------

type queueLease struct {
	topic       string
	subID       string
	sowKey      string
	bookmark    string
	payload     []byte
	timestamp   string
	messageType string
	deliveredAt time.Time
	leasePeriod time.Duration
}

type queuePendingMessage struct {
	topic       string
	sowKey      string
	bookmark    string
	payload     []byte
	timestamp   string
	messageType string
}

var (
	queueLeasesMu sync.RWMutex
	queueLeases   = make(map[string]*queueLease) // sowKey -> lease

	queueRedeliveryMu     sync.Mutex
	queueRedeliveryCursor = make(map[string]int) // topic -> round-robin cursor

	queueDeliveryMu     sync.Mutex
	queueDeliveryCursor = make(map[string]int) // topic -> round-robin cursor

	queuePendingMu sync.Mutex
	queuePending   = make(map[string][]*queuePendingMessage) // topic -> pending queue messages
)

func addQueueLease(topic, subID, sowKey, bookmark string, payload []byte, timestamp, messageType string, leasePeriod time.Duration) {
	queueLeasesMu.Lock()
	defer queueLeasesMu.Unlock()

	queueLeases[sowKey] = &queueLease{
		topic:       topic,
		subID:       subID,
		sowKey:      sowKey,
		bookmark:    bookmark,
		payload:     payload,
		timestamp:   timestamp,
		messageType: messageType,
		deliveredAt: time.Now(),
		leasePeriod: leasePeriod,
	}
}

func removeQueueLease(sowKey string) *queueLease {
	queueLeasesMu.Lock()
	defer queueLeasesMu.Unlock()

	lease := queueLeases[sowKey]
	delete(queueLeases, sowKey)
	return lease
}

func removeQueueLeaseByBookmark(bookmark string) *queueLease {
	queueLeasesMu.Lock()
	defer queueLeasesMu.Unlock()

	var key string
	for key = range queueLeases {
		var lease = queueLeases[key]
		if lease != nil && lease.bookmark == bookmark {
			delete(queueLeases, key)
			return lease
		}
	}

	return nil
}

func getQueueLease(sowKey string) *queueLease {
	queueLeasesMu.RLock()
	defer queueLeasesMu.RUnlock()
	return queueLeases[sowKey]
}

func requeueExpiredLeases() {
	queueLeasesMu.Lock()
	defer queueLeasesMu.Unlock()

	now := time.Now()
	for key, lease := range queueLeases {
		if now.After(lease.deliveredAt.Add(lease.leasePeriod)) {
			// Lease expired - requeue the message.
			delete(queueLeases, key)
			go requeueMessage(lease)
		}
	}
}

func requeueMessage(lease *queueLease) {
	if lease == nil {
		return
	}

	var target = selectQueueRedeliveryTarget(lease)
	if target == nil {
		return
	}

	buf := getWriteBuf()
	defer putWriteBuf(buf)
	var frame = buildPublishDelivery(buf, lease.topic, target.subID,
		lease.payload, lease.bookmark, lease.timestamp, lease.sowKey, lease.messageType, true)
	target.writer.send(frame)

	addQueueLease(lease.topic, target.subID, lease.sowKey, lease.bookmark, lease.payload, lease.timestamp, lease.messageType, lease.leasePeriod)
}

func queueInFlightCountForSub(subID string) int {
	if subID == "" {
		return 0
	}

	var count int
	queueLeasesMu.RLock()
	for _, lease := range queueLeases {
		if lease != nil && lease.subID == subID {
			count++
		}
	}
	queueLeasesMu.RUnlock()
	return count
}

func queueSubCanAccept(sub *subscription) bool {
	if sub == nil {
		return false
	}

	if sub.maxBacklog <= 0 {
		return true
	}

	return queueInFlightCountForSub(sub.subID) < sub.maxBacklog
}

func selectQueuePublishTarget(topic string, payload []byte, publisher net.Conn) *subscription {
	var eligible []*subscription

	topicSubscribers.Range(func(_ interface{}, value interface{}) bool {
		var ss = value.(*subscriberSet)
		ss.mu.RLock()
		for sub := range ss.subs {
			if !sub.isQueue {
				continue
			}
			if sub.pullMode {
				continue
			}
			if sub.paused.Load() {
				continue
			}
			if publisher != nil && !*flagEcho && sub.conn == publisher {
				continue
			}
			if !topicMatches(topic, sub.topic) {
				continue
			}
			if sub.filter != "" && len(payload) > 0 && !evaluateFilter(sub.filter, payload) {
				continue
			}
			if !queueSubCanAccept(sub) {
				continue
			}
			eligible = append(eligible, sub)
		}
		ss.mu.RUnlock()
		return true
	})

	if len(eligible) == 0 {
		return nil
	}

	sort.Slice(eligible, func(i, j int) bool {
		return eligible[i].subID < eligible[j].subID
	})

	queueDeliveryMu.Lock()
	defer queueDeliveryMu.Unlock()

	var topicCursor = queueDeliveryCursor[topic]
	if topicCursor < 0 {
		topicCursor = 0
	}

	var index = topicCursor % len(eligible)
	var target = eligible[index]
	queueDeliveryCursor[topic] = (index + 1) % len(eligible)
	return target
}

func enqueuePendingQueueMessage(topic, sowKey, bookmark string, payload []byte, timestamp, messageType string) {
	if topic == "" {
		return
	}

	var payloadCopy = make([]byte, len(payload))
	copy(payloadCopy, payload)

	var pending = &queuePendingMessage{
		topic:       topic,
		sowKey:      sowKey,
		bookmark:    bookmark,
		payload:     payloadCopy,
		timestamp:   timestamp,
		messageType: messageType,
	}

	queuePendingMu.Lock()
	queuePending[topic] = append(queuePending[topic], pending)
	queuePendingMu.Unlock()
}

func popPendingQueueMessage(topic string) *queuePendingMessage {
	queuePendingMu.Lock()
	defer queuePendingMu.Unlock()

	var messages = queuePending[topic]
	if len(messages) == 0 {
		return nil
	}

	var next = messages[0]
	if len(messages) == 1 {
		delete(queuePending, topic)
	} else {
		queuePending[topic] = messages[1:]
	}

	return next
}

func prependPendingQueueMessage(topic string, message *queuePendingMessage) {
	if message == nil {
		return
	}

	queuePendingMu.Lock()
	queuePending[topic] = append([]*queuePendingMessage{message}, queuePending[topic]...)
	queuePendingMu.Unlock()
}

func deliverQueueMessageToSubscription(message *queuePendingMessage, sub *subscription) bool {
	if message == nil || sub == nil {
		return false
	}

	if !sub.matches(message.topic, message.payload) {
		return false
	}

	if !queueSubCanAccept(sub) {
		return false
	}

	var deliveryPayload = fireOnDeliver(message.topic, message.payload, sub.subID)
	var buf = getWriteBuf()
	var frame = buildPublishDelivery(buf, message.topic, sub.subID,
		deliveryPayload, message.bookmark, message.timestamp, message.sowKey, message.messageType, true)
	putWriteBuf(buf)
	sub.writer.send(frame)

	var leasePeriod = *flagLease
	if sub.leaseDuration > 0 {
		leasePeriod = sub.leaseDuration
	}
	addQueueLease(message.topic, sub.subID, message.sowKey, message.bookmark, deliveryPayload, message.timestamp, message.messageType, leasePeriod)
	return true
}

func dispatchPendingQueueTopic(topic string) {
	if topic == "" {
		return
	}

	for {
		var message = popPendingQueueMessage(topic)
		if message == nil {
			return
		}

		var target = selectQueuePublishTarget(topic, message.payload, nil)
		if target == nil {
			prependPendingQueueMessage(topic, message)
			return
		}

		if !deliverQueueMessageToSubscription(message, target) {
			prependPendingQueueMessage(topic, message)
			return
		}
	}
}

func pullQueueMessagesForSubscription(topic string, sub *subscription, limit int) int {
	if sub == nil || topic == "" || limit <= 0 {
		return 0
	}

	var delivered int
	for delivered < limit {
		var message = popPendingQueueMessage(topic)
		if message == nil {
			break
		}

		if !deliverQueueMessageToSubscription(message, sub) {
			prependPendingQueueMessage(topic, message)
			break
		}

		delivered++
	}

	return delivered
}

func selectQueueRedeliveryTarget(lease *queueLease) *subscription {
	var eligible []*subscription

	topicSubscribers.Range(func(_ interface{}, value interface{}) bool {
		var ss = value.(*subscriberSet)
		ss.mu.RLock()
		for sub := range ss.subs {
			if !sub.isQueue {
				continue
			}
			if sub.paused.Load() {
				continue
			}
			if !topicMatches(lease.topic, sub.topic) {
				continue
			}
			eligible = append(eligible, sub)
		}
		ss.mu.RUnlock()
		return true
	})

	if len(eligible) == 0 {
		return nil
	}

	sort.Slice(eligible, func(i, j int) bool {
		return eligible[i].subID < eligible[j].subID
	})

	queueRedeliveryMu.Lock()
	defer queueRedeliveryMu.Unlock()

	var topicCursor = queueRedeliveryCursor[lease.topic]
	if topicCursor < 0 {
		topicCursor = 0
	}

	var base = topicCursor % len(eligible)
	var offset int
	for offset = 0; offset < len(eligible); offset++ {
		var index = (base + offset) % len(eligible)
		if eligible[index].subID == lease.subID {
			continue
		}
		if eligible[index].pullMode {
			continue
		}
		if !queueSubCanAccept(eligible[index]) {
			continue
		}
		queueRedeliveryCursor[lease.topic] = (index + 1) % len(eligible)
		return eligible[index]
	}

	queueRedeliveryCursor[lease.topic] = (base + 1) % len(eligible)
	return nil
}

// StartLeaseWatcher starts a background goroutine to check for expired leases.
func StartLeaseWatcher(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			requeueExpiredLeases()
		}
	}()
}

// ---------------------------------------------------------------------------
// Acknowledgment handling for queues.
// ---------------------------------------------------------------------------

func handleQueueAck(sowKey string) *queueLease {
	// Remove the lease when client acknowledges.
	return removeQueueLease(sowKey)
}

func handleQueueAckByBookmark(bookmark string) *queueLease {
	return removeQueueLeaseByBookmark(bookmark)
}

// matches returns true if a published message should be delivered to this sub.
func (sub *subscription) matches(publishTopic string, payload []byte) bool {
	// Paused subscriptions don't receive messages.
	if sub.paused.Load() {
		return false
	}
	// Check topic match (exact or wildcard).
	if !topicMatches(publishTopic, sub.topic) {
		return false
	}
	// Check content filter.
	if sub.filter != "" && len(payload) > 0 {
		return evaluateFilter(sub.filter, payload)
	}
	return true
}

// matchesTopic returns true if the topic pattern matches, regardless of
// content filter. Used for OOF-on-filter-mismatch: the topic matches
// but the content filter does not.
func (sub *subscription) matchesTopic(publishTopic string) bool {
	return topicMatches(publishTopic, sub.topic)
}

// ---------------------------------------------------------------------------
// subscriberSet — per-topic set of active subscribers.
//
// Uses RWMutex: writes (register/unregister) are infrequent; reads
// (fan-out iteration) are the hot path.
// ---------------------------------------------------------------------------

type subscriberSet struct {
	mu   sync.RWMutex
	subs map[*subscription]struct{}
}

func newSubscriberSet() *subscriberSet {
	return &subscriberSet{subs: make(map[*subscription]struct{})}
}

func (ss *subscriberSet) add(sub *subscription) {
	ss.mu.Lock()
	ss.subs[sub] = struct{}{}
	ss.mu.Unlock()
}

func (ss *subscriberSet) remove(sub *subscription) {
	ss.mu.Lock()
	delete(ss.subs, sub)
	ss.mu.Unlock()
}

func (ss *subscriberSet) forEach(fn func(*subscription)) {
	ss.mu.RLock()
	for sub := range ss.subs {
		fn(sub)
	}
	ss.mu.RUnlock()
}

// ---------------------------------------------------------------------------
// Global topic → *subscriberSet registry.
//
// For wildcard subscriptions, subscribers are stored under their pattern
// topic. Fan-out iterates all subscriber sets to check matches.
// ---------------------------------------------------------------------------

var topicSubscribers sync.Map // string → *subscriberSet

func getOrCreateSubscriberSet(topic string) *subscriberSet {
	actual, _ := topicSubscribers.LoadOrStore(topic, newSubscriberSet())
	return actual.(*subscriberSet)
}

func registerSubscription(topic string, sub *subscription) {
	getOrCreateSubscriberSet(topic).add(sub)
}

func unregisterSubscription(topic string, sub *subscription) {
	if ss, ok := topicSubscribers.Load(topic); ok {
		ss.(*subscriberSet).remove(sub)
	}
}

func unregisterAll(conn net.Conn) {
	topicSubscribers.Range(func(key, value interface{}) bool {
		ss := value.(*subscriberSet)
		ss.mu.Lock()
		for sub := range ss.subs {
			if sub.conn == conn {
				delete(ss.subs, sub)
			}
		}
		ss.mu.Unlock()
		return true
	})
}

// forEachMatchingSubscriber calls fn for every subscriber that matches
// the given publish topic and payload. It scans all subscriber sets to
// support wildcard topic patterns and content filters.
func forEachMatchingSubscriber(publishTopic string, payload []byte, fn func(*subscription)) {
	topicSubscribers.Range(func(key, value interface{}) bool {
		ss := value.(*subscriberSet)
		ss.mu.RLock()
		for sub := range ss.subs {
			if sub.matches(publishTopic, payload) {
				fn(sub)
			}
		}
		ss.mu.RUnlock()
		return true
	})
}

// forEachOOFCandidate calls fn for every delta subscriber whose topic matches
// but whose content filter does NOT match (or the record was deleted).
// This is used for OOF-on-filter-mismatch.
func forEachOOFCandidate(publishTopic string, payload []byte, fn func(*subscription)) {
	topicSubscribers.Range(func(key, value interface{}) bool {
		ss := value.(*subscriberSet)
		ss.mu.RLock()
		for sub := range ss.subs {
			if !sub.isDelta {
				continue
			}
			if sub.paused.Load() {
				continue
			}
			// Topic must match but content filter must NOT match.
			if sub.matchesTopic(publishTopic) {
				if sub.filter != "" && len(payload) > 0 && !evaluateFilter(sub.filter, payload) {
					fn(sub)
				}
			}
		}
		ss.mu.RUnlock()
		return true
	})
}

// pauseSubscription pauses a subscription by subID in the local subs map.
func pauseSubscription(localSubs map[string]*localSub, subID string) {
	if ls, ok := localSubs[subID]; ok {
		ls.sub.paused.Store(true)
	}
}

// resumeSubscription resumes a paused subscription.
func resumeSubscription(localSubs map[string]*localSub, subID string) {
	if ls, ok := localSubs[subID]; ok {
		ls.sub.paused.Store(false)
	}
}

// ---------------------------------------------------------------------------
// localSub — per-connection subscription tracking for unsubscribe cleanup.
// ---------------------------------------------------------------------------

type localSub struct {
	topic string
	sub   *subscription
}
