package main

import (
	"net"
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

var (
	queueLeasesMu sync.RWMutex
	queueLeases   = make(map[string]*queueLease) // sowKey -> lease
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
	// Re-deliver the message to the same subscriber.
	buf := getWriteBuf()
	defer putWriteBuf(buf)

	topicSubscribers.Range(func(key, value interface{}) bool {
		ss := value.(*subscriberSet)
		ss.mu.RLock()
		for sub := range ss.subs {
			if sub.subID == lease.subID && sub.topic == lease.topic {
				if sub.paused.Load() {
					continue
				}
				frame := buildPublishDelivery(buf, lease.topic, lease.subID,
					lease.payload, lease.bookmark, lease.timestamp, lease.sowKey, lease.messageType, true)
				sub.writer.send(frame)
			}
		}
		ss.mu.RUnlock()
		return true
	})
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

func handleQueueAck(sowKey string) {
	// Remove the lease when client acknowledges.
	removeQueueLease(sowKey)
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
