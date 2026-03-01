package main

import (
	"bytes"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Conflation — merge pending messages for slow consumers.
//
// Real AMPS supports per-subscription conflation: when a subscriber falls
// behind, instead of dropping messages or disconnecting, AMPS merges
// updates to the same SOW key within a conflation window and sends only
// the latest value. This dramatically reduces backlog for slow consumers
// while preserving data correctness.
//
// Configuration: subscribe with options "conflation=100ms" to enable a
// 100ms conflation window on that subscription.
//
// Advanced: "conflation_key=/field" uses a specific JSON field as the
// merge key instead of the SOW key.
//
// Architecture:
//   - Each conflated subscription gets a conflationBuffer
//   - Incoming messages are stored in a map keyed by topic+conflationKey
//   - A timer goroutine flushes the buffer at the configured interval
//   - On flush, only the latest value per key is delivered
// ---------------------------------------------------------------------------

type conflationBuffer struct {
	mu            sync.Mutex
	interval      time.Duration
	pending       map[string]*conflationEntry // key = topic + "\x00" + conflationKey
	sub           *subscription
	timer         *time.Timer
	stopped       bool
	conflationKey string // JSON field to use as conflation key (empty = sowKey)
}

type conflationEntry struct {
	topic       string
	subID       string
	sowKey      string
	bookmark    string
	timestamp   string
	messageType string
	payload     []byte
	isQueue     bool
}

func newConflationBuffer(sub *subscription, interval time.Duration) *conflationBuffer {
	cb := &conflationBuffer{
		interval: interval,
		pending:  make(map[string]*conflationEntry),
		sub:      sub,
	}
	cb.timer = time.AfterFunc(interval, cb.flush)
	return cb
}

func newConflationBufferWithKey(sub *subscription, interval time.Duration, conflationKey string) *conflationBuffer {
	cb := newConflationBuffer(sub, interval)
	cb.conflationKey = conflationKey
	return cb
}

func (cb *conflationBuffer) add(topic, subID string, payload []byte, bookmark, timestamp, sowKey, messageType string, isQueue bool) {
	// Determine the conflation key: use custom field or default to sowKey.
	mergeKey := sowKey
	if cb.conflationKey != "" && len(payload) > 0 {
		if extracted := extractJSONStringField(payload, cb.conflationKey); extracted != "" {
			mergeKey = extracted
		}
	}
	key := topic + "\x00" + mergeKey

	cb.mu.Lock()
	cb.pending[key] = &conflationEntry{
		topic:       topic,
		subID:       subID,
		sowKey:      sowKey,
		bookmark:    bookmark,
		timestamp:   timestamp,
		messageType: messageType,
		payload:     append([]byte(nil), payload...), // defensive copy
		isQueue:     isQueue,
	}
	cb.mu.Unlock()
}

func (cb *conflationBuffer) flush() {
	cb.mu.Lock()
	if cb.stopped {
		cb.mu.Unlock()
		return
	}
	entries := cb.pending
	cb.pending = make(map[string]*conflationEntry, len(entries))
	cb.timer.Reset(cb.interval)
	cb.mu.Unlock()

	buf := getWriteBuf()
	defer putWriteBuf(buf)

	for _, e := range entries {
		frame := buildPublishDelivery(buf, e.topic, e.subID, e.payload,
			e.bookmark, e.timestamp, e.sowKey, e.messageType, e.isQueue)
		cb.sub.writer.send(frame)
	}
}

func (cb *conflationBuffer) stop() {
	cb.mu.Lock()
	cb.stopped = true
	cb.timer.Stop()
	cb.mu.Unlock()
}

// ---------------------------------------------------------------------------
// parseConflationInterval extracts the conflation interval from subscribe
// options string. Format: "conflation=100ms" or "conflation=1s".
// Returns 0 if no conflation is configured.
// ---------------------------------------------------------------------------

func parseConflationInterval(options string) time.Duration {
	const prefix = "conflation="
	idx := bytes.Index([]byte(options), []byte(prefix))
	if idx < 0 {
		return 0
	}
	rest := options[idx+len(prefix):]
	// Find end of value (comma or end of string).
	end := len(rest)
	for i := 0; i < len(rest); i++ {
		if rest[i] == ',' {
			end = i
			break
		}
	}
	d, err := time.ParseDuration(rest[:end])
	if err != nil {
		return 0
	}
	return d
}

// parseConflationKey extracts the conflation_key from subscribe options.
// Format: "conflation_key=/field" or "conflation_key=field".
func parseConflationKey(options string) string {
	const prefix = "conflation_key="
	idx := bytes.Index([]byte(options), []byte(prefix))
	if idx < 0 {
		return ""
	}
	rest := options[idx+len(prefix):]
	end := len(rest)
	for i := 0; i < len(rest); i++ {
		if rest[i] == ',' {
			end = i
			break
		}
	}
	key := rest[:end]
	// Strip leading "/" if present (XPath-style).
	if len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}
	return key
}
