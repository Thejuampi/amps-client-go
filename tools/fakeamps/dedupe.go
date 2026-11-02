package main

import (
	"strconv"
	"sync"
)

type commandDedupeClientState struct {
	seen  map[string]struct{}
	order []string
}

type commandDedupeTracker struct {
	mu           sync.Mutex
	maxPerClient int
	clients      map[string]*commandDedupeClientState
}

func newCommandDedupeTracker(maxPerClient int) *commandDedupeTracker {
	if maxPerClient <= 0 {
		maxPerClient = 4096
	}
	return &commandDedupeTracker{
		maxPerClient: maxPerClient,
		clients:      make(map[string]*commandDedupeClientState),
	}
}

func (tracker *commandDedupeTracker) seenBefore(clientID string, commandID string) bool {
	if tracker == nil || clientID == "" || commandID == "" {
		return false
	}

	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	var clientState = tracker.clients[clientID]
	if clientState == nil {
		clientState = &commandDedupeClientState{
			seen: make(map[string]struct{}),
		}
		tracker.clients[clientID] = clientState
	}

	var _, exists = clientState.seen[commandID]
	if exists {
		return true
	}

	clientState.seen[commandID] = struct{}{}
	clientState.order = append(clientState.order, commandID)

	if len(clientState.order) > tracker.maxPerClient {
		var evicted = clientState.order[0]
		clientState.order = clientState.order[1:]
		delete(clientState.seen, evicted)
	}

	return false
}

func (tracker *commandDedupeTracker) reset() {
	if tracker == nil {
		return
	}

	tracker.mu.Lock()
	tracker.clients = make(map[string]*commandDedupeClientState)
	tracker.mu.Unlock()
}

var commandDedupe = newCommandDedupeTracker(4096)

type publishSequenceTracker struct {
	mu           sync.Mutex
	lastByClient map[string]uint64
}

func newPublishSequenceTracker() *publishSequenceTracker {
	return &publishSequenceTracker{lastByClient: make(map[string]uint64)}
}

func (tracker *publishSequenceTracker) seenBefore(clientID string, sequenceID string) bool {
	if tracker == nil || clientID == "" || sequenceID == "" {
		return false
	}

	var seq, err = strconv.ParseUint(sequenceID, 10, 64)
	if err != nil {
		return false
	}

	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	var last, ok = tracker.lastByClient[clientID]
	if ok && seq <= last {
		return true
	}

	tracker.lastByClient[clientID] = seq
	return false
}

func (tracker *publishSequenceTracker) reset() {
	if tracker == nil {
		return
	}

	tracker.mu.Lock()
	tracker.lastByClient = make(map[string]uint64)
	tracker.mu.Unlock()
}

var publishSequenceDedupe = newPublishSequenceTracker()

type publisherIdentity struct {
	name      string
	user      string
	anonymous string
}

type publisherSequenceState struct {
	mu        sync.Mutex
	accepted  uint64
	persisted uint64
}

type brokerPublishTracker struct {
	mu         sync.Mutex
	publishers map[publisherIdentity]*publisherSequenceState
}

func newBrokerPublishTracker() *brokerPublishTracker {
	return &brokerPublishTracker{publishers: make(map[publisherIdentity]*publisherSequenceState)}
}

// begin serializes a publisher until the caller commits the applied sequence.
func (tracker *brokerPublishTracker) begin(identity publisherIdentity, raw string) (*publisherSequenceState, uint64, bool, bool) {
	var sequence, err = strconv.ParseUint(raw, 10, 64)
	if err != nil || sequence == 0 {
		return nil, 0, false, false
	}
	tracker.mu.Lock()
	var state = tracker.publishers[identity]
	if state == nil {
		state = &publisherSequenceState{}
		tracker.publishers[identity] = state
	}
	tracker.mu.Unlock()
	state.mu.Lock()
	if sequence <= state.accepted {
		var persisted = sequence <= state.persisted
		state.mu.Unlock()
		return nil, sequence, true, persisted
	}
	return state, sequence, false, false
}

func (state *publisherSequenceState) finish(sequence uint64, persisted bool) {
	if state == nil {
		return
	}
	state.accepted = sequence
	if persisted {
		state.persisted = sequence
	}
	state.mu.Unlock()
}

func (state *publisherSequenceState) abort() {
	if state != nil {
		state.mu.Unlock()
	}
}

var fallbackBrokerPublishTracker = newBrokerPublishTracker()
