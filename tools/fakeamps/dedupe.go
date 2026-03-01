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

var publishSequenceDedupe = newPublishSequenceTracker()
