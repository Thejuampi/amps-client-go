package amps

import "testing"

func TestSubscriptionManagerHelperCoverage(t *testing.T) {
	if got := trackedSubscriptionID(nil); got != "" {
		t.Fatalf("expected nil tracked subscription id to be empty, got %q", got)
	}
	if got := removeReplayOnlyOptions(""); got != "" {
		t.Fatalf("expected empty options to stay empty, got %q", got)
	}
	if got := removeReplayOnlyOptions(" replace, ,live , replace , pause "); got != "live,pause" {
		t.Fatalf("unexpected replay option stripping result: %q", got)
	}

	if command := prepareResubscribeCommand(nil, trackedSubscription{}); command == nil {
		t.Fatalf("expected nil tracked command to normalize to an empty command")
	}

	base := NewCommand("subscribe").SetSubID("sub-1").SetTopic("orders")
	prepared := prepareResubscribeCommand(nil, trackedSubscription{command: base, requestedAckTypes: AckTypeProcessed})
	if prepared == nil {
		t.Fatalf("expected prepared command")
	}
	if _, ok := prepared.Bookmark(); ok {
		t.Fatalf("expected bookmarkless command to remain bookmarkless")
	}
	if ackType, ok := prepared.AckType(); !ok || ackType != AckTypeProcessed {
		t.Fatalf("expected prepared ack type, got %d ok=%v", ackType, ok)
	}

	prepared = prepareResubscribeCommand(nil, trackedSubscription{
		command:           NewCommand("subscribe").SetSubID("sub-nil-client").SetBookmark("9|9|"),
		requestedAckTypes: AckTypeReceived,
	})
	if bookmark, ok := prepared.Bookmark(); !ok || bookmark != "9|9|" {
		t.Fatalf("expected nil-client replay to preserve bookmark, got %q ok=%v", bookmark, ok)
	}

	client := NewClient("resubscribe-helper")
	prepared = prepareResubscribeCommand(client, trackedSubscription{
		command:           NewCommand("subscribe").SetSubID("sub-no-bookmark").SetTopic("orders"),
		requestedAckTypes: AckTypeParsed,
	})
	if _, ok := prepared.Bookmark(); ok {
		t.Fatalf("expected client-present replay without bookmark to stay bookmarkless")
	}
	if ackType, ok := prepared.AckType(); !ok || ackType != AckTypeParsed {
		t.Fatalf("expected bookmarkless replay ack type, got %d ok=%v", ackType, ok)
	}

	prepared = prepareResubscribeCommand(client, trackedSubscription{
		command:           NewCommand("subscribe").SetSubID("sub-2").SetBookmark("1|1|").SetOptions("replace"),
		requestedAckTypes: AckTypeCompleted,
	})
	if bookmark, ok := prepared.Bookmark(); !ok || bookmark != "1|1|" {
		t.Fatalf("expected missing bookmark-store replay to preserve original bookmark, got %q ok=%v", bookmark, ok)
	}
	if options, ok := prepared.Options(); ok && options != "" {
		t.Fatalf("expected replace-only options to be removed, got %q", options)
	}
}

func TestDefaultSubscriptionManagerResubscribeQueuesWhenDisconnected(t *testing.T) {
	manager := NewDefaultSubscriptionManager()
	command := NewCommand("subscribe").SetTopic("orders").SetSubID("sub-1")
	manager.Subscribe(nil, command, AckTypeNone)

	client := NewClient("resubscribe-test")
	client.connected.Store(false)
	client.SetRetryOnDisconnect(true)

	if err := manager.Resubscribe(client); err != nil {
		t.Fatalf("resubscribe should queue retry, got error: %v", err)
	}

	state := ensureClientState(client)
	state.lock.Lock()
	pending := len(state.pendingRetry)
	state.lock.Unlock()
	if pending != 1 {
		t.Fatalf("expected one queued retry command, got %d", pending)
	}
}

func TestDefaultSubscriptionManagerNoResubscribeRoutes(t *testing.T) {
	manager := NewDefaultSubscriptionManager()
	command := NewCommand("subscribe").SetTopic("orders").SetSubID("sub-skip")
	manager.Subscribe(nil, command, AckTypeNone)

	client := NewClient("resubscribe-skip-test")
	client.connected.Store(false)
	client.SetRetryOnDisconnect(true)

	state := ensureClientState(client)
	state.lock.Lock()
	state.noResubscribeRoutes["sub-skip"] = struct{}{}
	state.pendingRetry = nil
	state.lock.Unlock()

	if err := manager.Resubscribe(client); err != nil {
		t.Fatalf("resubscribe should skip blocked route without error: %v", err)
	}

	state.lock.Lock()
	pending := len(state.pendingRetry)
	state.lock.Unlock()
	if pending != 0 {
		t.Fatalf("expected zero queued retries for no-resubscribe route, got %d", pending)
	}
}

func TestDefaultSubscriptionManagerResubscribeUsesMostRecentBookmarkAndStripsReplace(t *testing.T) {
	manager := NewDefaultSubscriptionManager()
	command := NewCommand("subscribe").
		SetTopic("orders").
		SetSubID("sub-1").
		SetBookmark("1|100|").
		SetOptions("live,replace")
	manager.Subscribe(nil, command, AckTypeProcessed)

	client := NewClient("resubscribe-bookmark-test")
	client.connected.Store(false)
	client.SetRetryOnDisconnect(true)
	client.SetBookmarkStore(NewMemoryBookmarkStore())
	client.BookmarkStore().Log(bookmarkMessage("sub-1", "2|200|"))

	if err := manager.Resubscribe(client); err != nil {
		t.Fatalf("resubscribe should queue retry, got error: %v", err)
	}

	state := ensureClientState(client)
	state.lock.Lock()
	defer state.lock.Unlock()
	if len(state.pendingRetry) != 1 {
		t.Fatalf("expected one queued retry command, got %d", len(state.pendingRetry))
	}

	pending := state.pendingRetry[0].command
	if bookmark, ok := pending.Bookmark(); !ok || bookmark != "2|200|" {
		t.Fatalf("expected resubscribe bookmark to use store most recent value, got %q ok=%v", bookmark, ok)
	}
	if options, ok := pending.Options(); !ok || options != "live" {
		t.Fatalf("expected replace option to be removed for replay, got %q ok=%v", options, ok)
	}
	if ackType, ok := pending.AckType(); !ok || ackType != AckTypeProcessed {
		t.Fatalf("expected replayed ack type to match tracked subscription, got %d ok=%v", ackType, ok)
	}
}
