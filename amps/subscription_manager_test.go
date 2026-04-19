package amps

import (
	"strings"
	"testing"
)

type fixedBookmarkStore struct {
	recent map[string]string
}

func (store *fixedBookmarkStore) Log(message *Message) uint64 { return 0 }

func (store *fixedBookmarkStore) Discard(subID string, bookmarkSeqNo uint64) {}

func (store *fixedBookmarkStore) DiscardMessage(message *Message) {}

func (store *fixedBookmarkStore) GetMostRecent(subID string) string {
	if store == nil || store.recent == nil {
		return ""
	}
	return store.recent[subID]
}

func (store *fixedBookmarkStore) IsDiscarded(message *Message) bool { return false }

func (store *fixedBookmarkStore) Purge(subID ...string) {}

func (store *fixedBookmarkStore) GetOldestBookmarkSeq(subID string) uint64 { return 0 }

func (store *fixedBookmarkStore) Persisted(subID string, bookmark string) string { return bookmark }

func (store *fixedBookmarkStore) SetServerVersion(version string) {}

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

func TestSubscriptionManagerAdditionalHelperCoverage(t *testing.T) {
	splitIDs := splitSubscriptionIDs
	prependOption := prependCommandOption
	parseToken := parseBookmarkToken
	splitBookmarks := splitBookmarkList
	invokeHandler := invokeFailedResubscribeHandler
	combineBookmarks := combinedResumeBookmark

	if ids := splitIDs(""); ids != nil {
		t.Fatalf("expected empty split ids to be nil, got %v", ids)
	}
	ids := splitIDs("sub-a,, sub-b ,")
	if len(ids) != 2 || ids[0] != "sub-a" || ids[1] != "sub-b" {
		t.Fatalf("unexpected split ids: %v", ids)
	}

	if got := prependOption("", "resume"); got != "resume" {
		t.Fatalf("expected prepend into empty options, got %q", got)
	}
	if got := prependOption("resume,live", "resume"); got != "resume,live" {
		t.Fatalf("expected duplicate option prepend to preserve input, got %q", got)
	}

	if _, _, ok := parseToken("invalid"); ok {
		t.Fatalf("expected invalid bookmark token to fail parsing")
	}
	if _, _, ok := parseToken("x|1|"); ok {
		t.Fatalf("expected invalid publisher bookmark token to fail parsing")
	}
	if _, _, ok := parseToken("1|x|"); ok {
		t.Fatalf("expected invalid sequence bookmark token to fail parsing")
	}
	publisher, sequence, ok := parseToken("0|0|")
	if !ok || publisher != 0 || sequence != 0 {
		t.Fatalf("expected zero bookmark token to parse, got publisher=%d sequence=%d ok=%v", publisher, sequence, ok)
	}

	if tokens := splitBookmarks(""); tokens != nil {
		t.Fatalf("expected empty bookmark list to be nil, got %v", tokens)
	}
	tokens := splitBookmarks("1|1|,,2|2|")
	if len(tokens) != 2 || tokens[0] != "1|1|" || tokens[1] != "2|2|" {
		t.Fatalf("unexpected bookmark list split: %v", tokens)
	}

	if handled, err := invokeHandler(nil, nil, 0, nil); handled || err != nil {
		t.Fatalf("nil failed resubscribe handler should noop, handled=%v err=%v", handled, err)
	}
	if handled, err := invokeHandler(FailedResubscribeHandlerFunc(func(*Command, int, error) bool {
		return false
	}), nil, 0, nil); handled || err != nil {
		t.Fatalf("false failed resubscribe handler should return handled=false err=nil, got handled=%v err=%v", handled, err)
	}

	if bookmark := combineBookmarks(nil, &trackedResumeGroup{}); bookmark != "" {
		t.Fatalf("expected nil client combined bookmark to be empty, got %q", bookmark)
	}
	client := NewClient("combined-resume-helper")
	if bookmark := combineBookmarks(client, nil); bookmark != "" {
		t.Fatalf("expected nil group combined bookmark to be empty, got %q", bookmark)
	}
	if bookmark := combineBookmarks(client, &trackedResumeGroup{members: map[string]struct{}{"sub-a": {}}}); bookmark != "" {
		t.Fatalf("expected nil bookmark store combined bookmark to be empty, got %q", bookmark)
	}
	client.SetBookmarkStore(&fixedBookmarkStore{recent: map[string]string{"sub-a": "bad", "sub-b": "0|0|"}})
	if bookmark := combineBookmarks(client, &trackedResumeGroup{members: map[string]struct{}{"sub-a": {}, "sub-b": {}}}); bookmark != "" {
		t.Fatalf("expected invalid/zero combined bookmark to be empty, got %q", bookmark)
	}

	manager := NewDefaultSubscriptionManager()
	removeResumeGroupFn := manager.removeResumeGroup
	removeTrackedSubscriptionFn := manager.removeTrackedSubscription
	subscribeResumeFn := manager.subscribeResume
	subscribePauseFn := manager.subscribePause
	removeResumeGroupFn(nil)
	removeTrackedSubscriptionFn("")
	subscribeResumeFn(NewCommand("subscribe"), AckTypeNone)
	subscribePauseFn(nil, NewCommand("subscribe"), AckTypeNone, "pause")

	group := &trackedResumeGroup{
		command:           NewCommand("subscribe").SetSubID("sub-existing"),
		requestedAckTypes: AckTypeProcessed,
		members:           map[string]struct{}{"sub-existing": {}},
	}
	manager.resumed["sub-existing"] = group
	manager.resumedGroups[group] = struct{}{}
	subscribeResumeFn(NewCommand("subscribe").SetSubID("sub-existing"), AckTypeProcessed)
	if len(manager.resumedGroups) != 1 {
		t.Fatalf("expected duplicate resume subscribe to leave resume groups unchanged, got %d", len(manager.resumedGroups))
	}

	manager.subscriptions["sub-existing"] = trackedSubscription{command: NewCommand("subscribe").SetSubID("sub-existing").SetOptions("bookmark"), requestedAckTypes: AckTypeProcessed}
	subscribePauseFn(func(*Message) error { return nil }, NewCommand("subscribe").SetSubID("sub-existing").SetOptions("pause,replace"), AckTypeReceived, "pause,replace")
	if tracked := manager.subscriptions["sub-existing"]; tracked.requestedAckTypes != AckTypeReceived || !tracked.paused {
		t.Fatalf("expected replace pause to overwrite tracked subscription, got %+v", tracked)
	}

	removeResumeGroupFn(group)
	if len(manager.resumed) != 0 || len(manager.resumedGroups) != 0 {
		t.Fatalf("expected explicit resume group removal to clear resumed state")
	}

	var nilManager *DefaultSubscriptionManager
	getTimeoutFn := (*DefaultSubscriptionManager).GetResubscriptionTimeout
	resubscribeFn := (*DefaultSubscriptionManager).Resubscribe
	if got := getTimeoutFn(nilManager); got != 0 {
		t.Fatalf("expected nil manager timeout 0, got %d", got)
	}
	if err := resubscribeFn(nilManager, client); err != nil {
		t.Fatalf("expected nil manager resubscribe to succeed, got %v", err)
	}
	if err := resubscribeFn(manager, nil); err != nil {
		t.Fatalf("expected nil client resubscribe to succeed, got %v", err)
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

func TestPrepareResubscribeCommandPreservesBookmarkWhenStoreHasNoEntry(t *testing.T) {
	client := NewClient("resubscribe-empty-store")
	client.SetBookmarkStore(NewMemoryBookmarkStore())

	prepared := prepareResubscribeCommand(client, trackedSubscription{
		command:           NewCommand("subscribe").SetSubID("sub-empty").SetBookmark("7|7|"),
		requestedAckTypes: AckTypeCompleted,
	})
	if bookmark, ok := prepared.Bookmark(); !ok || bookmark != "7|7|" {
		t.Fatalf("expected empty bookmark store to preserve original bookmark, got %q ok=%v", bookmark, ok)
	}
}

func TestDefaultSubscriptionManagerPauseResumeReplayUsesCombinedBookmarks(t *testing.T) {
	manager := NewDefaultSubscriptionManager()
	handler := func(*Message) error { return nil }

	manager.Subscribe(handler, NewCommand("subscribe").SetTopic("orders").SetSubID("sub-a").SetBookmark("1|1|").SetOptions("bookmark"), AckTypeProcessed)
	manager.Subscribe(handler, NewCommand("subscribe").SetTopic("orders").SetSubID("sub-b").SetBookmark("1|1|").SetOptions("bookmark"), AckTypeProcessed)
	manager.Subscribe(handler, NewCommand("subscribe").SetTopic("orders").SetSubID("sub-a,sub-b").SetBookmark("1|1|").SetOptions("bookmark,pause"), AckTypeProcessed)
	manager.Subscribe(nil, NewCommand("subscribe").SetTopic("orders").SetSubID("sub-a,sub-b").SetOptions("resume"), AckTypeNone)

	client := NewClient("pause-resume-resubscribe-test")
	client.connected.Store(false)
	client.SetRetryOnDisconnect(true)
	client.SetBookmarkStore(NewMemoryBookmarkStore())
	client.BookmarkStore().Log(bookmarkMessage("sub-a", "1|100|"))
	client.BookmarkStore().Log(bookmarkMessage("sub-a", "2|200|"))
	client.BookmarkStore().Log(bookmarkMessage("sub-b", "1|150|"))
	client.BookmarkStore().Log(bookmarkMessage("sub-b", "3|300|"))

	if err := manager.Resubscribe(client); err != nil {
		t.Fatalf("resubscribe should queue paused/replay commands, got error: %v", err)
	}

	state := ensureClientState(client)
	state.lock.Lock()
	defer state.lock.Unlock()
	if len(state.pendingRetry) != 3 {
		t.Fatalf("expected two paused retries plus one resume retry, got %d", len(state.pendingRetry))
	}

	var commandsBySubID = make(map[string]*Command)
	for _, retry := range state.pendingRetry {
		command := retry.command
		subID, _ := command.SubID()
		commandsBySubID[subID] = command
	}

	for _, subID := range []string{"sub-a", "sub-b"} {
		command := commandsBySubID[subID]
		if command == nil {
			t.Fatalf("expected paused replay command for %s", subID)
		}
		bookmark, ok := command.Bookmark()
		if !ok || bookmark != "1|100|,2|200|,3|300|" {
			t.Fatalf("expected combined bookmark replay for %s, got %q ok=%v", subID, bookmark, ok)
		}
		options, _ := command.Options()
		if !hasCommandOption(options, "pause") {
			t.Fatalf("expected paused replay command for %s to keep pause option, got %q", subID, options)
		}
	}

	resume := commandsBySubID["sub-a,sub-b"]
	if resume == nil {
		t.Fatalf("expected resume replay command")
	}
	options, _ := resume.Options()
	if !hasCommandOption(options, "resume") {
		t.Fatalf("expected grouped resume replay command to keep resume option, got %q", options)
	}
}

func TestDefaultSubscriptionManagerHandledFailedReplayRemovesTrackedState(t *testing.T) {
	manager := NewDefaultSubscriptionManager()
	handler := func(*Message) error { return nil }

	manager.Subscribe(handler, NewCommand("subscribe").SetTopic("orders").SetSubID("sub-a").SetBookmark("1|1|").SetOptions("bookmark,pause"), AckTypeProcessed)
	manager.Subscribe(handler, NewCommand("subscribe").SetTopic("orders").SetSubID("sub-b").SetBookmark("1|1|").SetOptions("bookmark,pause"), AckTypeProcessed)
	manager.Subscribe(nil, NewCommand("subscribe").SetTopic("orders").SetSubID("sub-a,sub-b").SetOptions("resume"), AckTypeNone)

	client := NewClient("failed-replay-cleanup-test")
	defer forgetClientState(client)
	client.connected.Store(false)
	client.SetRetryOnDisconnect(false)

	failedCalls := 0
	manager.SetFailedResubscribeHandler(FailedResubscribeHandlerFunc(func(command *Command, requestedAckTypes int, err error) bool {
		_ = command
		_ = requestedAckTypes
		_ = err
		failedCalls++
		return true
	}))

	if err := manager.Resubscribe(client); err != nil {
		t.Fatalf("failure handler true should absorb replay errors, got %v", err)
	}
	if failedCalls != 3 {
		t.Fatalf("expected handler for two paused subscriptions and one resume group, got %d", failedCalls)
	}

	manager.lock.RLock()
	remainingSubscriptions := len(manager.subscriptions)
	remainingResumed := len(manager.resumed)
	remainingResumeGroups := len(manager.resumedGroups)
	manager.lock.RUnlock()
	if remainingSubscriptions != 0 || remainingResumed != 0 || remainingResumeGroups != 0 {
		t.Fatalf("expected handled failed replay to clear tracked state, got subscriptions=%d resumed=%d groups=%d", remainingSubscriptions, remainingResumed, remainingResumeGroups)
	}

	failedCalls = 0
	if err := manager.Resubscribe(client); err != nil {
		t.Fatalf("second resubscribe should succeed with cleared replay state, got %v", err)
	}
	if failedCalls != 0 {
		t.Fatalf("expected no further failed replay callbacks after cleanup, got %d", failedCalls)
	}
}

func TestDefaultSubscriptionManagerResubscriptionTimeoutConfiguration(t *testing.T) {
	previousDefault := GetDefaultResubscriptionTimeout()
	SetDefaultResubscriptionTimeout(0)
	defer SetDefaultResubscriptionTimeout(previousDefault)

	SetDefaultResubscriptionTimeout(1250)
	if got := GetDefaultResubscriptionTimeout(); got != 1250 {
		t.Fatalf("expected default resubscription timeout 1250, got %d", got)
	}

	manager := NewDefaultSubscriptionManager()
	if got := manager.GetResubscriptionTimeout(); got != 1250 {
		t.Fatalf("expected manager to inherit default resubscription timeout, got %d", got)
	}

	manager.SetResubscriptionTimeout(-1)
	if got := manager.GetResubscriptionTimeout(); got != 1250 {
		t.Fatalf("negative timeout should be ignored, got %d", got)
	}

	manager.SetResubscriptionTimeout(250)
	if got := manager.GetResubscriptionTimeout(); got != 250 {
		t.Fatalf("expected configured resubscription timeout 250, got %d", got)
	}
}

func TestDefaultSubscriptionManagerResubscribeAppliesConfiguredTimeout(t *testing.T) {
	manager := NewDefaultSubscriptionManager()
	manager.SetResubscriptionTimeout(345)
	manager.Subscribe(nil, NewCommand("subscribe").SetTopic("orders").SetSubID("sub-timeout"), AckTypeProcessed)

	client := NewClient("resubscribe-timeout-test")
	client.connected.Store(false)
	client.SetRetryOnDisconnect(true)

	if err := manager.Resubscribe(client); err != nil {
		t.Fatalf("resubscribe should queue retry with configured timeout, got %v", err)
	}

	state := ensureClientState(client)
	state.lock.Lock()
	defer state.lock.Unlock()
	if len(state.pendingRetry) != 1 {
		t.Fatalf("expected one queued retry command, got %d", len(state.pendingRetry))
	}
	if got := state.pendingRetry[0].command.GetTimeout(); got != 345 {
		t.Fatalf("expected queued retry timeout 345, got %d", got)
	}
}

func TestDefaultSubscriptionManagerFailedResubscribeHandlerPanicReturnsError(t *testing.T) {
	manager := NewDefaultSubscriptionManager()
	manager.Subscribe(nil, NewCommand("subscribe").SetTopic("orders").SetSubID("sub-panic"), AckTypeProcessed)

	client := NewClient("failed-handler-panic-test")
	defer forgetClientState(client)
	client.connected.Store(false)
	client.SetRetryOnDisconnect(false)

	manager.SetFailedResubscribeHandler(FailedResubscribeHandlerFunc(func(command *Command, requestedAckTypes int, err error) bool {
		_ = command
		_ = requestedAckTypes
		_ = err
		panic("handler panic")
	}))

	err := manager.Resubscribe(client)
	if err == nil {
		t.Fatalf("expected failed resubscribe handler panic to return an error")
	}
	if !strings.Contains(err.Error(), "handler panic") {
		t.Fatalf("expected panic text in error, got %v", err)
	}

	manager.lock.RLock()
	remainingSubscriptions := len(manager.subscriptions)
	manager.lock.RUnlock()
	if remainingSubscriptions != 1 {
		t.Fatalf("expected tracked replay state to remain after handler panic, got %d subscriptions", remainingSubscriptions)
	}
}

func TestDefaultSubscriptionManagerSubscribeResumeTracksOnlyOwnedIDs(t *testing.T) {
	manager := NewDefaultSubscriptionManager()
	var existing = &trackedResumeGroup{
		command:           NewCommand("subscribe").SetSubID("sub-a"),
		requestedAckTypes: AckTypeProcessed,
		members:           map[string]struct{}{"sub-a": {}},
	}
	manager.resumed["sub-a"] = existing
	manager.resumedGroups[existing] = struct{}{}

	manager.subscribeResume(NewCommand("subscribe").SetSubID("sub-a,sub-b"), AckTypeCompleted)

	group := manager.resumed["sub-b"]
	if group == nil || group == existing {
		t.Fatalf("expected sub-b to be tracked by a new resume group")
	}
	subID, ok := group.command.SubID()
	if !ok || subID != "sub-b" {
		t.Fatalf("expected resume group command to track only owned ids, got %q ok=%v", subID, ok)
	}
	if len(manager.resumedGroups) != 2 {
		t.Fatalf("expected two resume groups after overlap, got %d", len(manager.resumedGroups))
	}
}

func TestTrackedResumeGroupConcurrentBookmarkSnapshot(t *testing.T) {
	client := NewClient("resume-group-race")
	client.SetBookmarkStore(&fixedBookmarkStore{recent: map[string]string{
		"sub-a": "1|100|",
		"sub-b": "2|200|",
		"sub-c": "3|300|",
	}})
	group := &trackedResumeGroup{members: map[string]struct{}{"sub-a": {}, "sub-b": {}}}
	done := make(chan struct{})

	go func() {
		for idx := 0; idx < 1000; idx++ {
			_ = combinedResumeBookmark(client, group)
		}
		close(done)
	}()

	for idx := 0; idx < 1000; idx++ {
		group.addMember("sub-c")
		group.removeMember("sub-c")
	}

	<-done
}
