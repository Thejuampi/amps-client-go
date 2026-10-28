package amps

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBookmarkDiscardSequenceIsIndividual(t *testing.T) {
	var stores = []struct {
		name  string
		store BookmarkStore
	}{
		{"memory", NewMemoryBookmarkStore()},
		{"file", NewFileBookmarkStore(filepath.Join(t.TempDir(), "bookmarks"))},
	}
	for _, entry := range stores {
		t.Run(entry.name, func(t *testing.T) {
			var first = auditBookmarkMessage("1|1|")
			entry.store.Log(first)
			var second = auditBookmarkMessage("1|2|")
			var sequence = entry.store.Log(second)
			entry.store.Discard("sub-1", sequence)
			if entry.store.IsDiscarded(first) || !entry.store.IsDiscarded(second) || entry.store.GetMostRecent("sub-1") != BookmarksEPOCH {
				t.Fatal("individual discard skipped an unprocessed message")
			}
		})
	}
}

func TestRingBookmarkStoreKeepsActiveEntries(t *testing.T) {
	var store = NewRingBookmarkStoreWithSize(1)
	var first = auditBookmarkMessage("1|1|")
	var firstSequence = store.Log(first)
	var second = auditBookmarkMessage("1|2|")
	store.Log(second)
	store.DiscardMessage(second)
	if store.GetOldestBookmarkSeq("sub-1") != firstSequence || store.GetMostRecent("sub-1") != BookmarksEPOCH || store.IsDiscarded(first) {
		t.Fatal("ring capacity evicted or skipped an unprocessed message")
	}
}

func TestRingBookmarkStoreRemembersEvictedDiscards(t *testing.T) {
	var store = NewRingBookmarkStoreWithSize(1)
	var processed = auditBookmarkMessage("1|1|")
	store.Log(processed)
	store.DiscardMessage(processed)
	store.Log(auditBookmarkMessage("1|2|"))
	if !store.IsDiscarded(processed) {
		t.Fatal("ring eviction forgot a processed bookmark and permits duplicate delivery")
	}
}

func TestFileBookmarkRecoveryDoesNotSkipActiveEntries(t *testing.T) {
	var path = filepath.Join(t.TempDir(), "bookmarks")
	var store = NewFileBookmarkStore(path)
	var first = auditBookmarkMessage("1|1|")
	store.Log(first)
	var second = auditBookmarkMessage("1|2|")
	store.Log(second)
	store.DiscardMessage(second)
	var recovered = NewFileBookmarkStore(path)
	if recovered.GetMostRecent("sub-1") != BookmarksEPOCH || recovered.IsDiscarded(first) || !recovered.IsDiscarded(second) {
		t.Fatal("WAL recovery skipped or forgot an individual discard")
	}
}

func TestFreshFilePublishStoreDoesNotReuseAnOldPublisherSequence(t *testing.T) {
	var store = NewFilePublishStore(filepath.Join(t.TempDir(), "publish"))
	store.SetInitialSequence(42)
	var sequence, err = store.Store(NewCommand("publish").SetTopic("orders"))
	if err != nil || sequence <= 100 {
		t.Fatalf("fresh file store reused an old publisher sequence: %d %v", sequence, err)
	}
}

func TestBoundedStreamRetriesWithoutLosingMessages(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	var stream = newMessageStream(client).SetSubscription("sub-1", "sub-1").SetMaxDepth(1)
	var blocked = make(chan struct{}, 1)
	client.routes.Store("sub-1", func(message *Message) error {
		var err = stream.messageHandler(message)
		if err != nil {
			select {
			case blocked <- struct{}{}:
			default:
			}
		}
		if err != nil {
			return fmt.Errorf("wrapped backpressure: %w", err)
		}
		return nil
	})
	if err := client.onMessage(auditBookmarkMessage("1|1|")); err != nil {
		t.Fatal(err)
	}
	var done = make(chan error, 1)
	go func() { done <- client.onMessage(auditBookmarkMessage("1|2|")) }()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("full stream did not signal backpressure")
	}
	var first, _ = stream.Next().Bookmark()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("delivery did not resume after dequeue")
	}
	var second, _ = stream.Next().Bookmark()
	if first != "1|1|" || second != "1|2|" {
		t.Fatalf("stream reordered/lost messages: %q %q", first, second)
	}
}

func TestBoundedStreamDoesNotCopyWhileFull(t *testing.T) {
	for _, conflate := range []bool{false, true} {
		var name = "plain"
		if conflate {
			name = "conflated"
		}
		t.Run(name, func(t *testing.T) {
			var stream = newMessageStream(nil).SetSubscription("sub-1", "sub-1").SetMaxDepth(1)
			if conflate {
				stream.Conflate()
			}
			var first = auditBookmarkMessage("1|1|")
			first.header.sowKey = []byte("first")
			if err := stream.messageHandler(first); err != nil {
				t.Fatal(err)
			}
			var incoming = auditBookmarkMessage("1|2|")
			incoming.header.sowKey = []byte("other")
			var allocations = testing.AllocsPerRun(1000, func() {
				if err := stream.messageHandler(incoming); err != errMessageStreamFull {
					t.Fatal("full stream did not apply backpressure")
				}
			})
			if allocations != 0 {
				t.Fatalf("full stream copied/reallocated each retry: %.1f allocs", allocations)
			}
		})
	}
}

func TestStreamAutoAckFollowsConsumerAdvance(t *testing.T) {
	var client, connection = auditConnectedClient(t)
	client.SetAutoAck(true)
	var stream = newMessageStream(client).SetSubscription("sub-1", "sub-1")
	client.routes.Store("sub-1", stream.messageHandler)
	for _, bookmark := range []string{"1|1|", "1|2|"} {
		var message = auditBookmarkMessage(bookmark)
		message.header.textExtras = &_HeaderTextExtras{leasePeriod: []byte("30000")}
		if err := client.onMessage(message); err != nil {
			t.Fatal(err)
		}
	}
	_ = stream.Next()
	if got := connection.WrittenPayload(); got != "" {
		t.Fatalf("ack preceded consumer advance: %s", got)
	}
	_ = stream.Next()
	var got = connection.WrittenPayload()
	if !strings.Contains(got, `"bm":"1|1|"`) || strings.Contains(got, `"bm":"1|2|"`) {
		t.Fatalf("wrong message acknowledged: %s", got)
	}
}

func TestStreamDoesNotAdvanceAfterDeferredAckFailure(t *testing.T) {
	var client, connection = auditConnectedClient(t)
	client.SetAutoAck(true)
	var stream = newMessageStream(client).SetSubscription("sub-1", "sub-1")
	client.routes.Store("sub-1", stream.messageHandler)
	for _, bookmark := range []string{"1|1|", "1|2|"} {
		var message = auditBookmarkMessage(bookmark)
		message.header.textExtras = &_HeaderTextExtras{leasePeriod: []byte("30000")}
		if err := client.onMessage(message); err != nil {
			t.Fatal(err)
		}
	}
	_ = stream.Next()
	_ = connection.Close()
	if stream.HasNext() || stream.Depth() != 1 {
		t.Fatal("consumer advanced despite a failed deferred ack")
	}
}

func TestDuplicateFilteringIsPerSubscription(t *testing.T) {
	var client = NewClient("per-subscription")
	var store = NewMemoryBookmarkStore()
	var first = auditBookmarkMessage("1|1|")
	store.Log(first)
	store.DiscardMessage(first)
	client.SetBookmarkStore(store)
	var delivered []string
	var handler = func(message *Message) error {
		var subID, _ = message.SubID()
		delivered = append(delivered, subID)
		return nil
	}
	client.routes.Store("sub-1", handler)
	client.routes.Store("sub-2", handler)
	var message = auditBookmarkMessage("1|1|")
	message.header.subID = nil
	message.header.subIDs = []byte("sub-1,sub-2")
	if err := client.onMessage(message); err != nil {
		t.Fatal(err)
	}
	if len(delivered) != 1 || delivered[0] != "sub-2" {
		t.Fatalf("only the active subscription must receive the message: %v", delivered)
	}
}

func TestQueueAckFlushFailureRetainsBatch(t *testing.T) {
	var client, connection = auditConnectedClient(t)
	client.SetAckBatchSize(2).SetAckTimeout(0)
	if err := client.Ack("orders", "1|1|"); err != nil {
		t.Fatal(err)
	}
	_ = connection.Close()
	if err := client.Ack("orders", "1|2|"); err == nil {
		t.Fatal("batch send failure was swallowed")
	}
	var state = ensureClientState(client)
	state.lock.Lock()
	defer state.lock.Unlock()
	if state.pendingAckCount != 2 {
		t.Fatalf("unsent acknowledgements lost: pending=%d", state.pendingAckCount)
	}
}

func TestExplicitSequenceWithoutStoreDoesNotSuppressSendError(t *testing.T) {
	var client, connection = auditConnectedClient(t)
	_ = connection.Close()
	if _, err := client.ExecuteAsync(NewCommand("publish").SetTopic("orders").SetSequenceID(42), nil); err == nil {
		t.Fatal("explicit sequence was mistaken for a retained publish")
	}
}

func TestRetainedPublishDoesNotReportFailedWriteBeforeReplay(t *testing.T) {
	var client, connection = auditConnectedClient(t)
	client.SetPublishStore(NewMemoryPublishStore())
	var failures atomic.Int64
	client.SetFailedWriteHandler(FailedWriteHandlerFunc(func(*Message, string) { failures.Add(1) }))
	_ = connection.Close()
	if err := client.Publish("orders", "retained"); err != nil {
		t.Fatal(err)
	}
	if failures.Load() != 0 {
		t.Fatal("saved publish reported a terminal failure and invited a duplicate retry")
	}
}
