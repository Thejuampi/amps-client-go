package amps

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type perfObservedBookmarkStore struct {
	*MemoryBookmarkStore
	seen    []string
	discard func(string) bool
}

func (store *perfObservedBookmarkStore) IsDiscarded(message *Message) bool {
	var subID, _ = message.SubID()
	store.seen = append(store.seen, subID)
	return store.discard != nil && store.discard(subID)
}

func TestPerfBookmarkPresenceNilAndEmpty(t *testing.T) {
	for _, entry := range []struct {
		name          string
		bookmark      []byte
		wantDuplicate bool
	}{
		{"absent", nil, false},
		{"empty-present", []byte{}, true},
		{"nonempty", []byte("1|1|"), true},
	} {
		t.Run(entry.name, func(t *testing.T) {
			var client = NewClient("bookmark-presence")
			var store = &perfObservedBookmarkStore{MemoryBookmarkStore: NewMemoryBookmarkStore(), discard: func(string) bool { return true }}
			client.SetBookmarkStore(store)
			var routes, duplicates int
			client.routes.Store("sub", func(*Message) error { routes++; return nil })
			client.SetDuplicateMessageHandler(func(*Message) error { duplicates++; return nil })
			var message = &Message{header: &_Header{command: CommandPublish, subID: []byte("sub"), bookmark: entry.bookmark}}
			if err := client.onMessage(message); err != nil {
				t.Fatal(err)
			}
			if entry.wantDuplicate {
				if routes != 0 || duplicates != 1 || !reflect.DeepEqual(store.seen, []string{"sub"}) {
					t.Fatalf("present bookmark: routes=%d duplicates=%d observed=%v", routes, duplicates, store.seen)
				}
			} else if routes != 1 || duplicates != 0 || len(store.seen) != 0 {
				t.Fatalf("absent bookmark: routes=%d duplicates=%d observed=%v", routes, duplicates, store.seen)
			}
		})
	}
}

func TestPerfBookmarkIsRecheckedAfterEachRoute(t *testing.T) {
	var client = NewClient("bookmark-change")
	var store = &perfObservedBookmarkStore{MemoryBookmarkStore: NewMemoryBookmarkStore(), discard: func(subID string) bool { return subID == "sub-2" }}
	client.SetBookmarkStore(store)
	var events []string
	client.routes.Store("sub-1", func(message *Message) error {
		events = append(events, "sub-1")
		message.Replace(&Message{header: &_Header{command: CommandPublish, bookmark: []byte("2|1|")}})
		return nil
	})
	client.routes.Store("sub-2", func(*Message) error { events = append(events, "unexpected-sub-2"); return nil })
	client.SetDuplicateMessageHandler(func(message *Message) error {
		var subID, _ = message.SubID()
		events = append(events, "duplicate-"+subID)
		return nil
	})
	client.SetGlobalCommandTypeMessageHandler(CommandPublish, func(*Message) error { events = append(events, "global"); return nil })
	var message = &Message{header: &_Header{command: CommandPublish, subID: []byte("original"), subIDs: []byte("sub-1,sub-2")}}
	if err := client.onMessage(message); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(events, []string{"sub-1", "duplicate-sub-2", "global"}) || !reflect.DeepEqual(store.seen, []string{"sub-2"}) {
		t.Fatalf("events=%v bookmark checks=%v", events, store.seen)
	}
	if got, _ := message.SubID(); got != "original" {
		t.Fatalf("restored subID = %q", got)
	}
}

func TestPerfGlobalHandlersUpdateBetweenRoutes(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(fmt.Sprintf("remove-%t", remove), func(t *testing.T) {
			var client = NewClient("global-reentrant")
			var events []string
			var handler = func(*Message) error { events = append(events, "global"); return nil }
			if remove {
				client.SetGlobalCommandTypeMessageHandler(CommandPublish, handler)
			}
			client.routes.Store("sub-1", func(*Message) error {
				events = append(events, "sub-1")
				if remove {
					client.SetGlobalCommandTypeMessageHandler(CommandPublish, nil)
				} else {
					client.SetGlobalCommandTypeMessageHandler(CommandPublish, handler)
				}
				return nil
			})
			client.routes.Store("sub-2", func(*Message) error { events = append(events, "sub-2"); return nil })
			var message = &Message{header: &_Header{command: CommandPublish, subIDs: []byte("sub-1,sub-2")}}
			if err := client.onMessage(message); err != nil {
				t.Fatal(err)
			}
			var want = []string{"sub-1", "sub-2"}
			if !remove {
				want = append(want, "global")
			}
			if !reflect.DeepEqual(events, want) {
				t.Fatalf("events=%v want=%v", events, want)
			}
		})
	}
}

func TestPerfGlobalHandlersPreserveOtherCommandOnRemoval(t *testing.T) {
	var client = NewClient("global-command-map")
	var events []string
	var publish = func(*Message) error { events = append(events, "publish"); return nil }
	var sow = func(*Message) error { events = append(events, "sow"); return nil }
	client.SetGlobalCommandTypeMessageHandler(CommandPublish, publish)
	client.SetGlobalCommandTypeMessageHandler(CommandSOW, sow)
	client.SetGlobalCommandTypeMessageHandler(CommandPublish, nil)
	if _, handled := client.callGlobalCommandTypeHandler(CommandPublish, &Message{}); handled {
		t.Fatal("removed publish handler ran")
	}
	if _, handled := client.callGlobalCommandTypeHandler(CommandSOW, &Message{}); !handled {
		t.Fatal("remaining SOW handler was lost")
	}
	client.SetGlobalCommandTypeMessageHandler(CommandSOW, nil)
	if _, handled := client.callGlobalCommandTypeHandler(CommandSOW, &Message{}); handled {
		t.Fatal("last handler was not removed")
	}
	if !reflect.DeepEqual(events, []string{"sow"}) {
		t.Fatalf("events=%v", events)
	}
}

func TestPerfGlobalHandlersConcurrentRegistration(t *testing.T) {
	var client = NewClient("global-concurrent")
	var calls atomic.Int64
	var handler = func(*Message) error { calls.Add(1); return nil }
	var start = make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		for index := 0; index < 1000; index++ {
			client.SetGlobalCommandTypeMessageHandler(CommandPublish, handler)
			client.SetGlobalCommandTypeMessageHandler(CommandPublish, nil)
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for index := 0; index < 1000; index++ {
			_, _ = client.callGlobalCommandTypeHandler(CommandPublish, &Message{})
		}
	}()
	close(start)
	workers.Wait()
	client.SetGlobalCommandTypeMessageHandler(CommandPublish, handler)
	var before = calls.Load()
	if _, handled := client.callGlobalCommandTypeHandler(CommandPublish, &Message{}); !handled || calls.Load() != before+1 {
		t.Fatal("registered handler was not visible after the setter returned")
	}
}

func TestPerfAckBookkeepingOnlyForAck(t *testing.T) {
	var client = NewClient("ack-gate")
	var store = NewMemoryPublishStore()
	client.SetPublishStore(store)
	var sequence, err = store.Store(NewCommand("publish").SetTopic("orders"))
	if err != nil {
		t.Fatal(err)
	}
	var ack = AckTypePersisted
	var message = &Message{header: &_Header{command: CommandPublish, ackType: &ack, status: []byte("success"), sequenceID: &sequence}}
	if err = client.onMessage(message); err != nil {
		t.Fatal(err)
	}
	if got := store.UnpersistedCount(); got != 1 {
		t.Fatalf("non-ack discarded %d entries", 1-got)
	}
	message.header.command = CommandAck
	if err = client.onMessage(message); err != nil {
		t.Fatal(err)
	}
	if got := store.UnpersistedCount(); got != 0 {
		t.Fatalf("persisted ack left %d entries", got)
	}
}

func TestPerfHeartbeatStillUsesTimestampWhenEnabled(t *testing.T) {
	for _, entry := range []struct {
		name     string
		interval uint32
		age      uint32
		wantSend bool
	}{
		{"disabled", 0, 10, false},
		{"not-due", 60, 1, false},
		{"due", 1, 10, true},
	} {
		t.Run(entry.name, func(t *testing.T) {
			var client, connection = auditConnectedClient(t)
			client.hbCommand.header.command = commandHeartbeat
			client.heartbeatInterval.Store(entry.interval)
			client.heartbeatTimeout.Store(60)
			client.heartbeatTimestamp.Store(uint32(time.Now().Unix()) - entry.age)
			if err := client.onMessage(&Message{header: &_Header{command: commandHeartbeat}}); err != nil {
				t.Fatal(err)
			}
			var sent = connection.WrittenBytes()
			if (len(sent) > 0) != entry.wantSend {
				t.Fatalf("heartbeat output=%q", sent)
			}
			if entry.wantSend && !bytes.Contains(sent, []byte(`"c":"heartbeat"`)) {
				t.Fatalf("wrong heartbeat frame=%q", sent)
			}
		})
	}
}

func TestPerfRouteErrorStillDeliversRemainingSIDAndRestores(t *testing.T) {
	var client = NewClient("routing-errors")
	var expected = errors.New("first handler failed")
	var events []string
	client.routes.Store("sub-1", func(*Message) error { events = append(events, "sub-1"); return expected })
	client.routes.Store("sub-2", func(*Message) error { events = append(events, "sub-2"); return nil })
	var message = &Message{header: &_Header{command: CommandPublish, subID: []byte("old"), subIDs: []byte("sub-1,sub-2")}}
	if err := client.onMessage(message); !errors.Is(err, expected) {
		t.Fatalf("error=%v", err)
	}
	if !reflect.DeepEqual(events, []string{"sub-1", "sub-2"}) {
		t.Fatalf("events=%v", events)
	}
	if got, _ := message.SubID(); got != "old" {
		t.Fatalf("restored subID=%q", got)
	}
}
