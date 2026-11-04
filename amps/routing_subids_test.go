package amps

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"
)

type routingSIDEvent struct {
	route   string
	subID   string
	payload string
}

func TestRoutingTrimASCIIWhitespaceAndEmptyAliasing(t *testing.T) {
	if got := trimASCIISpaces(nil); got != nil {
		t.Fatalf("nil input returned non-nil %v", got)
	}
	var allWhitespace = []byte(" \t\n\r")
	var empty = trimASCIISpaces(allWhitespace)
	if len(empty) != 0 || cap(empty) != cap(allWhitespace) || &empty[:1][0] != &allWhitespace[0] {
		t.Fatal("all-whitespace trim did not retain the original empty slice")
	}
	for value := 0; value < 256; value++ {
		var ch = byte(value)
		var input = []byte{ch, 'x', ch}
		var got = trimASCIISpaces(input)
		var isWhitespace = ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
		if isWhitespace {
			if len(got) != 1 || got[0] != 'x' || &got[0] != &input[1] {
				t.Fatalf("byte 0x%02x trim = %q", ch, got)
			}
		} else if len(got) != len(input) || &got[0] != &input[0] {
			t.Fatalf("byte 0x%02x unexpectedly trimmed", ch)
		}
	}
}

func TestRoutingSubIDsSeparatorsWhitespaceAndFallback(t *testing.T) {
	var cases = []struct {
		name      string
		subIDs    []byte
		queryID   string
		subID     string
		commandID string
		want      []routingSIDEvent
	}{
		{"nil query fallback", nil, "query", "original", "command", []routingSIDEvent{{"query", "original", "payload\x00"}}},
		{"empty query fallback", []byte{}, "query", "original", "command", []routingSIDEvent{{"query", "original", "payload\x00"}}},
		{"subID fallback", nil, "", "original", "command", []routingSIDEvent{{"original", "original", "payload\x00"}}},
		{"command fallback", nil, "", "", "command", []routingSIDEvent{{"command", "", "payload\x00"}}},
		{"only commas suppress fallback", []byte(",,,"), "query", "original", "command", []routingSIDEvent{{"unhandled", "original", "payload\x00"}}},
		{"empty and four spaces", []byte(" ,\t,\n,\r, "), "query", "original", "command", []routingSIDEvent{{"unhandled", "original", "payload\x00"}}},
		{"leading consecutive trailing", []byte(",a,, b,\tc\r,\n"), "query", "original", "command", []routingSIDEvent{{"a", "a", "payload\x00"}, {"b", "b", "payload\x00"}, {"c", "c", "payload\x00"}}},
		{"repeated and missing", []byte("a,a,missing,b,"), "query", "original", "command", []routingSIDEvent{{"a", "a", "payload\x00"}, {"a", "a", "payload\x00"}, {"b", "b", "payload\x00"}}},
		{"missing suppresses fallback", []byte("missing"), "query", "original", "command", []routingSIDEvent{{"unhandled", "original", "payload\x00"}}},
		{"other spaces retained", []byte(" \vraw\v ,\fraw\f,\u00a0raw\u00a0"), "query", "original", "command", []routingSIDEvent{{"\vraw\v", "\vraw\v", "payload\x00"}, {"\fraw\f", "\fraw\f", "payload\x00"}, {"\u00a0raw\u00a0", "\u00a0raw\u00a0", "payload\x00"}}},
	}
	for _, entry := range cases {
		t.Run(entry.name, func(t *testing.T) {
			var client = NewClient("routing-sids")
			var events []routingSIDEvent
			var record = func(route string) func(*Message) error {
				return func(message *Message) error {
					var subID, _ = message.SubID()
					events = append(events, routingSIDEvent{route, subID, string(message.Data())})
					return nil
				}
			}
			for _, route := range []string{"query", "original", "command", "a", "b", "c", "\vraw\v", "\fraw\f", "\u00a0raw\u00a0"} {
				client.routes.Store(route, record(route))
			}
			client.SetUnhandledMessageHandler(record("unhandled"))
			var original []byte
			if entry.subID != "" {
				original = []byte(entry.subID)
			}
			var message = &Message{header: &_Header{command: CommandPublish, subIDs: entry.subIDs, queryID: []byte(entry.queryID), subID: original, commandID: []byte(entry.commandID)}, data: []byte("payload\x00")}
			if err := client.onMessage(message); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(events, entry.want) {
				t.Fatalf("events = %#v; want %#v", events, entry.want)
			}
			if string(message.header.subID) != entry.subID || (original == nil && message.header.subID != nil) || (len(original) > 0 && &message.header.subID[0] != &original[0]) {
				t.Fatalf("original subID was not restored: %q", message.header.subID)
			}
		})
	}
}

func TestRoutingSubIDsObserveReentrantRouteChangesAndFirstError(t *testing.T) {
	var client = NewClient("routing-sids-reentrant")
	var firstErr = errors.New("first error")
	var laterErr = errors.New("later error")
	var events []routingSIDEvent
	var record = func(route string, message *Message) {
		var subID, _ = message.SubID()
		events = append(events, routingSIDEvent{route, subID, string(message.Data())})
	}
	client.routes.Store("first", func(message *Message) error {
		record("first", message)
		message.Replace(&Message{header: &_Header{command: CommandPublish, subIDs: []byte("ignored")}, data: []byte("after")})
		client.routes.Delete("second")
		client.routes.Store("third", func(message *Message) error { record("third-new", message); return laterErr })
		return firstErr
	})
	client.routes.Store("second", func(message *Message) error { record("second-removed", message); return nil })
	client.routes.Store("third", func(message *Message) error { record("third-old", message); return nil })
	var original = []byte("original")
	var message = &Message{header: &_Header{command: CommandPublish, subIDs: []byte("first,second,third"), subID: original}, data: []byte("before")}
	if err := client.onMessage(message); !errors.Is(err, firstErr) || errors.Is(err, laterErr) {
		t.Fatalf("error = %v, want first error only", err)
	}
	var want = []routingSIDEvent{{"first", "first", "before"}, {"third-new", "third", "after"}}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v; want %#v", events, want)
	}
	if string(message.header.subID) != "original" || &message.header.subID[0] != &original[0] {
		t.Fatalf("original subID was not restored: %q", message.header.subID)
	}
}

func TestRoutingSubIDsRecheckDuplicateAfterReplacement(t *testing.T) {
	var client = NewClient("routing-sids-duplicate")
	var store = &perfObservedBookmarkStore{MemoryBookmarkStore: NewMemoryBookmarkStore(), discard: func(subID string) bool { return subID == "second" }}
	client.SetBookmarkStore(store)
	var events []routingSIDEvent
	client.routes.Store("first", func(message *Message) error {
		var subID, _ = message.SubID()
		events = append(events, routingSIDEvent{"first", subID, string(message.Data())})
		message.Replace(&Message{header: &_Header{command: CommandPublish, bookmark: []byte{}}, data: []byte("after")})
		return nil
	})
	client.routes.Store("second", func(*Message) error { t.Error("duplicate was delivered to route"); return nil })
	client.SetDuplicateMessageHandler(func(message *Message) error {
		var subID, _ = message.SubID()
		events = append(events, routingSIDEvent{"duplicate", subID, string(message.Data())})
		return nil
	})
	var message = &Message{header: &_Header{command: CommandPublish, subIDs: []byte("first,second"), subID: []byte("original")}, data: []byte("before")}
	if err := client.onMessage(message); err != nil {
		t.Fatal(err)
	}
	var want = []routingSIDEvent{{"first", "first", "before"}, {"duplicate", "second", "after"}}
	if !reflect.DeepEqual(events, want) || !reflect.DeepEqual(store.seen, []string{"second"}) {
		t.Fatalf("events = %#v, checks = %v; want %#v and [second]", events, store.seen, want)
	}
	if got, _ := message.SubID(); got != "original" {
		t.Fatalf("restored subID = %q", got)
	}
}

func TestRoutingSubIDsRetriesWrappedFullOncePerAttempt(t *testing.T) {
	var client = NewClient("routing-sids-retry")
	client.connected.Store(true)
	t.Cleanup(func() { _ = client.Close() })
	var store = &perfObservedBookmarkStore{MemoryBookmarkStore: NewMemoryBookmarkStore(), discard: func(subID string) bool { return subID == "second" }}
	client.SetBookmarkStore(store)
	var events []string
	var attempts int
	var duplicateErr = errors.New("duplicate handler error")
	client.routes.Store("first", func(message *Message) error {
		attempts++
		if attempts == 1 {
			events = append(events, "first-1")
			return errors.Join(errors.New("wrapped"), errMessageStreamFull)
		}
		events = append(events, "first-2")
		message.Replace(&Message{header: &_Header{command: CommandPublish, bookmark: []byte("new-bookmark")}, data: []byte("after")})
		return nil
	})
	client.routes.Store("second", func(*Message) error { t.Error("duplicate reached route"); return nil })
	client.SetDuplicateMessageHandler(func(message *Message) error {
		var subID, _ = message.SubID()
		events = append(events, "duplicate-"+subID+":"+string(message.Data()))
		return duplicateErr
	})
	client.SetGlobalCommandTypeMessageHandler(CommandPublish, func(*Message) error { events = append(events, "global"); return nil })
	var message = &Message{header: &_Header{command: CommandPublish, subIDs: []byte("first,second"), subID: []byte("original"), bookmark: []byte{}}}
	if err := client.onMessage(message); !errors.Is(err, duplicateErr) {
		t.Fatalf("error = %v, want duplicate error", err)
	}
	var want = []string{"first-1", "first-2", "duplicate-second:after", "global"}
	if !reflect.DeepEqual(events, want) || !reflect.DeepEqual(store.seen, []string{"first", "second"}) {
		t.Fatalf("events = %v, duplicate checks = %v; want %v and [first second]", events, store.seen, want)
	}
	if got, _ := message.SubID(); got != "original" {
		t.Fatalf("restored subID = %q", got)
	}
}

func TestRoutingSubIDsRetriesTwoFullResponsesWithoutRepeatingDedup(t *testing.T) {
	var client = NewClient("routing-sids-two-retries")
	client.connected.Store(true)
	t.Cleanup(func() { _ = client.Close() })
	var store = &perfObservedBookmarkStore{MemoryBookmarkStore: NewMemoryBookmarkStore(), discard: func(subID string) bool { return subID == "second" }}
	client.SetBookmarkStore(store)
	var events []string
	var attempts int
	client.routes.Store("first", func(message *Message) error {
		attempts++
		switch attempts {
		case 1:
			events = append(events, "first-1")
			message.header.bookmark = []byte("changed")
			return errors.Join(errors.New("wrapped"), errMessageStreamFull)
		case 2:
			events = append(events, "first-2")
			return errMessageStreamFull
		default:
			events = append(events, "first-3")
			return nil
		}
	})
	client.routes.Store("second", func(*Message) error { t.Error("duplicate reached route"); return nil })
	client.SetDuplicateMessageHandler(func(message *Message) error {
		var subID, _ = message.SubID()
		events = append(events, "duplicate-"+subID)
		return nil
	})
	var message = &Message{header: &_Header{command: CommandPublish, subIDs: []byte("first,second"), bookmark: []byte{}}}
	if err := client.onMessage(message); err != nil {
		t.Fatal(err)
	}
	var want = []string{"first-1", "first-2", "first-3", "duplicate-second"}
	if !reflect.DeepEqual(events, want) || !reflect.DeepEqual(store.seen, []string{"first", "second"}) {
		t.Fatalf("events = %v, duplicate checks = %v; want %v and [first second]", events, store.seen, want)
	}
}

func TestRoutingSubIDsFullDisconnectedStillDeliversRemainingRoutes(t *testing.T) {
	var client = NewClient("routing-sids-retry-disconnected")
	var events []string
	client.routes.Store("first", func(*Message) error {
		events = append(events, "first")
		return errors.Join(errors.New("wrapped"), errMessageStreamFull)
	})
	client.routes.Store("second", func(*Message) error { events = append(events, "second"); return nil })
	client.SetGlobalCommandTypeMessageHandler(CommandPublish, func(*Message) error { events = append(events, "global"); return nil })
	var message = &Message{header: &_Header{command: CommandPublish, subIDs: []byte("first,second"), subID: []byte("original")}}
	if err := client.onMessage(message); !IsErrorKind(err, DisconnectedError) {
		t.Fatalf("error = %v, want disconnected", err)
	}
	if !reflect.DeepEqual(events, []string{"first", "second", "global"}) {
		t.Fatalf("events = %v", events)
	}
	if got, _ := message.SubID(); got != "original" {
		t.Fatalf("restored subID = %q", got)
	}
}

func TestRoutingSubIDsRetryPreservesFirstTerminalError(t *testing.T) {
	var client = NewClient("routing-sids-retry-error")
	client.connected.Store(true)
	t.Cleanup(func() { _ = client.Close() })
	var firstErr = errors.New("first terminal error")
	var laterErr = errors.New("later route error")
	var events []string
	var attempts int
	client.routes.Store("first", func(*Message) error {
		attempts++
		if attempts == 1 {
			events = append(events, "first-full")
			return errMessageStreamFull
		}
		events = append(events, "first-error")
		return firstErr
	})
	client.routes.Store("second", func(*Message) error { events = append(events, "second-error"); return laterErr })
	client.SetGlobalCommandTypeMessageHandler(CommandPublish, func(*Message) error { events = append(events, "global"); return nil })
	var message = &Message{header: &_Header{command: CommandPublish, subIDs: []byte("first,second")}}
	if err := client.onMessage(message); !errors.Is(err, firstErr) || errors.Is(err, laterErr) {
		t.Fatalf("error = %v, want first error only", err)
	}
	if !reflect.DeepEqual(events, []string{"first-full", "first-error", "second-error", "global"}) {
		t.Fatalf("events = %v", events)
	}
}

func TestRoutingSubIDsRetrySendsHeartbeatBeforeRetry(t *testing.T) {
	var client, connection = auditConnectedClient(t)
	client.hbCommand.header.command = commandHeartbeat
	client.heartbeatInterval.Store(1)
	client.heartbeatTimeout.Store(60)
	client.heartbeatTimestamp.Store(uint32(time.Now().Unix()) - 10)
	var attempts int
	client.routes.Store("first", func(*Message) error {
		attempts++
		var wire = connection.WrittenBytes()
		if attempts == 1 {
			if len(wire) != 0 {
				t.Fatalf("heartbeat preceded first attempt: %q", wire)
			}
			return errMessageStreamFull
		}
		if !bytes.Contains(wire, []byte(`"c":"heartbeat"`)) {
			t.Fatalf("retry occurred before heartbeat: %q", wire)
		}
		return nil
	})
	var message = &Message{header: &_Header{command: CommandPublish, subIDs: []byte("first")}}
	if err := client.onMessage(message); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || bytes.Count(connection.WrittenBytes(), []byte(`"c":"heartbeat"`)) != 1 {
		t.Fatalf("attempts=%d wire=%q", attempts, connection.WrittenBytes())
	}
}

func TestRoutingSubIDsBookmarkModesDoNotAllocate(t *testing.T) {
	for _, entry := range []struct {
		name     string
		bookmark []byte
	}{
		{"absent", nil},
		{"present", []byte("bookmark")},
	} {
		t.Run(entry.name, func(t *testing.T) {
			var client = NewClient("routing-sids-allocations")
			var calls int
			var handler = func(*Message) error { calls++; return nil }
			for _, subID := range []string{"a", "b", "c"} {
				client.routes.Store(subID, handler)
			}
			var message = &Message{header: &_Header{command: CommandPublish, subIDs: []byte("a,b,c"), bookmark: entry.bookmark}}
			const iterations = 1000
			var allocations = testing.AllocsPerRun(iterations, func() {
				if err := client.onMessage(message); err != nil {
					t.Fatal(err)
				}
			})
			if allocations != 0 || calls != 3*(iterations+1) {
				t.Fatalf("allocations = %v, callbacks = %d; want 0 and %d", allocations, calls, 3*(iterations+1))
			}
		})
	}
}

func BenchmarkRoutingSubIDsBookmark(b *testing.B) {
	for _, entry := range []struct {
		name     string
		bookmark []byte
	}{
		{"absent", nil},
		{"present", []byte("bookmark")},
	} {
		b.Run(entry.name, func(b *testing.B) {
			var client = NewClient("routing-sids-benchmark")
			var calls int
			var handler = func(*Message) error { calls++; return nil }
			for _, subID := range []string{"a", "b", "c"} {
				client.routes.Store(subID, handler)
			}
			var message = &Message{header: &_Header{command: CommandPublish, subIDs: []byte("a,b,c"), bookmark: entry.bookmark}}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if err := client.onMessage(message); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if calls != 3*b.N {
				b.Fatalf("callbacks = %d, want %d", calls, 3*b.N)
			}
		})
	}
}
