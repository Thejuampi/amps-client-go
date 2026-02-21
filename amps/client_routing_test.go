package amps

import (
	"reflect"
	"testing"
)

func TestOnMessageHandlerOrder(t *testing.T) {
	client := NewClient("routing-test")
	store := NewMemoryBookmarkStore()
	client.SetBookmarkStore(store)

	events := make([]string, 0, 8)
	client.routes.Store("sub-1", func(message *Message) error {
		events = append(events, "route")
		return nil
	})
	client.SetGlobalCommandTypeMessageHandler(CommandPublish, func(message *Message) error {
		events = append(events, "global")
		return nil
	})
	client.SetDuplicateMessageHandler(func(message *Message) error {
		events = append(events, "duplicate")
		return nil
	})
	client.SetUnhandledMessageHandler(func(message *Message) error {
		events = append(events, "unhandled")
		return nil
	})
	client.SetLastChanceMessageHandler(func(message *Message) error {
		events = append(events, "last-chance")
		return nil
	})

	message := &Message{
		header: &_Header{
			command:  CommandPublish,
			subID:    []byte("sub-1"),
			topic:    []byte("orders"),
			bookmark: []byte("99|1|"),
		},
		data: []byte(`{"id":1}`),
	}

	if err := client.onMessage(message); err != nil {
		t.Fatalf("unexpected message error: %v", err)
	}
	if err := client.onMessage(message); err != nil {
		t.Fatalf("unexpected duplicate message error: %v", err)
	}

	expected := []string{"route", "global", "route", "global", "duplicate"}
	if !reflect.DeepEqual(events, expected) {
		t.Fatalf("unexpected handler order: got %+v want %+v", events, expected)
	}
}

func TestUnhandledAndLastChanceOrder(t *testing.T) {
	client := NewClient("unhandled-test")

	events := make([]string, 0, 2)
	client.SetUnhandledMessageHandler(func(message *Message) error {
		events = append(events, "unhandled")
		return nil
	})
	client.SetLastChanceMessageHandler(func(message *Message) error {
		events = append(events, "last-chance")
		return nil
	})

	message := &Message{
		header: &_Header{
			command: CommandPublish,
			subID:   []byte("unknown-sub"),
			topic:   []byte("orders"),
		},
	}

	if err := client.onMessage(message); err != nil {
		t.Fatalf("unexpected message error: %v", err)
	}

	expected := []string{"unhandled", "last-chance"}
	if !reflect.DeepEqual(events, expected) {
		t.Fatalf("unexpected fallback order: got %+v want %+v", events, expected)
	}
}

func TestOnMessageMultiSubIDsWithWhitespace(t *testing.T) {
	client := NewClient("multi-sids")
	events := make([]string, 0, 4)

	client.routes.Store("sub-1", func(message *Message) error {
		events = append(events, "sub-1")
		return nil
	})
	client.routes.Store("sub-2", func(message *Message) error {
		events = append(events, "sub-2")
		return nil
	})

	message := &Message{
		header: &_Header{
			command: CommandPublish,
			subIDs:  []byte(" , sub-1, , sub-2 ,, "),
			topic:   []byte("orders"),
		},
		data: []byte(`{"id":1}`),
	}

	if err := client.onMessage(message); err != nil {
		t.Fatalf("unexpected message error: %v", err)
	}

	expected := []string{"sub-1", "sub-2"}
	if !reflect.DeepEqual(events, expected) {
		t.Fatalf("unexpected route dispatch: got %+v want %+v", events, expected)
	}
}
