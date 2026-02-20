package amps

import (
	"reflect"
	"testing"
	"time"
)

func TestClientParityAliases(t *testing.T) {
	client := NewClient("alias-test")
	client.SetName("alias-renamed")
	if name := client.Name(); name != "alias-renamed" {
		t.Fatalf("expected Name alias to return updated name, got %q", name)
	}

	client.SetLogonCorrelationData("corr-123")
	if correlation := client.LogonCorrelationData(); correlation != "corr-123" {
		t.Fatalf("expected correlation alias to round-trip, got %q", correlation)
	}

	client.SetRetryOnDisconnect(true)
	if !client.RetryOnDisconnect() {
		t.Fatalf("expected retry-on-disconnect to be true")
	}

	client.SetDefaultMaxDepth(42)
	if depth := client.DefaultMaxDepth(); depth != 42 {
		t.Fatalf("expected default max depth 42, got %d", depth)
	}
}

func TestConnectionStateListeners(t *testing.T) {
	client := NewClient("listener-test")
	var states []ConnectionState
	listener := ConnectionStateListenerFunc(func(state ConnectionState) {
		states = append(states, state)
	})

	client.AddConnectionStateListener(listener)
	client.notifyConnectionState(ConnectionStateConnected)
	client.notifyConnectionState(ConnectionStateLoggedOn)

	client.RemoveConnectionStateListener(listener)
	client.notifyConnectionState(ConnectionStateDisconnected)

	expected := []ConnectionState{ConnectionStateConnected, ConnectionStateLoggedOn}
	if !reflect.DeepEqual(states, expected) {
		t.Fatalf("unexpected connection-state sequence: got %+v want %+v", states, expected)
	}
}

func TestPublishFlushUsesStore(t *testing.T) {
	client := NewClient("publish-flush")
	store := NewMemoryPublishStore()
	client.SetPublishStore(store)
	_, _ = store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte("a")))

	if err := client.PublishFlush(10 * time.Millisecond); err == nil {

		t.Fatalf("expected publish flush timeout with undrained store")
	}
}

func TestTransportFilterHook(t *testing.T) {
	client := NewClient("transport-filter")
	conn := newTestConn()
	client.connected = true
	client.connection = conn

	called := false
	client.SetTransportFilter(func(direction TransportFilterDirection, payload []byte) []byte {
		if direction == TransportFilterOutbound {
			called = true
		}
		return payload
	})

	if err := client.Ack("orders", "1|1|"); err != nil {
		t.Fatalf("ack failed: %v", err)
	}
	if !called {
		t.Fatalf("expected outbound transport filter to be called")
	}
}
