package amps

import "testing"

func TestDefaultSubscriptionManagerResubscribeQueuesWhenDisconnected(t *testing.T) {
	manager := NewDefaultSubscriptionManager()
	command := NewCommand("subscribe").SetTopic("orders").SetSubID("sub-1")
	manager.Subscribe(nil, command, AckTypeNone)

	client := NewClient("resubscribe-test")
	client.connected = false
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
	client.connected = false
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
