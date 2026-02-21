package amps

import (
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

type trackingSubscriptionManager struct {
	unsubscribed []string
}

func (manager *trackingSubscriptionManager) Subscribe(func(*Message) error, *Command, int) {}
func (manager *trackingSubscriptionManager) Unsubscribe(subID string) {
	manager.unsubscribed = append(manager.unsubscribed, subID)
}
func (manager *trackingSubscriptionManager) Clear() {}
func (manager *trackingSubscriptionManager) Resubscribe(*Client) error {
	return nil
}
func (manager *trackingSubscriptionManager) SetFailedResubscribeHandler(FailedResubscribeHandler) {}

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
	client.connected.Store(true)
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

func TestClientParityAdditionalWrappers(t *testing.T) {
	client := NewClient("parity-extra")
	state := ensureClientState(client)

	client.markManualDisconnect(true)
	state.lock.Lock()
	if !state.manualDisconnect {
		state.lock.Unlock()
		t.Fatalf("expected manual disconnect flag true")
	}
	state.lock.Unlock()
	client.markManualDisconnect(false)
	state.lock.Lock()
	if state.manualDisconnect {
		state.lock.Unlock()
		t.Fatalf("expected manual disconnect flag false")
	}
	state.lock.Unlock()

	parsedURL, _ := url.Parse("tcp://localhost:9007/amps/json")
	client.url = parsedURL
	info := client.GetConnectionInfo()
	if info["uri"] == "" {
		t.Fatalf("expected connection info uri")
	}
	if gathered := client.GatherConnectionInfo(); gathered["uri"] == "" {
		t.Fatalf("expected gathered connection info uri")
	}

	if _, err := client.BookmarkSubscribe("orders", "1|1|", "/id > 0"); err == nil {
		t.Fatalf("expected disconnected BookmarkSubscribe error")
	}
	if _, err := client.BookmarkSubscribeAsync(nil, "orders", "1|1|", "/id > 0"); err == nil {
		t.Fatalf("expected disconnected BookmarkSubscribeAsync error")
	}

	if err := client.AckMessage(nil); err == nil {
		t.Fatalf("expected nil AckMessage error")
	}
	if err := client.AckMessage(&Message{header: new(_Header)}); err == nil {
		t.Fatalf("expected missing topic/bookmark AckMessage error")
	}

	client.SetAutoAck(true).SetAckBatchSize(0).SetAckTimeout(200 * time.Millisecond)
	if !client.AutoAck() || client.AckBatchSize() != 1 || client.AckTimeout() != 200*time.Millisecond {
		t.Fatalf("unexpected auto-ack settings")
	}

	manager := &trackingSubscriptionManager{}
	client.SetSubscriptionManager(manager)
	client.SetRetryOnDisconnect(true)
	command := NewCommand("subscribe").SetSubID("sub-no-resub").SetTopic("orders")
	routeID, err := client.ExecuteAsyncNoResubscribe(command, nil)
	if err != nil || routeID == "" {
		t.Fatalf("expected no-resubscribe route registration, route=%q err=%v", routeID, err)
	}
	state.lock.Lock()
	_, exists := state.noResubscribeRoutes[routeID]
	state.lock.Unlock()
	if !exists {
		t.Fatalf("expected route in no-resubscribe set")
	}
	if len(manager.unsubscribed) == 0 || manager.unsubscribed[0] != routeID {
		t.Fatalf("expected subscription manager unsubscribe callback for %q", routeID)
	}

	client.ClearConnectionStateListeners()
	client.AddHTTPPreflightHeader("   ")
	client.AddHTTPPreflightHeader("X-Test: 1")
	client.SetHTTPPreflightHeaders([]string{"A: 1", "", "B: 2"})
	client.ClearHTTPPreflightHeaders()

	conn := newTestConn()
	client.connection = conn
	if client.RawConnection() != conn {
		t.Fatalf("unexpected raw connection")
	}

	started := 0
	stopped := 0
	client.SetReceiveRoutineStartedCallback(func() { started++ })
	client.SetReceiveRoutineStoppedCallback(func() { stopped++ })
	client.callReceiveRoutineStartedCallback()
	client.callReceiveRoutineStoppedCallback()
	if started != 1 || stopped != 1 {
		t.Fatalf("unexpected callback counters started=%d stopped=%d", started, stopped)
	}

	if summary := client.String(); !strings.Contains(summary, "client_name=") {
		t.Fatalf("expected diagnostic client summary, got %q", summary)
	}
}
