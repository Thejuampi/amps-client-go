package amps

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestClientDisconnectKeepsRouteMapsStableAndClosesRegisteredStreams(t *testing.T) {
	client := NewClient("disconnect-route-reset")
	client.connected.Store(true)
	client.connection = newTestConn()

	initialRoutes := client.routes
	initialStreams := client.messageStreams

	stream := newMessageStream(client)
	stream.commandID = "stream-1"
	stream.setState(messageStreamStateReading)
	client.messageStreams.Store("stream-1", stream)
	client.routes.Store("stream-1", func(*Message) error { return nil })

	if err := client.Disconnect(); err != nil {
		t.Fatalf("disconnect failed: %v", err)
	}
	if client.routes != initialRoutes {
		t.Fatalf("disconnect replaced the routes map pointer")
	}
	if client.messageStreams != initialStreams {
		t.Fatalf("disconnect replaced the messageStreams map pointer")
	}
	if atomic.LoadInt32(&stream.state) != messageStreamStateComplete {
		t.Fatalf("expected disconnect to complete registered streams, got state=%d", atomic.LoadInt32(&stream.state))
	}
	if _, exists := client.routes.Load("stream-1"); exists {
		t.Fatalf("expected disconnect to remove the registered route")
	}
	if _, exists := client.messageStreams.Load("stream-1"); exists {
		t.Fatalf("expected disconnect to remove the registered message stream")
	}
}

func TestClientUnsubscribeAllKeepsRouteMapsStable(t *testing.T) {
	client := NewClient("unsubscribe-all-route-reset")
	client.connected.Store(true)
	client.connection = newTestConn()

	initialRoutes := client.routes

	stream := newMessageStream(client)
	stream.commandID = "stream-1"
	stream.setState(messageStreamStateReading)
	client.messageStreams.Store("stream-1", stream)
	client.routes.Store("stream-1", func(*Message) error { return nil })

	routeID, err := client.ExecuteAsync(NewCommand("unsubscribe").SetSubID("all"), nil)
	if err != nil {
		t.Fatalf("unsubscribe all failed: %v", err)
	}
	if routeID != "" {
		t.Fatalf("expected unsubscribe all route id to be empty, got %q", routeID)
	}
	if client.routes != initialRoutes {
		t.Fatalf("unsubscribe all replaced the routes map pointer")
	}
	if atomic.LoadInt32(&stream.state) != messageStreamStateComplete {
		t.Fatalf("expected unsubscribe all to complete registered streams, got state=%d", atomic.LoadInt32(&stream.state))
	}
	if _, exists := client.routes.Load("stream-1"); exists {
		t.Fatalf("expected unsubscribe all to remove the registered route")
	}
	if _, exists := client.messageStreams.Load("stream-1"); exists {
		t.Fatalf("expected unsubscribe all to remove the registered message stream")
	}
}

func TestClientDisconnectRouteResetDoesNotRaceWithDispatch(t *testing.T) {
	client := NewClient("disconnect-route-race")
	client.routes.Store("route-1", func(*Message) error { return nil })

	message := &Message{
		header: &_Header{
			command: CommandPublish,
			subID:   []byte("route-1"),
		},
	}

	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				_ = client.onMessage(message)
			}
		}
	}()

	time.Sleep(20 * time.Millisecond)
	client.connected.Store(true)
	client.connection = newTestConn()
	if err := client.Disconnect(); err != nil {
		t.Fatalf("disconnect failed: %v", err)
	}
	close(done)
}
