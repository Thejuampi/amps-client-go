package main

import (
	"net"
	"sync"
	"strings"
	"testing"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

func TestClientHeartbeatStartReceivesServerHeartbeats(t *testing.T) {
	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	var done = make(chan struct{})
	go func() {
		defer close(done)
		var conn, acceptErr = listener.Accept()
		if acceptErr != nil {
			return
		}
		handleConnection(conn)
	}()

	var client = amps.NewClient("fakeamps-heartbeat-client")
	var errCh = make(chan error, 1)
	var heartbeatCh = make(chan struct{}, 4)
	client.SetErrorHandler(func(err error) {
		select {
		case errCh <- err:
		default:
		}
	})
	client.SetTransportFilter(func(direction amps.TransportFilterDirection, payload []byte) []byte {
		if direction == amps.TransportFilterInbound && strings.Contains(string(payload), `"c":"heartbeat"`) {
			select {
			case heartbeatCh <- struct{}{}:
			default:
			}
		}
		return payload
	})
	client.SetHeartbeat(1, 2)

	var uri = "tcp://" + listener.Addr().String() + "/amps/json"
	if err := client.Connect(uri); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer func() { _ = client.Close() }()
	if err := client.Logon(); err != nil {
		t.Fatalf("logon failed: %v", err)
	}

	select {
	case <-heartbeatCh:
	case err := <-errCh:
		t.Fatalf("unexpected client error before heartbeat: %v", err)
	case <-time.After(4 * time.Second):
		t.Fatalf("timed out waiting for broker heartbeat after start")
	}

	select {
	case err := <-errCh:
		t.Fatalf("unexpected client heartbeat error: %v", err)
	case <-time.After(2500 * time.Millisecond):
	}

	_ = client.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("handleConnection did not exit in time")
	}
}

func TestBookmarkSubscribePersistsMostRecentOnCompletedAck(t *testing.T) {
	var oldSow = sow
	var oldJournal = journal
	sow = newSOWCache()
	journal = newMessageJournal(1000)

	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	var serverDone = make(chan struct{})
	var handlerDone sync.WaitGroup
	go func() {
		defer close(serverDone)
		for {
			var conn, acceptErr = listener.Accept()
			if acceptErr != nil {
				return
			}
			handlerDone.Add(1)
			go func() {
				defer handlerDone.Done()
				handleConnection(conn)
			}()
		}
	}()

	var client = amps.NewClient("fakeamps-bookmark-client")
	var errCh = make(chan error, 1)
	client.SetErrorHandler(func(err error) {
		select {
		case errCh <- err:
		default:
		}
	})

	var uri = "tcp://" + listener.Addr().String() + "/amps/json"
	if err := client.Connect(uri); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	if err := client.Logon(); err != nil {
		t.Fatalf("logon failed: %v", err)
	}

	var topic = "orders.bookmark.completed"
	if err := client.Publish(topic, `{"id":1}`); err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	if err := client.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	var store = amps.NewMemoryBookmarkStore()
	client.SetBookmarkStore(store)

	var deliveryBookmarkCh = make(chan string, 1)
	var subID, subErr = client.BookmarkSubscribeAsync(func(message *amps.Message) error {
		var command, _ = message.Command()
		if command != amps.CommandPublish {
			return nil
		}

		var bookmark, _ = message.Bookmark()
		select {
		case deliveryBookmarkCh <- bookmark:
		default:
		}
		return nil
	}, topic, amps.BOOKMARK_EPOCH())
	if subErr != nil {
		t.Fatalf("bookmark subscribe failed: %v", subErr)
	}
	defer func() {
		_ = client.Unsubscribe(subID)
		_ = client.Close()
		_ = listener.Close()

		select {
		case <-serverDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("accept loop did not exit in time")
		}

		var handlersExited = make(chan struct{})
		go func() {
			handlerDone.Wait()
			close(handlersExited)
		}()

		select {
		case <-handlersExited:
		case <-time.After(2 * time.Second):
			t.Fatalf("handleConnection did not exit in time")
		}

		sow = oldSow
		journal = oldJournal
	}()

	var deliveredBookmark string
	select {
	case deliveredBookmark = <-deliveryBookmarkCh:
	case err := <-errCh:
		t.Fatalf("unexpected client error before bookmark delivery: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for bookmark replay delivery")
	}

	var deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var mostRecent = store.GetMostRecent(subID)
		if mostRecent != "" {
			if mostRecent != deliveredBookmark {
				t.Fatalf("most recent bookmark = %q, want %q", mostRecent, deliveredBookmark)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for bookmark store persistence after completed ack")
}
