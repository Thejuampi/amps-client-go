package amps

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStreamNonterminalNextDoesNotWaitForLifecycleAction(t *testing.T) {
	for _, state := range []int32{messageStreamStateReading, messageStreamStateSubscribed} {
		var stream = newMessageStream(nil)
		stream.setState(state)
		var message = commandToMessage(NewCommand("publish").SetData([]byte("payload")))
		stream.current = message
		stream.lifecycleLock.Lock()
		var unlock sync.Once
		var exited = make(chan struct{})
		var result = make(chan *Message, 1)
		go func() {
			defer close(exited)
			result <- stream.Next()
		}()
		var release = func() { unlock.Do(stream.lifecycleLock.Unlock) }
		t.Cleanup(func() { release(); <-exited })
		select {
		case got := <-result:
			if got != message {
				t.Fatalf("state %d returned %p, want %p", state, got, message)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("state %d blocked a ready message behind route lifecycle work", state)
		}
		release()
	}
}

func TestStreamTerminalNextKeepsLifecycleSynchronization(t *testing.T) {
	for _, command := range []string{"group_end", "ack"} {
		t.Run(command, func(t *testing.T) {
			var stream = newMessageStream(nil)
			var message *Message
			if command == "group_end" {
				stream.setState(messageStreamStateSOWOnly)
				message = &Message{header: &_Header{command: CommandGroupEnd}}
			} else {
				stream.setState(messageStreamStateStatsOnly)
				message = &Message{header: &_Header{command: CommandAck, ackType: intPointer(AckTypeStats)}}
			}
			stream.current = message
			stream.commandID, stream.queryID = "command", "query"
			stream.lifecycleLock.Lock()
			var unlock sync.Once
			var result = make(chan *Message, 1)
			var started = make(chan struct{})
			var exited = make(chan struct{})
			go func() {
				defer close(exited)
				close(started)
				result <- stream.Next()
			}()
			var release = func() { unlock.Do(stream.lifecycleLock.Unlock) }
			t.Cleanup(func() { release(); <-exited })
			<-started
			select {
			case <-result:
				t.Fatal("terminal cleanup bypassed lifecycle synchronization")
			case <-time.After(20 * time.Millisecond):
			}
			release()
			select {
			case got := <-result:
				if got != message || atomic.LoadInt32(&stream.state) != messageStreamStateComplete || stream.commandID != "" || stream.queryID != "" {
					t.Fatal("terminal message did not finish route cleanup")
				}
			case <-time.After(time.Second):
				t.Fatal("terminal cleanup did not finish after lifecycle action")
			}
		})
	}
}

func TestStreamNextRechecksLifecycleStateBeforeCleaningRoutes(t *testing.T) {
	var stream = newMessageStream(nil)
	stream.setState(messageStreamStateSOWOnly)
	stream.current = &Message{header: &_Header{command: CommandGroupEnd}}
	stream.lifecycleLock.Lock()
	var unlock sync.Once
	var started = make(chan struct{})
	var exited = make(chan struct{})
	var result = make(chan *Message, 1)
	go func() {
		defer close(exited)
		close(started)
		result <- stream.Next()
	}()
	var release = func() { unlock.Do(stream.lifecycleLock.Unlock) }
	t.Cleanup(func() { release(); <-exited })
	<-started
	select {
	case <-result:
		t.Fatal("terminal message bypassed lifecycle synchronization")
	case <-time.After(20 * time.Millisecond):
	}
	stream.commandID, stream.queryID = "replacement", "replacement-query"
	stream.setState(messageStreamStateSubscribed)
	release()
	select {
	case <-result:
		if atomic.LoadInt32(&stream.state) != messageStreamStateSubscribed || stream.commandID != "replacement" || stream.queryID != "replacement-query" {
			t.Fatal("old terminal message cleaned routes from its replacement")
		}
	case <-time.After(time.Second):
		t.Fatal("Next did not finish after lifecycle action")
	}
}
