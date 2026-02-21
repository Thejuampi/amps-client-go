package amps

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
)

type recordingSubscriptionManager struct {
	resubscribeCalls int
}

func (manager *recordingSubscriptionManager) Subscribe(func(*Message) error, *Command, int) {}
func (manager *recordingSubscriptionManager) Unsubscribe(string)                            {}
func (manager *recordingSubscriptionManager) Clear()                                        {}
func (manager *recordingSubscriptionManager) SetFailedResubscribeHandler(FailedResubscribeHandler) {
}

func (manager *recordingSubscriptionManager) Resubscribe(*Client) error {
	manager.resubscribeCalls++
	return nil
}

func TestRetryOnDisconnectToggleBehavior(t *testing.T) {
	client := NewClient("retry-toggle")
	command := NewCommand("subscribe").SetTopic("orders")

	client.SetRetryOnDisconnect(false)
	_, err := client.ExecuteAsync(cloneCommand(command), nil)
	if err == nil {
		t.Fatalf("expected disconnected error when retry-on-disconnect is disabled")
	}

	state := ensureClientState(client)
	state.lock.Lock()
	pendingWithoutRetry := len(state.pendingRetry)
	state.lock.Unlock()
	if pendingWithoutRetry != 0 {
		t.Fatalf("expected no queued retry commands when retry disabled, got %d", pendingWithoutRetry)
	}

	client.SetRetryOnDisconnect(true)
	_, err = client.ExecuteAsync(cloneCommand(command), nil)
	if err != nil {
		t.Fatalf("expected command to queue for retry when retry enabled, got error: %v", err)
	}

	state.lock.Lock()
	pendingWithRetry := len(state.pendingRetry)
	state.lock.Unlock()
	if pendingWithRetry != 1 {
		t.Fatalf("expected one queued retry command when retry enabled, got %d", pendingWithRetry)
	}
}

func TestTimerCommandPathWritesStartAndStop(t *testing.T) {
	client := NewClient("timer-command")
	conn := newTestConn()
	client.connected = true
	client.connection = conn

	if _, err := client.StartTimer("timer-1", "interval=250ms"); err != nil {
		t.Fatalf("start timer failed: %v", err)
	}
	if _, err := client.StopTimer("timer-1"); err != nil {
		t.Fatalf("stop timer failed: %v", err)
	}

	written := string(conn.WrittenBytes())
	if !strings.Contains(written, `"c":"start_timer"`) {
		t.Fatalf("expected start_timer command payload, got %q", written)
	}
	if !strings.Contains(written, `"c":"stop_timer"`) {
		t.Fatalf("expected stop_timer command payload, got %q", written)
	}
}

func TestApplyAckBookkeepingDiscardPublishOnPersistedAck(t *testing.T) {
	client := NewClient("ack-bookkeeping")
	store := NewMemoryPublishStore()
	client.SetPublishStore(store)

	command := NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":1}`)).SetCommandID("cmd-1")
	sequence, err := store.Store(command)
	if err != nil {
		t.Fatalf("store publish command failed: %v", err)
	}
	command.SetSequenceID(sequence)

	state := ensureClientState(client)
	state.lock.Lock()
	state.pendingPublishByCmdID["cmd-1"] = cloneCommand(command)
	state.lock.Unlock()

	ackType := AckTypePersisted
	message := &Message{
		header: &_Header{
			command:    CommandAck,
			commandID:  []byte("cmd-1"),
			status:     []byte("success"),
			ackType:    &ackType,
			sequenceID: &sequence,
		},
	}

	client.applyAckBookkeeping(message)

	if store.UnpersistedCount() != 0 {
		t.Fatalf("expected persisted ack to discard stored publish command")
	}

	state.lock.Lock()
	_, exists := state.pendingPublishByCmdID["cmd-1"]
	state.lock.Unlock()
	if exists {
		t.Fatalf("expected pending publish command to be removed after persisted ack")
	}
}

func TestExecuteAsyncFailedWriteHandlerOnSendFailure(t *testing.T) {
	client := NewClient("failed-write")
	conn := newTestConn()
	client.connected = true
	client.connection = conn
	_ = conn.Close()

	called := false
	client.SetFailedWriteHandler(FailedWriteHandlerFunc(func(message *Message, reason string) {
		called = true
		if message == nil {
			t.Fatalf("expected failed write callback to include message")
		}
		if command, _ := message.Command(); command != CommandPublish {
			t.Fatalf("expected failed publish callback, got command=%d", command)
		}
		if reason == "" {
			t.Fatalf("expected non-empty failed write reason")
		}
	}))

	_, err := client.ExecuteAsync(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":2}`)), nil)
	if err == nil {
		t.Fatalf("expected publish send failure")
	}
	if !called {
		t.Fatalf("expected failed write handler to be invoked")
	}
}

func TestPostLogonRecoveryReplaysPublishStoreAndResubscribes(t *testing.T) {
	client := NewClient("recovery")
	conn := newTestConn()
	client.connected = true
	client.connection = conn

	store := NewMemoryPublishStore()
	client.SetPublishStore(store)
	if _, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":3}`))); err != nil {
		t.Fatalf("store publish for replay failed: %v", err)
	}

	manager := &recordingSubscriptionManager{}
	client.SetSubscriptionManager(manager)

	var states []ConnectionState
	client.AddConnectionStateListener(ConnectionStateListenerFunc(func(state ConnectionState) {
		states = append(states, state)
	}))

	state := ensureClientState(client)
	state.lock.Lock()
	state.pendingRetry = append(state.pendingRetry, retryCommand{
		command: NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":"retry"}`)),
	})
	state.lock.Unlock()

	client.postLogonRecovery()

	payload := string(conn.WrittenBytes())
	if !strings.Contains(payload, `"c":"p"`) {
		t.Fatalf("expected publish replay payload, got %q", payload)
	}
	if manager.resubscribeCalls != 1 {
		t.Fatalf("expected one resubscribe call, got %d", manager.resubscribeCalls)
	}

	foundPublishReplayed := false
	foundResubscribed := false
	for _, current := range states {
		if current == ConnectionStatePublishReplayed {
			foundPublishReplayed = true
		}
		if current == ConnectionStateResubscribed {
			foundResubscribed = true
		}
	}
	if !foundPublishReplayed || !foundResubscribed {
		t.Fatalf("expected publish replay and resubscribe states, got %+v", states)
	}
}

func TestPostLogonRecoveryCoalescesConcurrentRequests(t *testing.T) {
	client := NewClient("recovery-coalesce")

	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	firstDone := make(chan struct{})
	secondRan := atomic.Bool{}

	client.DeferredExecution(func(callbackClient *Client, userData any) {
		_ = callbackClient
		_ = userData
		close(firstStarted)
		<-firstRelease
		close(firstDone)
	}, "first")

	recoveryDone := make(chan struct{})
	go func() {
		client.postLogonRecovery()
		close(recoveryDone)
	}()

	<-firstStarted
	client.DeferredExecution(func(callbackClient *Client, userData any) {
		_ = callbackClient
		_ = userData
		secondRan.Store(true)
	}, "second")

	// Concurrent recovery request while one is active.
	client.postLogonRecovery()
	close(firstRelease)

	<-firstDone
	<-recoveryDone
	if !secondRan.Load() {
		t.Fatalf("expected concurrent recovery request to trigger coalesced rerun")
	}
}

func TestHandleSendFailureOnlyForPublishCommands(t *testing.T) {
	client := NewClient("send-failure-filter")
	called := false
	client.SetFailedWriteHandler(FailedWriteHandlerFunc(func(message *Message, reason string) {
		called = true
		_ = message
		_ = reason
	}))

	client.handleSendFailure(NewCommand("subscribe").SetTopic("orders"), errors.New("fail"))
	if called {
		t.Fatalf("did not expect failed write callback for non-publish command")
	}

	client.handleSendFailure(NewCommand("publish").SetTopic("orders").SetData([]byte("x")), errors.New("fail"))
	if !called {
		t.Fatalf("expected failed write callback for publish command")
	}
}
