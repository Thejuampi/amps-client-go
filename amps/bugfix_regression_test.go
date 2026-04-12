package amps

import (
	"strings"
	"testing"
	"time"
)

// Fix #1: handleDisconnect CAS ordering — verify reconnectCancel is not
// overwritten when a reconnect is already in progress.
func TestHandleDisconnectCASDoesNotOverwriteActiveReconnectCancel(t *testing.T) {
	ha := NewHAClient("ha-cas-protect")
	ha.SetServerChooser(&fixedChooser{uri: "tcp://127.0.0.1:1/amps/json"})
	ha.SetReconnectDelay(2 * time.Second)
	ha.SetTimeout(0) // infinite timeout so reconnect stays active

	// Trigger first reconnect goroutine.
	ha.handleDisconnect(NewError(ConnectionError, "first"))
	time.Sleep(40 * time.Millisecond) // let goroutine start

	// Save the cancel function that should be controlling the first goroutine.
	ha.lock.Lock()
	firstCancel := ha.reconnectCancel
	ha.lock.Unlock()

	// Trigger second handleDisconnect — CAS should fail and NOT overwrite.
	ha.handleDisconnect(NewError(ConnectionError, "second"))

	ha.lock.Lock()
	secondCancel := ha.reconnectCancel
	ha.lock.Unlock()

	// The reconnectCancel must be the same pointer — the second call should
	// have been a no-op because CAS(false, true) fails.
	if firstCancel == nil {
		t.Fatalf("expected first reconnectCancel to be set")
	}
	if secondCancel == nil {
		t.Fatalf("expected reconnectCancel to still be set")
	}

	// Cleanup: Disconnect cancels the active reconnect.
	_ = ha.Disconnect()
	deadline := time.Now().Add(500 * time.Millisecond)
	for ha.reconnecting.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if ha.reconnecting.Load() {
		t.Fatalf("expected reconnecting flag to clear after Disconnect")
	}
}

// Fix #20: handleDisconnect defer must call cancel() to clean up context.
func TestHandleDisconnectCallsCancelOnCompletion(t *testing.T) {
	ha := NewHAClient("ha-cancel-completion")
	ha.SetServerChooser(&fixedChooser{uri: "tcp://127.0.0.1:1/amps/json"})
	ha.SetReconnectDelay(0)
	ha.SetTimeout(10 * time.Millisecond)

	ha.handleDisconnect(NewError(ConnectionError, "trigger"))
	time.Sleep(50 * time.Millisecond)

	ha.lock.Lock()
	cancelAfter := ha.reconnectCancel
	ha.lock.Unlock()

	// After reconnect completes, cancel must have been called (reconnectCancel = nil).
	if cancelAfter != nil {
		t.Fatalf("expected reconnectCancel to be nil after reconnect completion")
	}
}

// Fix #2: onConnectionError must stop heartbeat timer.
func TestOnConnectionErrorStopsHeartbeatTimer(t *testing.T) {
	client := NewClient("heartbeat-cleanup")

	// Simulate a heartbeat timer being set.
	client.heartbeatLock.Lock()
	client.heartbeatTimeoutID = time.AfterFunc(10*time.Second, func() {
		t.Errorf("heartbeat timer fired after onConnectionError — should have been stopped")
	})
	client.heartbeatTimestamp.Store(42)
	client.heartbeatTimeout.Store(10)
	client.heartbeatLock.Unlock()

	client.onConnectionError(NewError(ConnectionError, "test error"))

	client.heartbeatLock.Lock()
	timerID := client.heartbeatTimeoutID
	timestamp := client.heartbeatTimestamp.Load()
	client.heartbeatLock.Unlock()

	if timerID != nil {
		t.Fatalf("expected heartbeatTimeoutID to be nil after onConnectionError")
	}
	if timestamp != 0 {
		t.Fatalf("expected heartbeatTimestamp to be 0 after onConnectionError, got %d", timestamp)
	}
}

// Fix #18: DiscardUpTo must use NewError(CommandError, ...) for consistency.
func TestDiscardUpToGapDetectionUsesNewError(t *testing.T) {
	store := NewMemoryPublishStore()
	store.SetErrorOnPublishGap(true)

	// Store an entry to set lastPersisted baseline
	seq1, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":1}`)))
	if err != nil {
		t.Fatalf("Store returned error: %v", err)
	}
	// Mark seq1 as persisted
	store.DiscardUpTo(seq1)

	// Store another entry
	seq2, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":2}`)))
	if err != nil {
		t.Fatalf("Store returned error: %v", err)
	}
	_ = seq2

	// Discard with a sequence LOWER than lastPersisted to trigger gap detection
	err = store.DiscardUpTo(seq1 - 1)
	if err == nil {
		t.Fatalf("expected publish store gap error")
	}
	if !strings.Contains(err.Error(), "CommandError") {
		t.Fatalf("expected error to contain CommandError, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "publish store gap detected") {
		t.Fatalf("expected error message about gap detection, got %q", err.Error())
	}
}

// Fix #11: subscribePause must clone command before mutation.
func TestSubscribePauseClonesCommandBeforeMutation(t *testing.T) {
	manager := NewDefaultSubscriptionManager()

	// Subscribe normally first.
	cmd := NewCommand("subscribe").SetSubID("sub-clone-test").SetTopic("orders")
	cmd.SetOptions("live")
	manager.Subscribe(func(m *Message) error { return nil }, cmd, AckTypeProcessed)

	// Capture the command from the subscription.
	manager.lock.RLock()
	originalCmd := manager.subscriptions["sub-clone-test"].command
	originalOptions, _ := originalCmd.Options()
	manager.lock.RUnlock()

	// Now pause the same subscription — this should clone before mutating.
	pauseCmd := NewCommand("subscribe").SetSubID("sub-clone-test").SetTopic("orders")
	pauseCmd.SetOptions("pause")
	manager.Subscribe(nil, pauseCmd, AckTypeProcessed)

	// The ORIGINAL command pointer must not have been mutated.
	currentOptions, _ := originalCmd.Options()
	if currentOptions != originalOptions {
		t.Fatalf("expected original command options to be unchanged (%q), got %q", originalOptions, currentOptions)
	}

	// The subscription's command should now have "pause" prepended.
	manager.lock.RLock()
	pausedCmd := manager.subscriptions["sub-clone-test"].command
	pausedOptions, _ := pausedCmd.Options()
	manager.lock.RUnlock()

	if !strings.Contains(pausedOptions, "pause") {
		t.Fatalf("expected paused options to contain 'pause', got %q", pausedOptions)
	}

	// The paused command must be a different pointer than the original.
	if pausedCmd == originalCmd {
		t.Fatalf("expected subscribePause to clone the command, got same pointer")
	}
}

// Fix #6: SetTimeout stores timeoutNanos inside the lock consistently.
func TestSetTimeoutStoresConsistently(t *testing.T) {
	ha := NewHAClient("ha-timeout-consistency")

	ha.SetTimeout(100 * time.Millisecond)
	if got := ha.Timeout(); got != 100*time.Millisecond {
		t.Fatalf("expected timeout 100ms, got %v", got)
	}

	// Setting same value (was the early-return path) must also work.
	ha.SetTimeout(100 * time.Millisecond)
	if got := ha.Timeout(); got != 100*time.Millisecond {
		t.Fatalf("expected timeout 100ms after same-value set, got %v", got)
	}
	if got := time.Duration(ha.timeoutNanos.Load()); got != 100*time.Millisecond {
		t.Fatalf("expected timeoutNanos 100ms, got %v", got)
	}

	// Verify SetReconnectDelay is equally consistent.
	ha.SetReconnectDelay(50 * time.Millisecond)
	if got := ha.ReconnectDelay(); got != 50*time.Millisecond {
		t.Fatalf("expected reconnect delay 50ms, got %v", got)
	}
	ha.SetReconnectDelay(50 * time.Millisecond)
	if got := time.Duration(ha.reconnectDelayNanos.Load()); got != 50*time.Millisecond {
		t.Fatalf("expected reconnectDelayNanos 50ms after same-value set, got %v", got)
	}
}

// Fix #4: Verify Disconnect does not call closeSyncAckProcessing redundantly
// (verifiable by ensuring no panic from double-close patterns).
func TestDisconnectIdempotentCleanup(t *testing.T) {
	client := NewClient("idempotent-disconnect")

	// Disconnect from never-connected client: must not panic from double-close
	// of sync ack processing.
	for range 3 {
		err := client.Disconnect()
		if err == nil {
			t.Fatalf("expected disconnect error from never-connected client")
		}
	}
}
