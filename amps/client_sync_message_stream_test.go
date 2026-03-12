package amps

import (
	"strings"
	"testing"
	"time"
)

func TestClientDisconnectClosesSynchronousSubscribeStream(t *testing.T) {
	client, _, stream := executeSyncStreamWithProcessedAck(
		t,
		"execute-sync-disconnect-close",
		NewCommand("subscribe").SetTopic("orders").SetSubID("sync-disconnect-sub"),
		"sync-disconnect-sub",
	)
	t.Cleanup(func() {
		if stream != nil && stream.queue != nil {
			stream.queue.close()
		}
	})

	hasNextCh := make(chan bool, 1)
	go func() {
		hasNextCh <- stream.HasNext()
	}()

	select {
	case hasNext := <-hasNextCh:
		t.Fatalf("HasNext returned before disconnect: %v", hasNext)
	case <-time.After(20 * time.Millisecond):
	}

	if err := client.Disconnect(); err != nil {
		t.Fatalf("disconnect failed: %v", err)
	}

	select {
	case hasNext := <-hasNextCh:
		if hasNext {
			t.Fatalf("expected disconnect to close synchronous subscribe stream")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for synchronous subscribe stream to close on disconnect")
	}
}

func TestClientExecuteRegistersSynchronousSubscribeStreamForCleanup(t *testing.T) {
	client, _, stream := executeSyncStreamWithProcessedAck(
		t,
		"execute-sync-register",
		NewCommand("subscribe").SetTopic("orders").SetSubID("sync-registered-sub"),
		"sync-registered-sub",
	)

	if _, exists := client.messageStreams.Load(stream.commandID); !exists {
		t.Fatalf("expected synchronous subscribe stream to be registered for cleanup")
	}
}

func TestClientExecuteRegistersSynchronousSubscribeStreamBeforeProcessedAck(t *testing.T) {
	var client = NewClient("execute-sync-register-before-ack")
	var conn = newTestConn()
	client.connected.Store(true)
	client.connection = conn

	type executeResult struct {
		stream *MessageStream
		err    error
	}

	var resultCh = make(chan executeResult, 1)
	go func() {
		var stream, err = client.Execute(
			NewCommand("subscribe").SetTopic("orders").SetSubID("sync-register-before-ack"),
		)
		resultCh <- executeResult{stream: stream, err: err}
	}()

	var handler = waitForRouteHandler(t, client, "sync-register-before-ack")
	if _, exists := client.messageStreams.Load("sync-register-before-ack"); !exists {
		t.Fatalf("expected synchronous subscribe stream to be registered before processed ack")
	}

	var ack = AckTypeProcessed
	_ = handler(&Message{header: &_Header{
		command: CommandAck,
		ackType: &ack,
		status:  []byte("success"),
	}})

	var result = <-resultCh
	if result.err != nil || result.stream == nil {
		t.Fatalf("expected execute success, stream=%v err=%v", result.stream, result.err)
	}
}

func TestClientExecuteWarnsWhenProcessedAckWaitIsSlow(t *testing.T) {
	var previousInitialDelay = syncAckWarningInitialDelay
	var previousMaxDelay = syncAckWarningMaxDelay
	syncAckWarningInitialDelay = 10 * time.Millisecond
	syncAckWarningMaxDelay = 40 * time.Millisecond
	t.Cleanup(func() {
		syncAckWarningInitialDelay = previousInitialDelay
		syncAckWarningMaxDelay = previousMaxDelay
	})

	var client = NewClient("execute-sync-warn")
	var conn = newTestConn()
	client.connected.Store(true)
	client.connection = conn

	var warnings = make(chan error, 1)
	client.SetErrorHandler(func(err error) {
		select {
		case warnings <- err:
		default:
		}
	})

	type executeResult struct {
		stream *MessageStream
		err    error
	}

	var resultCh = make(chan executeResult, 1)
	go func() {
		var stream, err = client.Execute(
			NewCommand("subscribe").SetTopic("orders").SetSubID("sync-warn-sub"),
		)
		resultCh <- executeResult{stream: stream, err: err}
	}()

	var handler = waitForRouteHandler(t, client, "sync-warn-sub")

	select {
	case warning := <-warnings:
		if warning == nil || !strings.Contains(warning.Error(), "processed ack") || !strings.Contains(warning.Error(), "subscribe") {
			t.Fatalf("unexpected warning: %v", warning)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("expected warning for slow processed ack wait")
	}

	var ack = AckTypeProcessed
	_ = handler(&Message{header: &_Header{
		command: CommandAck,
		ackType: &ack,
		status:  []byte("success"),
	}})

	var result = <-resultCh
	if result.err != nil || result.stream == nil {
		t.Fatalf("expected execute success after delayed ack, stream=%v err=%v", result.stream, result.err)
	}
}

func TestClientExecuteDoesNotWarnForFastProcessedAck(t *testing.T) {
	var previousInitialDelay = syncAckWarningInitialDelay
	var previousMaxDelay = syncAckWarningMaxDelay
	syncAckWarningInitialDelay = 40 * time.Millisecond
	syncAckWarningMaxDelay = 80 * time.Millisecond
	t.Cleanup(func() {
		syncAckWarningInitialDelay = previousInitialDelay
		syncAckWarningMaxDelay = previousMaxDelay
	})

	var client = NewClient("execute-sync-fast-ack")
	var conn = newTestConn()
	client.connected.Store(true)
	client.connection = conn

	var warnings = make(chan error, 1)
	client.SetErrorHandler(func(err error) {
		select {
		case warnings <- err:
		default:
		}
	})

	type executeResult struct {
		stream *MessageStream
		err    error
	}

	var resultCh = make(chan executeResult, 1)
	go func() {
		var stream, err = client.Execute(
			NewCommand("subscribe").SetTopic("orders").SetSubID("sync-fast-sub"),
		)
		resultCh <- executeResult{stream: stream, err: err}
	}()

	var handler = waitForRouteHandler(t, client, "sync-fast-sub")
	var ack = AckTypeProcessed
	_ = handler(&Message{header: &_Header{
		command: CommandAck,
		ackType: &ack,
		status:  []byte("success"),
	}})

	var result = <-resultCh
	if result.err != nil || result.stream == nil {
		t.Fatalf("expected execute success for fast ack, stream=%v err=%v", result.stream, result.err)
	}

	select {
	case warning := <-warnings:
		t.Fatalf("unexpected warning for fast processed ack: %v", warning)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestClientExecuteBacksOffProcessedAckWarnings(t *testing.T) {
	var previousInitialDelay = syncAckWarningInitialDelay
	var previousMaxDelay = syncAckWarningMaxDelay
	syncAckWarningInitialDelay = 10 * time.Millisecond
	syncAckWarningMaxDelay = 40 * time.Millisecond
	t.Cleanup(func() {
		syncAckWarningInitialDelay = previousInitialDelay
		syncAckWarningMaxDelay = previousMaxDelay
	})

	var client = NewClient("execute-sync-backoff")
	var conn = newTestConn()
	client.connected.Store(true)
	client.connection = conn

	var warnings = make(chan error, 4)
	client.SetErrorHandler(func(err error) {
		select {
		case warnings <- err:
		default:
		}
	})

	type executeResult struct {
		stream *MessageStream
		err    error
	}

	var resultCh = make(chan executeResult, 1)
	go func() {
		var stream, err = client.Execute(
			NewCommand("subscribe").SetTopic("orders").SetSubID("sync-backoff-sub"),
		)
		resultCh <- executeResult{stream: stream, err: err}
	}()

	var handler = waitForRouteHandler(t, client, "sync-backoff-sub")

	select {
	case <-warnings:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected first warning for slow processed ack wait")
	}

	select {
	case warning := <-warnings:
		t.Fatalf("unexpected second warning before backoff delay elapsed: %v", warning)
	case <-time.After(15 * time.Millisecond):
	}

	select {
	case <-warnings:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected second warning after backoff delay elapsed")
	}

	var ack = AckTypeProcessed
	_ = handler(&Message{header: &_Header{
		command: CommandAck,
		ackType: &ack,
		status:  []byte("success"),
	}})

	var result = <-resultCh
	if result.err != nil || result.stream == nil {
		t.Fatalf("expected execute success after backoff warnings, stream=%v err=%v", result.stream, result.err)
	}
}

func TestClientWarningHelpersCoverage(t *testing.T) {
	var client = NewClient("warning-helpers")
	client.clientNameBytes = nil
	if bytes := client.effectiveClientNameBytes(); string(bytes) != "warning-helpers" {
		t.Fatalf("expected lazy client name bytes rebuild, got %q", string(bytes))
	}
	if result := (*Client)(nil).effectiveClientNameBytes(); result != nil {
		t.Fatalf("expected nil client name bytes for nil client")
	}

	var reported []error
	client.SetErrorHandler(func(err error) {
		reported = append(reported, err)
	})
	var state = ensureClientState(client)
	state.lock.Lock()
	state.exceptionListener = ExceptionListenerFunc(func(err error) {
		reported = append(reported, err)
	})
	state.lock.Unlock()
	client.reportWarning(nil)
	client.reportWarning(NewError(CommandError, "slow ack"))
	if len(reported) != 2 {
		t.Fatalf("expected error handler and exception listener reports, got %d", len(reported))
	}

	var command = NewCommand("subscribe").SetSubID("warn-helper")
	var done = client.startSyncAckWarning(command, "cid-1", "route-1")
	if done == nil {
		t.Fatalf("expected warning goroutine signal for valid command")
	}
	closeSignal(done)
	closeSignal(nil)
	if signal := (*Client)(nil).startSyncAckWarning(command, "cid-2", "route-2"); signal != nil {
		t.Fatalf("expected nil signal for nil client")
	}
	if signal := client.startSyncAckWarning(nil, "cid-3", "route-3"); signal != nil {
		t.Fatalf("expected nil signal for nil command")
	}
}
