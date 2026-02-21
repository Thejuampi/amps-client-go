package amps

import (
	"strings"
	"testing"
	"time"
)

func TestClientCppParityCoreHelpers(t *testing.T) {
	client := NewClient("cpp-core")
	if !client.IsValid() {
		t.Fatalf("expected client to be valid")
	}
	if client.GetHandle() == 0 {
		t.Fatalf("expected non-zero opaque handle")
	}
	if client.GetName() != "cpp-core" {
		t.Fatalf("unexpected GetName result: %q", client.GetName())
	}
	if client.GetNameHash() == "" {
		t.Fatalf("expected non-empty name hash")
	}
	if client.GetNameHashValue() == 0 {
		t.Fatalf("expected non-zero name hash value")
	}

	client.serverVersion = "5.3.5.1"
	if version := client.GetServerVersion(); version != 5030501 {
		t.Fatalf("unexpected numeric server version: %d", version)
	}
	info := client.GetServerVersionInfo()
	if info.Major != 5 || info.Minor != 3 || info.Maintenance != 5 || info.Hotfix != 1 {
		t.Fatalf("unexpected parsed server version: %+v", info)
	}
}

func TestClientCppParityHandlersAndPreflight(t *testing.T) {
	client := NewClient("cpp-routes")
	client.AddHttpPreflightHeaderKV("X-Test", "1")
	client.AddHTTPPreflightHeaderKV("X-Other", "2")

	state := ensureClientState(client)
	state.lock.Lock()
	headers := append([]string(nil), state.httpPreflightHeaders...)
	state.lock.Unlock()
	if len(headers) != 2 {
		t.Fatalf("expected two preflight headers, got %d", len(headers))
	}
	if headers[0] != "X-Test: 1" || headers[1] != "X-Other: 2" {
		t.Fatalf("unexpected preflight headers: %+v", headers)
	}

	routeHandler := func(*Message) error { return nil }
	if err := client.AddMessageHandler("route-1", routeHandler, AckTypeProcessed, false); err != nil {
		t.Fatalf("AddMessageHandler failed: %v", err)
	}
	if !client.RemoveMessageHandler("route-1") {
		t.Fatalf("expected route to be removed")
	}
	if client.RemoveMessageHandler("route-1") {
		t.Fatalf("expected second route removal to report false")
	}
}

func TestClientCppParitySendAndPublishSequence(t *testing.T) {
	client := NewClient("cpp-send")
	conn := newTestConn()
	client.connected = true
	client.connection = conn

	rawMessage := NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":1}`)).GetMessage()
	if err := client.Send(rawMessage); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	store := NewMemoryPublishStore()
	client.SetPublishStore(store)
	sequence, err := client.PublishWithSequence("orders", `{"id":2}`)
	if err != nil {
		t.Fatalf("PublishWithSequence failed: %v", err)
	}
	if sequence == 0 {
		t.Fatalf("expected non-zero publish sequence id")
	}
	if store.UnpersistedCount() == 0 {
		t.Fatalf("expected publish store to track unpersisted publish")
	}

	payload := string(conn.WrittenBytes())
	if !strings.Contains(payload, `"c":"p"`) || !strings.Contains(payload, `"t":"orders"`) {
		t.Fatalf("unexpected send payload: %q", payload)
	}
}

func TestClientCppParityBatchingAndDeferredExecution(t *testing.T) {
	client := NewClient("cpp-batch")
	client.SetPublishBatching(4096, 250)
	size, timeout := client.PublishBatching()
	if size != 4096 || timeout != 250*time.Millisecond {
		t.Fatalf("unexpected publish batching config: size=%d timeout=%s", size, timeout)
	}

	called := false
	client.DeferredExecution(func(callbackClient *Client, userData any) {
		called = callbackClient == client && userData == "done"
	}, "done")
	client.postLogonRecovery()
	if !called {
		t.Fatalf("expected deferred execution callback to run during postLogonRecovery")
	}
}

func TestCommandParityHelpers(t *testing.T) {
	command := NewCommand("subscribe").
		SetIds("cid-1", "qid-1", "sid-1").
		AddAckType(AckTypeProcessed).
		AddAckType(AckTypeStats).
		SetTimeout(1500).
		SetSequence(42)

	if command.GetTimeout() != 1500 {
		t.Fatalf("unexpected timeout value")
	}
	if command.GetSequence() != 42 {
		t.Fatalf("unexpected sequence id")
	}
	if !command.HasProcessedAck() || !command.HasStatsAck() {
		t.Fatalf("expected processed and stats ack flags")
	}
	if !command.IsSubscribe() {
		t.Fatalf("expected subscribe command classification")
	}
	if command.IsSow() {
		t.Fatalf("did not expect subscribe command to classify as sow")
	}
	if !NewCommand("publish").NeedsSequenceNumber() {
		t.Fatalf("expected publish command to need sequence number")
	}

	message := command.GetMessage()
	if message == nil {
		t.Fatalf("expected command.GetMessage to return a message")
	}
	if commandID, ok := command.CommandID(); !ok || commandID != "cid-1" {
		t.Fatalf("unexpected command id after SetIds: %q", commandID)
	}
}

func TestMessageParityHelpers(t *testing.T) {
	message := &Message{header: new(_Header)}
	message.Reset()
	message.SetData([]byte("payload"))

	source := []byte("copy-me")
	message.AssignData(source)
	source[0] = 'X'
	if string(message.Data()) != "copy-me" {
		t.Fatalf("expected AssignData to copy source bytes")
	}

	copyMessage := message.DeepCopy()
	if copyMessage == nil || copyMessage == message {
		t.Fatalf("expected DeepCopy to return an independent message")
	}
	copyMessage.SetData([]byte("changed"))
	if string(message.Data()) == "changed" {
		t.Fatalf("expected deep copy writes to not mutate original message")
	}

	message.SetBookmarkSeqNo(10).SetIgnoreAutoAck(true).SetSubscriptionHandle("sub-10")
	if message.GetBookmarkSeqNo() != 10 || !message.GetIgnoreAutoAck() || message.GetSubscriptionHandle() != "sub-10" {
		t.Fatalf("unexpected parity helper values")
	}

	message.Invalidate()
	if message.IsValid() {
		t.Fatalf("expected invalid message after Invalidate")
	}
	message.Reset()
	if !message.IsValid() {
		t.Fatalf("expected Reset to restore message validity")
	}

	replacement := &Message{header: new(_Header)}
	replacement.Reset()
	replacement.SetData([]byte("replacement"))
	message.Replace(replacement)
	if string(message.Data()) != "replacement" {
		t.Fatalf("expected message replacement payload")
	}

	message.header.status = []byte("failure")
	message.header.reason = []byte("bad filter")
	if err := message.ThrowFor(); err == nil || !strings.Contains(err.Error(), "BadFilterError") {
		t.Fatalf("expected bad filter error, got %v", err)
	}
}

func TestMessageAckParityPath(t *testing.T) {
	client := NewClient("message-ack")
	conn := newTestConn()
	client.connected = true
	client.connection = conn

	message := &Message{header: new(_Header)}
	message.Reset()
	message.SetClientImpl(client)
	message.header.topic = []byte("orders")
	message.header.bookmark = []byte("1|1|")

	if err := message.Ack(); err != nil {
		t.Fatalf("expected Ack helper to succeed: %v", err)
	}
	if !strings.Contains(string(conn.WrittenBytes()), `"c":"ack"`) {
		t.Fatalf("expected ack command to be written")
	}
}

func TestMessageStreamParitySelectors(t *testing.T) {
	client := NewClient("stream-parity")
	stream := newMessageStream(client)
	if stream.IsValid() {
		t.Fatalf("expected new stream to be invalid until configured")
	}

	stream.SetAcksOnly("cid-1")
	if !stream.IsValid() || stream.commandID != "cid-1" {
		t.Fatalf("unexpected ack-only stream state")
	}

	stream.SetSOWOnly("cid-2", "qid-2")
	if stream.commandID != "cid-2" || stream.queryID != "qid-2" {
		t.Fatalf("unexpected sow-only stream ids")
	}

	stream.SetStatsOnly("cid-3", "qid-3")
	if stream.commandID != "cid-3" || stream.queryID != "qid-3" {
		t.Fatalf("unexpected stats-only stream ids")
	}

	stream.SetSubscription("cid-4", "sub-4", "qid-4")
	if stream.commandID != "sub-4" || stream.queryID != "qid-4" {
		t.Fatalf("unexpected subscription stream ids")
	}

	invoked := false
	stream.SetAcksOnly("route-1").FromExistingHandler(func(*Message) error {
		invoked = true
		return nil
	})
	route, found := client.routes.Load("route-1")
	if !found {
		t.Fatalf("expected stream handler route to be registered")
	}
	if err := route.(func(*Message) error)(&Message{header: new(_Header)}); err != nil {
		t.Fatalf("unexpected route invocation error: %v", err)
	}
	if !invoked {
		t.Fatalf("expected existing handler to be invoked")
	}

	if message, ok := stream.End().Next(); ok || message != nil {
		t.Fatalf("expected end iterator to terminate immediately")
	}
}
