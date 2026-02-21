package amps

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"
)

type authFailureAuthenticator struct{}

func (authFailureAuthenticator) Authenticate(string, string) (string, error) {
	return "", errors.New("auth failed")
}
func (authFailureAuthenticator) Retry(string, string) (string, error) {
	return "", errors.New("retry failed")
}
func (authFailureAuthenticator) Completed(string, string, string) {}

func buildFrameFromCommand(t *testing.T, command *Command) []byte {
	t.Helper()
	buffer := bytes.NewBuffer(nil)
	if _, err := buffer.WriteString("    "); err != nil {
		t.Fatalf("write frame prefix failed: %v", err)
	}
	if err := command.header.write(buffer); err != nil {
		t.Fatalf("encode header failed: %v", err)
	}
	if payload := command.Data(); payload != nil {
		if _, err := buffer.Write(payload); err != nil {
			t.Fatalf("write payload failed: %v", err)
		}
	}
	frame := buffer.Bytes()
	length := uint32(len(frame) - 4)
	binary.BigEndian.PutUint32(frame[:4], length)
	return append([]byte(nil), frame...)
}

func waitForRouteHandler(t *testing.T, client *Client, routeID string) func(*Message) error {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if route, exists := client.routes.Load(routeID); exists {
			return route.(func(*Message) error)
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("route %q was not registered", routeID)
	return nil
}

func waitForAnyRouteHandler(t *testing.T, client *Client) (string, func(*Message) error) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		var routeID string
		var handler func(*Message) error
		client.routes.Range(func(key interface{}, value interface{}) bool {
			routeID = key.(string)
			handler = value.(func(*Message) error)
			return false
		})
		if handler != nil {
			return routeID, handler
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no route was registered")
	return "", nil
}

func buildSOWFrame(t *testing.T, routeID string, data []byte) []byte {
	t.Helper()
	outer := NewCommand("sow")

	buffer := bytes.NewBuffer(nil)
	if _, err := buffer.WriteString("    "); err != nil {
		t.Fatalf("frame prefix write failed: %v", err)
	}
	if err := outer.header.write(buffer); err != nil {
		t.Fatalf("outer header write failed: %v", err)
	}
	innerHeader := fmt.Sprintf(`{"c":"p","sub_id":"%s","l":%d}`, routeID, len(data))
	if _, err := buffer.WriteString(innerHeader); err != nil {
		t.Fatalf("inner header write failed: %v", err)
	}
	if _, err := buffer.Write(data); err != nil {
		t.Fatalf("inner payload write failed: %v", err)
	}

	frame := buffer.Bytes()
	binary.BigEndian.PutUint32(frame[:4], uint32(len(frame)-4))
	return append([]byte(nil), frame...)
}

func buildFramePrefix(length uint32) []byte {
	frame := make([]byte, 4)
	binary.BigEndian.PutUint32(frame, length)
	return frame
}

func TestClientCoreWrappersCoverage(t *testing.T) {
	client := NewClient("client-core-wrappers")
	errCalled := 0
	disconnectCalled := 0
	client.SetErrorHandler(func(error) { errCalled++ })
	client.SetDisconnectHandler(func(*Client, error) { disconnectCalled++ })
	client.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})

	if client.ErrorHandler() == nil || client.DisconnectHandler() == nil || client.tlsConfig == nil {
		t.Fatalf("expected handler and TLS setters to apply")
	}

	client.SetHeartbeat(2)
	if client.heartbeatInterval != 2 || client.heartbeatTimeout != 4 {
		t.Fatalf("unexpected default heartbeat timeout")
	}
	client.SetHeartbeat(2, 3)
	if client.heartbeatTimeout != 3 {
		t.Fatalf("unexpected explicit heartbeat timeout")
	}

	client.serverVersion = "5.3.5.1"
	if client.ServerVersion() != "5.3.5.1" {
		t.Fatalf("unexpected server version getter")
	}

	if err := client.Publish("orders", `{"id":1}`); err == nil {
		t.Fatalf("expected disconnected publish error")
	}
	if err := client.PublishBytes("orders", []byte(`{"id":2}`)); err == nil {
		t.Fatalf("expected disconnected publish-bytes error")
	}
	if err := client.DeltaPublish("orders", `{"id":3}`); err == nil {
		t.Fatalf("expected disconnected delta publish error")
	}
	if err := client.DeltaPublishBytes("orders", []byte(`{"id":4}`)); err == nil {
		t.Fatalf("expected disconnected delta publish bytes error")
	}
	if err := client.sendHeartbeat(); err == nil {
		t.Fatalf("expected disconnected sendHeartbeat error")
	}

	if _, err := client.Subscribe("orders", "/id > 0"); err == nil {
		t.Fatalf("expected disconnected subscribe error")
	}
	if _, err := client.DeltaSubscribe("orders", "/id > 0"); err == nil {
		t.Fatalf("expected disconnected delta subscribe error")
	}
	if _, err := client.Sow("orders", "/id > 0"); err == nil {
		t.Fatalf("expected disconnected sow error")
	}
	if _, err := client.SowAndSubscribe("orders", "/id > 0"); err == nil {
		t.Fatalf("expected disconnected sow-and-subscribe error")
	}
	if _, err := client.SowAndDeltaSubscribe("orders", "/id > 0"); err == nil {
		t.Fatalf("expected disconnected sow-and-delta-subscribe error")
	}

	if _, err := client.SubscribeAsync(nil, "orders", "/id > 0"); err == nil {
		t.Fatalf("expected disconnected subscribe async error")
	}
	if _, err := client.DeltaSubscribeAsync(nil, "orders", "/id > 0"); err == nil {
		t.Fatalf("expected disconnected delta subscribe async error")
	}
	if _, err := client.SowAsync(nil, "orders", "/id > 0"); err == nil {
		t.Fatalf("expected disconnected sow async error")
	}
	if _, err := client.SowAndSubscribeAsync(nil, "orders", "/id > 0"); err == nil {
		t.Fatalf("expected disconnected sow-and-subscribe async error")
	}
	if _, err := client.SowAndDeltaSubscribeAsync(nil, "orders", "/id > 0"); err == nil {
		t.Fatalf("expected disconnected sow-and-delta-subscribe async error")
	}

	if _, err := client.SowDelete("orders", "/id > 0"); err == nil {
		t.Fatalf("expected disconnected sow delete error")
	}
	if _, err := client.SowDeleteByData("orders", []byte(`{"id":1}`)); err == nil {
		t.Fatalf("expected disconnected sow delete by data error")
	}
	if _, err := client.SowDeleteByKeys("orders", "k1,k2"); err == nil {
		t.Fatalf("expected disconnected sow delete by keys error")
	}

	if err := client.Flush(); err == nil {
		t.Fatalf("expected disconnected flush error")
	}
	if err := client.Unsubscribe(); err == nil {
		t.Fatalf("expected disconnected unsubscribe error")
	}

	client.heartbeatTimeoutID = time.NewTimer(time.Hour)
	client.heartbeatTimeout = 0
	client.heartbeatTimestamp = 1
	client.onHeartbeatAbsence()

	client.heartbeatTimeoutID = time.NewTimer(time.Hour)
	client.heartbeatTimeout = 1
	client.heartbeatInterval = 0
	client.heartbeatTimestamp = uint(time.Now().Unix()) - 10
	client.checkAndSendHeartbeat(true)
	if errCalled == 0 {
		t.Fatalf("expected heartbeat send failure to report error")
	}

	if err := client.Close(); err == nil {
		t.Fatalf("expected close/disconnect error for already disconnected client")
	}
	if disconnectCalled == 0 {
		t.Fatalf("expected disconnect callback from heartbeat-triggered connection error")
	}
}

func TestClientConnectAndDisconnectCoverage(t *testing.T) {
	alreadyConnected := NewClient("already-connected")
	alreadyConnected.connected.Store(true)
	if err := alreadyConnected.Connect("tcp://127.0.0.1:9007/amps/json"); err == nil {
		t.Fatalf("expected already-connected error")
	}

	client := NewClient("connect-cases")
	client.SetErrorHandler(func(error) {})

	if err := client.Connect("%%%"); err == nil {
		t.Fatalf("expected invalid URI error")
	}
	if err := client.Connect("tcp://127.0.0.1:9007/notamps/json"); err == nil {
		t.Fatalf("expected protocol path error")
	}
	if err := client.Connect("tcp://127.0.0.1:1/amps/json"); err == nil {
		t.Fatalf("expected connection refused error")
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer listener.Close()

	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			time.Sleep(30 * time.Millisecond)
			_ = conn.Close()
		}
	}()

	uri := "tcp://" + listener.Addr().String() + "/amps/json"
	if err := client.Connect(uri); err != nil {
		t.Fatalf("expected successful connect, got %v", err)
	}
	if !client.connected.Load() {
		t.Fatalf("expected connected client")
	}
	if err := client.Disconnect(); err != nil {
		t.Fatalf("expected disconnect success after active connect, got %v", err)
	}
	<-accepted
}

func TestClientLogonAndExecuteAsyncDisconnectedCoverage(t *testing.T) {
	authFailureClient := NewClient("logon-auth-failure")
	parsed, _ := url.Parse("tcp://user:pass@127.0.0.1:9007/amps/json")
	authFailureClient.url = parsed
	if err := authFailureClient.Logon(LogonParams{Authenticator: authFailureAuthenticator{}}); err == nil {
		t.Fatalf("expected authenticator failure during logon")
	}

	client := NewClient("logon-disconnected")
	client.url = parsed
	if err := client.Logon(LogonParams{CorrelationID: "corr-id"}); err == nil {
		t.Fatalf("expected disconnected logon send error")
	}

	if _, err := client.ExecuteAsync(nil, nil); err == nil {
		t.Fatalf("expected nil command error")
	}
	if _, err := client.ExecuteAsync(NewCommand(""), nil); err == nil {
		t.Fatalf("expected unknown command error")
	}

	client.SetRetryOnDisconnect(false)
	if _, err := client.ExecuteAsync(NewCommand("subscribe").SetTopic("orders"), nil); err == nil {
		t.Fatalf("expected disconnected subscribe error without retry")
	}

	client.SetRetryOnDisconnect(true)
	routeID, err := client.ExecuteAsync(NewCommand("subscribe").SetTopic("orders"), nil)
	if err != nil || routeID == "" {
		t.Fatalf("expected queued retry route id, route=%q err=%v", routeID, err)
	}
	state := ensureClientState(client)
	state.lock.Lock()
	pendingRetry := len(state.pendingRetry)
	state.lock.Unlock()
	if pendingRetry == 0 {
		t.Fatalf("expected pending retry command")
	}

	_, err = client.ExecuteAsync(NewCommand("publish").SetTopic("orders").SetData([]byte("x")), nil)
	if err == nil {
		t.Fatalf("expected publish to fail while disconnected")
	}
}

func TestClientReadRoutineCoverage(t *testing.T) {
	client := NewClient("read-routine")
	conn := newTestConn()
	client.connection = conn
	client.connected.Store(true)
	client.stopped.Store(false)
	client.SetErrorHandler(func(error) {})

	delivered := 0
	client.routes.Store("sub-1", func(message *Message) error {
		if message == nil || string(message.Data()) != "payload" {
			t.Fatalf("unexpected delivered message: %#v", message)
		}
		delivered++
		return nil
	})

	command := NewCommand("publish").SetSubID("sub-1").SetTopic("orders").SetData([]byte("payload"))
	conn.enqueueRead(buildFrameFromCommand(t, command))
	client.readRoutine()

	if delivered != 1 {
		t.Fatalf("expected one delivered message, got %d", delivered)
	}
	if client.connected.Load() {
		t.Fatalf("expected connection to be closed after EOF")
	}
}

func TestClientReadRoutineAdditionalBranches(t *testing.T) {
	client := NewClient("read-routine-extra")
	conn := newTestConn()
	client.connection = conn
	client.connected.Store(true)
	client.stopped.Store(false)
	client.SetErrorHandler(func(error) {})

	delivered := 0
	client.routes.Store("sub-sow", func(message *Message) error {
		if message == nil || string(message.Data()) != "sow-data" {
			t.Fatalf("unexpected sow message: %#v", message)
		}
		delivered++
		return nil
	})
	conn.enqueueRead(buildSOWFrame(t, "sub-sow", []byte("sow-data")))
	client.readRoutine()
	if delivered != 1 {
		t.Fatalf("expected one SOW message delivery, got %d", delivered)
	}

	client = NewClient("read-routine-filter-error")
	conn = newTestConn()
	client.connection = conn
	client.connected.Store(true)
	client.stopped.Store(false)
	client.SetErrorHandler(func(error) {})
	client.SetTransportFilter(func(direction TransportFilterDirection, payload []byte) []byte {
		_ = direction
		_ = payload
		return []byte{1, 2, 3}
	})
	command := NewCommand("publish").SetSubID("sub-invalid").SetData([]byte("x"))
	conn.enqueueRead(buildFrameFromCommand(t, command))
	client.readRoutine()
}

func TestClientReadRoutineOversizedFrameCoverage(t *testing.T) {
	client := NewClient("read-routine-oversized")
	conn := newTestConn()
	client.connection = conn
	client.connected.Store(true)
	client.stopped.Store(false)
	errorCount := 0
	client.SetErrorHandler(func(error) {
		errorCount++
	})

	conn.enqueueRead(buildFramePrefix(maxInboundFrameLength + 1))
	client.readRoutine()

	if client.connected.Load() {
		t.Fatalf("expected oversized frame to disconnect client")
	}
	if errorCount == 0 {
		t.Fatalf("expected protocol error callback for oversized frame")
	}
}

func TestClientRouteHelpersCoverage(t *testing.T) {
	client := NewClient("route-helpers")

	handlerCalls := 0
	baseHandler := func(*Message) error {
		handlerCalls++
		return nil
	}

	if err := client.addRoute("route-1", baseHandler, AckTypeNone, AckTypeProcessed, false, false); err != nil {
		t.Fatalf("addRoute failed: %v", err)
	}

	routeValue, exists := client.routes.Load("route-1")
	if !exists {
		t.Fatalf("expected stored route")
	}
	ackType := AckTypeProcessed
	ackMessage := &Message{header: &_Header{
		command:   CommandAck,
		commandID: []byte("route-1"),
		status:    []byte("success"),
		ackType:   &ackType,
	}}
	if err := routeValue.(func(*Message) error)(ackMessage); err != nil {
		t.Fatalf("route ack handler failed: %v", err)
	}
	if handlerCalls == 0 {
		t.Fatalf("expected route handler callback")
	}

	if err := client.addRoute("route-dup", baseHandler, AckTypeNone, AckTypeNone, true, false); err != nil {
		t.Fatalf("initial subscribe route add failed: %v", err)
	}
	if err := client.addRoute("route-dup", baseHandler, AckTypeNone, AckTypeNone, true, false); err == nil {
		t.Fatalf("expected duplicate subscribe route error")
	}
	if err := client.addRoute("route-dup", baseHandler, AckTypeNone, AckTypeNone, false, false); err == nil {
		t.Fatalf("expected duplicate non-subscribe route error")
	}
	if err := client.addRoute("route-dup", nil, AckTypeNone, AckTypeNone, true, true); err != nil {
		t.Fatalf("expected replace route path to succeed: %v", err)
	}

	stream := newMessageStream(nil)
	stream.commandID = "stream-1"
	stream.setState(messageStreamStateReading)
	client.messageStreams.Store("stream-1", stream)
	client.routes.Store("stream-1", func(*Message) error { return nil })
	if err := client.deleteRoute("stream-1"); err != nil {
		t.Fatalf("deleteRoute failed: %v", err)
	}
	if _, exists := client.routes.Load("stream-1"); exists {
		t.Fatalf("expected route removal")
	}
}

func TestClientOnErrorAndConnectionErrorCoverage(t *testing.T) {
	client := NewClient("error-paths")
	conn := newTestConn()
	client.connected.Store(true)
	client.connection = conn
	client.logging = true

	disconnectCalled := 0
	client.SetDisconnectHandler(func(*Client, error) { disconnectCalled++ })
	client.SetErrorHandler(func(error) {})
	client.syncAckProcessing = make(chan _Result)
	state := ensureClientState(client)
	state.lock.Lock()
	state.ackTimer = time.NewTimer(time.Hour)
	state.pendingAcks["k"] = &pendingAckBatch{topic: "orders", bookmarks: []string{"1|1|"}}
	state.pendingAckCount = 1
	state.lock.Unlock()

	client.onConnectionError(NewError(ConnectionError, "boom"))
	if client.connected.Load() || client.connection != nil || !client.stopped.Load() {
		t.Fatalf("expected disconnected client state after connection error")
	}
	if disconnectCalled != 1 {
		t.Fatalf("expected disconnect handler callback")
	}

	client.onError(NewError(CommandError, "manual"))
}

func TestClientExecuteAsyncSyncAckCoverage(t *testing.T) {
	successClient := NewClient("execute-async-success")
	successConn := newTestConn()
	successClient.connected.Store(true)
	successClient.connection = successConn
	subscribeCommand := NewCommand("subscribe").SetTopic("orders").SetSubID("sub-sync")

	successResult := make(chan struct {
		route string
		err   error
	}, 1)
	go func() {
		routeID, err := successClient.ExecuteAsync(subscribeCommand, nil)
		successResult <- struct {
			route string
			err   error
		}{route: routeID, err: err}
	}()
	successHandler := waitForRouteHandler(t, successClient, "sub-sync")
	ack := AckTypeProcessed
	_ = successHandler(&Message{header: &_Header{
		command: CommandAck,
		ackType: &ack,
		status:  []byte("success"),
	}})
	successExecute := <-successResult
	if successExecute.err != nil || successExecute.route != "sub-sync" {
		t.Fatalf("expected successful subscribe execute async, route=%q err=%v", successExecute.route, successExecute.err)
	}

	failureClient := NewClient("execute-async-failure")
	failureConn := newTestConn()
	failureClient.connected.Store(true)
	failureClient.connection = failureConn
	failureCommand := NewCommand("subscribe").SetTopic("orders").SetSubID("sub-fail")

	failureResult := make(chan error, 1)
	go func() {
		_, err := failureClient.ExecuteAsync(failureCommand, nil)
		failureResult <- err
	}()
	failureHandler := waitForRouteHandler(t, failureClient, "sub-fail")
	_ = failureHandler(&Message{header: &_Header{
		command: CommandAck,
		ackType: &ack,
		status:  []byte("failure"),
		reason:  []byte("bad filter"),
	}})
	if err := <-failureResult; err == nil {
		t.Fatalf("expected subscribe execute async failure")
	}

	closedConnClient := NewClient("execute-async-send-error")
	closedConn := newTestConn()
	closedConnClient.connected.Store(true)
	closedConnClient.connection = closedConn
	_ = closedConn.Close()
	if _, err := closedConnClient.ExecuteAsync(NewCommand("subscribe").SetTopic("orders").SetSubID("sub-closed"), nil); err == nil {
		t.Fatalf("expected send error for closed connection")
	}

	unsubClient := NewClient("execute-async-unsub")
	unsubConn := newTestConn()
	unsubClient.connected.Store(true)
	unsubClient.connection = unsubConn
	unsubClient.messageStreams.Store("to-remove", newMessageStream(nil))
	unsubClient.routes.Store("to-remove", func(*Message) error { return nil })
	if routeID, err := unsubClient.ExecuteAsync(NewCommand("unsubscribe").SetSubID("all"), nil); err != nil || routeID != "" {
		t.Fatalf("expected unsubscribe execute async success, route=%q err=%v", routeID, err)
	}
}

func TestClientSowDeleteAndHeartbeatCoverage(t *testing.T) {
	sowClient := NewClient("sow-delete")
	sowConn := newTestConn()
	sowClient.connected.Store(true)
	sowClient.connection = sowConn

	type sowResult struct {
		message *Message
		err     error
	}
	runWithResponse := func(call func() (*Message, error), response *Message) sowResult {
		resultCh := make(chan sowResult, 1)
		go func() {
			message, err := call()
			resultCh <- sowResult{message: message, err: err}
		}()
		_, handler := waitForAnyRouteHandler(t, sowClient)
		_ = handler(response)
		return <-resultCh
	}

	ackStats := AckTypeStats
	successResponse := &Message{header: &_Header{
		command: CommandAck,
		ackType: &ackStats,
		status:  []byte("success"),
	}}
	if result := runWithResponse(func() (*Message, error) { return sowClient.SowDelete("orders", "/id > 0") }, successResponse); result.err != nil || result.message == nil {
		t.Fatalf("expected SowDelete success response, result=%+v", result)
	}
	if result := runWithResponse(func() (*Message, error) { return sowClient.SowDeleteByData("orders", []byte(`{"id":1}`)) }, successResponse); result.err != nil || result.message == nil {
		t.Fatalf("expected SowDeleteByData success response, result=%+v", result)
	}
	if result := runWithResponse(func() (*Message, error) { return sowClient.SowDeleteByKeys("orders", "k1,k2") }, successResponse); result.err != nil || result.message == nil {
		t.Fatalf("expected SowDeleteByKeys success response, result=%+v", result)
	}

	failureResponse := &Message{header: &_Header{
		command: CommandAck,
		ackType: &ackStats,
		status:  []byte("failure"),
		reason:  []byte("bad filter"),
	}}
	if result := runWithResponse(func() (*Message, error) { return sowClient.SowDelete("orders", "/id > 1") }, failureResponse); result.err == nil {
		t.Fatalf("expected SowDelete failure response")
	}

	heartbeatClient := NewClient("establish-heartbeat")
	heartbeatConn := newTestConn()
	heartbeatClient.connected.Store(true)
	heartbeatClient.connection = heartbeatConn
	heartbeatClient.heartbeatInterval = 1
	heartbeatClient.heartbeatTimeout = 1
	heartbeatClient.SetErrorHandler(func(error) {})

	hbResult := make(chan error, 1)
	go func() {
		hbResult <- heartbeatClient.establishHeartbeat()
	}()
	_, hbHandler := waitForAnyRouteHandler(t, heartbeatClient)
	ackProcessed := AckTypeProcessed
	_ = hbHandler(&Message{header: &_Header{
		command: CommandAck,
		ackType: &ackProcessed,
		status:  []byte("success"),
	}})
	if err := <-hbResult; err != nil {
		t.Fatalf("expected heartbeat establishment success, got %v", err)
	}
	if heartbeatClient.heartbeatTimeoutID != nil {
		_ = heartbeatClient.heartbeatTimeoutID.Stop()
	}
}

func TestClientLogonAckPathsCoverage(t *testing.T) {
	successClient := NewClient("logon-success")
	successConn := newTestConn()
	successClient.connected.Store(true)
	successClient.connection = successConn
	successClient.url, _ = url.Parse("tcp://user:pass@localhost:9007/amps/json")
	successClient.SetErrorHandler(func(error) {})

	successResult := make(chan error, 1)
	go func() {
		successResult <- successClient.Logon()
	}()
	_, successHandler := waitForAnyRouteHandler(t, successClient)
	ack := AckTypeProcessed
	_ = successHandler(&Message{header: &_Header{
		command:    CommandAck,
		ackType:    &ack,
		status:     []byte("success"),
		version:    []byte("5.3.5.1"),
		clientName: []byte("12345"),
	}})
	if err := <-successResult; err != nil {
		t.Fatalf("expected successful logon ack path, got %v", err)
	}

	failureClient := NewClient("logon-failure")
	failureConn := newTestConn()
	failureClient.connected.Store(true)
	failureClient.connection = failureConn
	failureClient.url, _ = url.Parse("tcp://user:pass@localhost:9007/amps/json")
	failureClient.SetErrorHandler(func(error) {})

	failureResult := make(chan error, 1)
	go func() {
		failureResult <- failureClient.Logon()
	}()
	_, failureHandler := waitForAnyRouteHandler(t, failureClient)
	_ = failureHandler(&Message{header: &_Header{
		command: CommandAck,
		ackType: &ack,
		status:  []byte("failure"),
		reason:  []byte("bad filter"),
	}})
	if err := <-failureResult; err == nil {
		t.Fatalf("expected failed logon ack path")
	}
}

func TestClientLogonTimeoutCoverage(t *testing.T) {
	timeoutClient := NewClient("logon-timeout")
	timeoutClient.connected.Store(true)
	timeoutClient.connection = newTestConn()
	timeoutClient.url, _ = url.Parse("tcp://localhost:9007/amps/json")
	timeoutClient.SetErrorHandler(func(error) {})

	start := time.Now()
	err := timeoutClient.Logon(LogonParams{Timeout: 10})
	if err == nil {
		t.Fatalf("expected logon timeout error")
	}
	if elapsed := time.Since(start); elapsed < 5*time.Millisecond {
		t.Fatalf("expected timeout wait, elapsed=%s", elapsed)
	}
	if !strings.Contains(err.Error(), "TimedOutError") {
		t.Fatalf("expected timeout error classification, got %v", err)
	}
}

func TestClientUtilityHelpersCoverage(t *testing.T) {
	if unsafeStringFromBytes(nil) != "" {
		t.Fatalf("expected empty unsafe string from nil bytes")
	}
	if unsafeStringFromBytes([]byte("abc")) != "abc" {
		t.Fatalf("unexpected unsafe string conversion")
	}
	if string(trimASCIISpaces([]byte("  a  "))) != "a" {
		t.Fatalf("unexpected ASCII trim result")
	}
	if trimmed := trimASCIISpaces([]byte(" \t\r\n ")); len(trimmed) != 0 {
		t.Fatalf("expected full whitespace trim to empty")
	}

	client := NewClient("cmd-id")
	initial := client.makeCommandID()
	if !strings.Contains(initial, "0") {
		t.Fatalf("expected initial command id to contain sequence 0")
	}
}
