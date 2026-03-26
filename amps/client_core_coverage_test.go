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
	"sync/atomic"
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

func isClientSignalClosed(signal <-chan struct{}) bool {
	select {
	case <-signal:
		return true
	default:
		return false
	}
}

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

func executeSyncStreamWithProcessedAck(t *testing.T, clientName string, command *Command, routeID string) (*Client, func(*Message) error, *MessageStream) {
	t.Helper()

	var client = NewClient(clientName)
	var conn = newTestConn()
	client.connected.Store(true)
	client.connection = conn

	type executeResult struct {
		stream *MessageStream
		err    error
	}

	var resultCh = make(chan executeResult, 1)
	go func() {
		var stream, err = client.Execute(command)
		resultCh <- executeResult{stream: stream, err: err}
	}()

	var handler = waitForRouteHandler(t, client, routeID)
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

	return client, handler, result.stream
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

func buildMultiSOWFrame(t *testing.T, routeID string, rows ...[]byte) []byte {
	t.Helper()
	var outer = NewCommand("sow")
	var buffer = bytes.NewBuffer(nil)
	if _, err := buffer.WriteString("    "); err != nil {
		t.Fatalf("frame prefix write failed: %v", err)
	}
	if err := outer.header.write(buffer); err != nil {
		t.Fatalf("outer header write failed: %v", err)
	}

	for _, row := range rows {
		var innerHeader = fmt.Sprintf(`{"c":"p","sub_id":"%s","l":%d}`, routeID, len(row))
		if _, err := buffer.WriteString(innerHeader); err != nil {
			t.Fatalf("inner header write failed: %v", err)
		}
		if _, err := buffer.Write(row); err != nil {
			t.Fatalf("inner payload write failed: %v", err)
		}
	}

	var frame = buffer.Bytes()
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

func TestClientSowAndSubscribeAsyncReadRoutineCopiesRetainedMessages(t *testing.T) {
	var client = NewClient("read-routine-sow-copy")
	var conn = newTestConn()
	client.connection = conn
	client.connected.Store(true)
	client.stopped.Store(false)
	client.SetErrorHandler(func(error) {})

	var retained []*Message
	var executeResult = make(chan error, 1)
	go func() {
		_, err := client.SowAndSubscribeAsync(func(message *Message) error {
			retained = append(retained, message)
			return nil
		}, "orders", "/id > 0")
		executeResult <- err
	}()

	routeID, handler := waitForAnyRouteHandler(t, client)
	var ack = AckTypeProcessed
	_ = handler(&Message{header: &_Header{
		command: CommandAck,
		ackType: &ack,
		status:  []byte("success"),
	}})

	if err := <-executeResult; err != nil {
		t.Fatalf("expected sow_and_subscribe async registration success: %v", err)
	}

	conn.enqueueRead(buildMultiSOWFrame(t, routeID, []byte(`{"id":1}`), []byte(`{"id":2}`)))
	client.readRoutine()

	if len(retained) != 2 || string(retained[0].Data()) != `{"id":1}` || string(retained[1].Data()) != `{"id":2}` || retained[0] == retained[1] {
		t.Fatalf("expected retained async messages to remain distinct and stable, got %#v %#v", retained, retained)
	}
}

func TestClientExecuteAsyncDirectSubscribeRouteCopiesRetainedMessages(t *testing.T) {
	var client = NewClient("direct-route-copy")
	var conn = newTestConn()
	client.connection = conn
	client.connected.Store(true)

	var retained []*Message
	var command = NewCommand("sow_and_subscribe").
		SetTopic("orders").
		SetSubID("sub-copy").
		AddAckType(AckTypeProcessed)

	if _, err := client.ExecuteAsync(command, func(message *Message) error {
		retained = append(retained, message)
		return nil
	}); err != nil {
		t.Fatalf("expected execute async success: %v", err)
	}

	var handler = waitForRouteHandler(t, client, "sub-copy")
	var message = &Message{header: &_Header{
		command: CommandSOW,
		subID:   []byte("sub-copy"),
		topic:   []byte("orders-a"),
	}, data: []byte(`{"id":1}`)}
	if err := handler(message); err != nil {
		t.Fatalf("first handler call failed: %v", err)
	}

	message.header.topic = []byte("orders-b")
	message.data = []byte(`{"id":2}`)
	if err := handler(message); err != nil {
		t.Fatalf("second handler call failed: %v", err)
	}

	if len(retained) != 2 || string(retained[0].Data()) != `{"id":1}` || string(retained[1].Data()) != `{"id":2}` || retained[0] == retained[1] {
		t.Fatalf("expected direct async route to retain copied messages, got %#v %#v", retained, retained)
	}
}

func TestClientSubscribeAsyncIgnoreAutoAckSkipsAutoAck(t *testing.T) {
	var client = NewClient("subscribe-async-ignore-auto-ack")
	var conn = newTestConn()
	client.connection = conn
	client.connected.Store(true)
	client.SetAutoAck(true).SetAckBatchSize(1).SetAckTimeout(5 * time.Second)

	var executeResult = make(chan error, 1)
	go func() {
		_, err := client.SubscribeAsync(func(message *Message) error {
			message.SetIgnoreAutoAck(true)
			return nil
		}, "queue://orders")
		executeResult <- err
	}()

	routeID, handler := waitForAnyRouteHandler(t, client)
	var ack = AckTypeProcessed
	_ = handler(&Message{header: &_Header{
		command: CommandAck,
		ackType: &ack,
		status:  []byte("success"),
	}})

	if err := <-executeResult; err != nil {
		t.Fatalf("expected subscribe async registration success: %v", err)
	}

	var writtenBefore = len(conn.WrittenBytes())
	var queueMessage = makeAutoAckMessage("9|1|")
	queueMessage.header.subID = []byte(routeID)
	if err := client.onMessage(queueMessage); err != nil {
		t.Fatalf("expected routed queue message success: %v", err)
	}

	if writtenAfter := len(conn.WrittenBytes()); writtenAfter != writtenBefore {
		t.Fatalf("expected ignore auto ack to suppress ack send, bytes before=%d after=%d", writtenBefore, writtenAfter)
	}
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

	if err := client.addRoute("route-1", baseHandler, AckTypeNone, AckTypeProcessed, false, false, false); err != nil {
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

	if err := client.addRoute("route-dup", baseHandler, AckTypeNone, AckTypeNone, true, false, false); err != nil {
		t.Fatalf("initial subscribe route add failed: %v", err)
	}
	if err := client.addRoute("route-dup", baseHandler, AckTypeNone, AckTypeNone, true, false, false); err == nil {
		t.Fatalf("expected duplicate subscribe route error")
	}
	if err := client.addRoute("route-dup", baseHandler, AckTypeNone, AckTypeNone, false, false, false); err == nil {
		t.Fatalf("expected duplicate non-subscribe route error")
	}
	if err := client.addRoute("route-dup", nil, AckTypeNone, AckTypeNone, true, true, false); err != nil {
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

func TestClientAddSubscribeRouteDirectCoverage(t *testing.T) {
	var client = NewClient("route-direct")

	var handlerCalls int
	var baseHandler = func(*Message) error {
		handlerCalls++
		return nil
	}

	var err = client.addSubscribeRouteDirect("route-direct", baseHandler, AckTypeProcessed, false)
	if err != nil {
		t.Fatalf("addSubscribeRouteDirect failed: %v", err)
	}

	err = client.addSubscribeRouteDirect("route-direct", baseHandler, AckTypeProcessed, false)
	if err == nil {
		t.Fatalf("expected duplicate subscribe route error")
	}

	err = client.addSubscribeRouteDirect("route-direct", nil, AckTypeProcessed, true)
	if err != nil {
		t.Fatalf("replace direct subscribe route should succeed: %v", err)
	}

	var routeValue, exists = client.routes.Load("route-direct")
	if !exists {
		t.Fatalf("expected direct route to be stored")
	}

	var ackType = AckTypeProcessed
	var ackMessage = &Message{header: &_Header{command: CommandAck, ackType: &ackType, status: []byte("success")}}
	var publishMessage = &Message{header: &_Header{command: CommandPublish}}

	err = routeValue.(func(*Message) error)(ackMessage)
	if err != nil {
		t.Fatalf("direct route ack callback failed: %v", err)
	}

	err = routeValue.(func(*Message) error)(ackMessage)
	if err != nil {
		t.Fatalf("second direct route ack callback failed: %v", err)
	}

	err = routeValue.(func(*Message) error)(publishMessage)
	if err != nil {
		t.Fatalf("direct route publish callback failed: %v", err)
	}

	if handlerCalls != 3 {
		t.Fatalf("expected handler calls for both processed acks and publish, got %d", handlerCalls)
	}

	handlerCalls = 0
	err = client.addSubscribeRouteDirect("route-direct-filtered", baseHandler, AckTypeProcessed|AckTypeCompleted, false)
	if err != nil {
		t.Fatalf("filtered direct subscribe route add failed: %v", err)
	}

	var filteredRouteValue, filteredExists = client.routes.Load("route-direct-filtered")
	if !filteredExists {
		t.Fatalf("expected filtered direct route to be stored")
	}

	err = filteredRouteValue.(func(*Message) error)(ackMessage)
	if err != nil {
		t.Fatalf("filtered direct route first ack failed: %v", err)
	}
	err = filteredRouteValue.(func(*Message) error)(ackMessage)
	if err != nil {
		t.Fatalf("filtered direct route duplicate ack failed: %v", err)
	}

	var completedAckType = AckTypeCompleted
	var completedAckMessage = &Message{header: &_Header{command: CommandAck, ackType: &completedAckType, status: []byte("success")}}
	err = filteredRouteValue.(func(*Message) error)(completedAckMessage)
	if err != nil {
		t.Fatalf("filtered direct route completed ack failed: %v", err)
	}

	err = filteredRouteValue.(func(*Message) error)(publishMessage)
	if err != nil {
		t.Fatalf("filtered direct route publish callback failed: %v", err)
	}

	if handlerCalls != 3 {
		t.Fatalf("expected filtered direct handler calls for first processed ack, completed ack, and publish, got %d", handlerCalls)
	}
}

func TestClientAddCommandRouteDirectCoverage(t *testing.T) {
	var client = NewClient("route-command-direct")

	var handlerCalls int
	var handler = func(*Message) error {
		handlerCalls++
		return nil
	}

	var err = client.addCommandRouteDirect("cmd-direct", handler, AckTypeProcessed)
	if err != nil {
		t.Fatalf("addCommandRouteDirect failed: %v", err)
	}

	err = client.addCommandRouteDirect("cmd-direct", handler, AckTypeProcessed)
	if err == nil {
		t.Fatalf("expected duplicate command route error")
	}

	var routeValue, exists = client.routes.Load("cmd-direct")
	if !exists {
		t.Fatalf("expected command direct route to be stored")
	}

	var ackType = AckTypeProcessed
	var ackMessage = &Message{header: &_Header{command: CommandAck, ackType: &ackType, status: []byte("success")}}
	err = routeValue.(func(*Message) error)(ackMessage)
	if err != nil {
		t.Fatalf("command direct route ack callback failed: %v", err)
	}

	if handlerCalls != 1 {
		t.Fatalf("expected one handler callback, got %d", handlerCalls)
	}

	if _, routeStillExists := client.routes.Load("cmd-direct"); routeStillExists {
		t.Fatalf("expected command direct route removal after ack")
	}

	err = client.addCommandRouteDirect("cmd-direct-none", handler, AckTypeNone)
	if err != nil {
		t.Fatalf("addCommandRouteDirect AckTypeNone failed: %v", err)
	}
	var noneRouteValue, noneExists = client.routes.Load("cmd-direct-none")
	if !noneExists {
		t.Fatalf("expected ack-none command direct route")
	}

	err = noneRouteValue.(func(*Message) error)(&Message{header: &_Header{command: CommandPublish}})
	if err != nil {
		t.Fatalf("command direct non-ack callback failed: %v", err)
	}
}

func TestExecuteAsyncReusedSubscriptionRoutesProcessedAckByNewCommandID(t *testing.T) {
	var client = NewClient("resubscribe-processed-ack")
	var conn = newTestConn()
	client.connected.Store(true)
	client.connection = conn

	var command = NewCommand("sow_and_subscribe").
		SetTopic("orders").
		SetSubID("sub-reused").
		SetQueryID("query-reused").
		AddAckType(AckTypeCompleted)

	type executeResult struct {
		routeID string
		err     error
	}

	var resultCh = make(chan executeResult, 1)
	go func() {
		var routeID, err = client.ExecuteAsync(command, nil)
		resultCh <- executeResult{routeID: routeID, err: err}
	}()

	_ = waitForRouteHandler(t, client, "query-reused")
	_ = waitForRouteHandler(t, client, "0")

	var ackType = AckTypeProcessed
	var ackErr = client.onMessage(&Message{header: &_Header{
		command:   CommandAck,
		commandID: []byte("0"),
		ackType:   &ackType,
		status:    []byte("success"),
	}})
	if ackErr != nil {
		t.Fatalf("processed ack routing failed: %v", ackErr)
	}

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("ExecuteAsync failed: %v", result.err)
		}
		if result.routeID != "query-reused" {
			t.Fatalf("routeID = %q, want query-reused", result.routeID)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("timed out waiting for ExecuteAsync to receive processed ack")
	}

	if _, exists := client.routes.Load("0"); exists {
		t.Fatalf("expected temporary processed ack alias route removal")
	}
	if _, exists := client.routes.Load("query-reused"); !exists {
		t.Fatalf("expected stable query route to remain after processed ack")
	}
}

func TestClientExecuteSowKeepsRouteUntilGroupEndAfterCompletedAck(t *testing.T) {
	var client = NewClient("execute-sow-completed-before-group-end")
	var conn = newTestConn()
	client.connected.Store(true)
	client.connection = conn

	type executeResult struct {
		stream *MessageStream
		err    error
	}

	var resultCh = make(chan executeResult, 1)
	go func() {
		var stream, err = client.Execute(NewCommand("sow").SetTopic("orders"))
		resultCh <- executeResult{stream: stream, err: err}
	}()

	var routeID = "0"
	_ = waitForRouteHandler(t, client, routeID)
	var processedAck = AckTypeProcessed
	var err = client.onMessage(&Message{header: &_Header{
		command:   CommandAck,
		commandID: []byte(routeID),
		ackType:   &processedAck,
		status:    []byte("success"),
	}})
	if err != nil {
		t.Fatalf("processed ack routing failed: %v", err)
	}

	var result = <-resultCh
	if result.err != nil || result.stream == nil {
		t.Fatalf("expected execute success, stream=%v err=%v", result.stream, result.err)
	}

	err = client.onMessage(&Message{
		header: &_Header{command: CommandPublish, subID: []byte(routeID)},
		data:   []byte(`{"id":1}`),
	})
	if err != nil {
		t.Fatalf("first SOW row routing failed: %v", err)
	}

	var completedAck = AckTypeCompleted
	var recordsReturned uint = 2
	err = client.onMessage(&Message{header: &_Header{
		command:         CommandAck,
		commandID:       []byte(routeID),
		ackType:         &completedAck,
		status:          []byte("success"),
		recordsReturned: &recordsReturned,
	}})
	if err != nil {
		t.Fatalf("completed ack routing failed: %v", err)
	}
	if _, exists := client.routes.Load(routeID); !exists {
		t.Fatalf("expected SOW route to remain registered until group_end")
	}

	err = client.onMessage(&Message{
		header: &_Header{command: CommandPublish, subID: []byte(routeID)},
		data:   []byte(`{"id":2}`),
	})
	if err != nil {
		t.Fatalf("second SOW row routing failed: %v", err)
	}

	err = client.onMessage(&Message{header: &_Header{command: CommandGroupEnd, queryID: []byte(routeID)}})
	if err != nil {
		t.Fatalf("group_end routing failed: %v", err)
	}

	var rowCount int
	for result.stream.HasNext() {
		var message = result.stream.Next()
		if message == nil {
			continue
		}
		if message.header.command == CommandPublish {
			rowCount++
		}
	}

	if rowCount != 2 {
		t.Fatalf("expected two SOW rows after completed ack, got %d", rowCount)
	}
	if _, exists := client.routes.Load(routeID); exists {
		t.Fatalf("expected SOW route deletion after group_end")
	}
	if atomic.LoadInt32(&result.stream.state) != messageStreamStateComplete {
		t.Fatalf("expected stream to complete after group_end")
	}
}

func TestClientExecuteZeroRowSowCompletesOnCompletedAck(t *testing.T) {
	var client = NewClient("execute-zero-row-sow")
	var conn = newTestConn()
	client.connected.Store(true)
	client.connection = conn

	type executeResult struct {
		stream *MessageStream
		err    error
	}

	var resultCh = make(chan executeResult, 1)
	go func() {
		var stream, err = client.Execute(NewCommand("sow").SetTopic("orders"))
		resultCh <- executeResult{stream: stream, err: err}
	}()

	var routeID = "0"
	_ = waitForRouteHandler(t, client, routeID)
	var processedAck = AckTypeProcessed
	var err = client.onMessage(&Message{header: &_Header{
		command:   CommandAck,
		commandID: []byte(routeID),
		ackType:   &processedAck,
		status:    []byte("success"),
	}})
	if err != nil {
		t.Fatalf("processed ack routing failed: %v", err)
	}

	var result = <-resultCh
	if result.err != nil || result.stream == nil {
		t.Fatalf("expected execute success, stream=%v err=%v", result.stream, result.err)
	}

	var completedAck = AckTypeCompleted
	var recordsReturned uint
	err = client.onMessage(&Message{header: &_Header{
		command:         CommandAck,
		commandID:       []byte(routeID),
		ackType:         &completedAck,
		status:          []byte("success"),
		recordsReturned: &recordsReturned,
	}})
	if err != nil {
		t.Fatalf("completed ack routing failed: %v", err)
	}

	if result.stream.HasNext() {
		t.Fatalf("expected zero-row SOW stream to report no messages")
	}
	if _, exists := client.routes.Load(routeID); exists {
		t.Fatalf("expected zero-row SOW route cleanup on completed ack")
	}
	if atomic.LoadInt32(&result.stream.state) != messageStreamStateComplete {
		t.Fatalf("expected zero-row SOW stream to complete on completed ack")
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

func TestClientOnConnectionErrorAlreadyStoppedCoverage(t *testing.T) {
	client := NewClient("error-paths-stopped")
	conn := newTestConn()
	client.connection = conn
	client.connected.Store(false)
	client.stopped.Store(true)

	var disconnectCalled = 0
	var errorCalled = 0
	client.SetDisconnectHandler(func(*Client, error) { disconnectCalled++ })
	client.SetErrorHandler(func(error) { errorCalled++ })

	client.onConnectionError(NewError(ConnectionError, "ignored"))

	if client.connection != conn {
		t.Fatalf("expected existing connection to remain untouched on already-disconnected client")
	}
	if disconnectCalled != 0 {
		t.Fatalf("expected no disconnect callback for already-disconnected client")
	}
	if errorCalled != 0 {
		t.Fatalf("expected no error callback for already-disconnected client")
	}
}

func TestClientReadRoutineReturnsWhenConnectionMissingCoverage(t *testing.T) {
	client := NewClient("read-missing-connection")
	client.connected.Store(true)

	var started = 0
	var stopped = 0
	client.SetReceiveRoutineStartedCallback(func() { started++ })
	client.SetReceiveRoutineStoppedCallback(func() { stopped++ })

	client.readRoutine()

	if started != 0 || stopped != 0 {
		t.Fatalf("expected readRoutine to return before callbacks when connection is missing, started=%d stopped=%d", started, stopped)
	}
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

func TestClientExecuteSowAndSubscribeWaitsForFirstMessage(t *testing.T) {
	var _, handler, stream = executeSyncStreamWithProcessedAck(
		t,
		"execute-sow-and-subscribe",
		NewCommand("sow_and_subscribe").SetTopic("orders").SetSubID("sub-sync"),
		"sub-sync",
	)

	var hasNextCh = make(chan bool, 1)
	go func() {
		hasNextCh <- stream.HasNext()
	}()

	select {
	case hasNext := <-hasNextCh:
		t.Fatalf("HasNext returned before any SOW message arrived: %v", hasNext)
	case <-time.After(20 * time.Millisecond):
	}

	_ = handler(&Message{header: &_Header{
		command: CommandSOW,
		queryID: []byte("sub-sync"),
		subID:   []byte("sub-sync"),
		topic:   []byte("orders"),
	}, data: []byte(`{"id":1}`)})

	select {
	case hasNext := <-hasNextCh:
		if !hasNext {
			t.Fatalf("expected HasNext to report queued SOW data")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for HasNext after SOW data arrival")
	}

	var message = stream.Next()
	if message == nil || string(message.Data()) != `{"id":1}` {
		t.Fatalf("unexpected SOW message: %#v", message)
	}
}

func TestClientExecuteSowAndDeltaSubscribeWaitsForFirstMessage(t *testing.T) {
	var _, handler, stream = executeSyncStreamWithProcessedAck(
		t,
		"execute-sow-and-delta-subscribe",
		NewCommand("sow_and_delta_subscribe").SetTopic("orders").SetSubID("delta-sync"),
		"delta-sync",
	)

	var hasNextCh = make(chan bool, 1)
	go func() {
		hasNextCh <- stream.HasNext()
	}()

	select {
	case hasNext := <-hasNextCh:
		t.Fatalf("HasNext returned before any delta SOW message arrived: %v", hasNext)
	case <-time.After(20 * time.Millisecond):
	}

	_ = handler(&Message{header: &_Header{
		command: CommandSOW,
		queryID: []byte("delta-sync"),
		subID:   []byte("delta-sync"),
		topic:   []byte("orders"),
	}, data: []byte(`{"id":2}`)})

	select {
	case hasNext := <-hasNextCh:
		if !hasNext {
			t.Fatalf("expected HasNext to report queued delta SOW data")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for HasNext after delta SOW data arrival")
	}

	var message = stream.Next()
	if message == nil || string(message.Data()) != `{"id":2}` {
		t.Fatalf("unexpected delta SOW message: %#v", message)
	}
}

func TestClientExecuteSowAndSubscribeCloseUnsubscribes(t *testing.T) {
	var client, _, stream = executeSyncStreamWithProcessedAck(
		t,
		"execute-sow-and-subscribe-close",
		NewCommand("sow_and_subscribe").SetTopic("orders").SetSubID("sub-close"),
		"sub-close",
	)

	if err := stream.Close(); err != nil {
		t.Fatalf("expected close success: %v", err)
	}

	var conn = client.connection.(*testConn)
	var payload = conn.WrittenPayload()
	if !strings.Contains(payload, `"c":"unsubscribe"`) || !strings.Contains(payload, `"sub_id":"sub-close"`) {
		t.Fatalf("expected unsubscribe payload for live subscription, got %q", payload)
	}
}

func TestClientExecuteSowAndSubscribeCloseRemovesQueryRoute(t *testing.T) {
	var client, _, stream = executeSyncStreamWithProcessedAck(
		t,
		"execute-sow-and-subscribe-query-close",
		NewCommand("sow_and_subscribe").SetTopic("orders").SetSubID("sub-close-query").SetQueryID("qid-close-query"),
		"qid-close-query",
	)

	if _, exists := client.routes.Load("qid-close-query"); !exists {
		t.Fatalf("expected query route registration before close")
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("expected close success: %v", err)
	}

	if _, exists := client.routes.Load("qid-close-query"); exists {
		t.Fatalf("expected close to remove query route for live subscription")
	}
}

func TestClientUnsubscribeRemovesQueryBackedSubscriptionState(t *testing.T) {
	var client, _, _ = executeSyncStreamWithProcessedAck(
		t,
		"execute-sow-and-subscribe-query-unsubscribe",
		NewCommand("sow_and_subscribe").SetTopic("orders").SetSubID("sub-unsub-query").SetQueryID("qid-unsub-query"),
		"qid-unsub-query",
	)

	if _, exists := client.messageStreams.Load("qid-unsub-query"); !exists {
		t.Fatalf("expected query-backed stream registration before unsubscribe")
	}

	if err := client.Unsubscribe("sub-unsub-query"); err != nil {
		t.Fatalf("expected unsubscribe success: %v", err)
	}

	if _, exists := client.routes.Load("qid-unsub-query"); exists {
		t.Fatalf("expected unsubscribe to remove query-backed route")
	}
	if _, exists := client.messageStreams.Load("qid-unsub-query"); exists {
		t.Fatalf("expected unsubscribe to remove query-backed stream")
	}
}

func TestExecuteAsyncDisconnectDuringRouteRegistrationDoesNotLeaveRoute(t *testing.T) {
	var client = NewClient("execute-async-disconnect-registration")
	var conn = newTestConn()
	client.connected.Store(true)
	client.connection = conn

	var routeID string
	_, err := client.executeAsync(
		NewCommand("subscribe").SetTopic("orders").SetSubID("sub-registration-disconnect"),
		nil,
		false,
		func(commandID string, currentRouteID string) {
			_ = commandID
			routeID = currentRouteID
			client.onConnectionError(NewError(ConnectionError, "disconnect during route registration"))
		},
	)
	if err == nil {
		t.Fatalf("expected disconnected error")
	}
	if routeID == "" {
		t.Fatalf("expected route id from route registration callback")
	}
	if _, exists := client.routes.Load(routeID); exists {
		t.Fatalf("expected route cleanup after disconnect during route registration")
	}
}

func TestExecuteAsyncRouteRegistrationFailureClosesSyncAckProcessing(t *testing.T) {
	var client = NewClient("execute-async-route-registration-failure")
	client.connected.Store(true)
	client.routes.Store("sub-registration-failure", func(*Message) error { return nil })

	_, err := client.executeAsync(
		NewCommand("subscribe").SetTopic("orders").SetSubID("sub-registration-failure"),
		nil,
		false,
		nil,
	)
	if err == nil {
		t.Fatalf("expected route registration failure")
	}
	if client.getSyncAckProcessing() != nil {
		t.Fatalf("expected sync ack processing cleanup after route registration failure")
	}
}

func TestExecuteAsyncSubscribeProcessedFailureRemovesRoute(t *testing.T) {
	var client = NewClient("execute-async-subscribe-failure-cleanup")
	var conn = newTestConn()
	client.connected.Store(true)
	client.connection = conn

	handled := 0
	_, err := client.ExecuteAsync(
		NewCommand("subscribe").SetTopic("orders").SetSubID("sub-failure-cleanup").SetAckType(AckTypeProcessed),
		func(*Message) error {
			handled++
			return nil
		},
	)
	if err != nil {
		t.Fatalf("expected subscribe registration success: %v", err)
	}

	var handler = waitForRouteHandler(t, client, "sub-failure-cleanup")
	var ack = AckTypeProcessed
	err = handler(&Message{header: &_Header{
		command: CommandAck,
		ackType: &ack,
		status:  []byte("failure"),
		reason:  []byte("bad filter"),
	}})
	if err != nil {
		t.Fatalf("expected failure ack handler success: %v", err)
	}
	if handled != 1 {
		t.Fatalf("expected subscribe failure callback once, got %d", handled)
	}
	if _, exists := client.routes.Load("sub-failure-cleanup"); exists {
		t.Fatalf("expected failed subscribe to remove route")
	}

	_, err = client.ExecuteAsync(
		NewCommand("subscribe").SetTopic("orders").SetSubID("sub-failure-cleanup").SetAckType(AckTypeProcessed),
		func(*Message) error { return nil },
	)
	if err != nil {
		t.Fatalf("expected failed subscribe route id to be reusable, got %v", err)
	}
}

func TestClientSowDeleteAndHeartbeatCoverage(t *testing.T) {
	sowClient := NewClient("sow-delete")
	sowConn := newTestConn()
	sowClient.connected.Store(true)
	sowClient.resetDisconnectSignal()
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
	heartbeatClient.resetDisconnectSignal()
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

func TestClientFlushAckPathsCoverage(t *testing.T) {
	ackCompleted := AckTypeCompleted
	ackProcessed := AckTypeProcessed

	runFlush := func(response *Message) error {
		client := NewClient("flush-ack-paths")
		client.connected.Store(true)
		client.connection = newTestConn()

		result := make(chan error, 1)
		go func() {
			result <- client.Flush()
		}()

		_, handler := waitForAnyRouteHandler(t, client)
		_ = handler(&Message{header: &_Header{
			command: CommandAck,
			ackType: &ackProcessed,
			status:  []byte("success"),
		}})
		_ = handler(response)

		select {
		case err := <-result:
			return err
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timed out waiting for Flush result")
		}
		return nil
	}

	if err := runFlush(&Message{header: &_Header{
		command: CommandAck,
		ackType: &ackCompleted,
		status:  []byte("success"),
	}}); err != nil {
		t.Fatalf("expected Flush success ack, got %v", err)
	}

	if err := runFlush(&Message{header: &_Header{
		command: CommandAck,
		ackType: &ackCompleted,
		status:  []byte("failure"),
		reason:  []byte("flush failed by test"),
	}}); err == nil || !strings.Contains(err.Error(), "flush failed by test") {
		t.Fatalf("expected Flush reason failure, got %v", err)
	}

	if err := runFlush(&Message{header: &_Header{
		command: CommandAck,
		ackType: &ackCompleted,
		status:  []byte("failure"),
	}}); err == nil || !strings.Contains(err.Error(), "flush failed") {
		t.Fatalf("expected generic Flush failure, got %v", err)
	}
}

func TestClientDisconnectSignalLifecycle(t *testing.T) {
	var client = NewClient("disconnect-signal")
	if !isClientSignalClosed(client.disconnectCh) {
		t.Fatalf("expected new client disconnect signal to start closed")
	}

	client.resetDisconnectSignal()
	if isClientSignalClosed(client.disconnectCh) {
		t.Fatalf("expected reset disconnect signal to open the active signal")
	}

	var activeSignal = client.disconnectCh
	client.signalDisconnect()
	if !isClientSignalClosed(activeSignal) {
		t.Fatalf("expected signalDisconnect to close the active signal")
	}

	client.resetDisconnectSignal()
	if client.disconnectCh == activeSignal {
		t.Fatalf("expected resetDisconnectSignal to allocate a fresh signal")
	}
	if isClientSignalClosed(client.disconnectCh) {
		t.Fatalf("expected refreshed disconnect signal to remain open")
	}
}

func TestClientSowDeleteWaitAbortsOnDisconnect(t *testing.T) {
	client := NewClient("sow-delete-disconnect")
	client.connected.Store(true)
	client.resetDisconnectSignal()
	client.connection = newTestConn()

	errCh := make(chan error, 1)
	go func() {
		_, err := client.SowDelete("orders", "/id > 0")
		errCh <- err
	}()

	_, _ = waitForAnyRouteHandler(t, client)
	client.connected.Store(false)
	client.signalDisconnect()

	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "DisconnectedError") {
			t.Fatalf("expected disconnected error while waiting for sow delete ack, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected SowDelete wait to abort after disconnect")
	}
}

func TestClientSyncAckWaitAbortsOnDisconnect(t *testing.T) {
	client := NewClient("sync-ack-disconnect")
	client.connected.Store(true)
	client.resetDisconnectSignal()
	client.connection = newTestConn()

	errCh := make(chan error, 1)
	go func() {
		_, err := client.ExecuteAsync(NewCommand("flush"), nil)
		errCh <- err
	}()

	_, _ = waitForAnyRouteHandler(t, client)
	if err := client.Disconnect(); err != nil {
		t.Fatalf("disconnect failed: %v", err)
	}

	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "sync ack channel closed") {
			t.Fatalf("expected sync ack wait to abort after disconnect, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected ExecuteAsync wait to abort after disconnect")
	}
}

func TestClientSubscribeSyncAckWaitReturnsDisconnectedOnDisconnect(t *testing.T) {
	client := NewClient("subscribe-sync-ack-disconnect")
	client.connected.Store(true)
	client.resetDisconnectSignal()
	client.connection = newTestConn()

	errCh := make(chan error, 1)
	go func() {
		_, err := client.SubscribeAsync(func(*Message) error { return nil }, "orders")
		errCh <- err
	}()

	_, _ = waitForAnyRouteHandler(t, client)
	if err := client.Disconnect(); err != nil {
		t.Fatalf("disconnect failed: %v", err)
	}

	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "DisconnectedError") {
			t.Fatalf("expected subscribe wait to abort with disconnected error, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected SubscribeAsync wait to abort after disconnect")
	}
}

func TestClientSyncAckCommandsRemainSerialized(t *testing.T) {
	client := NewClient("sync-ack-serialized")
	client.connected.Store(true)
	client.resetDisconnectSignal()
	client.connection = newTestConn()
	defer func() {
		_ = client.Disconnect()
	}()

	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() {
		_, err := client.ExecuteAsync(NewCommand("flush"), nil)
		firstDone <- err
	}()

	firstRouteID, firstHandler := waitForAnyRouteHandler(t, client)

	go func() {
		_, err := client.ExecuteAsync(NewCommand("flush"), nil)
		secondDone <- err
	}()

	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		routeCount := 0
		client.routes.Range(func(_, _ interface{}) bool {
			routeCount++
			return true
		})
		if routeCount > 1 {
			t.Fatalf("expected second sync-ack command to remain blocked while first waits")
		}
		time.Sleep(2 * time.Millisecond)
	}

	ackType := AckTypeProcessed
	if err := firstHandler(&Message{header: &_Header{
		command: CommandAck,
		ackType: &ackType,
		status:  []byte("success"),
	}}); err != nil {
		t.Fatalf("first flush ack failed: %v", err)
	}

	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first flush failed: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected first sync-ack command to complete after ack")
	}

	var secondRouteID string
	var secondHandler func(*Message) error
	deadline = time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		client.routes.Range(func(key interface{}, value interface{}) bool {
			candidateRouteID := key.(string)
			candidateHandler := value.(func(*Message) error)
			if candidateRouteID != firstRouteID {
				secondRouteID = candidateRouteID
				secondHandler = candidateHandler
				return false
			}
			return true
		})
		if secondHandler != nil {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if secondHandler == nil {
		t.Fatalf("expected second sync-ack command to register after first completion")
	}
	if secondRouteID == firstRouteID {
		t.Fatalf("expected distinct route ids for serialized sync-ack commands")
	}

	if err := secondHandler(&Message{header: &_Header{
		command: CommandAck,
		ackType: &ackType,
		status:  []byte("success"),
	}}); err != nil {
		t.Fatalf("second flush ack failed: %v", err)
	}

	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second flush failed: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected second sync-ack command to complete after ack")
	}
}

func TestClientEstablishHeartbeatAbortsOnDisconnect(t *testing.T) {
	var client = NewClient("heartbeat-disconnect")
	client.connected.Store(true)
	client.resetDisconnectSignal()
	client.connection = newTestConn()
	client.heartbeatInterval = 1
	client.heartbeatTimeout = 2
	client.SetErrorHandler(func(error) {})

	var errCh = make(chan error, 1)
	go func() {
		errCh <- client.establishHeartbeat()
	}()

	_, _ = waitForAnyRouteHandler(t, client)
	client.connected.Store(false)
	client.signalDisconnect()

	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "DisconnectedError") {
			t.Fatalf("expected disconnected error while waiting for heartbeat ack, got %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("expected heartbeat establishment to abort after disconnect")
	}
}

func TestClientEstablishHeartbeatTimeoutCoverage(t *testing.T) {
	client := NewClient("heartbeat-timeout")
	client.connected.Store(true)
	client.resetDisconnectSignal()
	client.connection = newTestConn()
	client.heartbeatInterval = 1
	client.heartbeatTimeout = 1
	client.SetErrorHandler(func(error) {})

	start := time.Now()
	err := client.establishHeartbeat()
	if err == nil || !strings.Contains(err.Error(), "TimedOutError") {
		t.Fatalf("expected heartbeat establishment timeout, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Fatalf("expected timeout close to one second, elapsed=%s", elapsed)
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
