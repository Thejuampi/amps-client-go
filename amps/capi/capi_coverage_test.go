package capi

import (
	"bytes"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

func TestHandleAndObjectHelpersCoverage(t *testing.T) {
	handle := newHandle(123)
	if value, ok := getObject[int](handle); !ok || value == nil || *value != 123 {
		t.Fatalf("unexpected typed handle lookup")
	}
	if _, ok := getObject[string](handle); ok {
		t.Fatalf("unexpected typed lookup success for mismatched type")
	}

	message := MessageCreate(0)
	if _, ok := getClientObject(message); ok {
		t.Fatalf("message handle should not resolve as client object")
	}
	if _, ok := getMessageObject(Handle(0)); ok {
		t.Fatalf("zero handle should not resolve as message object")
	}
}

func TestNormalizeFieldAndMessageCRUDCoverage(t *testing.T) {
	cases := map[string]string{
		"command":       "c",
		"cid":           "cid",
		"topic":         "t",
		"bookmark":      "bm",
		"sub_id":        "s",
		"query_id":      "query_id",
		"filter":        "filter",
		"opts":          "opts",
		"x":             "x",
		"acktype":       "a",
		"sow_keys":      "sow_keys",
		"expiration":    "e",
		"batchsize":     "bs",
		"custom_header": "custom_header",
	}
	for input, expected := range cases {
		if got := normalizeField(input); got != expected {
			t.Fatalf("normalizeField(%q)=%q want %q", input, got, expected)
		}
	}

	message := MessageCreate(0)
	MessageSetFieldValue(message, "command", "publish")
	MessageAssignFieldValue(message, "topic", "orders")
	MessageAssignFieldValueOwnership(message, "bookmark", "1|1|")
	MessageSetFieldValueNTS(message, "sub_id", "sub-1")
	MessageSetFieldGUID(message, "correlationid", "corr-1")
	MessageSetFieldValue(message, "batchsize", "15")
	MessageSetFieldValue(message, "expiration", "20")
	MessageSetFieldValue(message, "ack", "4")

	if got := MessageGetFieldValue(message, "c"); got != "publish" {
		t.Fatalf("unexpected command field: %q", got)
	}
	if got := MessageGetFieldLong(message, "expiration"); got != 20 {
		t.Fatalf("unexpected expiration parse: %d", got)
	}
	if got := MessageGetFieldUint64(message, "batchsize"); got != 15 {
		t.Fatalf("unexpected batch size parse: %d", got)
	}
	MessageSetFieldValue(message, "batchsize", "bad")
	if got := MessageGetFieldUint64(message, "batchsize"); got != 0 {
		t.Fatalf("invalid uint64 parse should be zero, got %d", got)
	}

	MessageSetData(message, []byte("payload"))
	MessageAssignData(message, []byte("payload2"))
	MessageSetDataNTS(message, "payload3")
	data := MessageGetData(message)
	if string(data) != "payload3" {
		t.Fatalf("unexpected message payload: %q", string(data))
	}
	data[0] = 'X'
	if string(MessageGetData(message)) != "payload3" {
		t.Fatalf("message payload should be copied on read")
	}

	copied := MessageCopy(message)
	if copied == 0 {
		t.Fatalf("expected message copy handle")
	}
	if got := MessageGetFieldValue(copied, "topic"); got != "orders" {
		t.Fatalf("unexpected copied field value: %q", got)
	}
	MessageReset(copied)
	if got := MessageGetFieldValue(copied, "topic"); got != "" {
		t.Fatalf("expected reset fields, got %q", got)
	}
	MessageDestroy(copied)
	MessageDestroy(message)
}

func TestCommandAndMessageConversionCoverage(t *testing.T) {
	if command := commandFromMessage(nil); command != nil {
		t.Fatalf("nil message object should return nil command")
	}
	if command := commandFromMessage(&messageObject{fields: map[string]string{}}); command != nil {
		t.Fatalf("missing command field should return nil command")
	}

	object := &messageObject{
		fields: map[string]string{
			"c":        "publish",
			"cid":      "cid-1",
			"t":        "orders",
			"bm":       "1|1|",
			"s":        "sub-1",
			"query_id": "qid-1",
			"filter":   "/id > 10",
			"opts":     "replace",
			"x":        "corr-1",
			"k":        "k1",
			"sow_keys": "k1,k2",
			"e":        "10",
			"bs":       "5",
			"a":        "4",
		},
		data: []byte("payload"),
	}
	command := commandFromMessage(object)
	if command == nil {
		t.Fatalf("expected command from message object")
	}
	if topic, _ := command.Topic(); topic != "orders" {
		t.Fatalf("unexpected topic: %q", topic)
	}
	if ackType, _ := command.AckType(); ackType != 4 {
		t.Fatalf("unexpected ack type: %d", ackType)
	}

	message := amps.NewCommand("publish").
		SetCommandID("cid-2").
		SetTopic("topic-2").
		SetBookmark("2|2|").
		SetSubID("sub-2").
		SetQueryID("qid-2").
		SetData([]byte("payload-2")).
		GetMessage()
	handle := messageFromAmps(message)
	converted, ok := getMessageObject(handle)
	if !ok {
		t.Fatalf("expected converted message handle")
	}
	if converted.fields["cid"] != "cid-2" || converted.fields["t"] != "topic-2" {
		t.Fatalf("unexpected converted fields: %+v", converted.fields)
	}
	if string(converted.data) != "payload-2" {
		t.Fatalf("unexpected converted payload: %q", string(converted.data))
	}
	nilHandle := messageFromAmps(nil)
	if _, ok := getMessageObject(nilHandle); !ok {
		t.Fatalf("expected non-nil handle for nil message conversion")
	}
}

func TestClientWrapperCoverage(t *testing.T) {
	if result := ClientSetName(0, "name"); result != ENotFound {
		t.Fatalf("expected ENotFound on invalid set name, got %d", result)
	}
	if result := ClientSetReadTimeout(0, 10); result != ENotFound {
		t.Fatalf("expected ENotFound on invalid read timeout, got %d", result)
	}
	if result := ClientSetIdleTime(0, 10); result != ENotFound {
		t.Fatalf("expected ENotFound on invalid idle timeout, got %d", result)
	}
	if result := ClientSetTransportFilterFunction(0, nil, nil); result != ENotFound {
		t.Fatalf("expected ENotFound on invalid transport filter, got %d", result)
	}
	if result := ClientSetThreadCreatedCallback(0, nil, nil); result != ENotFound {
		t.Fatalf("expected ENotFound on invalid created callback, got %d", result)
	}
	if result := ClientSetThreadExitCallback(0, nil, nil); result != ENotFound {
		t.Fatalf("expected ENotFound on invalid exit callback, got %d", result)
	}
	if result := ClientSetHTTPPreflightCallback(0, nil, nil); result != ENotFound {
		t.Fatalf("expected ENotFound on invalid preflight callback, got %d", result)
	}
	ClientSetBatchSend(0, 1, 1)
	if transport := ClientGetTransport(0); transport != 0 {
		t.Fatalf("expected zero transport handle for invalid client")
	}
	if socket := ClientGetSocket(0); socket != 0 {
		t.Fatalf("expected zero socket for invalid client")
	}
	if socket := TCPGetSocket(0); socket != 0 {
		t.Fatalf("expected zero TCP socket for invalid transport")
	}
	if socket := TCPSGetSocket(0); socket != 0 {
		t.Fatalf("expected zero TCPS socket for invalid transport")
	}
	if ssl := TCPSGetSSL(0); ssl != 0 {
		t.Fatalf("expected zero TCPS SSL for invalid transport")
	}

	handle := ClientCreate("wrapper-coverage")
	defer ClientDestroy(handle)
	if result := ClientSetName(handle, "renamed-client"); result != EOK {
		t.Fatalf("expected EOK set name, got %d", result)
	}

	message := MessageCreate(handle)
	MessageSetFieldValue(message, "command", "publish")
	MessageSetFieldValue(message, "topic", "orders")
	MessageSetDataNTS(message, `{"id":1}`)
	if result := ClientSend(handle, Handle(0)); result != EFailed {
		t.Fatalf("expected EFailed for invalid message handle, got %d", result)
	}
	if result := ClientSendWithVersion(handle, message, 1); result != EFailed {
		t.Fatalf("expected EFailed while disconnected, got %d", result)
	}
	if result := ClientSendBatch(handle, []Handle{message}); result != EFailed {
		t.Fatalf("expected EFailed batch send while disconnected, got %d", result)
	}
	if errText := ClientGetError(handle); errText == "" {
		t.Fatalf("expected populated client error after failed send")
	}
	if errText := ClientGetError(0); errText != "invalid handle" {
		t.Fatalf("unexpected invalid handle error text: %q", errText)
	}

	received := 0
	ClientSetMessageHandler(handle, func(message Handle, userData any) int {
		_ = message
		_ = userData
		received++
		return EOK
	}, "msg")
	ClientSetPreDisconnectHandler(handle, func(message Handle, userData any) int {
		_ = message
		_ = userData
		return EOK
	}, "predisconnect")
	ClientSetDisconnectHandler(handle, func(message Handle, userData any) int {
		_ = message
		_ = userData
		return EOK
	}, "disconnect")
	if transport := ClientGetTransport(handle); transport != handle {
		t.Fatalf("expected transport alias to match handle")
	}
	if result := ClientSetTransportFilterFunction(handle, func(direction int, payload []byte, userData any) []byte {
		_ = direction
		_ = userData
		return append([]byte(nil), payload...)
	}, "filter"); result != EOK {
		t.Fatalf("expected EOK setting transport filter, got %d", result)
	}
	if result := ClientSetTransportFilterFunction(handle, nil, nil); result != EOK {
		t.Fatalf("expected EOK clearing transport filter, got %d", result)
	}
	if result := ClientSetHTTPPreflightCallback(handle, func(userData any) []string {
		_ = userData
		return []string{"X-Test: 1"}
	}, "headers"); result != EOK {
		t.Fatalf("expected EOK setting preflight callback, got %d", result)
	}
	if result := ClientSetHTTPPreflightCallback(handle, nil, nil); result != EOK {
		t.Fatalf("expected EOK clearing preflight callback, got %d", result)
	}
	ClientSetBatchSend(handle, 4096, 250)

	object, ok := getClientObject(handle)
	if !ok {
		t.Fatalf("expected client object")
	}
	// Trigger handler registration paths.
	object.client.SetUnhandledMessageHandler(func(message *amps.Message) error {
		_ = message
		return nil
	})

	ClientSetMessageHandler(handle, nil, nil)
	ClientSetDisconnectHandler(handle, nil, nil)
}

func TestClientSocketAndTimeoutCoverage(t *testing.T) {
	server := newLifecycleTestServer(t)
	defer server.close()

	handle := ClientCreate("socket-timeout")
	defer ClientDestroy(handle)
	if result := ClientConnect(handle, server.uri()); result != EOK {
		t.Fatalf("connect failed: %d", result)
	}

	if socket := ClientGetSocket(handle); socket == 0 {
		t.Fatalf("expected non-zero socket descriptor")
	}
	if socket := TCPGetSocket(handle); socket == 0 {
		t.Fatalf("expected non-zero tcp socket descriptor")
	}
	if socket := TCPSGetSocket(handle); socket == 0 {
		t.Fatalf("expected non-zero tcps socket descriptor")
	}
	if ssl := TCPSGetSSL(handle); ssl == 0 {
		t.Fatalf("expected non-zero ssl alias handle")
	}

	if result := ClientSetReadTimeout(handle, 10); result != EOK {
		t.Fatalf("expected EOK read timeout, got %d", result)
	}
	if result := ClientSetReadTimeout(handle, 0); result != EOK {
		t.Fatalf("expected EOK read timeout reset, got %d", result)
	}
	if result := ClientSetIdleTime(handle, 25); result != EOK {
		t.Fatalf("expected EOK idle time, got %d", result)
	}

	ClientDisconnect(handle)
}

func TestSocketFromConnCoverage(t *testing.T) {
	if fd := socketFromConn(nil); fd != 0 {
		t.Fatalf("expected nil socket descriptor 0, got %d", fd)
	}
	if fd := socketFromConn(struct{}{}); fd != 0 {
		t.Fatalf("expected non-syscall conn descriptor 0, got %d", fd)
	}
}

func TestSSLAndZlibEdgeCoverage(t *testing.T) {
	if now := Now(); now == 0 {
		t.Fatalf("expected non-zero time")
	}

	if result := SSLInit(""); result != 0 {
		t.Fatalf("unexpected SSLInit result: %d", result)
	}
	if result := SSLInitFromContext(nil, ""); result != 0 {
		t.Fatalf("unexpected SSLInitFromContext result: %d", result)
	}
	if result := SSLSetVerify(1); result != 0 {
		t.Fatalf("unexpected SSLSetVerify result: %d", result)
	}
	if result := SSLLoadVerifyLocations("ca.pem", ""); result != 0 {
		t.Fatalf("unexpected SSLLoadVerifyLocations result: %d", result)
	}
	if result := SSLUseCertificateFile("cert.pem", 1); result != 0 {
		t.Fatalf("unexpected SSLUseCertificateFile result: %d", result)
	}
	if result := SSLUsePrivateKeyFile("key.pem", 1); result != 0 {
		t.Fatalf("unexpected SSLUsePrivateKeyFile result: %d", result)
	}
	SSLFree()
	if SSLGetError() != "" {
		t.Fatalf("expected empty SSL error after free")
	}
	SSLLibraryInit()
	SSLLoadErrorStrings()

	if code := ERRGetError(); code != 0 {
		t.Fatalf("expected zero ssl error code, got %d", code)
	}
	if n := ERRErrorStringN(1, nil); n != 0 {
		t.Fatalf("expected zero write for nil buffer, got %d", n)
	}
	buffer := make([]byte, 32)
	if n := ERRErrorStringN(2, buffer); n == 0 {
		t.Fatalf("expected non-zero write to error buffer")
	}
	ERRClearError()

	if context := SSLCTXNew(0); context != 0 {
		t.Fatalf("expected zero context for invalid method")
	}
	method := SSLv23ClientMethod()
	context := SSLCTXNew(method)
	if context == 0 {
		t.Fatalf("expected ssl context")
	}
	if handle := SSLNew(0); handle != 0 {
		t.Fatalf("expected zero ssl handle for invalid context")
	}
	if result := SSLSetFD(0, -1); result != -1 {
		t.Fatalf("expected invalid fd result -1, got %d", result)
	}
	handle := SSLNew(context)
	if handle == 0 {
		t.Fatalf("expected ssl handle from context")
	}
	SSLCTXFree(context)
	if missing := SSLNew(context); missing != 0 {
		t.Fatalf("expected missing-context ssl handle 0")
	}

	if result := SSLSetFD(handle, -1); result != -1 {
		t.Fatalf("expected SSLSetFD invalid fd -1, got %d", result)
	}
	if result := SSLSetFD(handle, 1); result != 1 {
		t.Fatalf("expected SSLSetFD success, got %d", result)
	}
	if result := SSLConnect(0); result != -1 {
		t.Fatalf("expected SSLConnect invalid -1, got %d", result)
	}
	if result := SSLConnect(handle); result != 1 {
		t.Fatalf("expected SSLConnect success, got %d", result)
	}
	if result := SSLRead(0, []byte("x")); result != -1 {
		t.Fatalf("expected SSLRead invalid -1, got %d", result)
	}
	if result := SSLRead(handle, nil); result != 0 {
		t.Fatalf("expected SSLRead empty buffer 0, got %d", result)
	}
	if result := SSLCtrl(0, 1, 0, 0); result != -1 {
		t.Fatalf("expected SSLCtrl invalid -1, got %d", result)
	}
	if result := SSLCtrl(handle, 1, 0, 0); result != 1 {
		t.Fatalf("expected SSLCtrl success, got %d", result)
	}
	if result := SSLWrite(0, []byte("x")); result != -1 {
		t.Fatalf("expected SSLWrite invalid -1, got %d", result)
	}
	if result := SSLWrite(handle, []byte("abc")); result != 3 {
		t.Fatalf("expected SSLWrite length 3, got %d", result)
	}
	if result := SSLShutdown(0); result != -1 {
		t.Fatalf("expected SSLShutdown invalid -1, got %d", result)
	}
	if result := SSLShutdown(handle); result != 1 {
		t.Fatalf("expected SSLShutdown success, got %d", result)
	}
	if pending := SSLPending(0); pending != 0 {
		t.Fatalf("expected SSLPending zero for invalid handle")
	}
	if pending := SSLPending(handle); pending != 0 {
		t.Fatalf("expected SSLPending zero for compatibility layer")
	}
	if code := SSLGetErrorCode(handle, -1); code == 0 {
		t.Fatalf("expected SSL error code on negative result")
	}
	if code := SSLGetErrorCode(handle, 1); code != 0 {
		t.Fatalf("expected SSL error code 0 on success, got %d", code)
	}
	SSLFreeHandle(handle)

	if result := ZlibInit(""); result != EOK {
		t.Fatalf("unexpected zlib init result: %d", result)
	}
	if loaded := ZlibIsLoaded(); loaded != 1 {
		t.Fatalf("expected zlib loaded state")
	}
	if version := ZlibVersion(); version == "" {
		t.Fatalf("expected zlib version")
	}
	if result := DeflateInit2(nil, 0, 0, 0, 0, 0); result != EUsage {
		t.Fatalf("expected deflate init usage error, got %d", result)
	}
	if result := DeflateInit2_(nil, 0, 0, 0, 0, 0); result != EUsage {
		t.Fatalf("expected deflate init alias usage error, got %d", result)
	}
	stream := &ZStream{NextIn: []byte("hello")}
	if result := DeflateInit2(stream, 0, 0, 0, 0, 0); result != EOK {
		t.Fatalf("expected deflate init ok, got %d", result)
	}
	if result := Deflate(stream, 0); result != EOK {
		t.Fatalf("expected deflate ok, got %d", result)
	}
	if result := DeflateEnd(stream); result != EOK {
		t.Fatalf("expected deflate end ok, got %d", result)
	}
	if result := DeflateEnd(nil); result != EUsage {
		t.Fatalf("expected deflate end usage error, got %d", result)
	}
	if result := Deflate(nil, 0); result != EUsage {
		t.Fatalf("expected deflate usage error, got %d", result)
	}

	if result := InflateInit2(nil, 0); result != EUsage {
		t.Fatalf("expected inflate init usage error, got %d", result)
	}
	if result := InflateInit2_(nil, 0); result != EUsage {
		t.Fatalf("expected inflate init alias usage error, got %d", result)
	}
	decoded := &ZStream{NextIn: append([]byte(nil), stream.NextOut...)}
	if result := InflateInit2(decoded, 0); result != EOK {
		t.Fatalf("expected inflate init ok, got %d", result)
	}
	if result := Inflate(decoded, 0); result != EOK {
		t.Fatalf("expected inflate ok, got %d", result)
	}
	if !bytes.Equal(decoded.NextOut, []byte("hello")) {
		t.Fatalf("unexpected inflate output: %q", string(decoded.NextOut))
	}
	if result := InflateEnd(decoded); result != EOK {
		t.Fatalf("expected inflate end ok, got %d", result)
	}
	if result := InflateEnd(nil); result != EUsage {
		t.Fatalf("expected inflate end usage error, got %d", result)
	}
	if result := Inflate(nil, 0); result != EUsage {
		t.Fatalf("expected inflate usage error, got %d", result)
	}
	if result := Inflate(&ZStream{NextIn: []byte("bad-zlib")}, 0); result != EFailed {
		t.Fatalf("expected inflate failed on invalid payload, got %d", result)
	}
	if ZlibLastError() == "" {
		t.Fatalf("expected zlib error text after failed inflate")
	}
}

func TestCallbackBridgeCoverage(t *testing.T) {
	NoOpFn(nil)

	waitingFunction = atomic.Value{}
	InvokeWaitingFunction()
	waited := 0
	SetWaitingFunction(func() {
		waited++
	})
	InvokeWaitingFunction()
	if waited != 1 {
		t.Fatalf("expected waiting callback")
	}

	removeRouteFunction = atomic.Value{}
	InvokeRemoveRouteFunction("ignored")
	removed := ""
	SetRemoveRouteFunction(func(argument any) {
		removed = argument.(string)
	})
	InvokeRemoveRouteFunction("route-1")
	if removed != "route-1" {
		t.Fatalf("unexpected remove callback argument: %q", removed)
	}

	copyRouteFunction = atomic.Value{}
	if copied := InvokeCopyRouteFunction("y"); copied != "y" {
		t.Fatalf("expected passthrough copy route result, got %v", copied)
	}
	SetCopyRouteFunction(func(argument any) any {
		return "copy:" + argument.(string)
	})
	if copied := InvokeCopyRouteFunction("x"); copied != "copy:x" {
		t.Fatalf("unexpected copied route argument: %v", copied)
	}
}

func TestSetClientErrorCoverage(t *testing.T) {
	if result := setClientError(nil, nil); result != EUsage {
		t.Fatalf("expected EUsage for nil client object, got %d", result)
	}
	object := &clientObject{}
	if result := setClientError(object, nil); result != EOK {
		t.Fatalf("expected EOK for nil error, got %d", result)
	}
	if object.lastError != "" {
		t.Fatalf("expected empty lastError")
	}
	if result := setClientError(object, net.ErrClosed); result != EFailed {
		t.Fatalf("expected EFailed for non-nil error, got %d", result)
	}
	if object.lastError == "" {
		t.Fatalf("expected populated lastError")
	}
}

func TestAttemptReconnectCoverage(t *testing.T) {
	handle := ClientCreate("reconnect")
	defer ClientDestroy(handle)
	if result := ClientAttemptReconnect(handle, 0); result != EFailed {
		t.Fatalf("expected reconnect failure with no URI, got %d", result)
	}

	server := newLifecycleTestServer(t)
	defer server.close()
	if result := ClientConnect(handle, server.uri()); result != EOK {
		t.Fatalf("connect failed: %d", result)
	}
	if result := ClientAttemptReconnect(handle, 0); result != EOK {
		t.Fatalf("expected reconnect success, got %d", result)
	}
	ClientDisconnect(handle)
}

func TestThreadCallbackSetterCoverage(t *testing.T) {
	handle := ClientCreate("thread-callback-setter")
	defer ClientDestroy(handle)

	if result := ClientSetThreadCreatedCallback(handle, func(threadID uint64, userData any) int {
		_ = threadID
		_ = userData
		return EOK
	}, "create"); result != EOK {
		t.Fatalf("expected EOK setting thread created callback, got %d", result)
	}
	if result := ClientSetThreadExitCallback(handle, func(threadID uint64, userData any) int {
		_ = threadID
		_ = userData
		return EOK
	}, "exit"); result != EOK {
		t.Fatalf("expected EOK setting thread exit callback, got %d", result)
	}

	// Ensure callback holders can be overwritten/cleared.
	if result := ClientSetThreadCreatedCallback(handle, nil, nil); result != EOK {
		t.Fatalf("expected EOK clearing thread created callback, got %d", result)
	}
	if result := ClientSetThreadExitCallback(handle, nil, nil); result != EOK {
		t.Fatalf("expected EOK clearing thread exit callback, got %d", result)
	}

	time.Sleep(5 * time.Millisecond)
}
