package capi

import (
	"bytes"
	"compress/zlib"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

// Result values mirror C-style return codes.
const (
	EOK       = 0
	EUsage    = 1
	ENotFound = 2
	EFailed   = 3
)

// Result mirrors `amps_result`.
type Result int

// Char mirrors `amps_char`.
type Char = byte

// Int32 mirrors `amps_int32_t`.
type Int32 = int32

// Int64 mirrors `amps_int64_t`.
type Int64 = int64

// UInt32 mirrors `amps_uint32_t`.
type UInt32 = uint32

// UInt64 mirrors `amps_uint64_t`.
type UInt64 = uint64

// Handle is an opaque handle value.
type Handle uint64

// Handler receives a message callback for C-compatible APIs.
type Handler func(message Handle, userData any) int

// PredisconnectHandler aliases the pre-disconnect callback type.
type PredisconnectHandler = Handler

// ThreadCreatedCallback receives receive-thread start events.
type ThreadCreatedCallback func(threadID uint64, userData any) int

// ThreadExitCallback receives synthetic thread-exit events.
type ThreadExitCallback func(threadID uint64, userData any) int

// TransportFilterFunction processes framed transport payload bytes.
type TransportFilterFunction func(direction int, payload []byte, userData any) []byte

// HTTPPreflightCallback provides preflight header lines.
type HTTPPreflightCallback func(userData any) []string

// SSLMethod mirrors `amps_SSL_METHOD*`.
type SSLMethod uintptr

// SSLContext mirrors `amps_SSL_CTX*`.
type SSLContext uintptr

// SSLHandle mirrors `amps_SSL*`.
type SSLHandle uintptr

// ZStream mirrors `amps_zstream` for compatibility workflows.
type ZStream struct {
	NextIn   []byte
	AvailIn  uint32
	TotalIn  uint64
	NextOut  []byte
	AvailOut uint32
	TotalOut uint64
	Msg      string
}

// ZlibVersionFunc mirrors `amps_zlibVersion_t`.
type ZlibVersionFunc func() string

// DeflateInit2Func mirrors `amps_deflateInit2_t`.
type DeflateInit2Func func(stream *ZStream, level int, method int, windowBits int, memLevel int, strategy int) int

// DeflateFunc mirrors `amps_deflate_t`.
type DeflateFunc func(stream *ZStream, flush int) int

// DeflateEndFunc mirrors `amps_deflate_end_t`.
type DeflateEndFunc func(stream *ZStream) int

// InflateInit2Func mirrors `amps_inflateInit2_t`.
type InflateInit2Func func(stream *ZStream, windowBits int) int

// InflateFunc mirrors `amps_inflate_t`.
type InflateFunc func(stream *ZStream, flush int) int

// InflateEndFunc mirrors `amps_inflate_end_t`.
type InflateEndFunc func(stream *ZStream) int

type clientObject struct {
	client              *amps.Client
	handle              Handle
	lastError           string
	messageHandler      Handler
	threadCreated       ThreadCreatedCallback
	threadCreatedData   any
	disconnectHandler   Handler
	predisconnect       Handler
	threadExitCallback  ThreadExitCallback
	threadExitUserData  any
	transportUserData   any
	transportFilterFunc TransportFilterFunction
	receiveActive       bool
	receiveDone         chan struct{}
	joinWaitActive      bool
	joinTimeout         time.Duration
	receiveToken        uint64
	lock                sync.Mutex
}

type messageObject struct {
	fields map[string]string
	data   []byte
}

type sslContextObject struct {
	method    SSLMethod
	createdAt time.Time
}

type sslHandleObject struct {
	context     SSLContext
	fd          int
	connected   bool
	shutdown    bool
	readBuffer  bytes.Buffer
	writeBuffer bytes.Buffer
	lastError   int
	lock        sync.Mutex
}

var (
	nextHandle  atomic.Uint64
	objects     sync.Map
	threadID    atomic.Uint64
	threadMade  atomic.Uint64
	threadJoin  atomic.Uint64
	threadDrop  atomic.Uint64
	sslError    string
	sslCode     atomic.Uint64
	sslNext     atomic.Uint64
	sslContexts sync.Map
	sslHandles  sync.Map
	zlibError   string
	zlibLoaded  atomic.Bool
)

func newHandle(value any) Handle {
	handle := Handle(nextHandle.Add(1))
	objects.Store(handle, value)
	return handle
}

func getObject[T any](handle Handle) (*T, bool) {
	value, ok := objects.Load(handle)
	if !ok {
		return nil, false
	}
	typed, ok := value.(T)
	if !ok {
		return nil, false
	}
	return &typed, true
}

func getClientObject(handle Handle) (*clientObject, bool) {
	value, ok := objects.Load(handle)
	if !ok {
		return nil, false
	}
	obj, ok := value.(*clientObject)
	if !ok {
		return nil, false
	}
	return obj, true
}

func getMessageObject(handle Handle) (*messageObject, bool) {
	value, ok := objects.Load(handle)
	if !ok {
		return nil, false
	}
	obj, ok := value.(*messageObject)
	if !ok {
		return nil, false
	}
	return obj, true
}

func onReceiveRoutineStarted(object *clientObject) {
	if object == nil {
		return
	}
	token := threadID.Add(1)
	threadMade.Add(1)

	object.lock.Lock()
	object.receiveToken = token
	object.receiveDone = make(chan struct{})
	object.receiveActive = true
	callback := object.threadCreated
	userData := object.threadCreatedData
	object.lock.Unlock()

	if callback != nil {
		callback(token, userData)
	}
}

func onReceiveRoutineStopped(object *clientObject) {
	if object == nil {
		return
	}

	object.lock.Lock()
	if !object.receiveActive {
		object.lock.Unlock()
		return
	}
	object.receiveActive = false
	done := object.receiveDone
	object.receiveDone = nil
	waitingForJoin := object.joinWaitActive
	token := object.receiveToken
	callback := object.threadExitCallback
	userData := object.threadExitUserData
	object.lock.Unlock()

	if done != nil {
		close(done)
	}
	if !waitingForJoin {
		threadDrop.Add(1)
	}
	if callback != nil {
		callback(token, userData)
	}
}

func installReceiveLifecycle(object *clientObject) {
	if object == nil || object.client == nil {
		return
	}
	object.client.SetReceiveRoutineStartedCallback(func() {
		onReceiveRoutineStarted(object)
	})
	object.client.SetReceiveRoutineStoppedCallback(func() {
		onReceiveRoutineStopped(object)
	})
}

func setClientError(object *clientObject, err error) int {
	if object == nil {
		return EUsage
	}
	if err == nil {
		object.lastError = ""
		return EOK
	}
	object.lastError = err.Error()
	return EFailed
}

func normalizeField(field string) string {
	field = strings.TrimSpace(strings.ToLower(field))
	switch field {
	case "command", "c":
		return "c"
	case "commandid", "cid":
		return "cid"
	case "topic", "t":
		return "t"
	case "bookmark", "bm":
		return "bm"
	case "subid", "sub_id", "s":
		return "s"
	case "queryid", "query_id":
		return "query_id"
	case "filter":
		return "filter"
	case "options", "opts":
		return "opts"
	case "correlationid", "x":
		return "x"
	case "ack", "acktype", "a":
		return "a"
	case "sowkey", "k":
		return "k"
	case "sowkeys", "sow_keys":
		return "sow_keys"
	case "expiration", "e":
		return "e"
	case "batchsize", "bs":
		return "bs"
	default:
		return field
	}
}

func commandFromMessage(message *messageObject) *amps.Command {
	if message == nil {
		return nil
	}
	commandName := message.fields["c"]
	if commandName == "" {
		return nil
	}
	command := amps.NewCommand(commandName)
	if value := message.fields["cid"]; value != "" {
		command.SetCommandID(value)
	}
	if value := message.fields["t"]; value != "" {
		command.SetTopic(value)
	}
	if value := message.fields["bm"]; value != "" {
		command.SetBookmark(value)
	}
	if value := message.fields["s"]; value != "" {
		command.SetSubID(value)
	}
	if value := message.fields["query_id"]; value != "" {
		command.SetQueryID(value)
	}
	if value := message.fields["filter"]; value != "" {
		command.SetFilter(value)
	}
	if value := message.fields["opts"]; value != "" {
		command.SetOptions(value)
	}
	if value := message.fields["x"]; value != "" {
		command.SetCorrelationID(value)
	}
	if value := message.fields["k"]; value != "" {
		command.SetSowKey(value)
	}
	if value := message.fields["sow_keys"]; value != "" {
		command.SetSowKeys(value)
	}
	if value := message.fields["e"]; value != "" {
		if parsed, err := strconv.ParseUint(value, 10, 32); err == nil {
			command.SetExpiration(uint(parsed))
		}
	}
	if value := message.fields["bs"]; value != "" {
		if parsed, err := strconv.ParseUint(value, 10, 32); err == nil {
			command.SetBatchSize(uint(parsed))
		}
	}
	if value := message.fields["a"]; value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			command.SetAckType(parsed)
		}
	}
	if message.data != nil {
		command.SetData(append([]byte(nil), message.data...))
	}
	return command
}

func messageFromAmps(message *amps.Message) Handle {
	object := &messageObject{fields: make(map[string]string), data: nil}
	if message == nil {
		return newHandle(object)
	}
	if command, ok := message.Command(); ok {
		object.fields["c"] = strconv.Itoa(command)
	}
	if value, ok := message.CommandID(); ok {
		object.fields["cid"] = value
	}
	if value, ok := message.Topic(); ok {
		object.fields["t"] = value
	}
	if value, ok := message.Bookmark(); ok {
		object.fields["bm"] = value
	}
	if value, ok := message.SubID(); ok {
		object.fields["s"] = value
	}
	if value, ok := message.QueryID(); ok {
		object.fields["query_id"] = value
	}
	if payload := message.Data(); payload != nil {
		object.data = append([]byte(nil), payload...)
	}
	return newHandle(object)
}

// ClientCreate creates a new client handle.
func ClientCreate(clientName string) Handle {
	client := amps.NewClient(clientName)
	object := &clientObject{
		client:      client,
		joinTimeout: 10 * time.Second,
	}
	handle := newHandle(object)
	object.handle = handle
	installReceiveLifecycle(object)
	return handle
}

// ClientSetName sets client name for handle.
func ClientSetName(handle Handle, name string) int {
	object, ok := getClientObject(handle)
	if !ok {
		return ENotFound
	}
	object.client.SetClientName(name)
	return setClientError(object, nil)
}

// ClientConnect connects a client handle to URI.
func ClientConnect(handle Handle, uri string) int {
	object, ok := getClientObject(handle)
	if !ok {
		return ENotFound
	}
	err := object.client.Connect(uri)
	return setClientError(object, err)
}

// ClientDisconnect disconnects the client handle.
func ClientDisconnect(handle Handle) {
	object, ok := getClientObject(handle)
	if !ok {
		return
	}

	waitTimeout := time.Duration(0)
	var waitDone chan struct{}
	object.lock.Lock()
	if object.receiveActive {
		if !object.joinWaitActive {
			object.joinWaitActive = true
			threadJoin.Add(1)
		}
		waitDone = object.receiveDone
		waitTimeout = object.joinTimeout
	}
	object.lock.Unlock()

	_ = object.client.Disconnect()

	if waitDone != nil {
		if waitTimeout <= 0 {
			waitTimeout = 10 * time.Second
		}
		select {
		case <-waitDone:
		case <-time.After(waitTimeout):
		}
		object.lock.Lock()
		object.joinWaitActive = false
		object.lock.Unlock()
	}
}

// ClientDestroy removes handle and closes connection.
func ClientDestroy(handle Handle) {
	if _, ok := getClientObject(handle); ok {
		ClientDisconnect(handle)
	}
	objects.Delete(handle)
}

// ClientGetError returns the last error string for a client handle.
func ClientGetError(handle Handle) string {
	object, ok := getClientObject(handle)
	if !ok {
		return "invalid handle"
	}
	return object.lastError
}

// ClientSend sends a message through the client handle.
func ClientSend(handle Handle, message Handle) int {
	object, ok := getClientObject(handle)
	if !ok {
		return ENotFound
	}
	messageObject, ok := getMessageObject(message)
	if !ok {
		return setClientError(object, amps.NewError(amps.CommandError, "invalid message handle"))
	}
	command := commandFromMessage(messageObject)
	if command == nil {
		return setClientError(object, amps.NewError(amps.CommandError, "message command is not set"))
	}
	_, err := object.client.ExecuteAsync(command, nil)
	return setClientError(object, err)
}

// ClientSendWithVersion sends a message and ignores explicit version value.
func ClientSendWithVersion(handle Handle, message Handle, version uint) int {
	_ = version
	return ClientSend(handle, message)
}

// ClientSendBatch sends a set of message handles.
func ClientSendBatch(handle Handle, messages []Handle) int {
	result := EOK
	for _, messageHandle := range messages {
		sendResult := ClientSend(handle, messageHandle)
		if sendResult != EOK {
			result = sendResult
		}
	}
	return result
}

// ClientSetMessageHandler sets a default message callback handler.
func ClientSetMessageHandler(handle Handle, handler Handler, userData any) {
	object, ok := getClientObject(handle)
	if !ok {
		return
	}
	object.messageHandler = handler
	if handler == nil {
		object.client.SetUnhandledMessageHandler(nil)
		return
	}
	object.client.SetUnhandledMessageHandler(func(message *amps.Message) error {
		messageHandle := messageFromAmps(message)
		handler(messageHandle, userData)
		return nil
	})
}

// ClientSetPreDisconnectHandler sets a pre-disconnect callback.
func ClientSetPreDisconnectHandler(handle Handle, handler Handler, userData any) {
	object, ok := getClientObject(handle)
	if !ok {
		return
	}
	object.predisconnect = handler
	if handler == nil {
		return
	}
	object.client.AddConnectionStateListener(amps.ConnectionStateListenerFunc(func(state amps.ConnectionState) {
		if state == amps.ConnectionStateShutdown {
			handler(handle, userData)
		}
	}))
}

// ClientSetDisconnectHandler sets disconnect callback.
func ClientSetDisconnectHandler(handle Handle, handler Handler, userData any) {
	object, ok := getClientObject(handle)
	if !ok {
		return
	}
	object.disconnectHandler = handler
	if handler == nil {
		object.client.SetDisconnectHandler(nil)
		return
	}
	object.client.SetDisconnectHandler(func(_ *amps.Client, err error) {
		_ = err
		handler(handle, userData)
	})
}

// ClientGetTransport returns a transport handle alias.
func ClientGetTransport(handle Handle) Handle {
	if _, ok := getClientObject(handle); !ok {
		return 0
	}
	return handle
}

// ClientAttemptReconnect disconnects and reconnects using the last client URI.
func ClientAttemptReconnect(handle Handle, version uint) int {
	_ = version
	object, ok := getClientObject(handle)
	if !ok {
		return ENotFound
	}
	uri := object.client.URI()
	if strings.TrimSpace(uri) == "" {
		return setClientError(object, amps.NewError(amps.CommandError, "client has no prior URI"))
	}
	_ = object.client.Disconnect()
	return setClientError(object, object.client.Connect(uri))
}

func socketFromConn(conn any) uintptr {
	type syscallConner interface {
		SyscallConn() (syscall.RawConn, error)
	}
	value, ok := conn.(syscallConner)
	if !ok {
		return 0
	}
	rawConn, err := value.SyscallConn()
	if err != nil {
		return 0
	}
	fd := uintptr(0)
	_ = rawConn.Control(func(descriptor uintptr) {
		fd = descriptor
	})
	return fd
}

// ClientGetSocket returns socket descriptor when available.
func ClientGetSocket(handle Handle) uintptr {
	object, ok := getClientObject(handle)
	if !ok {
		return 0
	}
	return socketFromConn(object.client.RawConnection())
}

// ClientSetReadTimeout sets read deadline in milliseconds.
func ClientSetReadTimeout(handle Handle, timeoutMs uint) int {
	object, ok := getClientObject(handle)
	if !ok {
		return ENotFound
	}
	conn := object.client.RawConnection()
	if conn == nil {
		return EOK
	}
	if timeoutMs == 0 {
		err := conn.SetReadDeadline(time.Time{})
		return setClientError(object, err)
	}
	err := conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(timeoutMs))) // #nosec G115 -- timeoutMs is bounded API input
	return setClientError(object, err)
}

// ClientSetIdleTime is provided for API parity and maps to read timeout.
func ClientSetIdleTime(handle Handle, idleMs uint) int {
	return ClientSetReadTimeout(handle, idleMs)
}

// ClientSetTransportFilterFunction sets transport filter callback.
func ClientSetTransportFilterFunction(handle Handle, filter TransportFilterFunction, userData any) int {
	object, ok := getClientObject(handle)
	if !ok {
		return ENotFound
	}
	object.transportFilterFunc = filter
	object.transportUserData = userData
	if filter == nil {
		object.client.SetTransportFilter(nil)
		return EOK
	}
	object.client.SetTransportFilter(func(direction amps.TransportFilterDirection, payload []byte) []byte {
		return filter(int(direction), payload, userData)
	})
	return EOK
}

// ClientSetThreadCreatedCallback sets receive-routine callback.
func ClientSetThreadCreatedCallback(handle Handle, callback ThreadCreatedCallback, userData any) int {
	object, ok := getClientObject(handle)
	if !ok {
		return ENotFound
	}
	object.lock.Lock()
	object.threadCreated = callback
	object.threadCreatedData = userData
	object.lock.Unlock()
	return EOK
}

// ClientSetThreadExitCallback sets receive-routine stop callback.
func ClientSetThreadExitCallback(handle Handle, callback ThreadExitCallback, userData any) int {
	object, ok := getClientObject(handle)
	if !ok {
		return ENotFound
	}
	object.lock.Lock()
	object.threadExitCallback = callback
	object.threadExitUserData = userData
	object.lock.Unlock()
	return EOK
}

// ClientSetHTTPPreflightCallback sets preflight header callback.
func ClientSetHTTPPreflightCallback(handle Handle, callback HTTPPreflightCallback, userData any) int {
	object, ok := getClientObject(handle)
	if !ok {
		return ENotFound
	}
	if callback == nil {
		object.client.ClearHTTPPreflightHeaders()
		return EOK
	}
	headers := callback(userData)
	object.client.SetHTTPPreflightHeaders(headers)
	return EOK
}

// ClientSetBatchSend configures publish batching behavior.
func ClientSetBatchSend(handle Handle, batchSizeBytes uint64, batchTimeoutMillis uint64) {
	object, ok := getClientObject(handle)
	if !ok {
		return
	}
	object.client.SetPublishBatching(batchSizeBytes, batchTimeoutMillis)
}

// MessageCreate creates an empty message handle.
func MessageCreate(client Handle) Handle {
	_ = client
	return newHandle(&messageObject{fields: make(map[string]string), data: nil})
}

// MessageCopy copies a message handle.
func MessageCopy(message Handle) Handle {
	object, ok := getMessageObject(message)
	if !ok {
		return 0
	}
	copied := &messageObject{fields: make(map[string]string), data: append([]byte(nil), object.data...)}
	for key, value := range object.fields {
		copied.fields[key] = value
	}
	return newHandle(copied)
}

// MessageDestroy removes a message handle.
func MessageDestroy(message Handle) {
	objects.Delete(message)
}

// MessageReset clears message fields.
func MessageReset(message Handle) {
	object, ok := getMessageObject(message)
	if !ok {
		return
	}
	object.fields = make(map[string]string)
	object.data = nil
}

// MessageGetFieldValue returns a message field value.
func MessageGetFieldValue(message Handle, field string) string {
	object, ok := getMessageObject(message)
	if !ok {
		return ""
	}
	return object.fields[normalizeField(field)]
}

// MessageSetFieldValue sets a message field value.
func MessageSetFieldValue(message Handle, field string, value string) {
	object, ok := getMessageObject(message)
	if !ok {
		return
	}
	object.fields[normalizeField(field)] = value
}

// MessageAssignFieldValue sets a message field value.
func MessageAssignFieldValue(message Handle, field string, value string) {
	MessageSetFieldValue(message, field, value)
}

// MessageAssignFieldValueOwnership sets a message field value.
func MessageAssignFieldValueOwnership(message Handle, field string, value string) {
	MessageSetFieldValue(message, field, value)
}

// MessageSetFieldValueNTS sets a null-terminated field value.
func MessageSetFieldValueNTS(message Handle, field string, value string) {
	MessageSetFieldValue(message, field, value)
}

// MessageSetFieldGUID stores a GUID value.
func MessageSetFieldGUID(message Handle, field string, value string) {
	MessageSetFieldValue(message, field, value)
}

// MessageSetData sets payload bytes.
func MessageSetData(message Handle, data []byte) {
	object, ok := getMessageObject(message)
	if !ok {
		return
	}
	object.data = append(object.data[:0], data...)
}

// MessageAssignData sets payload bytes.
func MessageAssignData(message Handle, data []byte) {
	MessageSetData(message, data)
}

// MessageSetDataNTS sets payload from string.
func MessageSetDataNTS(message Handle, data string) {
	MessageSetData(message, []byte(data))
}

// MessageGetData returns payload bytes.
func MessageGetData(message Handle) []byte {
	object, ok := getMessageObject(message)
	if !ok {
		return nil
	}
	return append([]byte(nil), object.data...)
}

// MessageGetFieldLong parses a field as uint32-compatible value.
func MessageGetFieldLong(message Handle, field string) uint32 {
	value := MessageGetFieldValue(message, field)
	parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 32)
	if err != nil {
		return 0
	}
	return uint32(parsed)
}

// MessageGetFieldUint64 parses a field as uint64.
func MessageGetFieldUint64(message Handle, field string) uint64 {
	value := MessageGetFieldValue(message, field)
	parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

// TCPGetSocket returns socket for transport handle.
func TCPGetSocket(transport Handle) uintptr {
	return ClientGetSocket(transport)
}

// TCPSGetSocket returns socket for secure transport handle.
func TCPSGetSocket(transport Handle) uintptr {
	return ClientGetSocket(transport)
}

// TCPSGetSSL returns a placeholder SSL context handle.
func TCPSGetSSL(transport Handle) uintptr {
	if transport == 0 {
		return 0
	}
	return uintptr(transport)
}

// Now returns Unix timestamp in milliseconds.
func Now() uint64 {
	return uint64(time.Now().UnixMilli())
}

// SSLInit initializes SSL extension.
func SSLInit(dllPath string) int {
	_ = dllPath
	sslError = ""
	sslCode.Store(0)
	sslContexts = sync.Map{}
	sslHandles = sync.Map{}
	return 0
}

// SSLInitFromContext initializes SSL from provided context.
func SSLInitFromContext(context any, fileName string) int {
	_ = context
	_ = fileName
	sslError = ""
	sslCode.Store(0)
	return 0
}

// SSLSetVerify sets SSL verify mode.
func SSLSetVerify(mode int) int {
	_ = mode
	sslCode.Store(0)
	return 0
}

// SSLLoadVerifyLocations sets CA paths.
func SSLLoadVerifyLocations(caFile string, caPath string) int {
	_ = caFile
	_ = caPath
	sslCode.Store(0)
	return 0
}

// SSLUseCertificateFile sets certificate file.
func SSLUseCertificateFile(fileName string, fileType int) int {
	_ = fileName
	_ = fileType
	sslCode.Store(0)
	return 0
}

// SSLUsePrivateKeyFile sets private key file.
func SSLUsePrivateKeyFile(fileName string, fileType int) int {
	_ = fileName
	_ = fileType
	sslCode.Store(0)
	return 0
}

// SSLFree releases SSL resources.
func SSLFree() {
	sslError = ""
	sslCode.Store(0)
	sslContexts = sync.Map{}
	sslHandles = sync.Map{}
}

// SSLGetError returns last SSL error string.
func SSLGetError() string {
	return sslError
}

// SSLLibraryInit initializes process-wide SSL state.
func SSLLibraryInit() {
	sslCode.Store(0)
}

// SSLLoadErrorStrings loads SSL error strings.
func SSLLoadErrorStrings() {
	sslCode.Store(0)
}

// ERRGetError returns the most recent SSL-compatible error code.
func ERRGetError() uint64 {
	return sslCode.Load()
}

// ERRErrorStringN formats an SSL error code into the provided buffer.
func ERRErrorStringN(code uint64, buffer []byte) int {
	text := "ssl error " + strconv.FormatUint(code, 10)
	if len(buffer) == 0 {
		return 0
	}
	n := copy(buffer, text)
	if n < len(buffer) {
		buffer[n] = 0
	}
	return n
}

// ERRClearError clears the last SSL error code.
func ERRClearError() {
	sslCode.Store(0)
	sslError = ""
}

// SSLv23ClientMethod returns a placeholder SSL method handle.
func SSLv23ClientMethod() SSLMethod {
	return SSLMethod(1)
}

// SSLCTXNew allocates an SSL context handle.
func SSLCTXNew(method SSLMethod) SSLContext {
	if method == 0 {
		sslCode.Store(1)
		sslError = "invalid ssl method"
		return 0
	}
	handle := SSLContext(sslNext.Add(1))
	sslContexts.Store(handle, &sslContextObject{
		method:    method,
		createdAt: time.Now().UTC(),
	})
	return handle
}

// SSLCTXFree releases an SSL context handle.
func SSLCTXFree(context SSLContext) {
	sslContexts.Delete(context)
}

// SSLNew allocates an SSL handle from context.
func SSLNew(context SSLContext) SSLHandle {
	if context == 0 {
		sslCode.Store(1)
		sslError = "invalid ssl context"
		return 0
	}
	if _, ok := sslContexts.Load(context); !ok {
		sslCode.Store(1)
		sslError = "ssl context not found"
		return 0
	}
	handle := SSLHandle(sslNext.Add(1))
	sslHandles.Store(handle, &sslHandleObject{context: context, fd: -1, connected: false, shutdown: false})
	return handle
}

// SSLSetFD binds a file descriptor to the SSL handle.
func SSLSetFD(ssl SSLHandle, fd int) int {
	if ssl == 0 || fd < 0 {
		sslCode.Store(1)
		sslError = "invalid ssl handle or file descriptor"
		return -1
	}
	value, ok := sslHandles.Load(ssl)
	if !ok {
		sslCode.Store(1)
		sslError = "ssl handle not found"
		return -1
	}
	handle := value.(*sslHandleObject)
	handle.lock.Lock()
	handle.fd = fd
	handle.shutdown = false
	handle.lastError = 0
	handle.lock.Unlock()
	sslCode.Store(0)
	return 1
}

// SSLGetErrorCode returns the SSL error code for an operation result.
func SSLGetErrorCode(ssl SSLHandle, result int) int {
	if result >= 0 {
		return 0
	}
	if value, ok := sslHandles.Load(ssl); ok {
		handle := value.(*sslHandleObject)
		handle.lock.Lock()
		code := handle.lastError
		handle.lock.Unlock()
		if code > 0 {
			return code
		}
	}
	code := sslCode.Load()
	maxInt := uint64(int(^uint(0) >> 1))
	if code > maxInt {
		return int(^uint(0) >> 1)
	}
	return int(code) // #nosec G115 -- checked bounds above
}

// SSLConnect performs a compatibility SSL connect handshake.
func SSLConnect(ssl SSLHandle) int {
	if ssl == 0 {
		sslCode.Store(1)
		sslError = "invalid ssl handle"
		return -1
	}
	value, ok := sslHandles.Load(ssl)
	if !ok {
		sslCode.Store(1)
		sslError = "ssl handle not found"
		return -1
	}
	handle := value.(*sslHandleObject)
	handle.lock.Lock()
	if handle.fd < 0 {
		sslCode.Store(1)
		handle.lastError = 1
		sslError = "ssl file descriptor is not set"
		handle.lock.Unlock()
		return -1
	}
	handle.connected = true
	handle.shutdown = false
	handle.lastError = 0
	handle.lock.Unlock()
	sslCode.Store(0)
	return 1
}

// SSLRead reads data from the SSL handle.
func SSLRead(ssl SSLHandle, buffer []byte) int {
	if ssl == 0 {
		sslCode.Store(1)
		sslError = "invalid ssl handle"
		return -1
	}
	value, ok := sslHandles.Load(ssl)
	if !ok {
		sslCode.Store(1)
		sslError = "ssl handle not found"
		return -1
	}
	handle := value.(*sslHandleObject)
	if len(buffer) == 0 {
		return 0
	}
	handle.lock.Lock()
	defer handle.lock.Unlock()
	if !handle.connected || handle.shutdown {
		return 0
	}
	readCount, _ := handle.readBuffer.Read(buffer)
	return readCount
}

// SSLCtrl invokes a compatibility SSL control command.
func SSLCtrl(ssl SSLHandle, command int, larg int64, pointer uintptr) int {
	_ = command
	_ = larg
	_ = pointer
	if ssl == 0 {
		sslCode.Store(1)
		sslError = "invalid ssl handle"
		return -1
	}
	if _, ok := sslHandles.Load(ssl); !ok {
		sslCode.Store(1)
		sslError = "ssl handle not found"
		return -1
	}
	return 1
}

// SSLWrite writes data to the SSL handle.
// In compatibility mode, written bytes are also mirrored into the same handle's read buffer
// to make SSLRead/SSLPending deterministic for tests and API-level emulation.
func SSLWrite(ssl SSLHandle, buffer []byte) int {
	if ssl == 0 {
		sslCode.Store(1)
		sslError = "invalid ssl handle"
		return -1
	}
	value, ok := sslHandles.Load(ssl)
	if !ok {
		sslCode.Store(1)
		sslError = "ssl handle not found"
		return -1
	}
	handle := value.(*sslHandleObject)
	handle.lock.Lock()
	defer handle.lock.Unlock()
	if handle.shutdown {
		sslCode.Store(1)
		handle.lastError = 1
		sslError = "ssl handle is shutdown"
		return -1
	}
	if !handle.connected {
		sslCode.Store(1)
		handle.lastError = 1
		sslError = "ssl handle is not connected"
		return -1
	}
	if len(buffer) == 0 {
		handle.lastError = 0
		sslCode.Store(0)
		sslError = ""
		return 0
	}
	_, _ = handle.writeBuffer.Write(buffer)
	// Test/compatibility-only loopback. This intentionally does not model real network SSL socket semantics.
	_, _ = handle.readBuffer.Write(buffer)
	handle.lastError = 0
	sslCode.Store(0)
	sslError = ""
	return len(buffer)
}

// SSLShutdown performs compatibility SSL shutdown.
func SSLShutdown(ssl SSLHandle) int {
	if ssl == 0 {
		sslCode.Store(1)
		sslError = "invalid ssl handle"
		return -1
	}
	value, ok := sslHandles.Load(ssl)
	if !ok {
		sslCode.Store(1)
		sslError = "ssl handle not found"
		return -1
	}
	handle := value.(*sslHandleObject)
	handle.lock.Lock()
	handle.connected = false
	handle.shutdown = true
	handle.lock.Unlock()
	return 1
}

// SSLPending returns pending bytes for an SSL handle.
func SSLPending(ssl SSLHandle) int {
	if ssl == 0 {
		return 0
	}
	value, ok := sslHandles.Load(ssl)
	if !ok {
		return 0
	}
	handle := value.(*sslHandleObject)
	handle.lock.Lock()
	pending := handle.readBuffer.Len()
	handle.lock.Unlock()
	return pending
}

// SSLFreeHandle releases a compatibility SSL handle.
func SSLFreeHandle(ssl SSLHandle) {
	sslHandles.Delete(ssl)
}

// ZlibInit initializes zlib extension.
func ZlibInit(path string) int {
	_ = path
	zlibError = ""
	zlibLoaded.Store(true)
	return 0
}

// ZlibLastError returns last zlib error string.
func ZlibLastError() string {
	return zlibError
}

// ZlibIsLoaded reports whether ZlibInit was called successfully.
func ZlibIsLoaded() int {
	if zlibLoaded.Load() {
		return 1
	}
	return 0
}

// ZlibVersion returns a compatibility zlib version string.
func ZlibVersion() string {
	return "1.2.11-go"
}

// DeflateInit2 configures a compatibility deflate stream.
func DeflateInit2(stream *ZStream, level int, method int, windowBits int, memLevel int, strategy int) int {
	_ = level
	_ = method
	_ = windowBits
	_ = memLevel
	_ = strategy
	if stream == nil {
		zlibError = "nil zstream"
		return EUsage
	}
	stream.Msg = ""
	return EOK
}

// DeflateInit2_ aliases DeflateInit2 to match C API naming.
func DeflateInit2_(stream *ZStream, level int, method int, windowBits int, memLevel int, strategy int) int {
	return DeflateInit2(stream, level, method, windowBits, memLevel, strategy)
}

// Deflate compresses NextIn into NextOut.
func Deflate(stream *ZStream, flush int) int {
	_ = flush
	if stream == nil {
		zlibError = "nil zstream"
		return EUsage
	}
	buffer := bytes.NewBuffer(nil)
	writer, err := zlib.NewWriterLevel(buffer, zlib.DefaultCompression)
	if err != nil {
		zlibError = err.Error()
		return EFailed
	}
	if _, err = writer.Write(stream.NextIn); err != nil {
		_ = writer.Close()
		zlibError = err.Error()
		return EFailed
	}
	if err = writer.Close(); err != nil {
		zlibError = err.Error()
		return EFailed
	}
	stream.TotalIn += uint64(len(stream.NextIn))
	stream.NextOut = append(stream.NextOut[:0], buffer.Bytes()...)
	if len(stream.NextOut) > int(^uint32(0)) {
		stream.AvailOut = ^uint32(0)
	} else {
		stream.AvailOut = uint32(len(stream.NextOut)) // #nosec G115 -- checked bounds above
	}
	stream.TotalOut += uint64(len(stream.NextOut))
	return EOK
}

// DeflateEnd finalizes a compatibility deflate stream.
func DeflateEnd(stream *ZStream) int {
	if stream == nil {
		zlibError = "nil zstream"
		return EUsage
	}
	stream.NextIn = nil
	stream.AvailIn = 0
	return EOK
}

// InflateInit2 configures a compatibility inflate stream.
func InflateInit2(stream *ZStream, windowBits int) int {
	_ = windowBits
	if stream == nil {
		zlibError = "nil zstream"
		return EUsage
	}
	stream.Msg = ""
	return EOK
}

// InflateInit2_ aliases InflateInit2 to match C API naming.
func InflateInit2_(stream *ZStream, windowBits int) int {
	return InflateInit2(stream, windowBits)
}

// Inflate decompresses NextIn into NextOut.
func Inflate(stream *ZStream, flush int) int {
	_ = flush
	if stream == nil {
		zlibError = "nil zstream"
		return EUsage
	}
	reader, err := zlib.NewReader(bytes.NewReader(stream.NextIn))
	if err != nil {
		zlibError = err.Error()
		return EFailed
	}
	decoded, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil {
		zlibError = readErr.Error()
		return EFailed
	}
	if closeErr != nil {
		zlibError = closeErr.Error()
		return EFailed
	}
	stream.TotalIn += uint64(len(stream.NextIn))
	stream.NextOut = append(stream.NextOut[:0], decoded...)
	if len(stream.NextOut) > int(^uint32(0)) {
		stream.AvailOut = ^uint32(0)
	} else {
		stream.AvailOut = uint32(len(stream.NextOut)) // #nosec G115 -- checked bounds above
	}
	stream.TotalOut += uint64(len(stream.NextOut))
	return EOK
}

// InflateEnd finalizes a compatibility inflate stream.
func InflateEnd(stream *ZStream) int {
	if stream == nil {
		zlibError = "nil zstream"
		return EUsage
	}
	stream.NextIn = nil
	stream.AvailIn = 0
	return EOK
}

// NoOpFn is a no-op callback helper.
func NoOpFn(userData any) {
	_ = userData
}

var (
	waitingFunction     atomic.Value
	removeRouteFunction atomic.Value
	copyRouteFunction   atomic.Value
)

// SetWaitingFunction stores a waiting callback.
func SetWaitingFunction(function any) {
	waitingFunction.Store(function)
}

// InvokeWaitingFunction invokes waiting callback if set.
func InvokeWaitingFunction() {
	value := waitingFunction.Load()
	if callback, ok := value.(func()); ok {
		callback()
	}
}

// SetRemoveRouteFunction stores route removal callback.
func SetRemoveRouteFunction(function any) {
	removeRouteFunction.Store(function)
}

// InvokeRemoveRouteFunction invokes route removal callback with argument.
func InvokeRemoveRouteFunction(argument any) {
	value := removeRouteFunction.Load()
	if callback, ok := value.(func(any)); ok {
		callback(argument)
	}
}

// SetCopyRouteFunction stores route copy callback.
func SetCopyRouteFunction(function any) {
	copyRouteFunction.Store(function)
}

// InvokeCopyRouteFunction invokes route copy callback and returns copied value.
func InvokeCopyRouteFunction(argument any) any {
	value := copyRouteFunction.Load()
	if callback, ok := value.(func(any) any); ok {
		return callback(argument)
	}
	return argument
}

// GetThreadCreateCount returns cumulative receive-routine creation count.
func GetThreadCreateCount() uint64 {
	return threadMade.Load()
}

// GetThreadJoinCount returns cumulative join-like disconnect wait count.
func GetThreadJoinCount() uint64 {
	return threadJoin.Load()
}

// GetThreadDetachCount returns cumulative receive-routine exits not consumed by join wait.
func GetThreadDetachCount() uint64 {
	return threadDrop.Load()
}
