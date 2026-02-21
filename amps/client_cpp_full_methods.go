package amps

import (
	"fmt"
	"strings"
	"time"
	"unsafe"
)

// ClientHandle represents an opaque client handle value.
type ClientHandle uintptr

// IsValid reports whether the client handle is valid.
func (client *Client) IsValid() bool {
	return client != nil
}

// GetHandle returns an opaque handle value for the client.
func (client *Client) GetHandle() ClientHandle {
	if client == nil {
		return 0
	}
	return ClientHandle(uintptr(unsafe.Pointer(client)))
}

// GetNameHash returns the current server-provided name hash.
func (client *Client) GetNameHash() string {
	if client == nil {
		return ""
	}
	if client.nameHash != "" {
		return client.nameHash
	}
	return fmt.Sprintf("%x", unsafeStringHash(client.clientName))
}

// GetNameHashValue returns the numeric hash value of GetNameHash.
func (client *Client) GetNameHashValue() uint64 {
	if client == nil {
		return 0
	}
	if client.nameHashValue != 0 {
		return client.nameHashValue
	}
	return unsafeStringHash(client.GetNameHash())
}

// GetName returns the configured client name.
func (client *Client) GetName() string {
	return client.Name()
}

// GetURI returns current URI value.
func (client *Client) GetURI() string {
	return client.URI()
}

// GetLogonCorrelationData returns logon correlation data.
func (client *Client) GetLogonCorrelationData() string {
	return client.LogonCorrelationData()
}

// GetServerVersionInfo returns parsed server version details.
func (client *Client) GetServerVersionInfo() VersionInfo {
	if client == nil {
		return VersionInfo{}
	}
	return ParseVersionInfo(client.serverVersion)
}

// GetServerVersion returns numeric server version representation.
func (client *Client) GetServerVersion() uint64 {
	if client == nil {
		return 0
	}
	return ConvertVersionToNumber(client.serverVersion)
}

// AddHttpPreflightHeader appends a preflight header.
func (client *Client) AddHttpPreflightHeader(header string) *Client {
	return client.AddHTTPPreflightHeader(header)
}

// AddHttpPreflightHeaderKV appends a key/value preflight header.
func (client *Client) AddHttpPreflightHeaderKV(key string, value string) *Client {
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key == "" {
		return client
	}
	return client.AddHTTPPreflightHeader(key + ": " + value)
}

// AddHTTPPreflightHeaderKV appends a key/value preflight header.
func (client *Client) AddHTTPPreflightHeaderKV(key string, value string) *Client {
	return client.AddHttpPreflightHeaderKV(key, value)
}

// SetHttpPreflightHeaders replaces preflight headers.
func (client *Client) SetHttpPreflightHeaders(headers []string) *Client {
	return client.SetHTTPPreflightHeaders(headers)
}

// ClearHttpPreflightHeaders clears preflight headers.
func (client *Client) ClearHttpPreflightHeaders() *Client {
	return client.ClearHTTPPreflightHeaders()
}

// AddMessageHandler adds a route-level message handler.
func (client *Client) AddMessageHandler(commandID string, messageHandler func(*Message) error, requestedAcks int, isSubscribe bool) error {
	if client == nil {
		return NewError(CommandError, "nil client")
	}
	if strings.TrimSpace(commandID) == "" {
		return NewError(CommandError, "missing command ID")
	}
	return client.addRoute(commandID, messageHandler, AckTypeNone, requestedAcks, isSubscribe, false)
}

// AddMessageHandlerForCommandType adds a route-level handler with explicit command semantics.
func (client *Client) AddMessageHandlerForCommandType(commandID string, messageHandler func(*Message) error, requestedAcks int, commandType int) error {
	isSubscribe := commandType == CommandSubscribe || commandType == CommandDeltaSubscribe || commandType == CommandSOWAndSubscribe || commandType == CommandSOWAndDeltaSubscribe
	return client.AddMessageHandler(commandID, messageHandler, requestedAcks, isSubscribe)
}

// RemoveMessageHandler removes a route-level message handler.
func (client *Client) RemoveMessageHandler(commandID string) bool {
	if client == nil {
		return false
	}
	_, exists := client.routes.Load(commandID)
	if exists {
		client.routes.Delete(commandID)
	}
	return exists
}

func messageToCommand(message *Message) *Command {
	if message == nil {
		return nil
	}
	commandValue, _ := message.Command()
	command := NewCommand(commandIntToString(commandValue))
	if command.header == nil {
		command.header = new(_Header)
	}
	command.header.command = commandValue
	if value, ok := message.AckType(); ok {
		command.header.ackType = &value
	}
	if value, ok := message.BatchSize(); ok {
		command.header.batchSize = &value
	}
	if value, ok := message.Bookmark(); ok {
		command.header.bookmark = []byte(value)
	}
	if value, ok := message.CommandID(); ok {
		command.header.commandID = []byte(value)
	}
	if value, ok := message.CorrelationID(); ok {
		command.header.correlationID = []byte(value)
	}
	if value, ok := message.Expiration(); ok {
		command.header.expiration = &value
	}
	if value, ok := message.Filter(); ok {
		command.header.filter = []byte(value)
	}
	if value, ok := message.Options(); ok {
		command.header.options = []byte(value)
	}
	if value, ok := message.OrderBy(); ok {
		command.header.orderBy = []byte(value)
	}
	if value, ok := message.QueryID(); ok {
		command.header.queryID = []byte(value)
	}
	if value, ok := message.SequenceID(); ok {
		command.header.sequenceID = &value
	}
	if value, ok := message.SowKey(); ok {
		command.header.sowKey = []byte(value)
	}
	if value, ok := message.SowKeys(); ok {
		command.header.sowKeys = []byte(value)
	}
	if value, ok := message.SubID(); ok {
		command.header.subID = []byte(value)
	}
	if value, ok := message.SubIDs(); ok {
		command.header.subIDs = []byte(value)
	}
	if value, ok := message.Topic(); ok {
		command.header.topic = []byte(value)
	}
	if value, ok := message.TopN(); ok {
		command.header.topN = &value
	}
	if payload := message.Data(); payload != nil {
		command.data = append([]byte(nil), payload...)
	}
	return command
}

// Send sends a raw message through the client send path.
func (client *Client) Send(message *Message) error {
	if client == nil {
		return NewError(CommandError, "nil client")
	}
	command := messageToCommand(message)
	if command == nil {
		return NewError(CommandError, "nil message")
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	return client.send(command)
}

// SendWithHandler sends a raw message and associates a callback handler.
func (client *Client) SendWithHandler(messageHandler func(*Message) error, message *Message, timeout ...int) (string, error) {
	if client == nil {
		return "", NewError(CommandError, "nil client")
	}
	command := messageToCommand(message)
	if command == nil {
		return "", NewError(CommandError, "nil message")
	}
	if len(timeout) > 0 && timeout[0] > 0 {
		command.SetTimeout(timeout[0])
	}
	return client.ExecuteAsync(command, messageHandler)
}

// AckDeferredAutoAck sends an ack for a deferred auto-ack entry.
func (client *Client) AckDeferredAutoAck(topic string, bookmark string, options ...string) error {
	if len(options) > 0 && strings.TrimSpace(options[0]) != "" {
		return client.Ack(topic, bookmark, options[0])
	}
	return client.Ack(topic, bookmark)
}

// PublishWithSequence publishes text payload and returns publish sequence when available.
func (client *Client) PublishWithSequence(topic string, data string, expiration ...uint) (uint64, error) {
	return client.PublishBytesWithSequence(topic, []byte(data), expiration...)
}

// PublishBytesWithSequence publishes payload and returns publish sequence when available.
func (client *Client) PublishBytesWithSequence(topic string, data []byte, expiration ...uint) (uint64, error) {
	return client.publishBytesWithCommand(CommandPublish, topic, data, expiration...)
}

// DeltaPublishWithSequence delta-publishes text payload and returns publish sequence when available.
func (client *Client) DeltaPublishWithSequence(topic string, data string, expiration ...uint) (uint64, error) {
	return client.DeltaPublishBytesWithSequence(topic, []byte(data), expiration...)
}

// DeltaPublishBytesWithSequence delta-publishes payload and returns publish sequence when available.
func (client *Client) DeltaPublishBytesWithSequence(topic string, data []byte, expiration ...uint) (uint64, error) {
	return client.publishBytesWithCommand(CommandDeltaPublish, topic, data, expiration...)
}

// SetPublishBatching configures publish command batching settings.
func (client *Client) SetPublishBatching(batchSizeBytes uint64, batchTimeoutMillis uint64) *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	state.publishBatchSizeBytes = batchSizeBytes
	state.publishBatchTimeout = time.Duration(batchTimeoutMillis) * time.Millisecond
	state.lock.Unlock()
	return client
}

// PublishBatching returns configured publish batching parameters.
func (client *Client) PublishBatching() (uint64, time.Duration) {
	state := ensureClientState(client)
	if state == nil {
		return 0, 0
	}
	state.lock.Lock()
	size := state.publishBatchSizeBytes
	timeout := state.publishBatchTimeout
	state.lock.Unlock()
	return size, timeout
}

// GetAutoAck returns the auto-ack setting.
func (client *Client) GetAutoAck() bool {
	return client.AutoAck()
}

// GetAckBatchSize returns the ack batch size setting.
func (client *Client) GetAckBatchSize() uint {
	return client.AckBatchSize()
}

// GetAckTimeout returns ack timeout in milliseconds.
func (client *Client) GetAckTimeout() int {
	return int(client.AckTimeout() / time.Millisecond)
}

// SetAckTimeoutMillis sets ack timeout using milliseconds.
func (client *Client) SetAckTimeoutMillis(timeout int) *Client {
	if timeout < 0 {
		timeout = 0
	}
	return client.SetAckTimeout(time.Duration(timeout) * time.Millisecond)
}

// GetRetryOnDisconnect returns retry-on-disconnect setting.
func (client *Client) GetRetryOnDisconnect() bool {
	return client.RetryOnDisconnect()
}

// GetDefaultMaxDepth returns default message stream max depth.
func (client *Client) GetDefaultMaxDepth() uint {
	return client.DefaultMaxDepth()
}

// GetBookmarkStore returns the configured bookmark store.
func (client *Client) GetBookmarkStore() BookmarkStore {
	return client.BookmarkStore()
}

// GetPublishStore returns the configured publish store.
func (client *Client) GetPublishStore() PublishStore {
	return client.PublishStore()
}

// GetSubscriptionManager returns the configured subscription manager.
func (client *Client) GetSubscriptionManager() SubscriptionManager {
	return client.SubscriptionManager()
}

// GetDuplicateMessageHandler returns duplicate handler callback.
func (client *Client) GetDuplicateMessageHandler() func(*Message) error {
	return client.DuplicateMessageHandler()
}

// GetFailedWriteHandler returns failed write handler.
func (client *Client) GetFailedWriteHandler() FailedWriteHandler {
	return client.FailedWriteHandler()
}

// GetExceptionListener returns exception listener.
func (client *Client) GetExceptionListener() ExceptionListener {
	return client.ExceptionListener()
}

// SetTransportFilterFunction aliases C++ transport filter naming.
func (client *Client) SetTransportFilterFunction(filter TransportFilter) *Client {
	return client.SetTransportFilter(filter)
}

// SetThreadCreatedCallback aliases C++ thread-created callback naming.
func (client *Client) SetThreadCreatedCallback(callback func()) *Client {
	return client.SetReceiveRoutineStartedCallback(callback)
}

// DeferredExecution queues a callback to execute after the next successful logon recovery.
func (client *Client) DeferredExecution(callback func(*Client, any), userData any) {
	state := ensureClientState(client)
	if state == nil || callback == nil {
		return
	}
	state.lock.Lock()
	state.deferredExecutions = append(state.deferredExecutions, deferredExecutionCall{
		callback: callback,
		userData: userData,
	})
	state.lock.Unlock()
}
