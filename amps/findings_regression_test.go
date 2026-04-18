package amps

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestClientConnectClearsStaleMessageTypeWhenURIHasNoPath(t *testing.T) {
	client := NewClient("stale-message-type")
	client.messageType = []byte("json")

	if err := client.Connect("tcp://127.0.0.1:1"); err == nil {
		t.Fatalf("expected connect error against unopened port")
	}
	if client.messageType != nil {
		t.Fatalf("expected bare URI connect to clear stale message type, got %q", string(client.messageType))
	}
}

func TestFileBookmarkStoreWithoutWALPersistsMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bookmark_store.json")
	options := FileStoreOptions{
		UseWAL:             false,
		SyncOnWrite:        false,
		CheckpointInterval: 1,
	}
	store := NewFileBookmarkStoreWithOptions(path, options)

	seqNo, err := store.LogWithError(bookmarkMessage("sub-no-wal", "10|1|"))
	if err != nil {
		t.Fatalf("LogWithError returned error: %v", err)
	}
	if seqNo == 0 {
		t.Fatalf("expected non-zero bookmark sequence")
	}

	reloaded := NewFileBookmarkStoreWithOptions(path, options)
	if got := reloaded.GetMostRecent("sub-no-wal"); got != "10|1|" {
		t.Fatalf("GetMostRecent() = %q, want %q", got, "10|1|")
	}
}

func TestFilePublishStoreWithoutWALPersistsMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "publish_store.json")
	options := FileStoreOptions{
		UseWAL:             false,
		SyncOnWrite:        false,
		CheckpointInterval: 1,
	}
	store := NewFilePublishStoreWithOptions(path, options)

	sequence, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":1}`)))
	if err != nil {
		t.Fatalf("Store returned error: %v", err)
	}
	if sequence == 0 {
		t.Fatalf("expected non-zero publish sequence")
	}

	reloaded := NewFilePublishStoreWithOptions(path, options)
	if got := reloaded.UnpersistedCount(); got != 1 {
		t.Fatalf("UnpersistedCount() = %d, want 1", got)
	}
}

func TestClientConnectDoesNotKeepConnectionWhenDialReturnsErrorWithConn(t *testing.T) {
	originalDial := clientNetDialContext
	defer func() {
		clientNetDialContext = originalDial
	}()

	conn := newTestConn()
	clientNetDialContext = func(ctx context.Context, network string, address string) (net.Conn, error) {
		_ = ctx
		_ = network
		_ = address
		return conn, errors.New("dial failed")
	}

	client := NewClient("connect-error-conn")
	if err := client.Connect("tcp://127.0.0.1:9007/amps/json"); err == nil {
		t.Fatalf("expected connect failure")
	}
	if client.connection != nil {
		t.Fatalf("expected failed connect to leave connection nil")
	}
	if !conn.closed {
		t.Fatalf("expected failed connect to close returned connection")
	}
}

func TestClientDisconnectConnectionStateListenerCanReenterClientLock(t *testing.T) {
	client := NewClient("disconnect-reentrant-listener")
	listenerRan := make(chan struct{}, 1)
	client.AddConnectionStateListener(ConnectionStateListenerFunc(func(state ConnectionState) {
		if state != ConnectionStateShutdown {
			return
		}
		client.lock.Lock()
		_ = client.connection
		client.lock.Unlock()
		listenerRan <- struct{}{}
	}))

	done := make(chan error, 1)
	go func() {
		done <- client.Disconnect()
	}()

	select {
	case <-listenerRan:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected connection state listener to reenter client lock without deadlock")
	}

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected Disconnect to return after listener reentry")
	}
}

func TestZeroValueFIXBuilderAppendUsesDefaultSeparator(t *testing.T) {
	var builder FixMessageBuilder
	done := make(chan error, 1)

	go func() {
		done <- builder.Append(8, "FIX.4.4")
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Append returned error: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("Append hung for zero-value FIX builder")
	}

	if got := string(builder.Bytes()); got != "8=FIX.4.4\x01" {
		t.Fatalf("Bytes() = %q, want %q", got, "8=FIX.4.4\x01")
	}
}

func TestZeroValueNVFIXBuilderAppendUsesDefaultSeparator(t *testing.T) {
	var builder NvfixMessageBuilder
	done := make(chan error, 1)

	go func() {
		done <- builder.AppendStrings("symbol", "AAPL")
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("AppendStrings returned error: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("AppendStrings hung for zero-value NVFIX builder")
	}

	if got := string(builder.Bytes()); got != "symbol=AAPL\x01" {
		t.Fatalf("Bytes() = %q, want %q", got, "symbol=AAPL\x01")
	}
}

func TestZeroValueMessageCopyPreservesUnknownCommand(t *testing.T) {
	var message Message
	var copied = message.Copy()

	if copied == nil || copied.GetCommandEnum() != CommandUnknown {
		t.Fatalf("Copy() zero value command = %v, want %d", copied.GetCommandEnum(), CommandUnknown)
	}
}

func TestZeroValueMessageGetCommandEnumReturnsUnknown(t *testing.T) {
	var message Message

	if got := message.GetCommandEnum(); got != CommandUnknown {
		t.Fatalf("GetCommandEnum() = %d, want %d", got, CommandUnknown)
	}
}

func TestZeroValueMessageTopicReturnsEmpty(t *testing.T) {
	var message Message
	var topic, ok = message.Topic()

	if ok || topic != "" {
		t.Fatalf("Topic() = (%q, %v), want (%q, false)", topic, ok, "")
	}
}

func TestZeroValueMessageSetCommandEnumInitializesHeader(t *testing.T) {
	var message Message
	message.SetCommandEnum(CommandPublish)

	if got := message.GetCommandEnum(); got != CommandPublish {
		t.Fatalf("GetCommandEnum() = %d, want %d", got, CommandPublish)
	}
}

func TestZeroValueMessageSetAckTypeEnumInitializesHeader(t *testing.T) {
	var message Message
	message.SetAckTypeEnum(AckTypeProcessed)

	if got := message.GetAckTypeEnum(); got != AckTypeProcessed {
		t.Fatalf("GetAckTypeEnum() = %d, want %d", got, AckTypeProcessed)
	}
}

func TestNilMessageCopyReturnsNil(t *testing.T) {
	var message *Message

	if message.Copy() != nil {
		t.Fatalf("Copy() on nil message should return nil")
	}
}

func TestZeroValueMessageCopyPreservesDataWithoutHeader(t *testing.T) {
	var message Message
	message.SetData([]byte("payload"))
	var copied = message.Copy()

	if string(copied.Data()) != "payload" {
		t.Fatalf("Copy().Data() = %q, want %q", string(copied.Data()), "payload")
	}
}

func TestZeroValueMessageHeaderAccessorsReturnZeroValues(t *testing.T) {
	var message Message
	var bookmark, hasBookmark = message.Bookmark()
	if hasBookmark || bookmark != "" {
		t.Fatalf("Bookmark() = (%q, %v), want (%q, false)", bookmark, hasBookmark, "")
	}
	var command, hasCommand = message.Command()
	if hasCommand || command != CommandUnknown {
		t.Fatalf("Command() = (%d, %v), want (%d, false)", command, hasCommand, CommandUnknown)
	}
	var commandID, hasCommandID = message.CommandID()
	if hasCommandID || commandID != "" {
		t.Fatalf("CommandID() = (%q, %v), want (%q, false)", commandID, hasCommandID, "")
	}
	var correlationID, hasCorrelationID = message.CorrelationID()
	if hasCorrelationID || correlationID != "" {
		t.Fatalf("CorrelationID() = (%q, %v), want (%q, false)", correlationID, hasCorrelationID, "")
	}
	var filter, hasFilter = message.Filter()
	if hasFilter || filter != "" {
		t.Fatalf("Filter() = (%q, %v), want (%q, false)", filter, hasFilter, "")
	}
	var leasePeriod, hasLeasePeriod = message.LeasePeriod()
	if hasLeasePeriod || leasePeriod != "" {
		t.Fatalf("LeasePeriod() = (%q, %v), want (%q, false)", leasePeriod, hasLeasePeriod, "")
	}
	var options, hasOptions = message.Options()
	if hasOptions || options != "" {
		t.Fatalf("Options() = (%q, %v), want (%q, false)", options, hasOptions, "")
	}
	var orderBy, hasOrderBy = message.OrderBy()
	if hasOrderBy || orderBy != "" {
		t.Fatalf("OrderBy() = (%q, %v), want (%q, false)", orderBy, hasOrderBy, "")
	}
	var queryID, hasQueryID = message.QueryID()
	if hasQueryID || queryID != "" {
		t.Fatalf("QueryID() = (%q, %v), want (%q, false)", queryID, hasQueryID, "")
	}
	var reason, hasReason = message.Reason()
	if hasReason || reason != "" {
		t.Fatalf("Reason() = (%q, %v), want (%q, false)", reason, hasReason, "")
	}
	var sowKey, hasSowKey = message.SowKey()
	if hasSowKey || sowKey != "" {
		t.Fatalf("SowKey() = (%q, %v), want (%q, false)", sowKey, hasSowKey, "")
	}
	var sowKeys, hasSowKeys = message.SowKeys()
	if hasSowKeys || sowKeys != "" {
		t.Fatalf("SowKeys() = (%q, %v), want (%q, false)", sowKeys, hasSowKeys, "")
	}
	var status, hasStatus = message.Status()
	if hasStatus || status != "" {
		t.Fatalf("Status() = (%q, %v), want (%q, false)", status, hasStatus, "")
	}
	var subID, hasSubID = message.SubID()
	if hasSubID || subID != "" {
		t.Fatalf("SubID() = (%q, %v), want (%q, false)", subID, hasSubID, "")
	}
	var subIDs, hasSubIDs = message.SubIDs()
	if hasSubIDs || subIDs != "" {
		t.Fatalf("SubIDs() = (%q, %v), want (%q, false)", subIDs, hasSubIDs, "")
	}
	var timestamp, hasTimestamp = message.Timestamp()
	if hasTimestamp || timestamp != "" {
		t.Fatalf("Timestamp() = (%q, %v), want (%q, false)", timestamp, hasTimestamp, "")
	}
	var userID, hasUserID = message.UserID()
	if hasUserID || userID != "" {
		t.Fatalf("UserID() = (%q, %v), want (%q, false)", userID, hasUserID, "")
	}
}

func TestZeroValueCommandTopicReturnsEmpty(t *testing.T) {
	var command Command
	var topic, ok = command.Topic()

	if ok || topic != "" {
		t.Fatalf("Topic() = (%q, %v), want (%q, false)", topic, ok, "")
	}
}

func TestZeroValueCommandSetTopicInitializesHeader(t *testing.T) {
	var command Command
	command.SetTopic("orders")

	if got, ok := command.Topic(); !ok || got != "orders" {
		t.Fatalf("Topic() = (%q, %v), want (%q, true)", got, ok, "orders")
	}
}

func TestZeroValueCommandSetAckTypeInitializesHeader(t *testing.T) {
	var command Command
	command.SetAckType(AckTypeProcessed)

	if got := command.GetAckType(); got != AckTypeProcessed {
		t.Fatalf("GetAckType() = %d, want %d", got, AckTypeProcessed)
	}
}

func TestZeroValueCommandHeaderAccessorsReturnZeroValues(t *testing.T) {
	var command Command
	var bookmark, hasBookmark = command.Bookmark()
	if hasBookmark || bookmark != "" {
		t.Fatalf("Bookmark() = (%q, %v), want (%q, false)", bookmark, hasBookmark, "")
	}
	var commandName, hasCommand = command.Command()
	if hasCommand || commandName != "" {
		t.Fatalf("Command() = (%q, %v), want (%q, false)", commandName, hasCommand, "")
	}
	var commandID, hasCommandID = command.CommandID()
	if hasCommandID || commandID != "" {
		t.Fatalf("CommandID() = (%q, %v), want (%q, false)", commandID, hasCommandID, "")
	}
	var correlationID, hasCorrelationID = command.CorrelationID()
	if hasCorrelationID || correlationID != "" {
		t.Fatalf("CorrelationID() = (%q, %v), want (%q, false)", correlationID, hasCorrelationID, "")
	}
	var filter, hasFilter = command.Filter()
	if hasFilter || filter != "" {
		t.Fatalf("Filter() = (%q, %v), want (%q, false)", filter, hasFilter, "")
	}
	var options, hasOptions = command.Options()
	if hasOptions || options != "" {
		t.Fatalf("Options() = (%q, %v), want (%q, false)", options, hasOptions, "")
	}
	var orderBy, hasOrderBy = command.OrderBy()
	if hasOrderBy || orderBy != "" {
		t.Fatalf("OrderBy() = (%q, %v), want (%q, false)", orderBy, hasOrderBy, "")
	}
	var queryID, hasQueryID = command.QueryID()
	if hasQueryID || queryID != "" {
		t.Fatalf("QueryID() = (%q, %v), want (%q, false)", queryID, hasQueryID, "")
	}
	var sowKey, hasSowKey = command.SowKey()
	if hasSowKey || sowKey != "" {
		t.Fatalf("SowKey() = (%q, %v), want (%q, false)", sowKey, hasSowKey, "")
	}
	var sowKeys, hasSowKeys = command.SowKeys()
	if hasSowKeys || sowKeys != "" {
		t.Fatalf("SowKeys() = (%q, %v), want (%q, false)", sowKeys, hasSowKeys, "")
	}
	var subID, hasSubID = command.SubID()
	if hasSubID || subID != "" {
		t.Fatalf("SubID() = (%q, %v), want (%q, false)", subID, hasSubID, "")
	}
	var subIDs, hasSubIDs = command.SubIDs()
	if hasSubIDs || subIDs != "" {
		t.Fatalf("SubIDs() = (%q, %v), want (%q, false)", subIDs, hasSubIDs, "")
	}
	var topic, hasTopic = command.Topic()
	if hasTopic || topic != "" {
		t.Fatalf("Topic() = (%q, %v), want (%q, false)", topic, hasTopic, "")
	}
}

func TestNilCommandHeaderAccessorReturnsZeroValue(t *testing.T) {
	var command *Command
	var topic, hasTopic = command.Topic()

	if hasTopic || topic != "" {
		t.Fatalf("Topic() on nil command = (%q, %v), want (%q, false)", topic, hasTopic, "")
	}
}

func TestNilCommandSettersReturnNil(t *testing.T) {
	var command *Command

	if command.SetTimeout(1) != nil ||
		command.Init("publish") != nil ||
		command.Reset() != nil ||
		command.SetIds("cid", "qid", "sid") != nil ||
		command.SetAckType(AckTypeProcessed) != nil ||
		command.AddAckType(AckTypeProcessed) != nil ||
		command.SetBatchSize(1) != nil ||
		command.SetBookmark("1|1|") != nil ||
		command.SetCommand("publish") != nil ||
		command.SetCommandEnum(CommandPublish) != nil ||
		command.SetCommandID("cid") != nil ||
		command.SetCorrelationID("corr") != nil ||
		command.SetData([]byte("payload")) != nil ||
		command.SetExpiration(1) != nil ||
		command.SetFilter("/id > 0") != nil ||
		command.SetOptions("replace") != nil ||
		command.SetOrderBy("/id") != nil ||
		command.SetQueryID("qid") != nil ||
		command.SetSequenceID(1) != nil ||
		command.SetSowKey("k1") != nil ||
		command.SetSowKeys("k1,k2") != nil ||
		command.SetSubID("sub") != nil ||
		command.SetSubIDs("sub,sub2") != nil ||
		command.SetLeasePeriod(1) != nil ||
		command.SetGroupSequenceNumber(1) != nil ||
		command.SetDataOnly(true) != nil ||
		command.SetSendEmpty(true) != nil ||
		command.SetSkipN(1) != nil ||
		command.SetMessageType("json") != nil ||
		command.SetTopN(1) != nil ||
		command.SetTopic("orders") != nil {
		t.Fatalf("expected nil command setters to return nil")
	}
}

func TestNilCommandResetNoop(t *testing.T) {
	var command *Command
	command.reset()
}

func TestZeroValueCommandGetMessagePreservesUnknownCommand(t *testing.T) {
	var command Command
	var message = command.GetMessage()

	if message == nil || message.GetCommandEnum() != CommandUnknown {
		t.Fatalf("GetMessage() zero value command = %v, want %d", message.GetCommandEnum(), CommandUnknown)
	}
}

func TestZeroValueCommandSetMessageTypeInitializesHeader(t *testing.T) {
	var command Command
	command.SetMessageType("json").SetTopic("orders")

	if command.header == nil || string(command.header.messageType) != "json" {
		t.Fatalf("expected zero-value SetMessageType to initialize header, got %+v", command.header)
	}
	if topic, ok := command.Topic(); !ok || topic != "orders" {
		t.Fatalf("expected chained SetTopic to keep working, got (%q, %v)", topic, ok)
	}
}

func TestNilCommandCloneReturnsNil(t *testing.T) {
	var command *Command

	if command.Clone() != nil {
		t.Fatalf("expected nil command Clone to return nil")
	}
}

func TestCommandCloneDeepCopiesExtendedFields(t *testing.T) {
	command := NewCommand("publish").
		SetTopic("orders").
		SetCommandID("cmd-1").
		SetCorrelationID("corr-1").
		SetData([]byte("payload")).
		SetMessageType("json").
		SetLeasePeriod(7).
		SetGroupSequenceNumber(11).
		SetDataOnly(true).
		SetSendEmpty(true).
		SetSkipN(3)

	cloned := command.Clone()
	if cloned == nil {
		t.Fatalf("expected Clone to return a command")
	}
	if cloned == command {
		t.Fatalf("expected Clone to return a distinct command instance")
	}
	if string(cloned.header.topic) != "orders" || string(cloned.header.commandID) != "cmd-1" {
		t.Fatalf("expected Clone to preserve command identity fields")
	}
	if string(cloned.header.messageType) != "json" || string(cloned.header.leasePeriod) != "7" {
		t.Fatalf("expected Clone to preserve extended header fields")
	}
	if cloned.header.groupSequenceNumber == nil || *cloned.header.groupSequenceNumber != 11 {
		t.Fatalf("expected Clone to preserve group sequence number")
	}
	if string(cloned.header.dataOnly) != "true" || string(cloned.header.sendEmpty) != "true" {
		t.Fatalf("expected Clone to preserve boolean header flags")
	}
	if cloned.header.skipN == nil || *cloned.header.skipN != 3 {
		t.Fatalf("expected Clone to preserve skip_n")
	}

	command.header.topic[0] = 'x'
	command.header.dataOnly = nil
	command.data[0] = 'X'

	if string(cloned.header.topic) != "orders" {
		t.Fatalf("expected Clone to deep copy header bytes, got %q", string(cloned.header.topic))
	}
	if string(cloned.Data()) != "payload" {
		t.Fatalf("expected Clone to deep copy payload bytes, got %q", string(cloned.Data()))
	}
	if string(cloned.header.dataOnly) != "true" {
		t.Fatalf("expected Clone to deep copy flag bytes")
	}
}

func TestCommandGetMessagePreservesExtendedFields(t *testing.T) {
	command := NewCommand("publish").
		SetTopic("orders").
		SetMessageType("json").
		SetLeasePeriod(7).
		SetGroupSequenceNumber(11).
		SetDataOnly(true).
		SetSendEmpty(true).
		SetSkipN(3)

	message := command.GetMessage()
	if message == nil {
		t.Fatalf("expected GetMessage to return a message")
	}
	if string(message.header.messageType) != "json" {
		t.Fatalf("expected GetMessage to preserve message type, got %q", string(message.header.messageType))
	}
	if leasePeriod, ok := message.LeasePeriodUint(); !ok || leasePeriod != 7 {
		t.Fatalf("expected GetMessage to preserve lease period, got (%d, %v)", leasePeriod, ok)
	}
	if groupSeq, ok := message.GroupSequenceNumber(); !ok || groupSeq != 11 {
		t.Fatalf("expected GetMessage to preserve group sequence number, got (%d, %v)", groupSeq, ok)
	}
	if dataOnly, ok := message.DataOnly(); !ok || !dataOnly {
		t.Fatalf("expected GetMessage to preserve data_only, got (%v, %v)", dataOnly, ok)
	}
	if sendEmpty, ok := message.SendEmpty(); !ok || !sendEmpty {
		t.Fatalf("expected GetMessage to preserve send_empty, got (%v, %v)", sendEmpty, ok)
	}
	if skipN, ok := message.SkipN(); !ok || skipN != 3 {
		t.Fatalf("expected GetMessage to preserve skip_n, got (%d, %v)", skipN, ok)
	}
}

func TestNilMessageExtendedAccessorsReturnZeroValues(t *testing.T) {
	var message *Message

	if dataOnly, ok := message.DataOnly(); ok || dataOnly {
		t.Fatalf("expected nil message DataOnly to return (false, false), got (%v, %v)", dataOnly, ok)
	}
	if sendEmpty, ok := message.SendEmpty(); ok || sendEmpty {
		t.Fatalf("expected nil message SendEmpty to return (false, false), got (%v, %v)", sendEmpty, ok)
	}
	if sendKeys, ok := message.SendKeys(); ok || sendKeys {
		t.Fatalf("expected nil message SendKeys to return (false, false), got (%v, %v)", sendKeys, ok)
	}
	if sendOOF, ok := message.SendOOF(); ok || sendOOF {
		t.Fatalf("expected nil message SendOOF to return (false, false), got (%v, %v)", sendOOF, ok)
	}
	if skipN, ok := message.SkipN(); ok || skipN != 0 {
		t.Fatalf("expected nil message SkipN to return (0, false), got (%d, %v)", skipN, ok)
	}
	if maximumMessages, ok := message.MaximumMessages(); ok || maximumMessages != 0 {
		t.Fatalf("expected nil message MaximumMessages to return (0, false), got (%d, %v)", maximumMessages, ok)
	}
	if timeoutInterval, ok := message.TimeoutInterval(); ok || timeoutInterval != 0 {
		t.Fatalf("expected nil message TimeoutInterval to return (0, false), got (%d, %v)", timeoutInterval, ok)
	}
	if gracePeriod, ok := message.GracePeriod(); ok || gracePeriod != 0 {
		t.Fatalf("expected nil message GracePeriod to return (0, false), got (%d, %v)", gracePeriod, ok)
	}
	if leasePeriod, ok := message.LeasePeriodUint(); ok || leasePeriod != 0 {
		t.Fatalf("expected nil message LeasePeriodUint to return (0, false), got (%d, %v)", leasePeriod, ok)
	}
}

func TestMessageCopyPreservesExtendedFields(t *testing.T) {
	original := &Message{
		header: &_Header{
			command:             CommandPublish,
			commandID:           []byte("cmd-1"),
			topic:               []byte("orders"),
			subID:               []byte("sub-1"),
			queryID:             []byte("qry-1"),
			options:             []byte("replace"),
			filter:              []byte("/id > 10"),
			messageType:         []byte("json"),
			leasePeriod:         []byte("7"),
			dataOnly:            []byte("true"),
			sendEmpty:           []byte("true"),
			sendKeys:            []byte("true"),
			sendOOF:             []byte("true"),
			groupSequenceNumber: func() *uint { value := uint(11); return &value }(),
			skipN:               func() *uint { value := uint(3); return &value }(),
			maximumMessages:     func() *uint { value := uint(5); return &value }(),
			timeoutInterval:     func() *uint { value := uint(13); return &value }(),
			gracePeriod:         func() *uint { value := uint(2); return &value }(),
		},
		data: []byte(`{"id":1}`),
	}

	copied := original.Copy()
	if copied == nil {
		t.Fatalf("expected Copy to return a message")
	}
	if copied == original {
		t.Fatalf("expected Copy to return a distinct message instance")
	}
	if copied.header == nil || string(copied.header.messageType) != "json" {
		t.Fatalf("expected Copy to preserve message type, got %q", string(copied.header.messageType))
	}
	if leasePeriod, ok := copied.LeasePeriodUint(); !ok || leasePeriod != 7 {
		t.Fatalf("expected Copy to preserve lease period, got (%d, %v)", leasePeriod, ok)
	}
	if dataOnly, ok := copied.DataOnly(); !ok || !dataOnly {
		t.Fatalf("expected Copy to preserve data_only, got (%v, %v)", dataOnly, ok)
	}
	if sendEmpty, ok := copied.SendEmpty(); !ok || !sendEmpty {
		t.Fatalf("expected Copy to preserve send_empty, got (%v, %v)", sendEmpty, ok)
	}
	if sendKeys, ok := copied.SendKeys(); !ok || !sendKeys {
		t.Fatalf("expected Copy to preserve send_keys, got (%v, %v)", sendKeys, ok)
	}
	if sendOOF, ok := copied.SendOOF(); !ok || !sendOOF {
		t.Fatalf("expected Copy to preserve send_oof, got (%v, %v)", sendOOF, ok)
	}
	if groupSeq, ok := copied.GroupSequenceNumber(); !ok || groupSeq != 11 {
		t.Fatalf("expected Copy to preserve group sequence number, got (%d, %v)", groupSeq, ok)
	}
	if skipN, ok := copied.SkipN(); !ok || skipN != 3 {
		t.Fatalf("expected Copy to preserve skip_n, got (%d, %v)", skipN, ok)
	}
	if maximumMessages, ok := copied.MaximumMessages(); !ok || maximumMessages != 5 {
		t.Fatalf("expected Copy to preserve maximum_messages, got (%d, %v)", maximumMessages, ok)
	}
	if timeoutInterval, ok := copied.TimeoutInterval(); !ok || timeoutInterval != 13 {
		t.Fatalf("expected Copy to preserve timeout_interval, got (%d, %v)", timeoutInterval, ok)
	}
	if gracePeriod, ok := copied.GracePeriod(); !ok || gracePeriod != 2 {
		t.Fatalf("expected Copy to preserve grace_period, got (%d, %v)", gracePeriod, ok)
	}
}

func TestMessageCopyAllocationsRemainLow(t *testing.T) {
	original := &Message{
		header: &_Header{
			command:     CommandPublish,
			commandID:   []byte("cmd-1"),
			topic:       []byte("orders"),
			subID:       []byte("sub-1"),
			queryID:     []byte("qry-1"),
			options:     []byte("replace"),
			filter:      []byte("/id > 10"),
			messageType: []byte("json"),
			leasePeriod: []byte("7"),
			dataOnly:    []byte("true"),
			sendEmpty:   []byte("true"),
			sendKeys:    []byte("true"),
			sendOOF:     []byte("true"),
		},
		data: []byte(`{"id":1,"name":"test","value":123.45,"active":true}`),
	}

	allocs := testing.AllocsPerRun(1000, func() {
		_ = original.Copy()
	})
	if allocs > 2 {
		t.Fatalf("Message.Copy allocations = %v, want <= 2", allocs)
	}
}
