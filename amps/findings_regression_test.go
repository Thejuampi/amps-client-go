package amps

import (
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
