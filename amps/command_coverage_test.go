package amps

import (
	"strings"
	"testing"
)

func optionTokenSet(options string) map[string]string {
	tokens := strings.Split(options, ",")
	values := make(map[string]string, len(tokens))
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		if key, value, found := strings.Cut(token, "="); found {
			values[key] = value
			continue
		}
		values[token] = ""
	}
	return values
}

func TestCommandMappingsCoverage(t *testing.T) {
	cases := map[string]int{
		"ack":                     CommandAck,
		"delta_publish":           CommandDeltaPublish,
		"delta_subscribe":         CommandDeltaSubscribe,
		"flush":                   CommandFlush,
		"group_begin":             CommandGroupBegin,
		"group_end":               CommandGroupEnd,
		"heartbeat":               commandHeartbeat,
		"logon":                   commandLogon,
		"oof":                     CommandOOF,
		"p":                       CommandPublish,
		"publish":                 CommandPublish,
		"sow":                     CommandSOW,
		"sow_and_delta_subscribe": CommandSOWAndDeltaSubscribe,
		"sow_and_subscribe":       CommandSOWAndSubscribe,
		"sow_delete":              CommandSOWDelete,
		"start_timer":             commandStartTimer,
		"stop_timer":              commandStopTimer,
		"subscribe":               CommandSubscribe,
		"unsubscribe":             CommandUnsubscribe,
	}

	for name, expected := range cases {
		if got := commandStringToInt(name); got != expected {
			t.Fatalf("commandStringToInt(%q)=%d want %d", name, got, expected)
		}
		if got := commandBytesToInt([]byte(name)); got != expected {
			t.Fatalf("commandBytesToInt(%q)=%d want %d", name, got, expected)
		}
	}

	if got := commandStringToInt("unknown"); got != CommandUnknown {
		t.Fatalf("unexpected unknown mapping: %d", got)
	}
	if got := commandBytesToInt([]byte("unknown")); got != CommandUnknown {
		t.Fatalf("unexpected unknown bytes mapping: %d", got)
	}

	if !bytesEqualString([]byte("abc"), "abc") {
		t.Fatalf("expected bytesEqualString true")
	}
	if bytesEqualString([]byte("ab"), "abc") {
		t.Fatalf("expected bytesEqualString false for length mismatch")
	}
	if bytesEqualString([]byte("abd"), "abc") {
		t.Fatalf("expected bytesEqualString false for content mismatch")
	}

	if got := commandIntToString(CommandUnknown); got != "" {
		t.Fatalf("expected empty unknown command string, got %q", got)
	}
	if got := commandIntToString(CommandPublish); got != "p" {
		t.Fatalf("expected publish command string \"p\", got %q", got)
	}

	intToStringCases := map[int]string{
		CommandAck:                  "ack",
		CommandDeltaPublish:         "delta_publish",
		CommandDeltaSubscribe:       "delta_subscribe",
		CommandFlush:                "flush",
		CommandGroupBegin:           "group_begin",
		CommandGroupEnd:             "group_end",
		commandHeartbeat:            "heartbeat",
		commandLogon:                "logon",
		CommandOOF:                  "oof",
		CommandSOW:                  "sow",
		CommandSOWAndDeltaSubscribe: "sow_and_delta_subscribe",
		CommandSOWAndSubscribe:      "sow_and_subscribe",
		CommandSOWDelete:            "sow_delete",
		commandStartTimer:           "start_timer",
		commandStopTimer:            "stop_timer",
		CommandSubscribe:            "subscribe",
		CommandUnsubscribe:          "unsubscribe",
		-999:                        "",
	}
	for command, expected := range intToStringCases {
		if got := commandIntToString(command); got != expected {
			t.Fatalf("commandIntToString(%d)=%q want %q", command, got, expected)
		}
	}

	if got := commandBytesToInt([]byte("zzzzz")); got != CommandUnknown {
		t.Fatalf("unexpected fallback bytes mapping: %d", got)
	}
	if got := commandBytesToInt([]byte("x")); got != CommandUnknown {
		t.Fatalf("unexpected one-byte unknown mapping: %d", got)
	}
	if got := commandBytesToInt([]byte("xyz")); got != CommandUnknown {
		t.Fatalf("unexpected three-byte unknown mapping: %d", got)
	}
	if got := commandBytesToInt([]byte{}); got != CommandUnknown {
		t.Fatalf("unexpected empty-byte mapping: %d", got)
	}
}

func TestCommandSettersAndGettersCoverage(t *testing.T) {
	command := NewCommand("publish")
	if command == nil {
		t.Fatalf("expected command")
	}

	command.SetCommandID("cid").
		SetCorrelationID("corr").
		SetTopic("orders").
		SetBookmark("1|1|").
		SetFilter("/id > 10").
		SetOptions("replace").
		SetOrderBy("/id desc").
		SetQueryID("qid").
		SetSowKey("k1").
		SetSowKeys("k1,k2").
		SetSubID("sub-1").
		SetSubIDs("sub-1,sub-2").
		SetData([]byte("payload")).
		SetTopN(15).
		SetBatchSize(20).
		SetExpiration(25).
		SetSequenceID(42).
		SetTimeout(1234).
		SetAckType(AckTypeProcessed | AckTypeStats)

	command.SetDataOnly(true).SetSendEmpty(true).SetReplace()
	message := command.GetMessage()
	if dataOnly, ok := message.DataOnly(); !ok || !dataOnly {
		t.Fatalf("unexpected data_only after SetDataOnly(true): %v ok=%v", dataOnly, ok)
	}
	if sendEmpty, ok := message.SendEmpty(); !ok || !sendEmpty {
		t.Fatalf("unexpected send_empty after SetSendEmpty(true): %v ok=%v", sendEmpty, ok)
	}
	if value, ok := command.Options(); !ok || value != OptionReplace {
		t.Fatalf("unexpected options after SetReplace: %q ok=%v", value, ok)
	}

	command.SetDataOnly(false).SetSendEmpty(false)
	message = command.GetMessage()
	if dataOnly, ok := message.DataOnly(); ok || dataOnly {
		t.Fatalf("expected data_only to clear after SetDataOnly(false), got %v ok=%v", dataOnly, ok)
	}
	if sendEmpty, ok := message.SendEmpty(); ok || sendEmpty {
		t.Fatalf("expected send_empty to clear after SetSendEmpty(false), got %v ok=%v", sendEmpty, ok)
	}

	if got := command.GetAckType(); got != (AckTypeProcessed | AckTypeStats) {
		t.Fatalf("unexpected ack type: %d", got)
	}
	if got := command.GetAckTypeEnum(); got != (AckTypeProcessed | AckTypeStats) {
		t.Fatalf("unexpected ack type enum: %d", got)
	}
	if !command.HasProcessedAck() || !command.HasStatsAck() {
		t.Fatalf("expected processed+stats flags")
	}
	if got := command.GetCommandEnum(); got != CommandPublish {
		t.Fatalf("unexpected command enum: %d", got)
	}
	if got := command.GetTimeout(); got != 1234 {
		t.Fatalf("unexpected timeout: %d", got)
	}
	if got := command.GetSequence(); got != 42 {
		t.Fatalf("unexpected sequence: %d", got)
	}

	if value, ok := command.BatchSize(); !ok || value != 20 {
		t.Fatalf("unexpected batch size: %d ok=%v", value, ok)
	}
	if value, ok := command.Bookmark(); !ok || value != "1|1|" {
		t.Fatalf("unexpected bookmark: %q ok=%v", value, ok)
	}
	if value, ok := command.Command(); !ok || value != "p" {
		t.Fatalf("unexpected command: %q ok=%v", value, ok)
	}
	if value, ok := command.CommandID(); !ok || value != "cid" {
		t.Fatalf("unexpected command id: %q ok=%v", value, ok)
	}
	if value, ok := command.CorrelationID(); !ok || value != "corr" {
		t.Fatalf("unexpected correlation id: %q ok=%v", value, ok)
	}
	if string(command.Data()) != "payload" {
		t.Fatalf("unexpected data")
	}
	if value, ok := command.Expiration(); !ok || value != 25 {
		t.Fatalf("unexpected expiration: %d ok=%v", value, ok)
	}
	if value, ok := command.Filter(); !ok || value != "/id > 10" {
		t.Fatalf("unexpected filter: %q ok=%v", value, ok)
	}
	if value, ok := command.Options(); !ok || value != "replace" {
		t.Fatalf("unexpected options: %q ok=%v", value, ok)
	}

	messageCommand := cloneCommand(command)
	messageCommand.SetSubIDs("s1,s2")
	message = messageCommand.GetMessage()
	if message == nil {
		t.Fatalf("expected command message conversion")
	}
	if ack, ok := message.AckType(); !ok || ack != (AckTypeProcessed|AckTypeStats) {
		t.Fatalf("unexpected converted ack type: %d ok=%v", ack, ok)
	}
	if batchSize, ok := message.BatchSize(); !ok || batchSize != 20 {
		t.Fatalf("unexpected converted batch size: %d ok=%v", batchSize, ok)
	}
	if filter, ok := message.Filter(); !ok || filter != "/id > 10" {
		t.Fatalf("unexpected converted filter: %q ok=%v", filter, ok)
	}
	if options, ok := message.Options(); !ok || options != "replace" {
		t.Fatalf("unexpected converted options: %q ok=%v", options, ok)
	}
	if queryID, ok := message.QueryID(); !ok || queryID != "qid" {
		t.Fatalf("unexpected converted query id: %q ok=%v", queryID, ok)
	}
	if subIDs, ok := message.SubIDs(); !ok || subIDs != "s1,s2" {
		t.Fatalf("unexpected converted sub ids: %q ok=%v", subIDs, ok)
	}
	if topN, ok := message.TopN(); !ok || topN != 15 {
		t.Fatalf("unexpected converted top_n: %d ok=%v", topN, ok)
	}
	if value, ok := command.OrderBy(); !ok || value != "/id desc" {
		t.Fatalf("unexpected order by: %q ok=%v", value, ok)
	}
	if value, ok := command.QueryID(); !ok || value != "qid" {
		t.Fatalf("unexpected query id: %q ok=%v", value, ok)
	}
	if value, ok := command.SequenceID(); !ok || value != 42 {
		t.Fatalf("unexpected sequence id: %d ok=%v", value, ok)
	}
	if value, ok := command.SowKey(); !ok || value != "k1" {
		t.Fatalf("unexpected sow key: %q ok=%v", value, ok)
	}
	if value, ok := command.SowKeys(); !ok || value != "k1,k2" {
		t.Fatalf("unexpected sow keys: %q ok=%v", value, ok)
	}
	if value, ok := command.SubID(); !ok || value != "sub-1" {
		t.Fatalf("unexpected sub id: %q ok=%v", value, ok)
	}
	if value, ok := command.SubIDs(); !ok || value != "sub-1,sub-2" {
		t.Fatalf("unexpected sub ids: %q ok=%v", value, ok)
	}
	if value, ok := command.TopN(); !ok || value != 15 {
		t.Fatalf("unexpected top_n: %d ok=%v", value, ok)
	}
	if value, ok := command.Topic(); !ok || value != "orders" {
		t.Fatalf("unexpected topic: %q ok=%v", value, ok)
	}

	if message := command.GetMessage(); message == nil {
		t.Fatalf("expected command message")
	}

	command.SetTimeout(-5)
	if got := command.GetTimeout(); got != 0 {
		t.Fatalf("negative timeout should clamp to zero, got %d", got)
	}

	command.SetAckType(-1)
	if _, ok := command.AckType(); ok {
		t.Fatalf("invalid ack type should clear value")
	}
	command.AddAckType(AckTypeProcessed)
	command.AddAckType(AckTypeStats)
	command.AddAckType(AckTypeNone)
	if got := command.GetAckType(); got != (AckTypeProcessed | AckTypeStats) {
		t.Fatalf("unexpected additive ack type: %d", got)
	}

	command.SetBatchSize(0).
		SetBookmark("").
		SetCommand("").
		SetCommandID("").
		SetCorrelationID("").
		SetExpiration(0).
		SetFilter("").
		SetOptions("").
		SetOrderBy("").
		SetQueryID("").
		SetSequenceID(0).
		SetSowKey("").
		SetSowKeys("").
		SetSubID("").
		SetSubIDs("").
		SetTopN(0).
		SetTopic("")

	if _, ok := command.BatchSize(); ok {
		t.Fatalf("batch size should be unset")
	}
	if _, ok := command.Bookmark(); ok {
		t.Fatalf("bookmark should be unset")
	}
	if _, ok := command.CommandID(); ok {
		t.Fatalf("command id should be unset")
	}
	if _, ok := command.CorrelationID(); ok {
		t.Fatalf("correlation id should be unset")
	}
	if _, ok := command.Expiration(); ok {
		t.Fatalf("expiration should be unset")
	}
	if _, ok := command.Filter(); ok {
		t.Fatalf("filter should be unset")
	}
	if _, ok := command.Options(); ok {
		t.Fatalf("options should be unset")
	}
	if _, ok := command.OrderBy(); ok {
		t.Fatalf("orderby should be unset")
	}
	if _, ok := command.QueryID(); ok {
		t.Fatalf("query id should be unset")
	}
	if _, ok := command.SequenceID(); ok {
		t.Fatalf("sequence should be unset")
	}
	if _, ok := command.SowKey(); ok {
		t.Fatalf("sow key should be unset")
	}
	if _, ok := command.SowKeys(); ok {
		t.Fatalf("sow keys should be unset")
	}
	if _, ok := command.SubID(); ok {
		t.Fatalf("sub id should be unset")
	}
	if _, ok := command.SubIDs(); ok {
		t.Fatalf("sub ids should be unset")
	}
	if _, ok := command.TopN(); ok {
		t.Fatalf("top_n should be unset")
	}
	if _, ok := command.Topic(); ok {
		t.Fatalf("topic should be unset")
	}

	command.SetCommandEnum(CommandSOWAndSubscribe)
	if !command.IsSow() || !command.IsSubscribe() {
		t.Fatalf("expected sow+subscribe classification")
	}
	command.SetCommandEnum(CommandPublish)
	if command.IsSow() || command.IsSubscribe() || !command.NeedsSequenceNumber() {
		t.Fatalf("unexpected publish classification")
	}
	command.SetCommandEnum(CommandDeltaPublish)
	if !command.NeedsSequenceNumber() {
		t.Fatalf("delta publish should need sequence")
	}
	command.SetCommandEnum(CommandSOWDelete)
	if !command.NeedsSequenceNumber() {
		t.Fatalf("sow delete should need sequence")
	}
	command.SetCommandEnum(CommandSubscribe)
	if command.NeedsSequenceNumber() {
		t.Fatalf("subscribe should not need sequence")
	}

	command.Init("subscribe")
	if cmd, _ := command.Command(); cmd != "subscribe" {
		t.Fatalf("unexpected init command: %q", cmd)
	}
	command.Reset()
	if command.Data() != nil {
		t.Fatalf("expected reset data to nil")
	}

	command.SetIds("cid-2", "qid-2", "sid-2")
	if got, _ := command.CommandID(); got != "cid-2" {
		t.Fatalf("unexpected command id after SetIds")
	}
	if got, _ := command.QueryID(); got != "qid-2" {
		t.Fatalf("unexpected query id after SetIds")
	}
	if got, _ := command.SubID(); got != "sid-2" {
		t.Fatalf("unexpected sub id after SetIds")
	}
}

func TestCommandNilReceiverCoverage(t *testing.T) {
	var nilCommand *Command
	if got := nilCommand.GetCommandEnum(); got != CommandUnknown {
		t.Fatalf("nil command enum should be unknown, got %d", got)
	}
	if nilCommand.Data() != nil {
		t.Fatalf("nil command data should be nil")
	}
	if got := nilCommand.GetTimeout(); got != 0 {
		t.Fatalf("nil command timeout should be zero, got %d", got)
	}
	if nilCommand.SetCommandEnum(CommandPublish) != nil {
		t.Fatalf("nil SetCommandEnum should return nil")
	}
}

func TestCommandOptionParityHelpersCoverage(t *testing.T) {
	if options := writeCommandOptions(nil); options != "" {
		t.Fatalf("writeCommandOptions(nil)=%q want empty string", options)
	}
	if options := writeCommandOptions([]commandOptionToken{
		{},
		{key: "send_keys"},
		{key: "max_backlog", value: "4", hasValue: true},
	}); options != "send_keys,max_backlog=4" {
		t.Fatalf("writeCommandOptions()=%q want %q", options, "send_keys,max_backlog=4")
	}

	nilCommand := (*Command)(nil)
	if nilCommand.SetSendKeys(true) != nil {
		t.Fatalf("expected nil SetSendKeys to return nil")
	}
	if nilCommand.SetFullyDurable(true) != nil {
		t.Fatalf("expected nil SetFullyDurable to return nil")
	}
	if nilCommand.SetMaxBacklog(1) != nil {
		t.Fatalf("expected nil SetMaxBacklog to return nil")
	}
	if nilCommand.SetBookmarkNotFoundNow() != nil {
		t.Fatalf("expected nil SetBookmarkNotFoundNow to return nil")
	}
	if nilCommand.SetBookmarkNotFound("fail") != nil {
		t.Fatalf("expected nil SetBookmarkNotFound to return nil")
	}

	empty := NewCommand("subscribe")
	if sendKeys, ok := empty.SendKeys(); ok || sendKeys {
		t.Fatalf("expected SendKeys() on empty options to be false,false got (%v,%v)", sendKeys, ok)
	}
	if fullyDurable, ok := empty.FullyDurable(); ok || fullyDurable {
		t.Fatalf("expected FullyDurable() on empty options to be false,false got (%v,%v)", fullyDurable, ok)
	}
	if maxBacklog, ok := empty.MaxBacklog(); ok || maxBacklog != 0 {
		t.Fatalf("expected MaxBacklog() on empty options to be 0,false got (%d,%v)", maxBacklog, ok)
	}
	if bookmarkNotFound, ok := empty.BookmarkNotFound(); ok || bookmarkNotFound != "" {
		t.Fatalf("expected BookmarkNotFound() on empty options to be empty,false got (%q,%v)", bookmarkNotFound, ok)
	}

	noBacklog := NewCommand("subscribe").SetOptions("send_keys")
	if maxBacklog, ok := noBacklog.MaxBacklog(); ok || maxBacklog != 0 {
		t.Fatalf("expected MaxBacklog() without max_backlog token to be 0,false got (%d,%v)", maxBacklog, ok)
	}

	command := NewCommand("subscribe").SetOptions("oof,send_keys,max_backlog=2,bookmark_not_found=now")

	command.SetSendKeys(true).
		SetFullyDurable(true).
		SetMaxBacklog(7).
		SetBookmarkNotFoundFail()

	if sendKeys, ok := command.SendKeys(); !ok || !sendKeys {
		t.Fatalf("SendKeys()=(%v,%v) want (true,true)", sendKeys, ok)
	}
	if fullyDurable, ok := command.FullyDurable(); !ok || !fullyDurable {
		t.Fatalf("FullyDurable()=(%v,%v) want (true,true)", fullyDurable, ok)
	}
	if maxBacklog, ok := command.MaxBacklog(); !ok || maxBacklog != 7 {
		t.Fatalf("MaxBacklog()=(%d,%v) want (7,true)", maxBacklog, ok)
	}
	if bookmarkNotFound, ok := command.BookmarkNotFound(); !ok || bookmarkNotFound != "fail" {
		t.Fatalf("BookmarkNotFound()=(%q,%v) want (fail,true)", bookmarkNotFound, ok)
	}
	command.SetBookmarkNotFoundNow()
	if bookmarkNotFound, ok := command.BookmarkNotFound(); !ok || bookmarkNotFound != "now" {
		t.Fatalf("BookmarkNotFound() after SetBookmarkNotFoundNow=(%q,%v) want (now,true)", bookmarkNotFound, ok)
	}
	command.SetBookmarkNotFoundFail()

	options, ok := command.Options()
	if !ok {
		t.Fatalf("expected options to remain set")
	}
	tokens := optionTokenSet(options)
	if _, exists := tokens[OptionOOF]; !exists {
		t.Fatalf("expected oof option to be preserved, got %q", options)
	}
	if _, exists := tokens[OptionSendKeys]; !exists {
		t.Fatalf("expected send_keys option to be present, got %q", options)
	}
	if _, exists := tokens[OptionFullyDurable]; !exists {
		t.Fatalf("expected fully_durable option to be present, got %q", options)
	}
	if value := tokens["max_backlog"]; value != "7" {
		t.Fatalf("expected max_backlog=7, got %q from %q", value, options)
	}
	if value := tokens["bookmark_not_found"]; value != "fail" {
		t.Fatalf("expected bookmark_not_found=fail, got %q from %q", value, options)
	}

	if strings.Count(options, OptionSendKeys) != 1 {
		t.Fatalf("expected send_keys to avoid duplicates, got %q", options)
	}
	if strings.Count(options, "max_backlog=") != 1 {
		t.Fatalf("expected single max_backlog token, got %q", options)
	}
	if strings.Count(options, "bookmark_not_found=") != 1 {
		t.Fatalf("expected single bookmark_not_found token, got %q", options)
	}

	cloned := command.Clone()
	if cloned == nil {
		t.Fatalf("expected clone")
	}
	if maxBacklog, ok := cloned.MaxBacklog(); !ok || maxBacklog != 7 {
		t.Fatalf("cloned MaxBacklog()=(%d,%v) want (7,true)", maxBacklog, ok)
	}
	if bookmarkNotFound, ok := cloned.BookmarkNotFound(); !ok || bookmarkNotFound != "fail" {
		t.Fatalf("cloned BookmarkNotFound()=(%q,%v) want (fail,true)", bookmarkNotFound, ok)
	}

	command.SetSendKeys(false).
		SetFullyDurable(false).
		SetMaxBacklog(0).
		SetBookmarkNotFoundEpoch()

	if sendKeys, ok := command.SendKeys(); ok || sendKeys {
		t.Fatalf("expected send_keys to clear, got (%v,%v)", sendKeys, ok)
	}
	if fullyDurable, ok := command.FullyDurable(); ok || fullyDurable {
		t.Fatalf("expected fully_durable to clear, got (%v,%v)", fullyDurable, ok)
	}
	if maxBacklog, ok := command.MaxBacklog(); !ok || maxBacklog != 0 {
		t.Fatalf("expected max_backlog=0 to remain explicit, got (%d,%v)", maxBacklog, ok)
	}
	options, ok = command.Options()
	if !ok {
		t.Fatalf("expected options to remain set after max_backlog=0")
	}
	if value := optionTokenSet(options)["max_backlog"]; value != "0" {
		t.Fatalf("expected max_backlog=0 token, got %q from %q", value, options)
	}
	if bookmarkNotFound, ok := command.BookmarkNotFound(); !ok || bookmarkNotFound != "epoch" {
		t.Fatalf("expected bookmark_not_found=epoch, got (%q,%v)", bookmarkNotFound, ok)
	}

	command.SetBookmarkNotFound("")
	if bookmarkNotFound, ok := command.BookmarkNotFound(); ok || bookmarkNotFound != "" {
		t.Fatalf("expected bookmark_not_found to clear, got (%q,%v)", bookmarkNotFound, ok)
	}

	command.SetBookmarkNotFound("invalid")
	if options, ok := command.Options(); ok && strings.Contains(options, "bookmark_not_found=") {
		t.Fatalf("expected invalid bookmark_not_found action to leave token cleared, got %q", options)
	}

	invalid := NewCommand("subscribe").SetOptions("max_backlog=abc,send_keys")
	if maxBacklog, ok := invalid.MaxBacklog(); ok || maxBacklog != 0 {
		t.Fatalf("expected invalid max_backlog parse to be 0,false got (%d,%v)", maxBacklog, ok)
	}
}

func TestCommandOptionHelpersPreserveBracketedOptionValues(t *testing.T) {
	command := NewCommand("sow").
		SetOptions("projection=[COUNT(/orderId) AS /count, /customer AS /customer],grouping=[/customer,/region]")

	command.SetSendKeys(true).SetBookmarkNotFoundFail()

	options, ok := command.Options()
	if !ok {
		t.Fatalf("expected options to remain set")
	}
	if !strings.Contains(options, "projection=[COUNT(/orderId) AS /count, /customer AS /customer]") {
		t.Fatalf("expected projection option to be preserved, got %q", options)
	}
	if !strings.Contains(options, "grouping=[/customer,/region]") {
		t.Fatalf("expected grouping option to be preserved, got %q", options)
	}
	if !strings.Contains(options, OptionSendKeys) {
		t.Fatalf("expected send_keys option to be added, got %q", options)
	}
	if !strings.Contains(options, OptionBookmarkNotFoundFail) {
		t.Fatalf("expected bookmark_not_found option to be added, got %q", options)
	}
}

func TestSplitCommandOptionsCoverage(t *testing.T) {
	if tokens := splitCommandOptions(""); tokens != nil {
		t.Fatalf("expected empty options to return nil, got %#v", tokens)
	}

	tokens := splitCommandOptions("send_keys,, grouping=[/customer,/region]],fully_durable")
	if len(tokens) != 3 {
		t.Fatalf("expected 3 tokens, got %d: %#v", len(tokens), tokens)
	}
	if tokens[0] != "send_keys" {
		t.Fatalf("unexpected first token: %q", tokens[0])
	}
	if tokens[1] != "grouping=[/customer,/region]]" {
		t.Fatalf("unexpected grouping token: %q", tokens[1])
	}
	if tokens[2] != "fully_durable" {
		t.Fatalf("unexpected last token: %q", tokens[2])
	}
}
