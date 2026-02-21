package amps

import "testing"

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
	if nilCommand.SetCommandEnum(CommandPublish) != nil {
		t.Fatalf("nil SetCommandEnum should return nil")
	}
}
