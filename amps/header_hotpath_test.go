package amps

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestHeaderNumericParseEdgeAndInvalid(t *testing.T) {
	message := &Message{header: new(_Header)}

	left, err := parseHeader(message, true, []byte(`{"c":"p","e":4294967295,"s":18446744073709551615,"bs":10,"top_n":25}payload`))
	if err != nil {
		t.Fatalf("parse header failed: %v", err)
	}
	if string(left) != "payload" {
		t.Fatalf("unexpected payload tail: got %q", string(left))
	}

	expiration, hasExpiration := message.Expiration()
	if !hasExpiration || expiration != 4294967295 {
		t.Fatalf("unexpected expiration: has=%v value=%d", hasExpiration, expiration)
	}
	sequenceID, hasSequenceID := message.SequenceID()
	if !hasSequenceID || sequenceID != 18446744073709551615 {
		t.Fatalf("unexpected sequence id: has=%v value=%d", hasSequenceID, sequenceID)
	}
	batchSize, hasBatchSize := message.BatchSize()
	if !hasBatchSize || batchSize != 10 {
		t.Fatalf("unexpected batch size: has=%v value=%d", hasBatchSize, batchSize)
	}
	topN, hasTopN := message.TopN()
	if !hasTopN || topN != 25 {
		t.Fatalf("unexpected topN: has=%v value=%d", hasTopN, topN)
	}

	_, err = parseHeader(message, true, []byte(`{"c":"p","e":"abc","s":"xyz","bs":"-1"}`))
	if err != nil {
		t.Fatalf("parse invalid header failed: %v", err)
	}
	if _, ok := message.Expiration(); ok {
		t.Fatalf("expiration should be absent for invalid value")
	}
	if _, ok := message.SequenceID(); ok {
		t.Fatalf("sequence should be absent for invalid value")
	}
	if _, ok := message.BatchSize(); ok {
		t.Fatalf("batch size should be absent for invalid value")
	}
}

func TestHeaderWriteExactOutput(t *testing.T) {
	command := CommandPublish
	expiration := uint(42)
	header := &_Header{
		command:    command,
		commandID:  []byte("cid-1"),
		topic:      []byte("orders"),
		expiration: &expiration,
	}

	buffer := bytes.NewBuffer(nil)
	if err := header.write(buffer); err != nil {
		t.Fatalf("header write failed: %v", err)
	}

	expected := `{"c":"p","cid":"cid-1","t":"orders","e":42}`
	if buffer.String() != expected {
		t.Fatalf("unexpected header payload: got %q want %q", buffer.String(), expected)
	}
}

func TestHeaderWriteSimplePublishFastPathExactOutput(t *testing.T) {
	command := CommandPublish
	header := &_Header{
		command:   command,
		commandID: []byte("cid-1"),
		topic:     []byte("orders"),
	}

	buffer := bytes.NewBuffer(nil)
	if err := header.write(buffer); err != nil {
		t.Fatalf("header write failed: %v", err)
	}

	expected := `{"c":"p","cid":"cid-1","t":"orders"}`
	if buffer.String() != expected {
		t.Fatalf("unexpected simple publish header payload: got %q want %q", buffer.String(), expected)
	}
}

func TestHeaderWriteSimplePublishFastPathEscapesStrings(t *testing.T) {
	command := CommandPublish
	header := &_Header{
		command:   command,
		commandID: []byte("cid\"1"),
		topic:     []byte("orders\\east"),
	}

	buffer := bytes.NewBuffer(nil)
	if err := header.write(buffer); err != nil {
		t.Fatalf("header write failed: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(buffer.Bytes(), &decoded); err != nil {
		t.Fatalf("expected valid escaped simple publish json, got %q err=%v", buffer.String(), err)
	}
	if decoded["cid"] != "cid\"1" || decoded["t"] != "orders\\east" {
		t.Fatalf("unexpected simple publish decoded values: cid=%#v topic=%#v", decoded["cid"], decoded["t"])
	}
}

func TestHeaderParseWithoutOpeningBrace(t *testing.T) {
	message := &Message{header: new(_Header)}

	left, err := parseHeader(message, true, []byte(`"c":"p","t":"orders","sub_id":"sub-1"}tail`))
	if err != nil {
		t.Fatalf("parse header failed: %v", err)
	}
	if string(left) != "tail" {
		t.Fatalf("unexpected payload tail: got %q", string(left))
	}
	topic, hasTopic := message.Topic()
	if !hasTopic || topic != "orders" {
		t.Fatalf("unexpected topic: has=%v value=%q", hasTopic, topic)
	}
	subID, hasSubID := message.SubID()
	if !hasSubID || subID != "sub-1" {
		t.Fatalf("unexpected sub id: has=%v value=%q", hasSubID, subID)
	}
}

func TestHeaderParseFallbackWithWhitespace(t *testing.T) {
	message := &Message{header: new(_Header)}

	left, err := parseHeader(message, true, []byte(`{ "c":"p", "filter":"/id > 10", "t":"orders" }tail`))
	if err != nil {
		t.Fatalf("parse header failed: %v", err)
	}
	if string(left) != "tail" {
		t.Fatalf("unexpected payload tail: got %q", string(left))
	}
	filter, hasFilter := message.Filter()
	if !hasFilter || filter != "/id > 10" {
		t.Fatalf("unexpected filter: has=%v value=%q", hasFilter, filter)
	}
}

func TestHeaderParseTrustedFieldCoverage(t *testing.T) {
	message := &Message{header: new(_Header)}

	left, err := parseHeader(message, true, []byte(`{"a":"processed,completed","c":"p","e":42,"f":"/id > 10","o":"replace","l":64,"s":99,"t":"orders","cid":"cmd-1","sub_id":"sub-1","bs":10}tail`))
	if err != nil {
		t.Fatalf("parse header failed: %v", err)
	}
	if string(left) != "tail" {
		t.Fatalf("unexpected payload tail: got %q", string(left))
	}
	ack, hasAck := message.AckType()
	if !hasAck || ack != AckTypeProcessed|AckTypeCompleted {
		t.Fatalf("unexpected ack type: has=%v value=%q", hasAck, ack)
	}
	expiration, hasExpiration := message.Expiration()
	if !hasExpiration || expiration != 42 {
		t.Fatalf("unexpected expiration: has=%v value=%d", hasExpiration, expiration)
	}
	batchSize, hasBatchSize := message.BatchSize()
	if !hasBatchSize || batchSize != 10 {
		t.Fatalf("unexpected batch size: has=%v value=%d", hasBatchSize, batchSize)
	}
	messageLength, hasMessageLength := message.MessageLength()
	if !hasMessageLength || messageLength != 64 {
		t.Fatalf("unexpected message length: has=%v value=%d", hasMessageLength, messageLength)
	}
}

func TestHeaderParseTopicOnlyQuotedFastPath(t *testing.T) {
	message := &Message{header: new(_Header)}

	left, err := parseHeader(message, true, []byte(`"t":"orders"}tail`))
	if err != nil {
		t.Fatalf("parse header failed: %v", err)
	}
	if string(left) != "tail" {
		t.Fatalf("unexpected payload tail: got %q", string(left))
	}
	topic, hasTopic := message.Topic()
	if !hasTopic || topic != "orders" {
		t.Fatalf("unexpected topic: has=%v value=%q", hasTopic, topic)
	}
}

func TestHeaderParseTopicOnlyQuotedFastPathWithBrace(t *testing.T) {
	message := &Message{header: new(_Header)}

	left, err := parseHeader(message, true, []byte(`{"t":"orders"}tail`))
	if err != nil {
		t.Fatalf("parse header failed: %v", err)
	}
	if string(left) != "tail" {
		t.Fatalf("unexpected payload tail: got %q", string(left))
	}
	topic, hasTopic := message.Topic()
	if !hasTopic || topic != "orders" {
		t.Fatalf("unexpected topic: has=%v value=%q", hasTopic, topic)
	}
}

func TestHeaderParseTopicOnlyUnquotedFastPath(t *testing.T) {
	message := &Message{header: new(_Header)}

	left, err := parseHeader(message, true, []byte(`"t":orders}tail`))
	if err != nil {
		t.Fatalf("parse header failed: %v", err)
	}
	if string(left) != "tail" {
		t.Fatalf("unexpected payload tail: got %q", string(left))
	}
	topic, hasTopic := message.Topic()
	if !hasTopic || topic != "orders" {
		t.Fatalf("unexpected topic: has=%v value=%q", hasTopic, topic)
	}
}

func TestHeaderParseTopicOnlyUnquotedFastPathRejectsTrailingFields(t *testing.T) {
	message := &Message{header: new(_Header)}

	left, err := parseHeader(message, true, []byte(`{"t":orders,"c":"p"}tail`))
	if err != nil {
		t.Fatalf("parse header failed: %v", err)
	}
	if string(left) != "tail" {
		t.Fatalf("unexpected payload tail: got %q", string(left))
	}
	topic, hasTopic := message.Topic()
	if !hasTopic || topic != "orders" {
		t.Fatalf("unexpected topic: has=%v value=%q", hasTopic, topic)
	}
	if message.header.command != CommandPublish {
		t.Fatalf("expected fallback parser to preserve trailing fields")
	}
}

func TestHeaderParseTopicOnlyUnquotedFastPathRejectsEmptyTopic(t *testing.T) {
	message := &Message{header: new(_Header)}

	if _, err := parseHeader(message, true, []byte(`{"t":}tail`)); err == nil {
		t.Fatalf("expected empty unquoted topic to fail parsing")
	}
	if _, err := parseHeader(message, true, []byte(`{"t":,"c":"p"}tail`)); err == nil {
		t.Fatalf("expected empty unquoted topic before comma to fail parsing")
	}
}

func TestHeaderWriteStrictParityFixtureExactOutput(t *testing.T) {
	var header = strictParityHeaderWriteBenchmarkHeader()
	var buffer = bytes.NewBuffer(nil)
	if err := header.write(buffer); err != nil {
		t.Fatalf("header write failed: %v", err)
	}

	var expected = `{"c":"p","cid":"cmd-1","t":"orders","e":42,"filter":"/id > 10","opts":"replace","query_id":"qry-1","s":123456789,"sub_id":"sub-1"}`
	if buffer.String() != expected {
		t.Fatalf("unexpected strict parity header output: got %q want %q", buffer.String(), expected)
	}
}

func TestHeaderWriteStrictParityFastPathNilReceiver(t *testing.T) {
	var header *_Header
	var buffer = bytes.NewBuffer(nil)
	if header.writeStrictParityFastPath(buffer) {
		t.Fatalf("expected nil receiver strict parity fast path to return false")
	}
}

func TestHeaderWriteEscapesStrictParityStrings(t *testing.T) {
	var header = strictParityHeaderWriteBenchmarkHeader()
	header.topic = []byte("orders\"east\\desk")
	header.filter = []byte("/name = \"A\\B\"")

	var buffer = bytes.NewBuffer(nil)
	if err := header.write(buffer); err != nil {
		t.Fatalf("header write failed: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(buffer.Bytes(), &decoded); err != nil {
		t.Fatalf("expected valid escaped json, got %q err=%v", buffer.String(), err)
	}
	if decoded["t"] != "orders\"east\\desk" {
		t.Fatalf("unexpected escaped topic: %#v", decoded["t"])
	}
	if decoded["filter"] != "/name = \"A\\B\"" {
		t.Fatalf("unexpected escaped filter: %#v", decoded["filter"])
	}
}

func TestHeaderWriteEscapesStrictParityControlBytes(t *testing.T) {
	var header = strictParityHeaderWriteBenchmarkHeader()
	header.topic = []byte("orders\x01east")

	var buffer = bytes.NewBuffer(nil)
	if err := header.write(buffer); err != nil {
		t.Fatalf("header write failed: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(buffer.Bytes(), &decoded); err != nil {
		t.Fatalf("expected valid escaped json for control bytes, got %q err=%v", buffer.String(), err)
	}
	if decoded["t"] != "orders\x01east" {
		t.Fatalf("unexpected escaped strict-parity topic: %#v", decoded["t"])
	}
}

func TestHeaderWriteEscapesGenericStrings(t *testing.T) {
	command := CommandPublish
	header := &_Header{
		command:    command,
		commandID:  []byte("cid-1"),
		topic:      []byte("orders"),
		clientName: []byte("client\"east\\ops"),
	}

	var buffer = bytes.NewBuffer(nil)
	if err := header.write(buffer); err != nil {
		t.Fatalf("header write failed: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(buffer.Bytes(), &decoded); err != nil {
		t.Fatalf("expected valid escaped json, got %q err=%v", buffer.String(), err)
	}
	if decoded["client_name"] != "client\"east\\ops" {
		t.Fatalf("unexpected escaped client_name: %#v", decoded["client_name"])
	}
}

func TestHeaderWriteEscapesGenericControlBytes(t *testing.T) {
	command := CommandPublish
	header := &_Header{
		command:    command,
		commandID:  []byte("cid-1"),
		topic:      []byte("orders"),
		clientName: []byte("client\veast"),
	}

	var buffer = bytes.NewBuffer(nil)
	if err := header.write(buffer); err != nil {
		t.Fatalf("header write failed: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(buffer.Bytes(), &decoded); err != nil {
		t.Fatalf("expected valid escaped json for generic control bytes, got %q err=%v", buffer.String(), err)
	}
	if decoded["client_name"] != "client\veast" {
		t.Fatalf("unexpected escaped generic client_name: %#v", decoded["client_name"])
	}
}

func TestHeaderWriteEscapesNamedControlBytes(t *testing.T) {
	command := CommandPublish
	header := &_Header{
		command:    command,
		commandID:  []byte("cid-1"),
		topic:      []byte("orders"),
		clientName: []byte("a\b\f\n\r\tz"),
	}

	var buffer = bytes.NewBuffer(nil)
	if err := header.write(buffer); err != nil {
		t.Fatalf("header write failed: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(buffer.Bytes(), &decoded); err != nil {
		t.Fatalf("expected valid escaped json for named control bytes, got %q err=%v", buffer.String(), err)
	}
	if decoded["client_name"] != "a\b\f\n\r\tz" {
		t.Fatalf("unexpected escaped named-control client_name: %#v", decoded["client_name"])
	}
}

func TestWriteQuotedBytesToBufferEscapesAllJSONControlForms(t *testing.T) {
	var buffer = bytes.NewBuffer(nil)
	writeQuotedBytesToBuffer(buffer, []byte("q\"\\\\\b\f\n\r\t\x01z"))

	var decoded string
	if err := json.Unmarshal(buffer.Bytes(), &decoded); err != nil {
		t.Fatalf("expected helper to produce valid json string, got %q err=%v", buffer.String(), err)
	}
	if decoded != "q\"\\\\\b\f\n\r\t\x01z" {
		t.Fatalf("unexpected decoded helper output: %#v", decoded)
	}
}

func TestWriteQuotedBytesToBufferEscapesQuoteAndBackslash(t *testing.T) {
	var buffer = bytes.NewBuffer(nil)
	writeQuotedBytesToBuffer(buffer, []byte("a\"b\\c"))
	if buffer.String() != `"a\"b\\c"` {
		t.Fatalf("unexpected quoted output: %q", buffer.String())
	}
}

func TestWriteQuotedBytesToBufferEscapesBackspace(t *testing.T) {
	var buffer = bytes.NewBuffer(nil)
	writeQuotedBytesToBuffer(buffer, []byte("a\bb"))
	if buffer.String() != `"a\bb"` {
		t.Fatalf("unexpected backspace output: %q", buffer.String())
	}
}

func TestWriteQuotedBytesToBufferEscapesFormfeed(t *testing.T) {
	var buffer = bytes.NewBuffer(nil)
	writeQuotedBytesToBuffer(buffer, []byte("a\fb"))
	if buffer.String() != `"a\fb"` {
		t.Fatalf("unexpected formfeed output: %q", buffer.String())
	}
}

func TestWriteQuotedBytesToBufferEscapesNewline(t *testing.T) {
	var buffer = bytes.NewBuffer(nil)
	writeQuotedBytesToBuffer(buffer, []byte("a\nb"))
	if buffer.String() != `"a\nb"` {
		t.Fatalf("unexpected newline output: %q", buffer.String())
	}
}

func TestWriteQuotedBytesToBufferEscapesCarriageReturn(t *testing.T) {
	var buffer = bytes.NewBuffer(nil)
	writeQuotedBytesToBuffer(buffer, []byte("a\rb"))
	if buffer.String() != `"a\rb"` {
		t.Fatalf("unexpected carriage return output: %q", buffer.String())
	}
}

func TestWriteQuotedBytesToBufferEscapesTab(t *testing.T) {
	var buffer = bytes.NewBuffer(nil)
	writeQuotedBytesToBuffer(buffer, []byte("a\tb"))
	if buffer.String() != `"a\tb"` {
		t.Fatalf("unexpected tab output: %q", buffer.String())
	}
}

func TestWriteQuotedBytesToBufferEscapesGenericControlByte(t *testing.T) {
	var buffer = bytes.NewBuffer(nil)
	writeQuotedBytesToBuffer(buffer, []byte("a\x01b"))
	if buffer.String() != `"a\u0001b"` {
		t.Fatalf("unexpected generic control output: %q", buffer.String())
	}
}

func TestStrictParityFieldsNeedJSONEscapeLateFields(t *testing.T) {
	var cases = []struct {
		name    string
		filter  []byte
		options []byte
		queryID []byte
		subID   []byte
	}{
		{name: "filter", filter: []byte("f\x01")},
		{name: "options", options: []byte("o\x01")},
		{name: "query_id", queryID: []byte("q\x01")},
		{name: "sub_id", subID: []byte("s\x01")},
	}

	for _, testCase := range cases {
		if !strictParityFieldsNeedJSONEscape([]byte("cid"), []byte("topic"), testCase.filter, testCase.options, testCase.queryID, testCase.subID) {
			t.Fatalf("expected %s bytes to require json escaping", testCase.name)
		}
	}
}

func TestCommandSettersInvalidateStrictParityEscapeCache(t *testing.T) {
	var cases = []struct {
		name  string
		apply func(*Command)
	}{
		{name: "command_id", apply: func(command *Command) { command.SetCommandID("cid-1") }},
		{name: "filter", apply: func(command *Command) { command.SetFilter("/id > 10") }},
		{name: "options", apply: func(command *Command) { command.SetOptions("replace") }},
		{name: "query_id", apply: func(command *Command) { command.SetQueryID("qry-1") }},
		{name: "sub_id", apply: func(command *Command) { command.SetSubID("sub-1") }},
		{name: "topic", apply: func(command *Command) { command.SetTopic("orders") }},
	}

	for _, testCase := range cases {
		var command = NewCommand("publish")
		command.header.strictParityEscapeState = 1
		testCase.apply(command)
		if command.header.strictParityEscapeState != 0 {
			t.Fatalf("expected %s setter to invalidate strict parity cache, got %d", testCase.name, command.header.strictParityEscapeState)
		}
	}
}

func TestHeaderWriteStrictParityFastPathUsesCachedEscapedBranch(t *testing.T) {
	var header = strictParityHeaderWriteBenchmarkHeader()
	header.strictParityEscapeState = 2

	var buffer = bytes.NewBuffer(nil)
	if !header.writeStrictParityFastPath(buffer) {
		t.Fatalf("expected strict parity fast path to handle cached escaped state")
	}

	var expected = `{"c":"p","cid":"cmd-1","t":"orders","e":42,"filter":"/id > 10","opts":"replace","query_id":"qry-1","s":123456789,"sub_id":"sub-1"}`
	if buffer.String() != expected {
		t.Fatalf("unexpected strict parity cached escaped output: got %q want %q", buffer.String(), expected)
	}
}

func TestHeaderWriteStrictParityEscapedFastPathExactOutput(t *testing.T) {
	var header = strictParityHeaderWriteBenchmarkHeader()
	header.topic = []byte("orders\\east")

	var buffer = bytes.NewBuffer(nil)
	header.writeStrictParityEscapedFastPath(buffer)

	var expected = `{"c":"p","cid":"cmd-1","t":"orders\\east","e":42,"filter":"/id > 10","opts":"replace","query_id":"qry-1","s":123456789,"sub_id":"sub-1"}`
	if buffer.String() != expected {
		t.Fatalf("unexpected strict parity escaped fast path output: got %q want %q", buffer.String(), expected)
	}
}

func TestHeaderWriteSimplePublishFastPathNilReceiver(t *testing.T) {
	var header *_Header
	var buffer = bytes.NewBuffer(nil)
	if header.writeSimplePublishFastPath(buffer) {
		t.Fatalf("expected nil receiver simple publish fast path to return false")
	}
}

func TestHeaderWriteSimplePublishFastPathUsesCachedEscapedBranch(t *testing.T) {
	var command = CommandPublish
	var header = &_Header{
		command:                 command,
		commandID:               []byte("cid-1"),
		topic:                   []byte("orders"),
		strictParityEscapeState: 2,
	}

	var buffer = bytes.NewBuffer(nil)
	if !header.writeSimplePublishFastPath(buffer) {
		t.Fatalf("expected simple publish fast path to handle cached escaped state")
	}

	var expected = `{"c":"p","cid":"cid-1","t":"orders"}`
	if buffer.String() != expected {
		t.Fatalf("unexpected simple publish cached escaped output: got %q want %q", buffer.String(), expected)
	}
}

func TestHeaderWriteSimplePublishEscapedFastPathExactOutput(t *testing.T) {
	var command = CommandPublish
	var header = &_Header{
		command:   command,
		commandID: []byte("cid\"1"),
		topic:     []byte("orders\\east"),
	}

	var buffer = bytes.NewBuffer(nil)
	header.writeSimplePublishEscapedFastPath(buffer)

	var expected = `{"c":"p","cid":"cid\"1","t":"orders\\east"}`
	if buffer.String() != expected {
		t.Fatalf("unexpected simple publish escaped fast path output: got %q want %q", buffer.String(), expected)
	}
}
