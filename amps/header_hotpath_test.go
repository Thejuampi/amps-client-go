package amps

import (
	"bytes"
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

	left, err := parseHeader(message, true, []byte(`{"a":"processed,completed","c":"p","e":42,"f":"/id > 10","o":"replace","s":99,"t":"orders","cid":"cmd-1","sub_id":"sub-1","bs":10}tail`))
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
