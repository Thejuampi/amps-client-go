package amps

import (
	"bytes"
	"fmt"
	"testing"
)

func TestPerfTrustedCTSubIDRejectsEveryUnsafeByteWithoutMutation(t *testing.T) {
	var fields = []struct{ prefix, suffix string }{
		{`"c":"`, `","t":"orders","sub_id":"sub"}`},
		{`"c":"p","t":"`, `","sub_id":"sub"}`},
		{`"c":"p","t":"orders","sub_id":"`, `"}`},
	}
	for fieldIndex, field := range fields {
		for character := 0; character <= 0x21; character++ {
			var value = byte(character)
			if character == 0x20 {
				value = '\\'
			} else if character == 0x21 {
				value = '"'
			}
			var input = append([]byte(field.prefix), value)
			input = append(input, field.suffix...)
			var header = &_Header{command: CommandOOF, topic: []byte("keep-topic"), subID: []byte("keep-sub")}
			if _, ok := parseHeaderTrustedCTSubID(header, input, 0); ok {
				t.Fatalf("field %d accepted byte 0x%02x", fieldIndex, value)
			}
			if header.command != CommandOOF || string(header.topic) != "keep-topic" || string(header.subID) != "keep-sub" {
				t.Fatalf("field %d byte 0x%02x changed header on failure", fieldIndex, value)
			}
		}
	}
}

func TestPerfTrustedCTSubIDRejectsTruncationAndWrongPrefixWithoutMutation(t *testing.T) {
	var headerBytes = []byte(`"c":"p","t":"orders","sub_id":"sub-1"}`)
	var inputs [][]byte
	for length := 0; length < len(headerBytes); length++ {
		inputs = append(inputs, headerBytes[:length])
	}
	for index := 0; index < 5; index++ {
		var mutated = append([]byte(nil), headerBytes...)
		mutated[index] = 'x'
		inputs = append(inputs, mutated)
	}
	for index, input := range inputs {
		var header = &_Header{command: CommandOOF, topic: []byte("keep-topic"), subID: []byte("keep-sub")}
		if _, ok := parseHeaderTrustedCTSubID(header, input, 0); ok {
			t.Fatalf("input %d accepted: %q", index, input)
		}
		if header.command != CommandOOF || string(header.topic) != "keep-topic" || string(header.subID) != "keep-sub" {
			t.Fatalf("input %d changed header on failure", index)
		}
	}
}

func TestPerfHeaderFastAndEscapedFallbackPreservePayload(t *testing.T) {
	for _, entry := range []struct {
		header  string
		command int
		topic   string
		subID   string
	}{
		{`"c":"p","t":"orders","sub_id":"sub-1"}`, CommandPublish, "orders", "sub-1"},
		{`"c":"x","t":"orders","sub_id":"sub-1"}`, CommandUnknown, "orders", "sub-1"},
		{`{"c":"publish","t":"orders","sub_id":"sub-1"}`, CommandPublish, "orders", "sub-1"},
		{`{"c":"subscribe","t":"orders","sub_id":"sub-1"}`, CommandSubscribe, "orders", "sub-1"},
		{`{"c":"pu\u0062lish","t":"ord\"ers","sub_id":"sub\\\u00e9"}`, CommandPublish, `ord"ers`, `sub\é`},
	} {
		t.Run(fmt.Sprintf("%q", entry.header), func(t *testing.T) {
			var payload = []byte{'{', '}', 0, 0xff}
			var wire = append([]byte(entry.header), payload...)
			var message = &Message{}
			var left, err = parseHeader(message, true, wire)
			if err != nil || message.header.command != entry.command || string(message.header.topic) != entry.topic || string(message.header.subID) != entry.subID || !bytes.Equal(left, payload) {
				t.Fatalf("command=%d topic=%q subID=%q payload=%x error=%v", message.header.command, message.header.topic, message.header.subID, left, err)
			}
		})
	}
}

func TestPerfHeaderRejectsUnescapedQuoteAsCommand(t *testing.T) {
	var input = []byte(`"c":""","t":"orders","sub_id":"sub"}`)
	if _, err := parseHeader(&Message{}, true, input); err == nil {
		t.Fatal("unescaped quote in command was accepted")
	}
}
