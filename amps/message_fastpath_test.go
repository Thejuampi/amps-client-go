package amps

import "testing"

func TestParseHeaderTrustedCTSubIDValid(t *testing.T) {
	var header = new(_Header)
	var input = []byte(`"c":"p","t":"orders","sub_id":"sub-1"}tail`)

	end, ok := parseHeaderTrustedCTSubID(header, input, 0)
	if !ok {
		t.Fatalf("expected trusted CT/sub_id parse to succeed")
	}
	if string(input[end:]) != "tail" {
		t.Fatalf("unexpected tail: %q", string(input[end:]))
	}
	if header.command != CommandPublish {
		t.Fatalf("unexpected command: %d", header.command)
	}
	if string(header.topic) != "orders" {
		t.Fatalf("unexpected topic: %q", string(header.topic))
	}
	if string(header.subID) != "sub-1" {
		t.Fatalf("unexpected sub_id: %q", string(header.subID))
	}
}

func TestParseHeaderTrustedCTSubIDInvalid(t *testing.T) {
	var cases = []struct {
		name  string
		input []byte
		index int
	}{
		{name: "too-short", input: []byte(`"c":"p"`), index: 0},
		{name: "wrong-prefix", input: []byte(`"x":"p","t":"orders","sub_id":"sub-1"}`), index: 0},
		{name: "missing-topic-separator", input: []byte(`"c":"p""t":"orders","sub_id":"sub-1"}`), index: 0},
		{name: "missing-subid-end", input: []byte(`"c":"p","t":"orders","sub_id":"sub-1"`), index: 0},
	}

	for _, testCase := range cases {
		var header = new(_Header)
		_, ok := parseHeaderTrustedCTSubID(header, testCase.input, testCase.index)
		if ok {
			t.Fatalf("case %q should fail", testCase.name)
		}
	}
}

func TestParseHeaderTrustedTopicOnlyModes(t *testing.T) {
	var quotedHeader = new(_Header)
	var quotedInput = []byte(`"t":"orders"}tail`)

	quotedEnd, quotedOk := parseHeaderTrustedTopicOnly(quotedHeader, quotedInput, 0)
	if !quotedOk {
		t.Fatalf("quoted topic-only should parse")
	}
	if string(quotedInput[quotedEnd:]) != "tail" {
		t.Fatalf("unexpected quoted tail: %q", string(quotedInput[quotedEnd:]))
	}
	if string(quotedHeader.topic) != "orders" {
		t.Fatalf("unexpected quoted topic: %q", string(quotedHeader.topic))
	}

	var unquotedHeader = new(_Header)
	var unquotedInput = []byte(`"t":orders}tail`)

	unquotedEnd, unquotedOk := parseHeaderTrustedTopicOnly(unquotedHeader, unquotedInput, 0)
	if !unquotedOk {
		t.Fatalf("unquoted topic-only should parse")
	}
	if string(unquotedInput[unquotedEnd:]) != "tail" {
		t.Fatalf("unexpected unquoted tail: %q", string(unquotedInput[unquotedEnd:]))
	}
	if string(unquotedHeader.topic) != "orders" {
		t.Fatalf("unexpected unquoted topic: %q", string(unquotedHeader.topic))
	}
}

func TestParseHeaderTrustedTopicOnlyInvalid(t *testing.T) {
	var cases = []struct {
		name  string
		input []byte
	}{
		{name: "too-short", input: []byte(`"t"`)},
		{name: "wrong-key", input: []byte(`"x":"orders"}`)},
		{name: "missing-quoted-end-brace", input: []byte(`"t":"orders"`)},
		{name: "missing-unquoted-end-brace", input: []byte(`"t":orders`)},
	}

	for _, testCase := range cases {
		var header = new(_Header)
		_, ok := parseHeaderTrustedTopicOnly(header, testCase.input, 0)
		if ok {
			t.Fatalf("case %q should fail", testCase.name)
		}
	}
}

func TestParseHeaderTrustedFallbackSignals(t *testing.T) {
	var header = new(_Header)
	var _, ok = parseHeaderTrusted(header, []byte(`{"c":"p","sub_id":"sub\\\"id","filter":"a\\\\b","topic":"orders"}`))
	if ok {
		t.Fatalf("trusted parser should reject escaped content for checked fallback")
	}

	var header2 = new(_Header)
	var end, ok2 = parseHeaderTrusted(header2, []byte(`{"bs":10,"c":"p"}tail`))
	if !ok2 {
		t.Fatalf("trusted parser should handle generic valid fields")
	}
	if string([]byte(`{"bs":10,"c":"p"}tail`)[end:]) != "tail" {
		t.Fatalf("unexpected generic tail")
	}
	if header2.command != CommandPublish {
		t.Fatalf("unexpected generic command: %d", header2.command)
	}
}
