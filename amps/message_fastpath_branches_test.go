package amps

import "testing"

func TestParseHeaderTrustedEmptyInputBranch(t *testing.T) {
	var header = new(_Header)
	var _, ok = parseHeaderTrusted(header, nil)
	if ok {
		t.Fatalf("expected empty input to fail trusted parse")
	}
}

func TestParseHeaderTrustedLeadingWhitespaceBranch(t *testing.T) {
	var header = new(_Header)
	var end, ok = parseHeaderTrusted(header, []byte(" \t\n{\"t\":\"orders\"}tail"))
	if !ok || end != len(" \t\n{\"t\":\"orders\"}") {
		t.Fatalf("expected leading-whitespace trusted parse success, end=%d ok=%v", end, ok)
	}
}

func TestParseHeaderTrustedEarlyCloseBraceBranch(t *testing.T) {
	var header = new(_Header)
	var end, ok = parseHeaderTrusted(header, []byte("{}tail"))
	if !ok || end != 2 {
		t.Fatalf("expected early close-brace trusted parse success, end=%d ok=%v", end, ok)
	}
}

func TestParseHeaderTrustedKeyMissingCloseQuoteBranch(t *testing.T) {
	var header = new(_Header)
	var _, ok = parseHeaderTrusted(header, []byte("\"abc"))
	if ok {
		t.Fatalf("expected missing key quote to fail trusted parse")
	}
}

func TestParseHeaderTrustedValueMissingAfterColonBranch(t *testing.T) {
	var header = new(_Header)
	var _, ok = parseHeaderTrusted(header, []byte("\"a\":"))
	if ok {
		t.Fatalf("expected missing value after colon to fail trusted parse")
	}
}

func TestParseHeaderTrustedQuotedValueMissingEndBranch(t *testing.T) {
	var header = new(_Header)
	var _, ok = parseHeaderTrusted(header, []byte("\"a\":\"abc"))
	if ok {
		t.Fatalf("expected unterminated quoted value to fail trusted parse")
	}
}

func TestParseHeaderTrustedUnquotedWhitespaceBranch(t *testing.T) {
	var header = new(_Header)
	var _, ok = parseHeaderTrusted(header, []byte("\"a\":12 3}"))
	if ok {
		t.Fatalf("expected unquoted whitespace to fail trusted parse")
	}
}

func TestParseHeaderTrustedUnquotedNoTerminatorBranch(t *testing.T) {
	var header = new(_Header)
	var _, ok = parseHeaderTrusted(header, []byte("\"a\":123"))
	if ok {
		t.Fatalf("expected unquoted value without terminator to fail trusted parse")
	}
}

func TestParseHeaderTrustedLoopEndBranch(t *testing.T) {
	var header = new(_Header)
	var _, ok = parseHeaderTrusted(header, []byte("{   "))
	if ok {
		t.Fatalf("expected trusted parse loop-end failure")
	}
}

func TestParseHeaderTrustedCTSubIDCommandScanFailBranch(t *testing.T) {
	var header = new(_Header)
	var _, ok = parseHeaderTrustedCTSubID(header, []byte("\"c\":\"publish"), 0)
	if ok {
		t.Fatalf("expected CT/sub_id command scan failure")
	}
}

func TestParseHeaderTrustedCTSubIDTopicScanFailBranch(t *testing.T) {
	var header = new(_Header)
	var _, ok = parseHeaderTrustedCTSubID(header, []byte("\"c\":\"p\",\"t\":\"orders"), 0)
	if ok {
		t.Fatalf("expected CT/sub_id topic scan failure")
	}
}

func TestParseHeaderTrustedCTSubIDKeyMismatchBranch(t *testing.T) {
	var header = new(_Header)
	var _, ok = parseHeaderTrustedCTSubID(header, []byte("\"c\":\"p\",\"t\":\"orders\",\"sid\":\"sub-1\"}"), 0)
	if ok {
		t.Fatalf("expected CT/sub_id key mismatch failure")
	}
}

func TestParseHeaderTrustedCTSubIDLongCommandBranch(t *testing.T) {
	var header = new(_Header)
	var end, ok = parseHeaderTrustedCTSubID(header, []byte("\"c\":\"publish\",\"t\":\"orders\",\"sub_id\":\"sub-1\"}tail"), 0)
	if !ok || end != len("\"c\":\"publish\",\"t\":\"orders\",\"sub_id\":\"sub-1\"}") || header.command != CommandPublish {
		t.Fatalf("expected long command path success, end=%d ok=%v command=%d", end, ok, header.command)
	}
}

func TestParseHeaderCheckedUnquotedCommaBranch(t *testing.T) {
	var header = new(_Header)
	var _, err = parseHeaderChecked(header, []byte(`{"bs":2,"c":"p"}`))
	if err != nil {
		t.Fatalf("expected parseHeaderChecked comma branch success: %v", err)
	}
}

func TestParseHeaderCheckedUnquotedCloseBraceBranch(t *testing.T) {
	var header = new(_Header)
	var _, err = parseHeaderChecked(header, []byte(`{"bs":2}`))
	if err != nil {
		t.Fatalf("expected parseHeaderChecked close-brace branch success: %v", err)
	}
}
