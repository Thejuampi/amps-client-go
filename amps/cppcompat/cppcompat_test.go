package cppcompat

import "testing"

func TestParseURI(t *testing.T) {
	uri := ParseURI("tcp://user:pass@localhost:9007/amps/json")
	if uri.Transport != "tcp" || uri.Host != "localhost" || uri.Port != "9007" || uri.Protocol != "json" {
		t.Fatalf("unexpected URI parse result: %+v", uri)
	}
}

func TestRecoveryPointAdapters(t *testing.T) {
	adapter := NewRecoveryPointAdapter(
		NewFixedRecoveryPoint(""),
		NewFixedRecoveryPoint("1|2|"),
	)
	if bookmark := adapter.Next(); bookmark != "1|2|" {
		t.Fatalf("unexpected bookmark: %s", bookmark)
	}
}

func TestFIXParseAndData(t *testing.T) {
	message := "8=FIX.4.4\x019=12\x0135=A\x01"
	fix := NewFIX().Parse(message)
	if size := fix.Size(); size != 3 {
		t.Fatalf("unexpected FIX size: %d", size)
	}
	if value, ok := fix.Get(35); !ok || value != "A" {
		t.Fatalf("unexpected FIX tag 35 value: %q", value)
	}
}
