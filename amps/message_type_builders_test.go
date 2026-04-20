package amps

import "testing"

func TestJSONMessageBuilderEscapesStrings(t *testing.T) {
	var builder JSONMessageBuilder
	builder.Append(`quote"key`, "line1\nline2")

	var payload = builder.Data()
	if payload != `{"quote\"key":"line1\nline2"}` {
		t.Fatalf("JSONMessageBuilder.Data() = %q, want escaped JSON object", payload)
	}
}
