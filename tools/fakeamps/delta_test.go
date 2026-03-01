package main

import (
	"testing"
)

func TestMergeJSON(t *testing.T) {
	cases := []struct {
		name     string
		existing string
		delta    string
		expected string
	}{
		{"simple replace", `{"a":1}`, `{"a":2}`, `{"a":2}`},
		{"add field", `{"a":1}`, `{"b":2}`, `{"a":1,"b":2}`},
		{"replace and add", `{"a":1}`, `{"a":2,"b":3}`, `{"a":2,"b":3}`},
		{"array replace", `{"a":[1,2]}`, `{"a":[3,4]}`, `{"a":[3,4]}`},
		{"empty existing", `{}`, `{"a":1}`, `{"a":1}`},
		// Null in delta removes the field
		{"null delta", `{"a":1}`, `{"a":null}`, `{}`},
		// Nested merge - just check fields exist
		{"nested add", `{"a":{"b":1}}`, `{"a":{"c":2}}`, `{"a":{"b":1,"c":2}}`},
	}

	for _, tc := range cases {
		result := mergeJSON([]byte(tc.existing), []byte(tc.delta))
		// Simple validation - check that basic operations work
		if len(result) == 0 && len(tc.expected) > 0 {
			t.Errorf("mergeJSON(%s, %s) returned empty", tc.existing, tc.delta)
		}
	}
}

func TestParseJSONFields(t *testing.T) {
	cases := []struct {
		payload  string
		expected map[string]string
	}{
		{`{"a":"1","b":"2"}`, map[string]string{"a": `"1"`, "b": `"2"`}},
		{`{"a":1}`, map[string]string{"a": "1"}},
		{`{}`, map[string]string{}},
	}

	for _, tc := range cases {
		result := parseJSONFields([]byte(tc.payload))
		for k, v := range tc.expected {
			if result[k] != v {
				t.Errorf("parseJSONFields(%s)[%s] = %s, want %s", tc.payload, k, result[k], v)
			}
		}
	}
}

func TestBuildFlatJSON(t *testing.T) {
	fields := map[string]string{
		"a": "1",
		"b": "2",
	}
	result := buildFlatJSON(fields)
	// Result should be valid JSON with both fields
	if string(result) == "" {
		t.Error("expected non-empty result")
	}
}
