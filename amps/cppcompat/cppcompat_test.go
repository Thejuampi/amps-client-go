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

func TestRecoveryPointAdapterAdvancesAcrossPoints(t *testing.T) {
	adapter := NewRecoveryPointAdapter(
		NewFixedRecoveryPoint("a"),
		NewFixedRecoveryPoint("b"),
	)
	if bookmark := adapter.Next(); bookmark != "a" {
		t.Fatalf("unexpected first bookmark: %s", bookmark)
	}
	if bookmark := adapter.Next(); bookmark != "b" {
		t.Fatalf("unexpected second bookmark: %s", bookmark)
	}
	if bookmark := adapter.Next(); bookmark != "" {
		t.Fatalf("expected adapter to stop after the last point, got %q", bookmark)
	}
}

func TestRecoveryPointAdapterWrapsBackToEarlierPointAfterLaterPointEmpties(t *testing.T) {
	firstValues := []string{"", "a"}
	firstIndex := 0
	first := NewDynamicRecoveryPoint(func() string {
		if firstIndex >= len(firstValues) {
			return ""
		}
		value := firstValues[firstIndex]
		firstIndex++
		return value
	})

	secondValues := []string{"b", ""}
	secondIndex := 0
	second := NewDynamicRecoveryPoint(func() string {
		if secondIndex >= len(secondValues) {
			return ""
		}
		value := secondValues[secondIndex]
		secondIndex++
		return value
	})

	adapter := NewRecoveryPointAdapter(first, second)
	if bookmark := adapter.Next(); bookmark != "b" {
		t.Fatalf("unexpected first wrapped bookmark: %q", bookmark)
	}
	if bookmark := adapter.Next(); bookmark != "a" {
		t.Fatalf("expected adapter to wrap back to earlier point, got %q", bookmark)
	}
	if bookmark := adapter.Next(); bookmark != "" {
		t.Fatalf("expected adapter to stop after dynamic points are exhausted, got %q", bookmark)
	}
}

func TestConflatingRecoveryPointAdapterReturnsLatestValue(t *testing.T) {
	adapter := NewConflatingRecoveryPointAdapter(
		NewFixedRecoveryPoint("a"),
		NewFixedRecoveryPoint("b"),
	)
	if bookmark := adapter.Next(); bookmark != "b" {
		t.Fatalf("expected conflating adapter to return latest bookmark, got %q", bookmark)
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
