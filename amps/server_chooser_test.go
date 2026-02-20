package amps

import "testing"

func TestDefaultServerChooserRoundRobin(t *testing.T) {
	chooser := NewDefaultServerChooser("tcp://one:9000/amps/json", "tcp://two:9000/amps/json")
	if current := chooser.CurrentURI(); current != "tcp://one:9000/amps/json" {
		t.Fatalf("expected first URI selected, got %q", current)
	}

	chooser.ReportFailure(nil, nil)
	if current := chooser.CurrentURI(); current != "tcp://two:9000/amps/json" {
		t.Fatalf("expected rotation to second URI, got %q", current)
	}

	chooser.Remove("tcp://two:9000/amps/json")
	if current := chooser.CurrentURI(); current != "tcp://one:9000/amps/json" {
		t.Fatalf("expected first URI after removal, got %q", current)
	}
}
