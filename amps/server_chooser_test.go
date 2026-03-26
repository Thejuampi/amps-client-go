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

func TestDefaultServerChooserRemovePreservesCurrentEndpoint(t *testing.T) {
	var chooser = NewDefaultServerChooser(
		"tcp://one:9000/amps/json",
		"tcp://two:9000/amps/json",
		"tcp://three:9000/amps/json",
	)

	chooser.ReportFailure(nil, nil)
	chooser.ReportFailure(nil, nil)
	chooser.Remove("tcp://one:9000/amps/json")

	if current := chooser.CurrentURI(); current != "tcp://three:9000/amps/json" {
		t.Fatalf("expected current URI to remain on third endpoint after earlier removal, got %q", current)
	}
}

func TestDefaultServerChooserRemoveNormalizesInvalidIndex(t *testing.T) {
	var chooser = NewDefaultServerChooser(
		"tcp://one:9000/amps/json",
		"tcp://two:9000/amps/json",
	)

	chooser.index = 99
	chooser.Remove("tcp://two:9000/amps/json")

	if current := chooser.CurrentURI(); current != "tcp://one:9000/amps/json" {
		t.Fatalf("expected invalid index removal to normalize to first endpoint, got %q", current)
	}
}
