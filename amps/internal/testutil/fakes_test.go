package testutil

import "testing"

func TestCounterNextCoverage(t *testing.T) {
	counter := &Counter{}
	if next := counter.Next(); next != 1 {
		t.Fatalf("expected first counter value 1, got %d", next)
	}
	if next := counter.Next(); next != 2 {
		t.Fatalf("expected second counter value 2, got %d", next)
	}
}

