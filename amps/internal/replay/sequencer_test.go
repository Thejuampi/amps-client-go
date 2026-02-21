package replay

import "testing"

func TestSequencerCoverage(t *testing.T) {
	sequencer := NewSequencer(10)
	if first := sequencer.Next(); first != 10 {
		t.Fatalf("expected first sequence 10, got %d", first)
	}
	if second := sequencer.Next(); second != 11 {
		t.Fatalf("expected second sequence 11, got %d", second)
	}

	var nilSequencer *Sequencer
	if got := nilSequencer.Next(); got != 0 {
		t.Fatalf("expected nil sequencer next=0, got %d", got)
	}
}

