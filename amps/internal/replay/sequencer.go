package replay

import "sync/atomic"

// Sequencer provides deterministic sequence IDs for replay.
type Sequencer struct {
	next uint64
}

// NewSequencer creates a sequencer starting at value.
func NewSequencer(start uint64) *Sequencer {
	return &Sequencer{next: start}
}

// Next returns next sequence value.
func (sequencer *Sequencer) Next() uint64 {
	if sequencer == nil {
		return 0
	}
	return atomic.AddUint64(&sequencer.next, 1) - 1
}
