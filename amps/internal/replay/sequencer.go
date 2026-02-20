package replay

import "sync/atomic"

type Sequencer struct {
	next uint64
}

func NewSequencer(start uint64) *Sequencer {
	return &Sequencer{next: start}
}

func (sequencer *Sequencer) Next() uint64 {
	if sequencer == nil {
		return 0
	}
	return atomic.AddUint64(&sequencer.next, 1) - 1
}
