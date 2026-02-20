package testutil

import "sync"

// Counter is a deterministic integer counter for tests.
type Counter struct {
	lock  sync.Mutex
	value int
}

// Next increments and returns counter value.
func (counter *Counter) Next() int {
	counter.lock.Lock()
	defer counter.lock.Unlock()
	counter.value++
	return counter.value
}
