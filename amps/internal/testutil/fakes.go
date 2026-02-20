package testutil

import "sync"

type Counter struct {
	lock  sync.Mutex
	value int
}

func (counter *Counter) Next() int {
	counter.lock.Lock()
	defer counter.lock.Unlock()
	counter.value++
	return counter.value
}
