package main

import (
	"strconv"
	"sync"
	"testing"
)

func TestSOWQueryDoesNotRaceWithConcurrentUpserts(t *testing.T) {
	var cache = newSOWCache()
	cache.upsert("orders", "order-1", []byte(`{"id":1}`), "1|1|1|", "ts-1", 1, 0)

	var start = make(chan struct{})
	var wg sync.WaitGroup

	var workerCount = 4
	var iterations = 2000

	for index := 0; index < workerCount; index++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			<-start
			for iteration := 0; iteration < iterations; iteration++ {
				var payload = []byte(`{"id":` + strconv.Itoa(offset*iterations+iteration) + `}`)
				cache.upsert("orders", "order-1", payload, "1|1|1|", "ts-1", uint64(iteration+1), 0)
			}
		}(index)
	}

	for index := 0; index < workerCount; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for iteration := 0; iteration < iterations; iteration++ {
				_ = cache.query("orders", "", -1, "")
			}
		}()
	}

	close(start)
	wg.Wait()
}
