package main

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/Thejuampi/amps-client-go/amps"
)

type countingFailedWriteHandler struct{}

func (handler *countingFailedWriteHandler) FailedWrite(message *amps.Message, reason string) {}

// retainedAfterPublishes reports heap growth retained after publishing count
// messages on a fresh client with a FailedWriteHandler installed and no publish
// store, so no ack can ever release the retained command.
func retainedAfterPublishes(t *testing.T, harness *deliveryHarness, name string, count int) int64 {
	t.Helper()

	var publisher = harness.newClient(t, name)
	publisher.SetFailedWriteHandler(&countingFailedWriteHandler{})
	var payload = strings.Repeat("x", 1024)

	var before runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	for index := 0; index < count; index++ {
		if err := publisher.Publish("orders", fmt.Sprintf(`{"id":%d,"p":"%s"}`, index, payload)); err != nil {
			t.Fatalf("publish %d failed: %v", index, err)
		}
	}

	var after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&after)
	return int64(after.HeapAlloc) - int64(before.HeapAlloc)
}

// Installing a FailedWriteHandler makes the client retain a cloned Command per
// publish so it can report which message failed. A publish that requests no acks
// can never be acknowledged, so without a bound the retention grows for the
// lifetime of the connection.
//
// The retention map is unexported, so this asserts the invariant an application
// can actually observe: retained memory must NOT scale with publish count.
// Publishing 4x as many messages must not retain ~4x the memory.
func TestPublishRetentionDoesNotScaleWithPublishCount(t *testing.T) {
	var harness = startDeliveryHarness(t)

	var small = retainedAfterPublishes(t, harness, "retention-small", 5000)
	var large = retainedAfterPublishes(t, harness, "retention-large", 20000)

	t.Logf("retained: 5000 publishes = %d bytes, 20000 publishes = %d bytes", small, large)

	// Unbounded retention shows up as ~4x the memory for 4x the publishes; a
	// bounded retention flattens out. Allow 2x for allocator noise.
	if small > 0 && large > small*2 {
		t.Fatalf("retention scales with publish count: 5000 publishes retained %d bytes, 20000 retained %d bytes (~%.1fx)",
			small, large, float64(large)/float64(small))
	}
}
