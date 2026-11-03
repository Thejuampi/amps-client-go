package amps

import (
	"fmt"
	"testing"
)

func TestMessageQueueResizePreservesOnlyLiveFIFO(t *testing.T) {
	var cases = []struct {
		name     string
		capacity uint64
		first    uint64
		length   uint64
	}{
		{"zero-capacity-empty", 0, 0, 0},
		{"empty", 5, 0, 0},
		{"singleton-full", 1, 0, 1},
		{"partial-start", 5, 0, 2},
		{"partial-middle", 5, 1, 2},
		{"partial-end", 5, 4, 1},
		{"partial-wrapped", 5, 3, 4},
		{"full-unwrapped", 4, 0, 4},
		{"full-wrapped", 5, 4, 5},
		{"non-power-of-two-wrapped", 7, 6, 3},
		{"large-full-wrapped", 65536, 65535, 65536},
		{"large-partial-wrapped", 65537, 65530, 65535},
	}
	for _, entry := range cases {
		t.Run(entry.name, func(t *testing.T) {
			var queue = newQueue(entry.capacity)
			var poison = &Message{data: []byte("stale")}
			for index := range queue.ring {
				queue.ring[index] = poison
			}
			var expected = make([]*Message, entry.length)
			for index := uint64(0); index < entry.length; index++ {
				expected[index] = &Message{data: []byte(fmt.Sprintf("id-%d", index))}
				queue.ring[(entry.first+index)%entry.capacity] = expected[index]
			}
			queue._length = entry.length
			queue.first = entry.first
			if entry.length > 0 {
				queue.last = (entry.first + entry.length - 1) % entry.capacity
			}
			queue.resize()
			var expectedLast uint64
			if entry.length > 0 {
				expectedLast = entry.length - 1
			}
			if queue.capacity != entry.capacity*2 || uint64(len(queue.ring)) != entry.capacity*2 || queue._length != entry.length || queue.first != 0 || queue.last != expectedLast {
				t.Fatalf("capacity=%d ring=%d length=%d first=%d last=%d", queue.capacity, len(queue.ring), queue._length, queue.first, queue.last)
			}
			for index, want := range expected {
				if queue.ring[index] != want {
					t.Fatalf("ring[%d] = %p, want original %p", index, queue.ring[index], want)
				}
			}
			for index := entry.length; index < queue.capacity; index++ {
				if queue.ring[index] != nil {
					t.Fatalf("unused ring[%d] retained %p", index, queue.ring[index])
				}
			}
			if queue.capacity == 0 {
				return
			}
			var added = &Message{data: []byte("after-resize")}
			queue.enqueue(added)
			expected = append(expected, added)
			for index, want := range expected {
				var got, ok = queue.tryDequeue()
				if !ok || got != want {
					t.Fatalf("dequeue[%d] = %p, %t; want original %p", index, got, ok, want)
				}
			}
			if got, ok := queue.tryDequeue(); ok || got != nil {
				t.Fatalf("empty dequeue = %p, %t", got, ok)
			}
		})
	}
}

var benchmarkMessageQueueResizeSink []*Message

func BenchmarkMessageQueueResize(b *testing.B) {
	for _, capacity := range []uint64{1024, 16384, 1048576} {
		for _, wrapped := range []bool{false, true} {
			b.Run(fmt.Sprintf("capacity=%d/wrapped=%t", capacity, wrapped), func(b *testing.B) {
				var ring = make([]*Message, capacity)
				var message = &Message{}
				for index := range ring {
					ring[index] = message
				}
				var first uint64
				if wrapped {
					first = capacity / 2
				}
				var last = (first + capacity - 1) % capacity
				b.ReportAllocs()
				b.ResetTimer()
				for index := 0; index < b.N; index++ {
					var queue = _MessageQueue{capacity: capacity, _length: capacity, first: first, last: last, ring: ring}
					queue.resize()
					benchmarkMessageQueueResizeSink = queue.ring
				}
			})
		}
	}
}
