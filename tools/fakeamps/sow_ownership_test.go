package main

import (
	"bytes"
	"testing"
)

func TestSOWRecordsOwnTheirBytesAndPreserveFIFO(t *testing.T) {
	var writer = &connWriter{ch: make(chan []byte, 3), done: make(chan struct{}), stats: new(connStats)}
	var scratch, expectedBuffer bytes.Buffer
	var firstPayload = []byte(`{"record":"first"}`)
	var secondPayload = []byte(`{"record":"second"}`)
	var first = append([]byte(nil), buildSOWRecord(&expectedBuffer, "orders", "q", "k1", "bm1", "json", firstPayload)...)
	var second = append([]byte(nil), buildSOWRecord(&expectedBuffer, "orders", "q", "k2", "bm2", "json", secondPayload)...)
	var ack = append([]byte(nil), buildSOWCompletedAck(&expectedBuffer, "cid", "q", 2, 0, 0, 0, 1)...)

	sendSOWRecord(writer, &scratch, "orders", "q", "k1", "bm1", "json", firstPayload)
	copy(firstPayload, []byte(`{"record":"xxxxx"}`))
	sendSOWRecord(writer, &scratch, "orders", "q", "k2", "bm2", "json", secondPayload)
	copy(secondPayload, []byte(`{"record":"xxxxxx"}`))
	writer.sendDirect(ack)
	_ = buildSOWRecord(&scratch, "other", "other-query", "other-key", "other-bm", "json", []byte(`{"overwritten":true}`))
	close(writer.ch)
	var output bytes.Buffer
	writer.run(&output)
	var expected = append(append(first, second...), ack...)
	if !bytes.Equal(output.Bytes(), expected) {
		t.Fatalf("queued SOW bytes or ordering changed: got %q, want %q", output.Bytes(), expected)
	}
}

func TestBorrowedWriterBytesAreStillCopied(t *testing.T) {
	var writer = &connWriter{ch: make(chan []byte, 1)}
	var borrowed = []byte("original")
	writer.send(borrowed)
	copy(borrowed, "changed!")
	if got := <-writer.ch; string(got) != "original" {
		t.Fatalf("borrowed bytes changed after enqueue: %q", got)
	}
}

func TestSOWRecordEnqueueAllocationBudget(t *testing.T) {
	var writer = &connWriter{ch: make(chan []byte, 1)}
	var scratch bytes.Buffer
	var payload = []byte(`{"id":1}`)
	var allocations = testing.AllocsPerRun(100, func() {
		sendSOWRecord(writer, &scratch, "orders", "q", "k", "bm", "json", payload)
		<-writer.ch
	})
	if allocations > 1 {
		t.Fatalf("SOW frame enqueue allocated %.1f times, want at most 1", allocations)
	}
}

func BenchmarkSOWRecordEnqueue(b *testing.B) {
	var writer = &connWriter{ch: make(chan []byte, 1)}
	var scratch bytes.Buffer
	var payload = append([]byte(`{"value":"`), bytes.Repeat([]byte("x"), 2500)...)
	payload = append(payload, '"', '}')
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		sendSOWRecord(writer, &scratch, "orders", "q", "k", "bm", "json", payload)
		<-writer.ch
	}
}
