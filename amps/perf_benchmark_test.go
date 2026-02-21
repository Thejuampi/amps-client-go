package amps

import (
	"bytes"
	"testing"
)

func benchmarkFrame(header *_Header, payload []byte) []byte {
	buffer := bytes.NewBuffer(nil)
	_, _ = buffer.WriteString("    ")
	_ = header.write(buffer)
	if len(payload) > 0 {
		_, _ = buffer.Write(payload)
	}
	length := uint32(buffer.Len() - 4)
	raw := buffer.Bytes()
	raw[0] = byte((length >> 24) & 0xFF)
	raw[1] = byte((length >> 16) & 0xFF)
	raw[2] = byte((length >> 8) & 0xFF)
	raw[3] = byte(length & 0xFF)
	return append([]byte(nil), raw...)
}

func BenchmarkHeaderHotWrite(b *testing.B) {
	command := CommandPublish
	ack := AckTypeProcessed | AckTypeCompleted
	expiration := uint(42)
	sequence := uint64(123456789)
	header := &_Header{
		command:    command,
		commandID:  []byte("cmd-1"),
		topic:      []byte("orders"),
		subID:      []byte("sub-1"),
		queryID:    []byte("qry-1"),
		options:    []byte("replace"),
		filter:     []byte("/id > 10"),
		ackType:    &ack,
		expiration: &expiration,
		sequenceID: &sequence,
	}

	buffer := bytes.NewBuffer(make([]byte, 0, 256))
	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		buffer.Reset()
		if err := header.write(buffer); err != nil {
			b.Fatalf("header write failed: %v", err)
		}
	}
}

func BenchmarkHeaderHotParse(b *testing.B) {
	header := &_Header{
		command:     CommandPublish,
		commandID:   []byte("cmd-1"),
		topic:       []byte("orders"),
		subID:       []byte("sub-1"),
		queryID:     []byte("qry-1"),
		options:     []byte("replace"),
		filter:      []byte("/id > 10"),
		leasePeriod: []byte("2026-01-01T00:00:00.000000Z"),
	}
	frame := benchmarkFrame(header, []byte(`{"id":1}`))
	msg := &Message{header: new(_Header)}

	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		if _, err := parseHeader(msg, true, frame[4:]); err != nil {
			b.Fatalf("parse header failed: %v", err)
		}
	}
}

func BenchmarkRouteHotSingle(b *testing.B) {
	client := NewClient("bench-route-single")
	client.routes.Store("sub-1", func(message *Message) error { return nil })
	message := &Message{
		header: &_Header{
			command: CommandPublish,
			subID:   []byte("sub-1"),
			topic:   []byte("orders"),
		},
		data: []byte(`{"id":1}`),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		if err := client.onMessage(message); err != nil {
			b.Fatalf("onMessage failed: %v", err)
		}
	}
}

func BenchmarkRouteHotMultiSIDs(b *testing.B) {
	client := NewClient("bench-route-multi")
	client.routes.Store("sub-1", func(message *Message) error { return nil })
	client.routes.Store("sub-2", func(message *Message) error { return nil })
	client.routes.Store("sub-3", func(message *Message) error { return nil })

	message := &Message{
		header: &_Header{
			command: CommandPublish,
			subIDs:  []byte("sub-1, sub-2, sub-3"),
			topic:   []byte("orders"),
		},
		data: []byte(`{"id":1}`),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		if err := client.onMessage(message); err != nil {
			b.Fatalf("onMessage failed: %v", err)
		}
	}
}

func BenchmarkReadHotFrameDecodeDispatch(b *testing.B) {
	client := NewClient("bench-read")
	client.routes.Store("sub-1", func(message *Message) error { return nil })
	frame := benchmarkFrame(&_Header{
		command: CommandPublish,
		subID:   []byte("sub-1"),
		topic:   []byte("orders"),
	}, []byte(`{"id":1}`))
	message := &Message{header: new(_Header)}

	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		left, err := parseHeader(message, true, frame[4:])
		if err != nil {
			b.Fatalf("parse header failed: %v", err)
		}
		message.data = left
		if err := client.onMessage(message); err != nil {
			b.Fatalf("onMessage failed: %v", err)
		}
	}
}

func BenchmarkStreamHotDequeue(b *testing.B) {
	stream := newMessageStream(nil)
	stream.setRunning()

	message := &Message{
		header: &_Header{
			command: CommandPublish,
			topic:   []byte("orders"),
		},
		data: []byte(`{"id":1}`),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		stream.queue.enqueue(message)
		if !stream.HasNext() {
			b.Fatalf("expected next message")
		}
		if next := stream.Next(); next == nil {
			b.Fatalf("expected non-nil message")
		}
	}
}

func BenchmarkStreamHotTimeout(b *testing.B) {
	stream := newMessageStream(nil)

	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		stream.timedOut.Store(true)
		if next := stream.Next(); next != nil {
			b.Fatalf("expected nil on timeout")
		}
	}
}
