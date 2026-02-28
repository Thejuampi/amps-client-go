package amps

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
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

func benchmarkFrameFixed(header *_Header, payload []byte) []byte {
	buf := getJsonBuffer(64)
	n := 0
	buf[n] = ' '
	buf[n+1] = ' '
	buf[n+2] = ' '
	buf[n+3] = ' '
	n += 4
	hdr := *header
	if hdr.topic != nil {
		copy(buf[n:], `"t":`)
		n += 4
		copy(buf[n:], hdr.topic)
		n += len(hdr.topic)
	}
	buf[n] = '}'
	length := uint32(n + 1 - 4)
	buf[0] = byte((length >> 24) & 0xFF)
	buf[1] = byte((length >> 16) & 0xFF)
	buf[2] = byte((length >> 8) & 0xFF)
	buf[3] = byte(length & 0xFF)
	return buf[:n+1]
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

func BenchmarkMessageCopy(b *testing.B) {
	original := &Message{
		header: &_Header{
			command:   CommandPublish,
			commandID: []byte("cmd-1"),
			topic:     []byte("orders"),
			subID:     []byte("sub-1"),
			queryID:   []byte("qry-1"),
			options:   []byte("replace"),
			filter:    []byte("/id > 10"),
		},
		data: []byte(`{"id":1,"name":"test","value":123.45,"active":true}`),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		_ = original.Copy()
	}
}

func BenchmarkHeaderReset(b *testing.B) {
	header := &_Header{
		command:    CommandPublish,
		commandID:  []byte("cmd-1"),
		topic:      []byte("orders"),
		subID:      []byte("sub-1"),
		expiration: new(uint),
	}

	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		header.reset()
	}
}

func BenchmarkMessageReset(b *testing.B) {
	msg := &Message{
		header: &_Header{
			command:   CommandPublish,
			commandID: []byte("cmd-1"),
		},
		data: []byte(`{"id":1}`),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		msg.reset()
	}
}

func BenchmarkUnsafeStringFromBytes(b *testing.B) {
	data := []byte("sub-1")

	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		_ = unsafeStringFromBytes(data)
	}
}

func BenchmarkTrimASCIISpaces(b *testing.B) {
	data := []byte("  sub-1,  sub-2, sub-3  ")

	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		_ = trimASCIISpaces(data)
	}
}

func BenchmarkSyncMapLoad(b *testing.B) {
	syncMap := new(sync.Map)
	syncMap.Store("sub-1", func(message *Message) error { return nil })
	syncMap.Store("sub-2", func(message *Message) error { return nil })
	syncMap.Store("sub-3", func(message *Message) error { return nil })

	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		if _, ok := syncMap.Load("sub-2"); !ok {
			b.Fatalf("expected to find key")
		}
	}
}

func BenchmarkSyncMapStore(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		syncMap := new(sync.Map)
		syncMap.Store("sub-1", func(message *Message) error { return nil })
		syncMap.Store("sub-2", func(message *Message) error { return nil })
		syncMap.Store("sub-3", func(message *Message) error { return nil })
	}
}

func BenchmarkPublishSendFull(b *testing.B) {
	client := NewClient("bench-publish")
	client.sendBuffer = bytes.NewBuffer(make([]byte, 0, 256))

	command := &Command{header: new(_Header)}
	command.header.command = CommandPublish
	command.header.topic = []byte("orders")
	command.header.commandID = []byte("cmd-1")
	command.data = []byte(`{"id":1}`)

	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		client.sendBuffer.Reset()
		command.header.expiration = nil

		_, _ = client.sendBuffer.WriteString("    ")
		_ = command.header.write(client.sendBuffer)
		if command.data != nil {
			_, _ = client.sendBuffer.Write(command.data)
		}

		frameSize := client.sendBuffer.Len()
		length := uint32(frameSize - 4)
		rawBytes := client.sendBuffer.Bytes()
		rawBytes[0] = (byte)((length & 0xFF000000) >> 24)
		rawBytes[1] = (byte)((length & 0x00FF0000) >> 16)
		rawBytes[2] = (byte)((length & 0x0000FF00) >> 8)
		rawBytes[3] = (byte)(length & 0x000000FF)
	}
}

func BenchmarkSOWBatchParse(b *testing.B) {
	frame := benchmarkFrameFixed(&_Header{
		command: CommandSOW,
		topic:   []byte("orders"),
		subID:   []byte("sub-1"),
	}, []byte(`{"id":1}`))

	msg := &Message{header: new(_Header)}

	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		for i := 0; i < 5; i++ {
			left, err := parseHeader(msg, true, frame[4:])
			if err != nil {
				b.Fatalf("parse header failed: %v", err)
			}
			msg.data = left
			_ = left
		}
	}
}

func BenchmarkRouteHotManySubscriptions(b *testing.B) {
	client := NewClient("bench-route-many")
	for i := 0; i < 50; i++ {
		client.routes.Store(fmt.Sprintf("sub-%d", i), func(message *Message) error { return nil })
	}

	message := &Message{
		header: &_Header{
			command: CommandPublish,
			subID:   []byte("sub-25"),
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

func BenchmarkStreamConcurrentDequeue(b *testing.B) {
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

	var wg sync.WaitGroup
	producer := func() {
		for i := 0; i < b.N; i++ {
			stream.queue.enqueue(message)
		}
		wg.Done()
	}
	consumer := func() {
		for i := 0; i < b.N; i++ {
			for !stream.HasNext() {
				runtime.Gosched()
			}
			_ = stream.Next()
		}
		wg.Done()
	}

	b.ResetTimer()
	wg.Add(2)
	go producer()
	go consumer()
	wg.Wait()
}

func BenchmarkParseUintBytes(b *testing.B) {
	data := []byte("1234567890")

	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		_, _ = parseUintBytes(data)
	}
}

func BenchmarkParseUint32Value(b *testing.B) {
	data := []byte("1234567890")

	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		_, _ = parseUint32Value(data)
	}
}

func BenchmarkAckToString(b *testing.B) {
	ack := AckTypeProcessed | AckTypeCompleted | AckTypePersisted

	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		_ = ackToString(ack)
	}
}
