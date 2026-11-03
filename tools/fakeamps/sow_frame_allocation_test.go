package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"strconv"
	"testing"
)

func TestSOWFrameExactWireBytes(t *testing.T) {
	var tests = []struct {
		name, topic, queryID, key, bookmark, messageType string
		payload                                          []byte
		header                                           string
	}{
		{
			name:  "all fields",
			topic: "orders", queryID: "q1", key: "k1", bookmark: "bm", messageType: "json",
			payload: []byte(`{"id":1}`),
			header:  `{"c":"sow","query_id":"q1","t":"orders"}{"t":"orders","query_id":"q1","k":"k1","bm":"bm","mt":"json","l":8}`,
		},
		{
			name:   "nil payload and empty fields",
			header: `{"c":"sow"}{"t":"","l":0}`,
		},
		{
			name: "empty payload", topic: "t", queryID: "q",
			payload: []byte{},
			header:  `{"c":"sow","query_id":"q","t":"t"}{"t":"t","query_id":"q","l":0}`,
		},
		{
			name: "escaped fields and UTF-8 payload", topic: `alpha"beta`, queryID: "q\"\\\n\r\t\x01", key: "k\x02", bookmark: `b\x`, messageType: `j"`,
			payload: []byte("é雪"),
			header:  `{"c":"sow","query_id":"q\"\\\n\r\t\u0001","t":"alpha\"beta"}{"t":"alpha\"beta","query_id":"q\"\\\n\r\t\u0001","k":"k\u0002","bm":"b\\x","mt":"j\"","l":5}`,
		},
		{
			name: "binary payload", topic: "binary",
			payload: []byte{0x00, 0xff, 0x80, '}', '\n'},
			header:  `{"c":"sow","t":"binary"}{"t":"binary","l":5}`,
		},
		{
			name: "multi-byte length prefix", topic: "large",
			payload: bytes.Repeat([]byte("x"), 256),
			header:  `{"c":"sow","t":"large"}{"t":"large","l":256}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var scratch bytes.Buffer
			var frame = buildSOWRecord(&scratch, test.topic, test.queryID, test.key, test.bookmark, test.messageType, test.payload)
			var expected = append([]byte(test.header), test.payload...)
			if len(frame) != len(expected)+4 {
				t.Fatalf("frame length = %d, want %d", len(frame), len(expected)+4)
			}
			if got := binary.BigEndian.Uint32(frame[:4]); got != uint32(len(expected)) {
				t.Fatalf("frame prefix = %d, want %d", got, len(expected))
			}
			if !bytes.Equal(frame[4:], expected) {
				t.Fatalf("frame body = %q, want %q", frame[4:], expected)
			}
			if scratch.Len() != 0 {
				t.Fatalf("scratch length after frame = %d, want 0", scratch.Len())
			}
		})
	}
}

func TestSOWFrameTopicJSONRoundTrip(t *testing.T) {
	var tests = []struct {
		name, topic string
	}{
		{name: "ASCII", topic: "orders"},
		{name: "empty"},
		{name: "quote", topic: `orders"west`},
		{name: "backslash", topic: `orders\archive`},
		{name: "backslash before JSON escape", topic: `orders\north`},
		{name: "newline", topic: "orders\nwest"},
		{name: "tab and return", topic: "\torders\r"},
		{name: "control boundary", topic: "\x00orders\x1f"},
		{name: "last control byte", topic: "orders\x1f"},
		{name: "Unicode", topic: "注文/雪🙂"},
		{name: "combined", topic: "雪\"\\\n\x01🙂"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var scratch bytes.Buffer
			var payload = []byte{0, 0xff, '{', '}'}
			var frame = buildSOWRecord(&scratch, test.topic, "q", "k", "bm", "json", payload)
			var decoder = json.NewDecoder(bytes.NewReader(frame[4:]))
			for index := 0; index < 2; index++ {
				var header struct {
					Topic string `json:"t"`
				}
				if err := decoder.Decode(&header); err != nil {
					t.Fatalf("header %d is not valid JSON: %v", index+1, err)
				}
				if header.Topic != test.topic {
					t.Fatalf("header %d topic = %q, want %q", index+1, header.Topic, test.topic)
				}
			}
			var remaining = frame[4+int(decoder.InputOffset()):]
			if !bytes.Equal(remaining, payload) {
				t.Fatalf("payload = %q, want %q", remaining, payload)
			}
		})
	}
}

func TestSOWFrameOwnsPayloadAndScratch(t *testing.T) {
	var scratch bytes.Buffer
	var payload = []byte("original payload")
	var frame = buildSOWRecord(&scratch, "orders", "q", "k", "bm", "json", payload)
	var expected = bytes.Clone(frame)
	copy(payload, "modified payload")
	_ = buildSOWRecord(&scratch, "different topic", "another query", "other key", "bookmark", "binary", bytes.Repeat([]byte("z"), 1000))
	if !bytes.Equal(frame, expected) {
		t.Fatalf("frame changed after modifying payload and reusing scratch: got %q, want %q", frame, expected)
	}
}

func TestSOWFrameScratchDoesNotGrowWithPayload(t *testing.T) {
	var scratch bytes.Buffer
	var payload = bytes.Repeat([]byte("x"), 1<<20)
	var frame = buildSOWRecord(&scratch, "orders", "q", "k", "bm", "json", payload)
	if len(frame) < len(payload) {
		t.Fatalf("frame length = %d, shorter than payload %d", len(frame), len(payload))
	}
	if scratch.Cap() > 4096 {
		t.Fatalf("scratch retained %d bytes for a %d-byte payload; want at most 4096", scratch.Cap(), len(payload))
	}
}

func TestSOWFrameAllocationBudget(t *testing.T) {
	var tests = []struct {
		name, topic string
	}{
		{name: "ASCII", topic: "orders"},
		{name: "escaped", topic: "orders\"\\\n\x1f"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var scratch bytes.Buffer
			var payload = bytes.Repeat([]byte("x"), 2500)
			var allocations = testing.AllocsPerRun(100, func() {
				sowFrameBenchmarkSink = buildSOWRecord(&scratch, test.topic, "q", "k", "bm", "json", payload)
			})
			if allocations > 1 {
				t.Fatalf("SOW frame allocated %.1f times, want at most 1", allocations)
			}
		})
	}
}

func TestSOWFrameBodyLength(t *testing.T) {
	var maxInt = int(^uint(0) >> 1)
	type lengthCase struct {
		name            string
		header, payload int
		want            uint32
		ok              bool
	}
	var tests = []lengthCase{
		{name: "empty", ok: true},
		{name: "header plus payload", header: 7, payload: 5, want: 12, ok: true},
		{name: "negative header", header: -1},
		{name: "negative payload", payload: -1},
	}
	if strconv.IntSize == 64 {
		var max = uint64(^uint32(0))
		tests = append(tests,
			lengthCase{name: "maximum body", header: int(max - 10), payload: 10, want: ^uint32(0), ok: true},
			lengthCase{name: "body over maximum", header: int(max - 10), payload: 11},
			lengthCase{name: "header over maximum", header: int(max + 1)},
			lengthCase{name: "payload over maximum", payload: int(max + 1)},
			lengthCase{name: "int maximum operands", header: maxInt, payload: maxInt},
		)
	} else {
		tests = append(tests, lengthCase{name: "int maximum operands", header: maxInt, payload: maxInt, want: ^uint32(0) - 1, ok: true})
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got, ok = sowFrameBodyLength(test.header, test.payload)
			if got != test.want || ok != test.ok {
				t.Fatalf("body length(%d, %d) = %d, %v; want %d, %v", test.header, test.payload, got, ok, test.want, test.ok)
			}
		})
	}
}

func TestSOWFrameAllocationSize(t *testing.T) {
	var max = int(^uint(0) >> 1)
	var tests = []struct {
		name            string
		header, payload int
		want            int
		tooLarge        bool
	}{
		{name: "empty", header: 4, want: 4},
		{name: "largest int", header: 4, payload: max - 4, want: max},
		{name: "one over largest int", header: 4, payload: max - 3, tooLarge: true},
		{name: "largest int plus one", header: max, payload: 1, tooLarge: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got int
			var panicValue any
			func() {
				defer func() { panicValue = recover() }()
				got = sowFrameAllocationSize(test.header, test.payload)
			}()
			if test.tooLarge {
				if panicValue != bytes.ErrTooLarge {
					t.Fatalf("panic = %v, want %v", panicValue, bytes.ErrTooLarge)
				}
			} else if panicValue != nil || got != test.want {
				t.Fatalf("size = %d, panic = %v; want %d, no panic", got, panicValue, test.want)
			}
		})
	}
}

var sowFrameBenchmarkSink []byte

func BenchmarkSOWFrameBuild(b *testing.B) {
	var tests = []struct {
		name string
		size int
	}{
		{name: "small", size: 8},
		{name: "stress", size: 2500},
		{name: "large", size: 64 << 10},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			var scratch bytes.Buffer
			var payload = bytes.Repeat([]byte("x"), test.size)
			var frame = buildSOWRecord(&scratch, "orders", "q", "k", "bm", "json", payload)
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				frame = buildSOWRecord(&scratch, "orders", "q", "k", "bm", "json", payload)
			}
			sowFrameBenchmarkSink = frame
		})
	}
}
