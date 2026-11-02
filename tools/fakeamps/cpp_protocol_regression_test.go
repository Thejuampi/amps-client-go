package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestCPPDeliveryRoutingFieldsAndEscapedHeaders(t *testing.T) {
	var tests = []struct {
		name    string
		command string
		topic   string
		subID   string
		queue   bool
	}{
		{name: "publish", command: "p", topic: "orders", subID: "sub-1"},
		{name: "queue publish", command: "p", topic: "queue://orders", subID: "queue-sub", queue: true},
		{name: "escaped publish", command: "p", topic: "orders\"\\\n\x01雪", subID: "sub\"\\\n\x02雪"},
		{name: "out of focus", command: "oof", topic: "orders", subID: "sub-1"},
		{name: "escaped out of focus", command: "oof", topic: "orders\"\\\n\x01雪", subID: "sub\"\\\n\x02雪"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			var frame []byte
			var payload = []byte(`{"id":1}`)
			if test.command == "p" {
				frame = buildPublishDelivery(&buf, test.topic, test.subID, payload, "bm", "ts", "key", "json", test.queue)
			} else {
				frame = buildOOFDeliveryWithReason(&buf, test.topic, test.subID, "key", "bm", "match")
				payload = nil
			}
			if int(binary.BigEndian.Uint32(frame[:4])) != len(frame)-4 {
				t.Fatalf("incorrect frame length: %d bytes", len(frame))
			}
			var decoder = json.NewDecoder(bytes.NewReader(frame[4:]))
			var header map[string]string
			if err := decoder.Decode(&header); err != nil {
				t.Fatalf("invalid delivery header %q: %v", frame[4:], err)
			}
			if header["c"] != test.command || header["t"] != test.topic || header["sids"] != test.subID || header["sub_id"] != test.subID {
				t.Fatalf("delivery header = %#v, want command=%q topic=%q sids=sub_id=%q", header, test.command, test.topic, test.subID)
			}
			if test.queue && header["lp"] == "" {
				t.Fatalf("queue delivery has no lease period: %#v", header)
			}
			if !bytes.Equal(frame[4+int(decoder.InputOffset()):], payload) {
				t.Fatalf("delivery payload changed: %q", frame[4+int(decoder.InputOffset()):])
			}
		})
	}
}

var cppDeliveryFrameSink []byte

func TestCPPDeliveryAllocationBudget(t *testing.T) {
	for _, topic := range []string{"orders", "orders\"\\\n"} {
		t.Run(topic, func(t *testing.T) {
			var buf bytes.Buffer
			var payload = []byte(`{"id":1}`)
			var allocations = testing.AllocsPerRun(100, func() {
				cppDeliveryFrameSink = buildPublishDelivery(&buf, topic, "sub-1", payload, "bm", "ts", "key", "json", false)
			})
			if allocations > 1 {
				t.Fatalf("delivery frame allocated %.1f times, want at most one", allocations)
			}
		})
	}
}

func BenchmarkCPPDeliveryFrame(b *testing.B) {
	var buf bytes.Buffer
	var payload = []byte(`{"id":1}`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cppDeliveryFrameSink = buildPublishDelivery(&buf, "orders", "sub-1", payload, "bm", "ts", "key", "json", false)
	}
}

func TestCPPBookmarkSubscribeReplayByBookmarkField(t *testing.T) {
	var previousJournal = journal
	t.Cleanup(func() { journal = previousJournal })

	var tests = []struct {
		name          string
		command       string
		bookmark      string
		shortBookmark bool
		options       string
		wantPayloads  []string
		wantCompleted bool
	}{
		{name: "epoch", command: "subscribe", bookmark: "0", wantPayloads: []string{`{"id":1}`, `{"id":2}`}, wantCompleted: true},
		{name: "short epoch", command: "subscribe", bookmark: "0", shortBookmark: true, wantPayloads: []string{`{"id":1}`, `{"id":2}`}, wantCompleted: true},
		{name: "after first", command: "subscribe", bookmark: "first", options: "replace", wantPayloads: []string{`{"id":2}`}, wantCompleted: true},
		{name: "now", command: "subscribe", bookmark: "0|1|", wantCompleted: true},
		{name: "legacy option", command: "subscribe", options: "bookmark", wantPayloads: []string{`{"id":1}`, `{"id":2}`}, wantCompleted: true},
		{name: "plain subscription", command: "subscribe"},
		{name: "delta epoch", command: "delta_subscribe", bookmark: "0", wantPayloads: []string{`{"id":1}`, `{"id":2}`}, wantCompleted: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			journal = newMessageJournal(10)
			var firstBookmark, _ = journal.append("cpp-bookmark", "1", []byte(`{"id":1}`))
			journal.append("other-topic", "3", []byte(`{"id":3}`))
			journal.append("cpp-bookmark", "2", []byte(`{"id":2}`))
			var bookmark = test.bookmark
			if bookmark == "first" {
				bookmark = firstBookmark
			}

			var server, client = net.Pipe()
			var done = make(chan struct{})
			go func() { defer close(done); handleConnection(server) }()
			t.Cleanup(func() { _ = client.Close(); <-done })

			var request = map[string]string{
				"c": test.command, "cid": "sub-command", "sub_id": "sub-1", "t": "cpp-bookmark", "a": "processed,completed",
			}
			if bookmark != "" {
				var field = "bookmark"
				if test.shortBookmark {
					field = "bm"
				}
				request[field] = bookmark
			}
			if test.options != "" {
				request["o"] = test.options
			}
			var encoded, err = json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			sendFrame(t, client, buildCommandFrame(string(encoded), nil))
			sendFrame(t, client, buildCommandFrame(`{"c":"flush","cid":"barrier","a":"processed"}`, nil))

			var payloads []string
			var completed bool
			for {
				var frame = readFrameBody(t, client, time.Second)
				var header, payload = parseAMPSHeader([]byte(frame))
				if header.c == "ack" && header.cid == "barrier" && header.a == "processed" {
					break
				}
				if header.c == "p" {
					payloads = append(payloads, string(payload))
				} else if header.c == "ack" && header.cid == "sub-command" && header.a == "completed" {
					completed = true
				}
			}
			if !reflect.DeepEqual(payloads, test.wantPayloads) || completed != test.wantCompleted {
				t.Fatalf("replay payloads=%v completed=%t, want payloads=%v completed=%t", payloads, completed, test.wantPayloads, test.wantCompleted)
			}
		})
	}
}
