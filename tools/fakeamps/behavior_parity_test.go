package main

import (
	"encoding/json"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestParseAMPSHeaderJSONParity(t *testing.T) {
	var tests = []struct {
		name  string
		frame string
		want  headerFields
		data  string
	}{
		{"escapes", `{"c":"publish","t":"a\\b\"c","filter":"/name = 'Jos\u00e9'","pw":"a\/b\t\n"}payload`, headerFields{c: "publish", t: "a\\b\"c", filter: "/name = 'José'", pw: "a/b\t\n"}, "payload"},
		{"escaped key", `{"\u0063":"publish","\u0074":"orders"}`, headerFields{c: "publish", t: "orders"}, ""},
		{"unknown keys", `{"c":"publish","subtle":"wrong","topxx":4,"querying":"wrong","client_fake":"wrong","otherxx":"wrong","t":"orders"}`, headerFields{c: "publish", t: "orders"}, ""},
		{"unknown object", `{"c":"publish","future":{"t":"wrong","items":[1,{"c":"wrong"}]},"t":"orders"}{"id":1}`, headerFields{c: "publish", t: "orders"}, `{"id":1}`},
		{"whitespace", " \t{\"c\"\t:\t\"publish\",\"s\": 42 }body", headerFields{c: "publish", s: "42"}, "body"},
		{"surrogate pair", `{"c":"publish","t":"\ud83d\ude80"}`, headerFields{c: "publish", t: "🚀"}, ""},
		{"numeric scalar", `{"c":"publish","s":1.25e2}`, headerFields{c: "publish", s: "1.25e2"}, ""},
		{"zero scalar", `{"c":"publish","s":0}`, headerFields{c: "publish", s: "0"}, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got, data = parseAMPSHeader([]byte(test.frame))
			if got != test.want || string(data) != test.data {
				t.Fatalf("header=%+v data=%q, want header=%+v data=%q", got, data, test.want, test.data)
			}
		})
	}
}

func TestParseAMPSHeaderRejectsMalformedJSON(t *testing.T) {
	var tests = []string{
		`{"c" "publish"}`,
		`{"c":"publish" "t":"orders"}`,
		`{"c":"publish",}`,
		`{,"c":"publish"}`,
		`{"c":"publish","s":01}`,
		`{"c":"publish","t":"bad\x41"}`,
		"{\"c\":\"publish\",\"t\":\"bad\nvalue\"}",
		`{"c":"publish","future":{"x":]}}`,
	}
	for _, frame := range tests {
		t.Run(frame, func(t *testing.T) {
			var got, data = parseAMPSHeader([]byte(frame))
			if got != (headerFields{}) || data != nil {
				t.Fatalf("accepted malformed header: %+v, payload=%q", got, data)
			}
		})
	}
}

func TestUnknownCommandWithStatsReturnsFailure(t *testing.T) {
	var server, client = net.Pipe()
	var done = make(chan struct{})
	go func() { defer close(done); handleConnection(server) }()
	t.Cleanup(func() { _ = client.Close(); <-done })
	sendFrame(t, client, buildCommandFrame(`{"c":"unknown","cid":"bad-command","a":"stats"}`, nil))
	var body = readFrameBody(t, client, time.Second)
	var got struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "failure" || got.Reason != "unknown command" {
		t.Fatalf("unsupported command reported success: %s", body)
	}
}

func TestCPPQueueAcknowledgementsReleaseEveryBatch(t *testing.T) {
	var oldQueue = *flagQueue
	var oldSOW = sow
	sow = newSOWCache()
	*flagQueue = true
	queueLeasesMu.Lock()
	var oldLeases = queueLeases
	queueLeases = make(map[string]*queueLease)
	queueLeasesMu.Unlock()
	t.Cleanup(func() {
		*flagQueue = oldQueue
		sow = oldSOW
		queueLeasesMu.Lock()
		queueLeases = oldLeases
		queueLeasesMu.Unlock()
	})
	addQueueLease("queue://audit", "sub-1", "audit-1", "1|1|", nil, "", "json", time.Hour)
	addQueueLease("queue://audit", "sub-1", "audit-2", "1|2|", nil, "", "json", time.Hour)
	addQueueLease("queue://other", "sub-2", "audit-other", "1|3|", nil, "", "json", time.Hour)
	queueLeasesMu.Lock()
	queueLeases["nil-lease"] = nil
	queueLeasesMu.Unlock()
	sow.upsertWithEvicted("queue://audit", "audit-1", []byte(`{"id":1}`), "1|1|", "", 1, 0)
	sow.upsertWithEvicted("queue://audit", "audit-2", []byte(`{"id":2}`), "1|2|", "", 2, 0)
	var server, client = net.Pipe()
	var done = make(chan struct{})
	go func() { defer close(done); handleConnection(server) }()
	t.Cleanup(func() { _ = client.Close(); <-done })
	for index, bookmark := range []string{"1|1|", "1|2|,1|3|", "unknown"} {
		// C++ deliberately uses this same command id for every queue ack.
		var header = `{"c":"sow_delete","cid":"AMPS-queue-ack","t":"queue://audit","bm":"` + bookmark + `","a":"processed,stats,persisted","s":7}`
		sendFrame(t, client, buildCommandFrame(header, nil))
		for _, ackType := range []string{"processed", "stats", "persisted"} {
			var body = readFrameBody(t, client, time.Second)
			var ack struct {
				Type    string `json:"a"`
				Status  string `json:"status"`
				Deleted int    `json:"records_deleted"`
				Seq     int    `json:"s"`
			}
			if err := json.Unmarshal([]byte(body), &ack); err != nil {
				t.Fatal(err)
			}
			var wantDeleted = 1
			if index == 2 || ackType == "persisted" {
				wantDeleted = 0
			}
			if ack.Type != ackType || ack.Status != "success" || ack.Deleted != wantDeleted || (ackType == "persisted" && ack.Seq != 7) {
				t.Fatalf("incorrect queue ack result: %s", body)
			}
		}
	}
	if getQueueLease("audit-1") != nil || getQueueLease("audit-2") != nil || getQueueLease("audit-other") == nil || sow.count("queue://audit") != 0 {
		t.Fatal("C++ sow_delete acknowledgements must release both batches and preserve other topics")
	}
}

func TestCPPQueueAckCancelPreservesMessage(t *testing.T) {
	var oldQueue, oldSOW = *flagQueue, sow
	*flagQueue = true
	sow = newSOWCache()
	queueLeasesMu.Lock()
	var oldLeases = queueLeases
	queueLeases = make(map[string]*queueLease)
	queueLeasesMu.Unlock()
	queuePendingMu.Lock()
	var oldPending = queuePending
	queuePending = make(map[string][]*queuePendingMessage)
	queuePendingMu.Unlock()
	t.Cleanup(func() {
		*flagQueue, sow = oldQueue, oldSOW
		queueLeasesMu.Lock()
		queueLeases = oldLeases
		queueLeasesMu.Unlock()
		queuePendingMu.Lock()
		queuePending = oldPending
		queuePendingMu.Unlock()
	})
	var topic = "queue://audit-cancel"
	addQueueLease(topic, "sub-1", "cancel-key", "1|1|", []byte(`{"id":1}`), "", "json", time.Hour)
	sow.upsertWithEvicted(topic, "cancel-key", []byte(`{"id":1}`), "1|1|", "", 1, 0)
	var server, client = net.Pipe()
	var done = make(chan struct{})
	go func() { defer close(done); handleConnection(server) }()
	t.Cleanup(func() { _ = client.Close(); <-done })
	sendFrame(t, client, buildCommandFrame(`{"c":"sow_delete","cid":"AMPS-queue-ack","t":"queue://audit-cancel","bm":"1|1|","opts":"cancel","a":"processed"}`, nil))
	_ = readFrameBody(t, client, time.Second)
	queuePendingMu.Lock()
	var pending = len(queuePending[topic])
	queuePendingMu.Unlock()
	if pending != 1 || sow.count(topic) != 1 {
		t.Fatalf("cancel must preserve and requeue message: pending=%d SOW=%d", pending, sow.count(topic))
	}
}

func TestAcknowledgeQueueBookmarksEmpty(t *testing.T) {
	if got := acknowledgeQueueBookmarks("queue://audit", " , ", false); got != 0 {
		t.Fatalf("empty batch released %d messages", got)
	}
}

func FuzzParseAMPSHeaderJSONParity(f *testing.F) {
	f.Add("orders", "command-1", `a\b"c`, []byte("payload"))
	f.Add("🚀", "", "\x00\n\t", []byte{0, 255, '}'})
	f.Fuzz(func(t *testing.T, topic string, id string, filter string, payload []byte) {
		var input = map[string]string{"c": "publish", "t": topic, "cid": id, "filter": filter}
		var encoded, err = json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		var normalized map[string]string
		if err := json.Unmarshal(encoded, &normalized); err != nil {
			t.Fatal(err)
		}
		var got, data = parseAMPSHeader(append(encoded, payload...))
		var fields = map[string]string{"c": got.c, "t": got.t, "cid": got.cid, "filter": got.filter}
		if !reflect.DeepEqual(fields, normalized) || string(data) != string(payload) {
			t.Fatalf("JSON round trip differs: fields=%q want=%q payload=%x want=%x", fields, normalized, data, payload)
		}
	})
}

var benchmarkHeader headerFields

func BenchmarkFakeAMPSHeader(b *testing.B) {
	var frames = []struct{ name, frame string }{
		{"plain", `{"c":"publish","cid":"123","t":"orders","s":42,"mt":"json"}{"id":1}`},
		{"escaped", `{"c":"publish","cid":"123","t":"orders\\new","filter":"/name = 'Jos\u00e9'"}{"id":1}`},
	}
	for _, frame := range frames {
		b.Run(frame.name, func(b *testing.B) {
			var input = []byte(frame.frame)
			b.ReportAllocs()
			for b.Loop() {
				benchmarkHeader, _ = parseAMPSHeader(input)
			}
		})
	}
}

func BenchmarkFakeAMPSQueueAckBatch(b *testing.B) {
	var topic = "queue://audit-benchmark"
	var leases = make([]*queueLease, 1024)
	var bookmarks = make([]string, 64)
	queueLeasesMu.Lock()
	var previousLeases = queueLeases
	queueLeases = make(map[string]*queueLease, len(leases))
	for index := range leases {
		var bookmark = "1|" + strconv.Itoa(index) + "|"
		leases[index] = &queueLease{topic: topic, sowKey: bookmark, bookmark: bookmark}
		queueLeases[bookmark] = leases[index]
		if index < len(bookmarks) {
			bookmarks[index] = bookmark
		}
	}
	queueLeasesMu.Unlock()
	var previousSOW = sow
	sow = nil
	b.Cleanup(func() {
		queueLeasesMu.Lock()
		queueLeases = previousLeases
		queueLeasesMu.Unlock()
		sow = previousSOW
	})
	var batch = strings.Join(bookmarks, ",")
	b.ReportAllocs()
	for b.Loop() {
		queueLeasesMu.Lock()
		for _, lease := range leases[:len(bookmarks)] {
			queueLeases[lease.bookmark] = lease
		}
		queueLeasesMu.Unlock()
		if got := acknowledgeQueueBookmarks(topic, batch, false); got != len(bookmarks) {
			b.Fatalf("released %d bookmarks, want %d", got, len(bookmarks))
		}
	}
}
