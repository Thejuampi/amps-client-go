package main

import (
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"
)

func TestCPPOOFOptionAndDeltaSubscriptions(t *testing.T) {
	var oldSOW, oldJournal, oldFanout = sow, journal, *flagFanout
	sow = newSOWCache()
	journal = newMessageJournal(100)
	*flagFanout = true
	t.Cleanup(func() { sow, journal, *flagFanout = oldSOW, oldJournal, oldFanout })

	var tests = []struct {
		name, command, options, action string
		wantOOF                        bool
	}{
		{"subscribe delete enabled", "subscribe", "oof", "delete", true},
		{"subscribe delete disabled", "subscribe", "", "delete", false},
		{"subscribe delete unrelated option", "subscribe", "not_oof", "delete", false},
		{"sow subscribe delete enabled", "sow_and_subscribe", "oof", "delete", true},
		{"sow subscribe delete disabled", "sow_and_subscribe", "", "delete", false},
		{"delta delete", "delta_subscribe", "", "delete", true},
		{"sow delta delete", "sow_and_delta_subscribe", "", "delete", true},
		{"subscribe mismatch enabled", "subscribe", "oof", "match", true},
		{"subscribe mismatch disabled", "subscribe", "", "match", false},
		{"sow subscribe mismatch enabled", "sow_and_subscribe", "replace,oof", "match", true},
		{"sow subscribe mismatch disabled", "sow_and_subscribe", "", "match", false},
		{"sow subscribe mismatch unrelated option", "sow_and_subscribe", "not_oof", "match", false},
		{"delta mismatch", "delta_subscribe", "", "match", true},
		{"sow delta mismatch", "sow_and_delta_subscribe", "", "match", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var topic = "cpp-oof-" + strings.ReplaceAll(test.name, " ", "-")
			sow.upsert(topic, "key-1", []byte(`{"id":1,"state":"active"}`), "bm", "ts", 1, 0)
			var server, client = net.Pipe()
			var done = make(chan struct{})
			go func() { defer close(done); handleConnection(server) }()
			t.Cleanup(func() { _ = client.Close(); <-done })

			var request = map[string]string{
				"c": test.command, "cid": "sub-command", "sub_id": "sub-1", "query_id": "query-1",
				"t": topic, "filter": `/state = 'active'`, "a": "processed,completed", "o": test.options,
			}
			var encoded, err = json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			sendFrame(t, client, buildCommandFrame(string(encoded), nil))
			cppOOFFlush(t, client, "before")

			var command = map[string]string{"cid": "change", "t": topic, "k": "key-1", "a": "processed"}
			var payload []byte
			if test.action == "delete" {
				command["c"] = "sow_delete"
			} else {
				command["c"] = "publish"
				payload = []byte(`{"id":1,"state":"inactive"}`)
			}
			encoded, err = json.Marshal(command)
			if err != nil {
				t.Fatal(err)
			}
			sendFrame(t, client, buildCommandFrame(string(encoded), payload))
			var headers = cppOOFFlush(t, client, "after")
			var count int
			for _, header := range headers {
				if header.c != "oof" {
					continue
				}
				count++
				if header.t != topic || header.k != "key-1" || header.subID != "sub-1" || header.subIDs != "sub-1" || header.reason != test.action {
					t.Fatalf("unexpected OOF header: %+v", header)
				}
			}
			var want int
			if test.wantOOF {
				want = 1
			}
			if count != want {
				t.Fatalf("received %d OOF messages, want %d", count, want)
			}
		})
	}
}

func cppOOFFlush(t *testing.T, conn net.Conn, id string) []headerFields {
	t.Helper()
	var request, err = json.Marshal(map[string]string{"c": "flush", "cid": id, "a": "processed"})
	if err != nil {
		t.Fatal(err)
	}
	sendFrame(t, conn, buildCommandFrame(string(request), nil))
	var headers []headerFields
	for {
		var body = readFrameBody(t, conn, time.Second)
		var header, _ = parseAMPSHeader([]byte(body))
		if header.c == "ack" && header.cid == id && header.a == "processed" {
			return headers
		}
		headers = append(headers, header)
	}
}
