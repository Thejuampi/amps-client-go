package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestParseAMPSHeaderBasic(t *testing.T) {
	frame := []byte(`{"c":"publish","cid":"1","t":"orders","a":"processed","k":"order-1","mt":"json"}{"id":1}`)
	h, payload := parseAMPSHeader(frame)

	if h.c != "publish" || h.cid != "1" || h.t != "orders" {
		t.Fatalf("unexpected header fields: %+v", h)
	}
	if h.k != "order-1" || h.mt != "json" {
		t.Fatalf("unexpected key/message type fields: %+v", h)
	}
	if string(payload) != `{"id":1}` {
		t.Fatalf("unexpected payload: %s", string(payload))
	}
}

func TestParseAMPSHeaderExtendedFields(t *testing.T) {
	frame := []byte(`{"c":"sow","query_id":"q1","sub_id":"s1","filter":"/id > 1","top_n":"10","orderby":"amount","user_id":"u","pw":"p","sids":"a,b"}`)
	h, payload := parseAMPSHeader(frame)

	if h.c != "sow" || h.queryID != "q1" || h.subID != "s1" {
		t.Fatalf("unexpected parsed fields: %+v", h)
	}
	if h.filter == "" || h.topN != "10" || h.orderBy != "amount" {
		t.Fatalf("unexpected query fields: %+v", h)
	}
	if h.userID != "u" || h.pw != "p" || h.subIDs != "a,b" {
		t.Fatalf("unexpected auth/sub fields: %+v", h)
	}
	if len(payload) != 0 {
		t.Fatalf("expected empty payload for header-only frame")
	}
}

func TestParseAMPSHeaderReplicationAndAckFields(t *testing.T) {
	var frame = []byte(`{"c":"ack","cid":"r1","status":"failure","reason":"replica rejected","_repl":"node-a","_repl_sync":"1"}`)
	var h, payload = parseAMPSHeader(frame)

	if h.c != "ack" || h.cid != "r1" || h.status != "failure" || h.reason != "replica rejected" || h.repl != "node-a" || h.replSync != "1" {
		t.Fatalf("unexpected replication/ack fields: %+v", h)
	}
	if len(payload) != 0 {
		t.Fatalf("expected empty payload for header-only frame")
	}
}

func TestParseAMPSHeaderRedirectURIField(t *testing.T) {
	var frame = []byte(`{"c":"redirect","cid":"r2","uri":"tcp://127.0.0.1:19001/amps/json"}`)
	var h, payload = parseAMPSHeader(frame)

	if h.c != "redirect" || h.cid != "r2" || h.uri != "tcp://127.0.0.1:19001/amps/json" {
		t.Fatalf("unexpected redirect header fields: %+v", h)
	}
	if len(payload) != 0 {
		t.Fatalf("expected empty payload for header-only frame")
	}
}

func TestFinalizeFrameAndStartFrame(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	startFrame(buf)
	buf.WriteString(`{"c":"heartbeat"}`)
	frame := finalizeFrame(buf)

	if len(frame) < 4 {
		t.Fatalf("frame too short")
	}

	declared := binary.BigEndian.Uint32(frame[:4])
	actual := uint32(len(frame) - 4)
	if declared != actual {
		t.Fatalf("declared length=%d actual=%d", declared, actual)
	}
}

func TestBuildAckVariants(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	ack := buildAck(buf, "processed", "cid-1", "success")
	body := string(ack[4:])
	if !strings.Contains(body, `"a":"processed"`) || !strings.Contains(body, `"cid":"cid-1"`) {
		t.Fatalf("unexpected ack body: %s", body)
	}

	persisted := buildPersistedAck(buf, "cid-2", "42")
	pBody := string(persisted[4:])
	if !strings.Contains(pBody, `"a":"persisted"`) || !strings.Contains(pBody, `"s":42`) {
		t.Fatalf("unexpected persisted ack body: %s", pBody)
	}

	sowDelete := buildSOWDeleteAck(buf, "stats", "cid-3", 4, 8)
	dBody := string(sowDelete[4:])
	if !strings.Contains(dBody, `"records_deleted":4`) || !strings.Contains(dBody, `"topic_matches":8`) {
		t.Fatalf("unexpected sow delete ack body: %s", dBody)
	}
}

func TestBuildPublishAndSOWFrames(t *testing.T) {
	buf := bytes.NewBuffer(nil)

	publish := buildPublishDelivery(buf, "orders", "sub-1", []byte(`{"id":1}`), "bm", "ts", "k1", "json", false)
	publishBody := string(publish[4:])
	if !strings.Contains(publishBody, `"c":"p"`) || !strings.Contains(publishBody, `"t":"orders"`) {
		t.Fatalf("unexpected publish delivery body: %s", publishBody)
	}

	sowRec := buildSOWRecord(buf, "orders", "q1", "k1", "bm", "json", []byte(`{"id":1}`))
	sowBody := string(sowRec[4:])
	if !strings.Contains(sowBody, `"c":"sow"`) || !strings.Contains(sowBody, `"query_id":"q1"`) {
		t.Fatalf("unexpected sow record body: %s", sowBody)
	}

	oof := buildOOFDelivery(buf, "orders", "sub-1", "k1", "bm")
	oofBody := string(oof[4:])
	if !strings.Contains(oofBody, `"c":"oof"`) {
		t.Fatalf("unexpected oof body: %s", oofBody)
	}

	oofWithReason := buildOOFDeliveryWithReason(buf, "orders", "sub-1", "k1", "bm", "evicted")
	oofReasonBody := string(oofWithReason[4:])
	if !strings.Contains(oofReasonBody, `"reason":"evicted"`) {
		t.Fatalf("unexpected oof-with-reason body: %s", oofReasonBody)
	}
}

func TestBuildRedirectFrame(t *testing.T) {
	var buf = bytes.NewBuffer(nil)
	var frame = buildRedirectFrame(buf, "cid-redirect", "tcp://127.0.0.1:19001/amps/json")
	var body = string(frame[4:])

	if !strings.Contains(body, `"c":"redirect"`) || !strings.Contains(body, `"cid":"cid-redirect"`) || !strings.Contains(body, `"uri":"tcp://127.0.0.1:19001/amps/json"`) {
		t.Fatalf("unexpected redirect frame body: %s", body)
	}
}

func TestBuildGroupAndHeartbeatFrames(t *testing.T) {
	buf := bytes.NewBuffer(nil)

	begin := buildGroupBegin(buf, "q1")
	end := buildGroupEnd(buf, "q1")
	hb := buildHeartbeatFrame(buf)

	if !strings.Contains(string(begin[4:]), `"c":"group_begin"`) {
		t.Fatalf("unexpected group begin body: %s", string(begin[4:]))
	}
	if !strings.Contains(string(end[4:]), `"c":"group_end"`) {
		t.Fatalf("unexpected group end body: %s", string(end[4:]))
	}
	if len(hb) < 4 {
		t.Fatalf("heartbeat frame too short")
	}
}

func TestBuildLogonCompletedAndStatsAcks(t *testing.T) {
	buf := bytes.NewBuffer(nil)

	logon := buildLogonAck(buf, "cid-1", "client-a", "corr-1")
	if !strings.Contains(string(logon[4:]), `"c":"ack"`) {
		t.Fatalf("unexpected logon ack body: %s", string(logon[4:]))
	}

	completed := buildSOWCompletedAck(buf, "cid-2", "q1", 10, 1, 2, 3, 11)
	compBody := string(completed[4:])
	if !strings.Contains(compBody, `"a":"completed"`) || !strings.Contains(compBody, `"records_deleted":3`) {
		t.Fatalf("unexpected sow completed body: %s", compBody)
	}

	stats := &connStats{}
	stats.messagesIn.Store(5)
	stats.messagesOut.Store(7)
	statsAck := buildStatsAck(buf, "cid-3", stats)
	statsBody := string(statsAck[4:])
	if !strings.Contains(statsBody, `"a":"stats"`) {
		t.Fatalf("unexpected stats ack body: %s", statsBody)
	}
}

func TestSowKeyAndUtilityHelpers(t *testing.T) {
	key := makeSowKey("orders", 42)
	if !strings.Contains(key, "orders") {
		t.Fatalf("unexpected sow key format: %s", key)
	}

	if !containsToken("processed,completed", "processed") {
		t.Fatalf("expected token match")
	}
	if containsToken("processed,completed", "stats") {
		t.Fatalf("did not expect token match")
	}

	if firstNonEmpty("", "a", "b") != "a" {
		t.Fatalf("unexpected firstNonEmpty result")
	}
}

func TestWriteBufPool(t *testing.T) {
	buf := getWriteBuf()
	buf.WriteString("hello")
	putWriteBuf(buf)

	buf2 := getWriteBuf()
	if buf2.Len() != 0 {
		t.Fatalf("expected reset pooled buffer")
	}
	putWriteBuf(buf2)
}

func TestIsClosedError(t *testing.T) {
	if !isClosedError(errors.New("use of closed network connection")) {
		t.Fatalf("expected closed network connection to be recognized")
	}
	if !isClosedError(errors.New("connection reset by peer")) {
		t.Fatalf("expected connection reset to be recognized")
	}
	if isClosedError(errors.New("some other error")) {
		t.Fatalf("did not expect generic error to be treated as closed")
	}
}
