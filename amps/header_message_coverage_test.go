package amps

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestHeaderParseAndWriteCoverage(t *testing.T) {
	if _, ok := parseUintBytes(nil); ok {
		t.Fatalf("empty parseUintBytes should fail")
	}
	if _, ok := parseUintBytes([]byte("12a")); ok {
		t.Fatalf("non-digit parseUintBytes should fail")
	}
	if _, ok := parseUintBytes([]byte("18446744073709551616")); ok {
		t.Fatalf("overflow parseUintBytes should fail")
	}
	if value, ok := parseUintBytes([]byte("42")); !ok || value != 42 {
		t.Fatalf("unexpected parseUintBytes value=%d ok=%v", value, ok)
	}
	if _, ok := parseUint32Value([]byte("4294967296")); ok {
		t.Fatalf("overflow parseUint32Value should fail")
	}
	if value, ok := parseUint64Value([]byte("18446744073709551615")); !ok || value == 0 {
		t.Fatalf("expected parseUint64Value success, got value=%d ok=%v", value, ok)
	}

	if ack := parseAckBytes([]byte("received,parsed,processed,persisted,completed,stats,unknown")); ack != (AckTypeReceived|AckTypeParsed|AckTypeProcessed|AckTypePersisted|AckTypeCompleted|AckTypeStats) {
		t.Fatalf("unexpected parsed ack bitset: %d", ack)
	}
	if value := ackToString(AckTypeReceived | AckTypeCompleted | AckTypeStats); value != "received,completed,stats" {
		t.Fatalf("unexpected ackToString value: %q", value)
	}
	if value := ackToString(AckTypeReceived | AckTypeParsed | AckTypeProcessed | AckTypePersisted | AckTypeCompleted | AckTypeStats); value != "received,parsed,processed,persisted,completed,stats" {
		t.Fatalf("unexpected full ackToString value: %q", value)
	}
	if value := ackToString(AckTypeNone); value != "" {
		t.Fatalf("unexpected empty ackToString value: %q", value)
	}
	if value := stringToAck("received,parsed,processed,persisted,completed,stats,unknown"); value != (AckTypeReceived|AckTypeParsed|AckTypeProcessed|AckTypePersisted|AckTypeCompleted|AckTypeStats) {
		t.Fatalf("unexpected stringToAck value: %d", value)
	}

	header := new(_Header)
	header.parseField([]byte("a"), []byte("processed,stats"))
	header.parseField([]byte("c"), []byte("publish"))
	header.parseField([]byte("e"), []byte("10"))
	header.parseField([]byte("f"), []byte("/id > 10"))
	header.parseField([]byte("o"), []byte("replace"))
	header.parseField([]byte("k"), []byte("k1"))
	header.parseField([]byte("l"), []byte("123"))
	header.parseField([]byte("s"), []byte("9"))
	header.parseField([]byte("t"), []byte("orders"))
	header.parseField([]byte("x"), []byte("corr"))
	header.parseField([]byte("bm"), []byte("1|1|"))
	header.parseField([]byte("bs"), []byte("20"))
	header.parseField([]byte("lp"), []byte("lease"))
	header.parseField([]byte("ts"), []byte("2026-01-01T00:00:00Z"))
	header.parseField([]byte("cid"), []byte("cid-1"))
	header.parseField([]byte("gsn"), []byte("7"))
	header.parseField([]byte("gseq"), []byte("8"))
	header.parseField([]byte("opts"), []byte("o1"))
	header.parseField([]byte("sids"), []byte("s1,s2"))
	header.parseField([]byte("top_n"), []byte("5"))
	header.parseField([]byte("top_n"), []byte("invalid"))
	header.parseField([]byte("reason"), []byte("bad filter"))
	header.parseField([]byte("sub_id"), []byte("sub-1"))
	header.parseField([]byte("status"), []byte("success"))
	header.parseField([]byte("matches"), []byte("2"))
	header.parseField([]byte("orderby"), []byte("/id desc"))
	header.parseField([]byte("user_id"), []byte("user"))
	header.parseField([]byte("version"), []byte("5.3.5.1"))
	header.parseField([]byte("client_name"), []byte("name-hash"))
	header.parseField([]byte("query_id"), []byte("qid"))
	header.parseField([]byte("records_deleted"), []byte("1"))
	header.parseField([]byte("records_inserted"), []byte("2"))
	header.parseField([]byte("records_returned"), []byte("3"))
	header.parseField([]byte("records_updated"), []byte("4"))
	header.parseField([]byte("records_invalid"), []byte("5"))
	header.parseField([]byte("sow_keys"), []byte("k1,k2"))
	header.parseField([]byte("topic_matches"), []byte("5"))
	header.parseField([]byte("topic_matches"), []byte("invalid"))
	header.parseField([]byte("bs"), []byte("bad"))
	header.parseField(nil, []byte("ignored"))

	if command, _ := (&Message{header: header}).Command(); command != CommandPublish {
		t.Fatalf("unexpected parsed command: %d", command)
	}
	if ack, _ := (&Message{header: header}).AckType(); ack != (AckTypeProcessed|AckTypeStats) {
		t.Fatalf("unexpected parsed ack: %d", ack)
	}

	buffer := bytes.NewBuffer(nil)
	header.password = []byte("pw")
	header.messageType = []byte("json")
	if err := header.write(buffer); err != nil {
		t.Fatalf("header write failed: %v", err)
	}
	written := buffer.String()
	for _, token := range []string{
		`"c":"p"`,
		`"cid":"cid-1"`,
		`"t":"orders"`,
		`"bs":20`,
		`"bm":"1|1|"`,
		`"x":"corr"`,
		`"e":10`,
		`"filter":"/id > 10"`,
		`"opts":"o1"`,
		`"a":"processed,stats"`,
		`"client_name":"name-hash"`,
		`"user_id":"user"`,
		`"pw":"pw"`,
		`"orderby":"/id desc"`,
		`"query_id":"qid"`,
		`"s":9`,
		`"k":"k1"`,
		`"sow_keys":"k1,k2"`,
		`"sub_id":"sub-1"`,
		`"sids":"s1,s2"`,
		`"top_n":5`,
		`"version":"5.3.5.1"`,
		`"mt":"json"`,
	} {
		if !strings.Contains(written, token) {
			t.Fatalf("expected header token %q in %q", token, written)
		}
	}

	header.reset()
	if header.command != CommandUnknown || header.topic != nil || header.sequenceID != nil {
		t.Fatalf("expected header reset")
	}

	header.command = -1
	noCommand := bytes.NewBuffer(nil)
	if err := header.write(noCommand); err != nil {
		t.Fatalf("header write without command failed: %v", err)
	}
	if noCommand.String() != "{}" {
		t.Fatalf("expected empty header encoding, got %q", noCommand.String())
	}

	var nilMessage *Message
	if _, err := parseHeader(nilMessage, true, []byte("{}")); err == nil {
		t.Fatalf("expected parseHeader nil message error")
	}
	message := &Message{header: new(_Header)}
	if _, err := parseHeader(message, true, []byte(`{"c":"p"`)); err == nil {
		t.Fatalf("expected unexpected-end parse error")
	}

	escaped := &Message{header: new(_Header)}
	leftover, err := parseHeader(escaped, true, []byte(`{"c":"p","sub_id":"sub\\\"id","filter":"a\\\\b","topic":"orders"}tail`))
	if err != nil {
		t.Fatalf("escaped header parse failed: %v", err)
	}
	if string(leftover) != "tail" {
		t.Fatalf("unexpected escaped parse leftover: %q", string(leftover))
	}
	if subID, ok := escaped.SubID(); !ok || subID != `sub\\\"id` {
		t.Fatalf("unexpected escaped sub_id value: %q (ok=%v)", subID, ok)
	}
	if filter, ok := escaped.Filter(); !ok || filter != `a\\\\b` {
		t.Fatalf("unexpected escaped filter value: %q (ok=%v)", filter, ok)
	}
}

func TestMessageUnsetGetterCoverage(t *testing.T) {
	message := &Message{header: new(_Header)}
	if value, ok := message.GroupSequenceNumber(); ok || value != 0 {
		t.Fatalf("expected empty GroupSequenceNumber")
	}
	if value, ok := message.Matches(); ok || value != 0 {
		t.Fatalf("expected empty Matches")
	}
	if value, ok := message.MessageLength(); ok || value != 0 {
		t.Fatalf("expected empty MessageLength")
	}
	if value, ok := message.RecordsDeleted(); ok || value != 0 {
		t.Fatalf("expected empty RecordsDeleted")
	}
	if value, ok := message.RecordsInserted(); ok || value != 0 {
		t.Fatalf("expected empty RecordsInserted")
	}
	if value, ok := message.RecordsReturned(); ok || value != 0 {
		t.Fatalf("expected empty RecordsReturned")
	}
	if value, ok := message.RecordsUpdated(); ok || value != 0 {
		t.Fatalf("expected empty RecordsUpdated")
	}
	if value, ok := message.TopicMatches(); ok || value != 0 {
		t.Fatalf("expected empty TopicMatches")
	}
}

func TestParseHeaderMalformedAndStateCoverage(t *testing.T) {
	message := &Message{header: new(_Header)}

	// resetMessage=false path.
	if _, err := parseHeader(message, false, []byte(`{"c":"p"}`)); err != nil {
		t.Fatalf("expected parse without reset to succeed: %v", err)
	}

	// inHeader malformed token path.
	if _, err := parseHeader(message, true, []byte(`x`)); err == nil {
		t.Fatalf("expected malformed in-header token error")
	}

	// afterKey malformed token path (non-whitespace, non-colon).
	if _, err := parseHeader(message, true, []byte(`{"c"?"p"}`)); err == nil {
		t.Fatalf("expected malformed after-key token error")
	}

	// inValue branch with numeric value and comma termination.
	if _, err := parseHeader(message, true, []byte(`{"bs":2,"c":"p"}`)); err != nil {
		t.Fatalf("expected numeric value parse to succeed: %v", err)
	}

	// inKey/inValueString escaped character handling.
	if _, err := parseHeader(message, true, []byte("{\"c\":\"p\",\"f\\\"ilter\":\"v\\\\x\"}")); err != nil {
		t.Fatalf("expected escaped key/value parse to succeed: %v", err)
	}
}

func TestMessageCopyAndReplaceCoverage(t *testing.T) {
	message := &Message{header: new(_Header)}
	message.Reset()
	message.client = NewClient("copy-src")
	message.valid = false
	message.ignoreAutoAck = true
	message.bookmarkSeqNo = 77
	message.subscriptionHandle = "sub-h"
	message.rawTransmissionTime = "2026-02-21T00:00:00Z"
	message.disowned = true
	message.data = []byte("payload")
	message.header.command = CommandPublish
	message.header.ackType = func() *int { v := AckTypeProcessed | AckTypeStats; return &v }()
	message.header.batchSize = func() *uint { v := uint(2); return &v }()
	message.header.bookmark = []byte("1|1|")
	message.header.commandID = []byte("cid")
	message.header.correlationID = []byte("corr")
	message.header.expiration = func() *uint { v := uint(3); return &v }()
	message.header.filter = []byte("/id > 1")
	message.header.groupSequenceNumber = func() *uint { v := uint(4); return &v }()
	message.header.leasePeriod = []byte("lease")
	message.header.matches = func() *uint { v := uint(5); return &v }()
	message.header.messageLength = func() *uint { v := uint(6); return &v }()
	message.header.options = []byte("opts")
	message.header.orderBy = []byte("/id desc")
	message.header.queryID = []byte("qid")
	message.header.reason = []byte("reason")
	message.header.recordsDeleted = func() *uint { v := uint(7); return &v }()
	message.header.recordsInserted = func() *uint { v := uint(8); return &v }()
	message.header.recordsReturned = func() *uint { v := uint(9); return &v }()
	message.header.recordsUpdated = func() *uint { v := uint(10); return &v }()
	message.header.sequenceID = func() *uint64 { v := uint64(11); return &v }()
	message.header.sowKey = []byte("k1")
	message.header.sowKeys = []byte("k1,k2")
	message.header.status = []byte("status")
	message.header.subID = []byte("sub")
	message.header.subIDs = []byte("sub,sub2")
	message.header.timestamp = []byte("ts")
	message.header.topN = func() *uint { v := uint(12); return &v }()
	message.header.topic = []byte("topic")
	message.header.topicMatches = func() *uint { v := uint(13); return &v }()
	message.header.userID = []byte("user")

	copied := message.Copy()
	if copied == nil || copied == message {
		t.Fatalf("expected independent copy")
	}
	copied.data[0] = 'X'
	copied.header.commandID[0] = 'X'
	if string(message.data) != "payload" || string(message.header.commandID) != "cid" {
		t.Fatalf("copy mutation should not affect source")
	}

	replacement := &Message{header: new(_Header)}
	replacement.SetData([]byte("replacement"))
	message.Replace(replacement)
	if string(message.Data()) != "replacement" {
		t.Fatalf("replace should copy payload")
	}
	message.Replace(nil)
	var nilMessage *Message
	if nilMessage.Replace(replacement) != nil {
		t.Fatalf("nil Replace should return nil")
	}
}

func TestMessageAccessorsCoverage(t *testing.T) {
	message := &Message{header: new(_Header)}
	message.reset()
	message.header.command = CommandPublish
	message.header.commandID = []byte("cid")
	message.header.correlationID = []byte("corr")
	message.header.filter = []byte("/id > 1")
	message.header.options = []byte("o")
	message.header.orderBy = []byte("/id desc")
	message.header.queryID = []byte("qid")
	message.header.reason = []byte("bad filter")
	message.header.sowKey = []byte("k1")
	message.header.sowKeys = []byte("k1,k2")
	message.header.status = []byte("failure")
	message.header.subID = []byte("sub")
	message.header.subIDs = []byte("sub,sub2")
	message.header.timestamp = []byte("2026-01-01T00:00:00Z")
	message.header.topic = []byte("orders")
	message.header.userID = []byte("user")
	message.header.bookmark = []byte("1|1|")
	message.header.leasePeriod = []byte("lease")
	message.rawTransmissionTime = time.Now().UTC().Format(time.RFC3339Nano)

	ack := AckTypeProcessed
	batch := uint(10)
	exp := uint(11)
	group := uint(12)
	length := uint(13)
	matches := uint(14)
	rd := uint(15)
	ri := uint(16)
	rr := uint(17)
	ru := uint(18)
	seq := uint64(19)
	topN := uint(20)
	tm := uint(21)
	message.header.ackType = &ack
	message.header.batchSize = &batch
	message.header.expiration = &exp
	message.header.groupSequenceNumber = &group
	message.header.messageLength = &length
	message.header.matches = &matches
	message.header.recordsDeleted = &rd
	message.header.recordsInserted = &ri
	message.header.recordsReturned = &rr
	message.header.recordsUpdated = &ru
	message.header.sequenceID = &seq
	message.header.topN = &topN
	message.header.topicMatches = &tm
	message.data = []byte("payload")

	if _, ok := message.AckType(); !ok {
		t.Fatalf("expected ack type")
	}
	if message.GetAckTypeEnum() == 0 {
		t.Fatalf("expected non-zero ack enum")
	}
	message.SetAckTypeEnum(AckTypeStats)
	if ackValue, _ := message.AckType(); ackValue != AckTypeStats {
		t.Fatalf("unexpected set ack enum")
	}
	if _, ok := message.BatchSize(); !ok {
		t.Fatalf("expected batch size")
	}
	if _, ok := message.Bookmark(); !ok {
		t.Fatalf("expected bookmark")
	}
	if message.GetCommandEnum() != CommandPublish {
		t.Fatalf("unexpected command enum")
	}
	message.SetCommandEnum(CommandSOW)
	if command, _ := message.Command(); command != CommandSOW {
		t.Fatalf("unexpected set command enum")
	}
	if _, ok := message.CommandID(); !ok {
		t.Fatalf("expected command id")
	}
	if _, ok := message.CorrelationID(); !ok {
		t.Fatalf("expected correlation id")
	}
	if string(message.Data()) != "payload" {
		t.Fatalf("unexpected data")
	}
	if string(message.GetRawData()) != "payload" {
		t.Fatalf("unexpected raw data")
	}
	message.SetData([]byte("new"))
	if string(message.Data()) != "new" {
		t.Fatalf("unexpected set data")
	}
	if _, ok := message.Expiration(); !ok {
		t.Fatalf("expected expiration")
	}
	if _, ok := message.Filter(); !ok {
		t.Fatalf("expected filter")
	}
	if _, ok := message.GroupSequenceNumber(); !ok {
		t.Fatalf("expected group sequence")
	}
	if _, ok := message.LeasePeriod(); !ok {
		t.Fatalf("expected lease period")
	}
	if _, ok := message.Matches(); !ok {
		t.Fatalf("expected matches")
	}
	if _, ok := message.MessageLength(); !ok {
		t.Fatalf("expected message length")
	}
	if _, ok := message.Options(); !ok {
		t.Fatalf("expected options")
	}
	if _, ok := message.OrderBy(); !ok {
		t.Fatalf("expected order by")
	}
	if _, ok := message.QueryID(); !ok {
		t.Fatalf("expected query id")
	}
	if _, ok := message.Reason(); !ok {
		t.Fatalf("expected reason")
	}
	if _, ok := message.RecordsDeleted(); !ok {
		t.Fatalf("expected records deleted")
	}
	if _, ok := message.RecordsInserted(); !ok {
		t.Fatalf("expected records inserted")
	}
	if _, ok := message.RecordsReturned(); !ok {
		t.Fatalf("expected records returned")
	}
	if _, ok := message.RecordsUpdated(); !ok {
		t.Fatalf("expected records updated")
	}
	if _, ok := message.SequenceID(); !ok {
		t.Fatalf("expected sequence id")
	}
	if _, ok := message.SowKey(); !ok {
		t.Fatalf("expected sow key")
	}
	if _, ok := message.SowKeys(); !ok {
		t.Fatalf("expected sow keys")
	}
	if _, ok := message.Status(); !ok {
		t.Fatalf("expected status")
	}
	if _, ok := message.SubID(); !ok {
		t.Fatalf("expected sub id")
	}
	if _, ok := message.SubIDs(); !ok {
		t.Fatalf("expected sub ids")
	}
	if _, ok := message.Timestamp(); !ok {
		t.Fatalf("expected timestamp")
	}
	if _, ok := message.TopN(); !ok {
		t.Fatalf("expected top n")
	}
	if _, ok := message.Topic(); !ok {
		t.Fatalf("expected topic")
	}
	if _, ok := message.TopicMatches(); !ok {
		t.Fatalf("expected topic matches")
	}
	if _, ok := message.UserID(); !ok {
		t.Fatalf("expected user id")
	}

	message.Disown()
	if !message.disowned {
		t.Fatalf("expected disowned flag")
	}
	if message.GetMessage() != message {
		t.Fatalf("expected GetMessage identity")
	}
	if message.GetRawTransmissionTime() == "" {
		t.Fatalf("expected raw transmission time")
	}
	message.SetSubscriptionHandle("sub-h")
	if message.GetSubscriptionHandle() != "sub-h" {
		t.Fatalf("unexpected subscription handle")
	}
	message.SetBookmarkSeqNo(55)
	if message.GetBookmarkSeqNo() != 55 {
		t.Fatalf("unexpected bookmark seq")
	}
	message.SetIgnoreAutoAck(true)
	if !message.GetIgnoreAutoAck() {
		t.Fatalf("expected ignore auto ack")
	}
	message.Invalidate()
	if message.IsValid() {
		t.Fatalf("expected invalid message")
	}
	message.Reset()
	if !message.IsValid() {
		t.Fatalf("expected reset valid message")
	}

	var nilMsg *Message
	if nilMsg.SetAckTypeEnum(1) != nil {
		t.Fatalf("expected nil SetAckTypeEnum")
	}
	if nilMsg.GetCommandEnum() != CommandUnknown {
		t.Fatalf("expected nil command unknown")
	}
	if nilMsg.SetCommandEnum(1) != nil {
		t.Fatalf("expected nil SetCommandEnum")
	}
	if nilMsg.SetData([]byte("x")) != nil {
		t.Fatalf("expected nil SetData")
	}
	nilMsg.AssignData([]byte("x"))
	if nilMsg.DeepCopy() != nil {
		t.Fatalf("expected nil DeepCopy")
	}
	nilMsg.Disown()
	if nilMsg.GetBookmarkSeqNo() != 0 || nilMsg.GetIgnoreAutoAck() || nilMsg.GetMessage() != nil || nilMsg.GetRawData() != nil || nilMsg.GetRawTransmissionTime() != "" || nilMsg.GetSubscriptionHandle() != "" {
		t.Fatalf("unexpected nil message getter values")
	}
	if nilMsg.SetBookmarkSeqNo(1) != nil || nilMsg.SetIgnoreAutoAck(true) != nil || nilMsg.SetSubscriptionHandle("x") != nil || nilMsg.SetClientImpl(nil) != nil {
		t.Fatalf("expected nil setters to return nil")
	}
	nilMsg.Invalidate()
	if nilMsg.IsValid() {
		t.Fatalf("expected nil IsValid false")
	}
	nilMsg.Reset()
	if err := nilMsg.ThrowFor(); err == nil || !strings.Contains(err.Error(), "UnknownError") {
		t.Fatalf("expected nil ThrowFor unknown error, got %v", err)
	}

	success := (&Message{header: &_Header{status: []byte("success")}}).ThrowFor()
	if success != nil {
		t.Fatalf("expected success ThrowFor nil error")
	}
	noReason := (&Message{header: &_Header{status: []byte("failure")}}).ThrowFor()
	if noReason == nil || !strings.Contains(noReason.Error(), "UnknownError") {
		t.Fatalf("expected failure without reason unknown error, got %v", noReason)
	}

	client := NewClient("ack-msg")
	conn := newTestConn()
	client.connected.Store(true)
	client.connection = conn
	message.SetClientImpl(client)
	message.header.topic = []byte("orders")
	message.header.bookmark = []byte("1|1|")
	if err := message.Ack("sub-ack"); err != nil {
		t.Fatalf("expected Ack with options to succeed: %v", err)
	}
	noClient := &Message{header: new(_Header)}
	if err := noClient.Ack(); err == nil {
		t.Fatalf("expected Ack without client error")
	}
	noTopicBookmark := &Message{header: new(_Header)}
	noTopicBookmark.SetClientImpl(client)
	if err := noTopicBookmark.Ack(); err == nil {
		t.Fatalf("expected Ack missing topic/bookmark error")
	}
}
