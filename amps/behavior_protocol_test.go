package amps

import (
	"encoding/json"
	"testing"
)

func TestHeaderMatchesEveryShortNameExactly(t *testing.T) {
	var fields = map[string]any{
		"c": "ack", "cid": "correct", "cat": "future",
		"lp": "123", "lq": "bad", "mt": "json", "mu": "bad", "ts": "today", "tz": "bad", "pw": "password",
		"gseq": 3, "gxxx": 9, "opts": "live", "oxxx": "bad", "sids": "sub-1", "sxxx": "bad",
		"matches": 4, "mxxxxxx": 9, "orderby": "/id", "oxxxxxx": "bad", "user_id": "user", "uxxxxxx": "bad", "version": "5.3.5.14", "vxxxxxx": "bad",
	}
	var frame, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	var message = newOwnedMessage()
	if _, err := parseHeader(message, true, frame); err != nil {
		t.Fatal(err)
	}
	var header = message.header
	if string(header.commandID) != "correct" || string(header.textExtras.leasePeriod) != "123" || string(header.messageType) != "json" || string(header.timestamp) != "today" || string(header.password) != "password" || *header.groupSequenceNumber != 3 || string(header.options) != "live" || string(header.subIDs) != "sub-1" || *header.matches != 4 || string(header.orderBy) != "/id" || string(header.userID) != "user" || string(header.version) != "5.3.5.14" {
		t.Fatal("unknown field aliased a known protocol field")
	}
	if ensureHeaderTextExtras(nil) != nil {
		t.Fatal("nil header acquired extras")
	}
}

func TestRouterAckVisitsDistinctIDsWithoutDuplicateCallbacks(t *testing.T) {
	var router MessageRouter
	var calls int
	for _, id := range []string{"command", "query", "subscription"} {
		router.AddRoute(id, func(*Message) error { calls++; return nil }, AckTypeProcessed, 0, CommandPublish)
	}
	var message = commandToMessage(NewCommand("ack").SetCommandID("command").SetQueryID("query").SetSubID("subscription").SetAckType(AckTypeProcessed))
	router.DeliverAck(message, AckTypeProcessed)
	if calls != 1 || router.FindRoute("command") != nil || router.FindRoute("query") != nil || router.FindRoute("subscription") != nil {
		t.Fatal("ack callbacks or route cleanup differ")
	}
}

func TestClosedStreamRejectsFurtherDelivery(t *testing.T) {
	var stream = newMessageStream(nil)
	stream.setState(messageStreamStateComplete)
	if err := stream.messageHandler(auditBookmarkMessage("1|1|")); !IsErrorKind(err, DisconnectedError) || stream.Depth() != 0 {
		t.Fatalf("closed stream accepted delivery: %v", err)
	}
}

func TestNamedServerVersionsWithDigitsUseCurrentCapabilities(t *testing.T) {
	for _, version := range []string{"development", "dev2026", "v5.3.5.1", "branch-5.4", "_1"} {
		if got := ConvertVersionToNumber(version); got != 99999999 {
			t.Fatalf("named branch %q parsed as numeric version %d", version, got)
		}
	}
}
