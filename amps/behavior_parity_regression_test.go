package amps

import (
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParityAuditBookmarkOutOfOrderDiscard(t *testing.T) {
	var store = NewMemoryBookmarkStore()
	var first = auditBookmarkMessage("1|1|")
	var second = auditBookmarkMessage("1|2|")
	store.Log(first)
	store.Log(second)
	store.DiscardMessage(second)
	if store.IsDiscarded(first) {
		t.Fatal("discarding message 2 must not discard still-active message 1")
	}
}

func TestParityAuditBookmarkActiveRedelivery(t *testing.T) {
	var store = NewMemoryBookmarkStore()
	var message = auditBookmarkMessage("1|1|")
	store.Log(message)
	store.Log(message)
	if store.IsDiscarded(message) {
		t.Fatal("logging an undiscarded bookmark twice must not mark it discarded")
	}
}

func TestParityAuditBookmarkLogSetsMessageSequence(t *testing.T) {
	var store = NewMemoryBookmarkStore()
	var message = auditBookmarkMessage("1|1|")
	var sequence = store.Log(message)
	if got := message.GetBookmarkSeqNo(); got != sequence {
		t.Fatalf("Log returned sequence=%d but message bookmark sequence=%d", sequence, got)
	}
}

func TestParityAuditBookmarkResumeKeepsUnprocessed(t *testing.T) {
	var store = NewMemoryBookmarkStore()
	var first = auditBookmarkMessage("1|1|")
	store.Log(first)
	store.DiscardMessage(first)
	store.Log(auditBookmarkMessage("1|2|"))
	if got := store.GetMostRecent("sub-1"); got != "1|1|" {
		t.Fatalf("resume must retain unprocessed message 2; got %q", got)
	}
}

func TestParityAuditDiscardedBookmarkNotDelivered(t *testing.T) {
	var client = NewClient("duplicate-audit")
	var store = NewMemoryBookmarkStore()
	var message = auditBookmarkMessage("1|1|")
	store.Log(message)
	store.DiscardMessage(message)
	client.SetBookmarkStore(store)
	var delivered int
	client.routes.Store("sub-1", func(*Message) error { delivered++; return nil })
	if err := client.onMessage(message); err != nil {
		t.Fatal(err)
	}
	if delivered != 0 {
		t.Fatalf("discarded duplicate reached application handler %d times", delivered)
	}
}

func TestParityAuditStoredPublishSendFailure(t *testing.T) {
	var client, connection = auditConnectedClient(t)
	var store = NewMemoryPublishStore()
	client.SetPublishStore(store)
	_ = connection.Close()
	var err = client.Publish("orders", "payload")
	if store.UnpersistedCount() != 1 {
		t.Fatal("probe requires message to have been saved for replay")
	}
	if err != nil {
		t.Fatalf("C++ AC-1450 retains the stored publish without returning a send failure: %v", err)
	}
}

func TestParityAuditDisconnectedPublishIsStored(t *testing.T) {
	var client = NewClient("offline-audit")
	var store = NewMemoryPublishStore()
	client.SetPublishStore(store)
	var err = client.Publish("orders", "payload")
	if err != nil || store.UnpersistedCount() != 1 {
		t.Fatalf("stored publish during disconnect: error=%v retained=%d, want nil and 1", err, store.UnpersistedCount())
	}
}

func TestParityAuditFreshMemoryPublishSequence(t *testing.T) {
	var store = NewMemoryPublishStore()
	store.SetInitialSequence(42)
	var sequence, err = store.Store(NewCommand("publish").SetTopic("orders"))
	if err != nil {
		t.Fatal(err)
	}
	if sequence <= 100 {
		t.Fatalf("fresh memory store reused an old publisher sequence (%d); C++ manufactures a timestamp sequence to avoid AC-1358", sequence)
	}
}

func TestParityAuditQueueAckCommand(t *testing.T) {
	var client, connection = auditConnectedClient(t)
	if err := client.Ack("queue://orders", "1|10|"); err != nil {
		t.Fatal(err)
	}
	if got := connection.WrittenPayload(); !strings.Contains(got, `"c":"sow_delete"`) {
		t.Fatalf("C++ sends sow_delete for queue acknowledgement; got %s", got)
	}
}

func TestParityAuditMessageAckOptions(t *testing.T) {
	var client, connection = auditConnectedClient(t)
	var message = auditBookmarkMessage("1|10|")
	message.client = client
	if err := message.Ack("cancel"); err != nil {
		t.Fatal(err)
	}
	if got := connection.WrittenPayload(); !strings.Contains(got, `"opts":"cancel"`) {
		t.Fatalf("cancel must be an option, got %s", got)
	}
}

func TestParityAuditManualAcksRespectBatchSize(t *testing.T) {
	var client, connection = auditConnectedClient(t)
	client.SetAckBatchSize(2)
	if err := client.Ack("queue://orders", "1|10|"); err != nil {
		t.Fatal(err)
	}
	if got := len(connection.WrittenBytes()); got != 0 {
		t.Fatalf("first manual ack must remain in batch; wrote %d bytes", got)
	}
}

func TestParityAuditStreamFullPreservesOldest(t *testing.T) {
	var stream = newMessageStream(nil).SetSubscription("sub-1", "sub-1").SetMaxDepth(1)
	if err := stream.messageHandler(auditBookmarkMessage("1|1|")); err != nil {
		t.Fatal(err)
	}
	_ = stream.messageHandler(auditBookmarkMessage("1|2|"))
	var got, _ = stream.Next().Bookmark()
	if got != "1|1|" {
		t.Fatalf("full stream lost oldest message: got %q, want 1|1|", got)
	}
}

func TestParityAuditStreamDefersAutoAck(t *testing.T) {
	var client, connection = auditConnectedClient(t)
	client.SetAutoAck(true)
	var stream = newMessageStream(client).SetSubscription("sub-1", "sub-1")
	client.routes.Store("sub-1", stream.messageHandler)
	var message = auditBookmarkMessage("1|1|")
	message.header.textExtras = &_HeaderTextExtras{leasePeriod: []byte("30000")}
	if err := client.onMessage(message); err != nil {
		t.Fatal(err)
	}
	if got := len(connection.WrittenBytes()); got != 0 {
		t.Fatalf("queue message acknowledged before stream consumer reads it: %d bytes", got)
	}
}

func TestParityAuditResubscribeFailurePropagatesFromLogon(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	client.SetSubscriptionManager(&auditFailedSubscriptionManager{NewDefaultSubscriptionManager()})
	var ack = commandToMessage(NewCommand("ack").SetAckType(AckTypeProcessed))
	ack.header.status = []byte("success")
	if err := auditLogon(t, client, LogonParams{Timeout: 100}, ack); !IsErrorKind(err, NotEntitledError) {
		t.Fatalf("logon must report failed resubscription, got %v", err)
	}
}

func TestParityAuditLogonNameHashComesFromBookmark(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	var ack = commandToMessage(NewCommand("ack").SetAckType(AckTypeProcessed).SetBookmark("123|42|"))
	ack.header.status = []byte("success")
	ack.header.clientName = []byte("behavior-audit")
	if err := auditLogon(t, client, LogonParams{Timeout: 100}, ack); err != nil {
		t.Fatal(err)
	}
	if client.GetNameHash() != "123" || client.GetNameHashValue() != 123 {
		t.Fatalf("server bookmark hash=123; got text=%q value=%d", client.GetNameHash(), client.GetNameHashValue())
	}
}

func TestParityAuditAuthenticatorRetryErrorTerminatesLogon(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	var ack = commandToMessage(NewCommand("ack").SetAckType(AckTypeProcessed))
	ack.header.status = []byte("retry")
	if err := auditLogon(t, client, LogonParams{Timeout: 200, Authenticator: auditRetryFailureAuthenticator{}}, ack); !IsErrorKind(err, AuthenticationError) {
		t.Fatalf("authenticator retry error must terminate logon immediately, got %v", err)
	}
}

func TestParityAuditSubscribeRecoveryGeneration(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	client.SetRetryOnDisconnect(true)
	var bookmarks = &auditSnapshotBookmarkStore{MemoryBookmarkStore: NewMemoryBookmarkStore(), snapshot: make(chan struct{})}
	client.SetBookmarkStore(bookmarks)
	var replayDone = make(chan error, 1)
	var once sync.Once
	client.SetTransportFilter(func(direction TransportFilterDirection, data []byte) []byte {
		if direction == TransportFilterOutbound {
			once.Do(func() {
				go func() { replayDone <- client.SubscriptionManager().Resubscribe(client) }()
				select {
				case <-bookmarks.snapshot:
				case <-time.After(time.Second):
					t.Error("resubscribe did not capture the in-flight subscription")
				}
			})
		}
		return data
	})
	var command = NewCommand("subscribe").SetTopic("orders").SetSubID("sub-1").SetBookmark("1|1|").SetAckType(AckTypeProcessed)
	if _, err := client.ExecuteAsync(command, func(*Message) error { return nil }); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-replayDone:
		if err != nil {
			t.Fatalf("same-connection subscription replay must be suppressed (AC-1454): %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("resubscribe did not finish")
	}
}

func TestParityAuditRouterAck(t *testing.T) {
	var router MessageRouter
	var deliveries int
	router.AddRoute("cmd-1", func(*Message) error { deliveries++; return nil }, AckTypeProcessed, 0, CommandPublish)
	var ackType = AckTypeProcessed
	var message = &Message{header: &_Header{command: CommandAck, commandID: []byte("cmd-1"), ackType: &ackType}}
	router.DeliverAck(message, ackType)
	if deliveries != 1 {
		t.Fatalf("ack delivered %d times, want 1", deliveries)
	}
}

func TestParityAuditEmptyCompositePart(t *testing.T) {
	var builder = NewCompositeMessageBuilder()
	if err := builder.Append(""); err != nil {
		t.Fatal(err)
	}
	var count, err = NewCompositeMessageParser().Parse(builder.GetBytes())
	if err != nil || count != 1 {
		t.Fatalf("empty part must have a 4-byte length prefix: count=%d error=%v", count, err)
	}
}

func TestParityAuditNVFIXSize(t *testing.T) {
	var builder = NewNVFIXBuilder()
	if err := builder.AppendStrings("id", "1"); err != nil {
		t.Fatal(err)
	}
	if got, want := builder.Size(), len(builder.Bytes()); got != want {
		t.Fatalf("NVFIX Size=%d, wire length=%d", got, want)
	}
}

func TestParityAuditUnknownHeaderDoesNotOverwriteCommandID(t *testing.T) {
	var message = newOwnedMessage()
	if _, err := parseHeader(message, true, []byte(`{"c":"ack","cid":"correct","cat":"future"}`)); err != nil {
		t.Fatal(err)
	}
	if got, _ := message.CommandID(); got != "correct" {
		t.Fatalf("unknown header replaced cid: %q", got)
	}
}

func TestParityAuditErrorClassification(t *testing.T) {
	var tests = []struct {
		reason string
		kind   int
	}{
		{"subid in use", SubidInUseError},
		{"name in use", nameInUseError},
		{"subscription already exists", SubscriptionAlreadyExistsError},
	}
	for _, test := range tests {
		t.Run(test.reason, func(t *testing.T) {
			if err := reasonToError(test.reason); !IsErrorKind(err, test.kind) {
				t.Fatalf("reason %q classified as %v", test.reason, err)
			}
		})
	}
}

func TestParityAuditReplayFailureIsClassified(t *testing.T) {
	if err := reasonToError("txn replay failed"); !IsErrorKind(err, ReplayFailedError) || err.Error() != "ReplayFailedError: txn replay failed" {
		t.Fatalf("C++ AC-1434 identifies failed bookmark replay, got %v", err)
	}
}

func TestParityAuditNamedVersion(t *testing.T) {
	if got := ConvertVersionToNumber("development"); got != 99999999 {
		t.Fatalf("C++ assumes current capabilities for a named server build: got %d, want 99999999", got)
	}
}

func TestParityAuditTopNZeroIsExplicit(t *testing.T) {
	var command = NewCommand("sow").SetTopN(0)
	if got, present := command.TopN(); !present || got != 0 {
		t.Fatalf("C++ sends explicit top_n=0; got value=%d present=%t", got, present)
	}
}

func TestParityAuditTopNDefaultSentinelClearsField(t *testing.T) {
	var command = NewCommand("sow").SetTopN(uint(^uint32(0)))
	if got, present := command.TopN(); present {
		t.Fatalf("C++ AMPS_DEFAULT_TOP_N clears top_n; Go sends %d", got)
	}
}

func TestParityAuditPublishBatching(t *testing.T) {
	var client, connection = auditConnectedClient(t)
	client.SetPublishBatching(1<<20, 60000)
	if err := client.Publish("orders", "small"); err != nil {
		t.Fatal(err)
	}
	if got := len(connection.WrittenBytes()); got != 0 {
		t.Fatalf("publish below batch size was sent immediately: %d bytes", got)
	}
}

func TestParityAuditConnectTimeoutURI(t *testing.T) {
	var _, err = parseSocketOptions(url.Values{"tcp_connecttimeout": {"3000"}})
	if err != nil {
		t.Fatalf("C++ 5.3.5.5 accepts tcp_connecttimeout: %v", err)
	}
}
