package amps

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

type deepAuditBorrowedReplayStore struct {
	*MemoryPublishStore
	command *Command
}

func TestDeepAuditLegacyReplayPreservesEveryBorrowedHeaderField(t *testing.T) {
	var client, conn = auditConnectedClient(t)
	var command = NewCommand("publish").SetTopic("orders").SetCommandID("id").SetSequenceID(100).SetAckType(AckTypeProcessed).SetDataOnly(true).SetSendEmpty(true).SetData([]byte("payload"))
	command.header.filter = []byte{}
	command.header.clientName = []byte("publisher")
	command.header.userID = []byte("user")
	command.header.password = []byte("password")
	command.header.version = []byte("5.3")
	var originalAck = command.header.ackType
	var originalExtras = command.header.textExtras
	client.SetPublishStore(&deepAuditBorrowedReplayStore{MemoryPublishStore: newMemoryPublishStore(1), command: command})
	if _, err := client.replayPublishStore(); err != nil {
		t.Fatal(err)
	}
	var wire = conn.WrittenPayload()
	for _, field := range []string{`"filter":""`, `"client_name":"publisher"`, `"user_id":"user"`, `"pw":"password"`, `"version":"5.3"`, `"a":"processed,persisted"`, `"data_only":"true"`, `"send_empty":"true"`} {
		if !strings.Contains(wire, field) {
			t.Errorf("replay dropped %s: %s", field, wire)
		}
	}
	if ack, _ := command.AckType(); ack != AckTypeProcessed || command.header.ackType != originalAck || command.header.textExtras != originalExtras || command.header.filter == nil || !bytes.Equal(command.data, []byte("payload")) {
		t.Fatal("replay mutated the store's command")
	}
}

func (store *deepAuditBorrowedReplayStore) Replay(replayer func(*Command) error) error {
	return replayer(store.command)
}

func TestDeepAuditLegacyReplayDoesNotMutateBorrowedCommand(t *testing.T) {
	for _, entry := range []struct {
		name    string
		ack     int
		present bool
	}{
		{"absent", AckTypeNone, false},
		{"none", AckTypeNone, true},
		{"processed", AckTypeProcessed, true},
		{"already-persisted", AckTypeProcessed | AckTypePersisted, true},
	} {
		t.Run(entry.name, func(t *testing.T) {
			var client, conn = auditConnectedClient(t)
			var command = NewCommand("delta_publish").SetTopic("orders").SetCommandID("existing-id").SetSequenceID(100).SetData([]byte("body"))
			if entry.present {
				command.SetAckType(entry.ack)
			}
			var store = &deepAuditBorrowedReplayStore{MemoryPublishStore: newMemoryPublishStore(1), command: command}
			client.SetPublishStore(store)
			if _, err := client.replayPublishStore(); err != nil {
				t.Fatal(err)
			}
			if ack, present := command.AckType(); present != entry.present || ack != entry.ack {
				t.Fatalf("borrowed command acknowledgement changed: %d present=%v", ack, present)
			}
			var raw = conn.WrittenBytes()
			if len(raw) < 4 || int(binary.BigEndian.Uint32(raw[:4])) != len(raw)-4 {
				t.Fatal("missing or malformed replay frame")
			}
			var message Message
			var payload, err = parseHeader(&message, true, raw[4:])
			if err != nil {
				t.Fatal(err)
			}
			var ack, _ = message.AckType()
			var id, _ = message.CommandID()
			var sequence, _ = message.SequenceID()
			var topic, _ = message.Topic()
			if ack != entry.ack|AckTypePersisted || id != "existing-id" || sequence != 100 || topic != "orders" || !bytes.Equal(payload, []byte("body")) {
				t.Fatalf("replay ack=%d id=%q sequence=%d topic=%q payload=%q", ack, id, sequence, topic, payload)
			}
		})
	}
}

func TestDeepAuditReplayModernAllocationBudget(t *testing.T) {
	var measure = func(ackType int) float64 {
		var client = NewClient("replay-allocations")
		client.connected.Store(true)
		client.connection = &noProgressConn{}
		defer client.Close()
		var command = NewCommand("publish").SetTopic("orders").SetSequenceID(100).SetAckType(ackType)
		client.SetPublishStore(&deepAuditBorrowedReplayStore{MemoryPublishStore: newMemoryPublishStore(1), command: command})
		return testing.AllocsPerRun(100, func() {
			if _, err := client.replayPublishStore(); err != nil {
				t.Fatal(err)
			}
		})
	}
	var modern = measure(AckTypeProcessed | AckTypePersisted)
	if modern > 1 {
		t.Fatalf("replaying a command that already requests persisted allocates %.0f times, want at most 1", modern)
	}
}

func BenchmarkPublishReplayAcknowledgements(b *testing.B) {
	for _, entry := range []struct {
		name string
		ack  int
	}{
		{"modern", AckTypeProcessed | AckTypePersisted},
		{"legacy", AckTypeProcessed},
	} {
		b.Run(entry.name, func(b *testing.B) {
			var client = NewClient("replay-benchmark")
			client.connected.Store(true)
			client.connection = &noProgressConn{}
			var command = NewCommand("publish").SetTopic("orders").SetCommandID("publisher-1").SetSequenceID(100).SetAckType(entry.ack).SetData([]byte(`{"id":1}`))
			client.SetPublishStore(&deepAuditBorrowedReplayStore{MemoryPublishStore: newMemoryPublishStore(1), command: command})
			b.Cleanup(func() { _ = client.Close() })
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := client.replayPublishStore(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
