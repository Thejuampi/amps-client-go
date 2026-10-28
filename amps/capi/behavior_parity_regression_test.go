package capi

import (
	"testing"

	"github.com/Thejuampi/amps-client-go/amps"
)

func TestParityAuditSendPreservesTextAckTypes(t *testing.T) {
	var command = commandFromMessage(&messageObject{fields: map[string]string{"c": "publish", "t": "orders", "a": "processed,persisted"}})
	if got := command.GetAckType(); got != amps.AckTypeProcessed|amps.AckTypePersisted {
		t.Fatalf("CAPI dropped textual wire ack types: %d", got)
	}
}

func TestParityAuditSendPreservesSequence(t *testing.T) {
	var command = commandFromMessage(&messageObject{fields: map[string]string{"c": "publish", "t": "orders", "s": "42"}})
	if got, ok := command.SequenceID(); !ok || got != 42 {
		t.Fatalf("CAPI mapped wire sequence to subscription instead: sequence=%d present=%t", got, ok)
	}
}

func TestCAPIWireAckAndSequenceRoundTrip(t *testing.T) {
	var handle = MessageCreate(0)
	t.Cleanup(func() { MessageDestroy(handle) })
	MessageSetFieldValue(handle, "command", "publish")
	MessageSetFieldValue(handle, "sub_id", "sub-1")
	MessageSetFieldValue(handle, "sequenceid", "42")
	MessageSetFieldValue(handle, "acktype", "received,parsed,processed,persisted,completed,stats")
	var object, ok = getMessageObject(handle)
	if !ok {
		t.Fatal("message handle unavailable")
	}
	var command = commandFromMessage(object)
	var sequence, present = command.SequenceID()
	var subID, _ = command.SubID()
	if !present || sequence != 42 || subID != "sub-1" || command.GetAckType() != 63 {
		t.Fatal("CAPI field normalization lost independent fields")
	}
}
