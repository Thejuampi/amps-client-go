package capi

import (
	"testing"

	"github.com/Thejuampi/amps-client-go/amps"
)

func TestMessageFromAmpsRoundTripPreservesCommandFields(t *testing.T) {
	message := amps.NewCommand("publish").
		SetCommandID("cid-1").
		SetTopic("orders").
		SetCorrelationID("corr-1").
		SetSowKey("k1").
		SetExpiration(7).
		SetSubID("sub-1").
		SetData([]byte("payload")).
		GetMessage()

	handle := messageFromAmps(message)
	object, ok := getMessageObject(handle)
	if !ok {
		t.Fatalf("expected message handle")
	}

	command := commandFromMessage(object)
	if command == nil {
		t.Fatalf("expected round-tripped command")
	}
	if got, _ := command.Command(); got != "p" {
		t.Fatalf("Command() = %q, want %q", got, "p")
	}
	if got, _ := command.CorrelationID(); got != "corr-1" {
		t.Fatalf("CorrelationID() = %q, want %q", got, "corr-1")
	}
	if got, _ := command.SowKey(); got != "k1" {
		t.Fatalf("SowKey() = %q, want %q", got, "k1")
	}
	if got, _ := command.SubID(); got != "sub-1" {
		t.Fatalf("SubID() = %q, want %q", got, "sub-1")
	}
	if got, _ := command.Expiration(); got != 7 {
		t.Fatalf("Expiration() = %d, want 7", got)
	}
}
