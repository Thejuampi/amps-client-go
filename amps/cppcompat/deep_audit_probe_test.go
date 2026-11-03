package cppcompat

import (
	"github.com/Thejuampi/amps-client-go/amps"
	"testing"
)

func TestDeepAuditFreshHybridDoesNotReuseOldPublisherSequences(t *testing.T) {
	var store = NewHybridPublishStore(amps.NewMemoryPublishStore(), amps.NewMemoryPublishStore())
	store.SetInitialSequence(42)
	var sequence, err = store.Store(amps.NewCommand("publish").SetTopic("orders"))
	if err != nil {
		t.Fatal(err)
	}
	if sequence <= 100 {
		t.Fatalf("hybrid overrode manufactured sequence with stale logon acknowledgement: got %d", sequence)
	}
}
