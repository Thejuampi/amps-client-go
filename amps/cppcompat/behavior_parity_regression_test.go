package cppcompat

import (
	"errors"
	"testing"

	"github.com/Thejuampi/amps-client-go/amps"
)

func TestParityAuditCRCMatchesCPP(t *testing.T) {
	// Golden value from the unmodified 5.3.5.14 CRC header, exercised by
	// tools/paritycheck/testdata/behavior_oracle.cpp.
	var want uint64 = 16845390139448941002
	if got := uint64((CRC{}).Compute([]byte("123456789"))); got != want {
		t.Fatalf("C++ CRC=%d, Go CRC=%d", want, got)
	}
}

func TestParityAuditHybridSequencesRemainUniqueOnFallback(t *testing.T) {
	var primary = &fakePublishStore{}
	var store = NewHybridPublishStore(primary, amps.NewMemoryPublishStore())
	var first, err = store.Store(amps.NewCommand("publish").SetTopic("orders"))
	if err != nil {
		t.Fatal(err)
	}
	primary.storeErr = errors.New("primary unavailable")
	var second, secondErr = store.Store(amps.NewCommand("publish").SetTopic("orders"))
	if secondErr != nil {
		t.Fatal(secondErr)
	}
	if second <= first {
		t.Fatalf("hybrid store reused/regressed sequence at fallback: first=%d second=%d", first, second)
	}
}
