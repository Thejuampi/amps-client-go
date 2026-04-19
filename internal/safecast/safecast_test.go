package safecast

import (
	"math"
	"testing"
)

func TestInt64FromUint64Saturating(t *testing.T) {
	if got := Int64FromUint64Saturating(0); got != 0 {
		t.Fatalf("Int64FromUint64Saturating(0) = %d, want 0", got)
	}
	if got := Int64FromUint64Saturating(math.MaxInt64); got != math.MaxInt64 {
		t.Fatalf("Int64FromUint64Saturating(MaxInt64) = %d, want MaxInt64", got)
	}
	if got := Int64FromUint64Saturating(math.MaxUint64); got != math.MaxInt64 {
		t.Fatalf("Int64FromUint64Saturating(MaxUint64) = %d, want MaxInt64", got)
	}
}

func TestInt64FromUint64Checked(t *testing.T) {
	if got, ok := Int64FromUint64Checked(math.MaxInt64); !ok || got != math.MaxInt64 {
		t.Fatalf("Int64FromUint64Checked(MaxInt64) = %d, %v, want MaxInt64, true", got, ok)
	}
	if _, ok := Int64FromUint64Checked(math.MaxInt64 + 1); ok {
		t.Fatalf("Int64FromUint64Checked(MaxInt64+1) = ok, want overflow failure")
	}
}

func TestUnsignedAndSignedWidthChecks(t *testing.T) {
	if got, ok := Uint16FromIntChecked(65535); !ok || got != 65535 {
		t.Fatalf("Uint16FromIntChecked(65535) = %d, %v, want 65535, true", got, ok)
	}
	if _, ok := Uint16FromIntChecked(65536); ok {
		t.Fatalf("Uint16FromIntChecked(65536) = ok, want overflow failure")
	}
	if _, ok := Uint16FromIntChecked(-1); ok {
		t.Fatalf("Uint16FromIntChecked(-1) = ok, want negative failure")
	}

	if got, ok := Uint32FromIntChecked(1<<20); !ok || got != 1<<20 {
		t.Fatalf("Uint32FromIntChecked(1<<20) = %d, %v, want 1<<20, true", got, ok)
	}
	if _, ok := Int32FromIntChecked(math.MaxInt32 + 1); ok {
		t.Fatalf("Int32FromIntChecked(MaxInt32+1) = ok, want overflow failure")
	}
	if got, ok := Uint64FromIntChecked(42); !ok || got != 42 {
		t.Fatalf("Uint64FromIntChecked(42) = %d, %v, want 42, true", got, ok)
	}
	if got, ok := Uint64FromInt64Checked(43); !ok || got != 43 {
		t.Fatalf("Uint64FromInt64Checked(43) = %d, %v, want 43, true", got, ok)
	}
	if _, ok := Uint64FromInt64Checked(-1); ok {
		t.Fatalf("Uint64FromInt64Checked(-1) = ok, want negative failure")
	}
}
