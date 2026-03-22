package gofercli

import "testing"

func TestParseRateIntervalRejectsRatesAboveTimerResolution(t *testing.T) {
	if _, err := parseRateInterval("1000000001"); err == nil {
		t.Fatalf("expected error for rate above 1/ns timer resolution")
	}
}
