package amps

import "testing"

func TestTimerCommandMappings(t *testing.T) {
	start := NewCommand("start_timer")
	stop := NewCommand("stop_timer")

	startValue, startSet := start.Command()
	if !startSet || startValue != "start_timer" {
		t.Fatalf("expected start_timer mapping, got %q (set=%v)", startValue, startSet)
	}

	stopValue, stopSet := stop.Command()
	if !stopSet || stopValue != "stop_timer" {
		t.Fatalf("expected stop_timer mapping, got %q (set=%v)", stopValue, stopSet)
	}
}

func TestBookmarkNowParity(t *testing.T) {
	if BookmarksNOW != "0|1|" {
		t.Fatalf("expected BookmarksNOW to be 0|1|, got %q", BookmarksNOW)
	}
	if BOOKMARK_NOW() != "0|1|" || NOW() != "0|1|" {
		t.Fatalf("bookmark now aliases should return 0|1|")
	}
}

func TestConvertVersionToNumber(t *testing.T) {
	if value := ConvertVersionToNumber("5.3.5.1"); value != 5030501 {
		t.Fatalf("expected 5030501, got %d", value)
	}
	if value := ConvertVersionToNumber("invalid"); value != 0 {
		t.Fatalf("expected 0 for invalid version, got %d", value)
	}
}
