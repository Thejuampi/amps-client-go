package amps

import (
	"testing"
	"time"
)

func TestFixedDelayStrategy(t *testing.T) {
	strategy := NewFixedDelayStrategy(250 * time.Millisecond)
	delay1, err := strategy.GetConnectWaitDuration("tcp://localhost:9000/amps/json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	delay2, _ := strategy.GetConnectWaitDuration("tcp://localhost:9000/amps/json")
	if delay1 != 250*time.Millisecond || delay2 != 250*time.Millisecond {
		t.Fatalf("expected fixed delay of 250ms, got %v and %v", delay1, delay2)
	}
}

func TestExponentialDelayStrategy(t *testing.T) {
	strategy := NewExponentialDelayStrategy(50*time.Millisecond, 400*time.Millisecond, 2)

	first, _ := strategy.GetConnectWaitDuration("a")
	second, _ := strategy.GetConnectWaitDuration("a")
	third, _ := strategy.GetConnectWaitDuration("a")
	if !(first < second && second <= third) {
		t.Fatalf("expected monotonic exponential delays, got %v, %v, %v", first, second, third)
	}

	strategy.Reset()
	reset, _ := strategy.GetConnectWaitDuration("a")
	if reset != first {
		t.Fatalf("expected reset delay to return to %v, got %v", first, reset)
	}
}
