package amps

import (
	"testing"
	"time"
)

func TestHAClientSetDisconnectHandlerUnsupported(t *testing.T) {
	ha := NewHAClient("ha-disconnect")
	err := ha.SetDisconnectHandler(func(client *HAClient, err error) {})
	if err == nil {
		t.Fatalf("expected usage error for HAClient.SetDisconnectHandler")
	}
}

func TestHAClientNoURIConnectAndLogon(t *testing.T) {
	ha := NewHAClient("ha-no-uri")
	ha.SetTimeout(10 * time.Millisecond)
	if err := ha.ConnectAndLogon(); err == nil {
		t.Fatalf("expected connect/logon error when no URI is configured")
	}
}

func TestCreateMemoryBackedHAClientSetsStores(t *testing.T) {
	ha := CreateMemoryBackedHAClient("ha-memory")
	if ha == nil || ha.Client() == nil {
		t.Fatalf("expected non-nil HA client")
	}
	if ha.Client().PublishStore() == nil {
		t.Fatalf("expected memory publish store to be configured")
	}
	if ha.Client().BookmarkStore() == nil {
		t.Fatalf("expected memory bookmark store to be configured")
	}
}

func TestHAReconnectDelayStrategySetters(t *testing.T) {
	ha := NewHAClient("ha-strategy")
	ha.SetReconnectDelay(123 * time.Millisecond)
	if delay := ha.ReconnectDelay(); delay != 123*time.Millisecond {
		t.Fatalf("unexpected reconnect delay: %v", delay)
	}

	strategy := NewExponentialDelayStrategy(10*time.Millisecond, 100*time.Millisecond, 2)
	ha.SetReconnectDelayStrategy(strategy)
	if ha.ReconnectDelayStrategy() != strategy {
		t.Fatalf("expected reconnect strategy to be set")
	}
}
