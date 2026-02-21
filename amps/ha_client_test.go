package amps

import (
	"path/filepath"
	"testing"
	"time"
)

type fixedChooser struct {
	uri string
}

func (chooser *fixedChooser) CurrentURI() string                         { return chooser.uri }
func (chooser *fixedChooser) CurrentAuthenticator() Authenticator        { return nil }
func (chooser *fixedChooser) ReportFailure(error, ConnectionInfo)        {}
func (chooser *fixedChooser) ReportSuccess(ConnectionInfo)               {}
func (chooser *fixedChooser) Error() string                              { return "" }
func (chooser *fixedChooser) Add(uri string) ServerChooser               { chooser.uri = uri; return chooser }
func (chooser *fixedChooser) Remove(string)                              {}

type errorReconnectStrategy struct{}

func (errorReconnectStrategy) GetConnectWaitDuration(string) (time.Duration, error) {
	return 0, NewError(ConnectionError, "delay strategy failure")
}
func (errorReconnectStrategy) Reset() {}

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

func TestHAClientAdditionalWrappers(t *testing.T) {
	var nilHA *HAClient
	if !nilHA.Disconnected() {
		t.Fatalf("nil HA client should be disconnected")
	}
	if nilHA.Timeout() != 0 || nilHA.ReconnectDelay() != 0 || nilHA.ReconnectDelayStrategy() != nil || nilHA.ServerChooser() != nil {
		t.Fatalf("unexpected nil HA getter values")
	}
	if nilHA.Client() != nil {
		t.Fatalf("expected nil HA client handle")
	}
	if info := nilHA.GetConnectionInfo(); len(info) != 0 {
		t.Fatalf("expected empty nil HA connection info")
	}
	if info := nilHA.GatherConnectionInfo(); len(info) != 0 {
		t.Fatalf("expected empty nil HA gathered connection info")
	}
	if err := nilHA.Disconnect(); err != nil {
		t.Fatalf("nil HA disconnect should be nil, got %v", err)
	}
	nilHA.SetTimeout(-1)
	nilHA.SetReconnectDelay(-1)
	nilHA.SetReconnectDelayStrategy(nil)
	nilHA.SetLogonOptions(LogonParams{})
	nilHA.SetServerChooser(nil)

	ha := NewHAClient("ha-wrappers")
	ha.SetTimeout(-1 * time.Second)
	if ha.Timeout() != 0 {
		t.Fatalf("negative timeout should clamp to zero")
	}

	logonOptions := LogonParams{CorrelationID: "corr"}
	ha.SetLogonOptions(logonOptions)
	if got := ha.LogonOptions(); got.CorrelationID != "corr" {
		t.Fatalf("unexpected logon options: %+v", got)
	}

	ha.SetServerChooser(nil)
	if ha.ServerChooser() == nil {
		t.Fatalf("expected default server chooser after nil setter")
	}

	if info := ha.GetConnectionInfo(); info == nil {
		t.Fatalf("expected non-nil connection info map")
	}
	if info := ha.GatherConnectionInfo(); info == nil {
		t.Fatalf("expected non-nil gathered connection info map")
	}
	if ha.Client() == nil {
		t.Fatalf("expected non-nil embedded client")
	}
	if err := ha.Disconnect(); err == nil {
		t.Fatalf("expected disconnected error from HA disconnect with no active conn")
	}
}

func TestHAConnectAndLogonFailurePaths(t *testing.T) {
	ha := NewHAClient("ha-fail")
	ha.SetReconnectDelay(0)
	ha.SetTimeout(25 * time.Millisecond)
	ha.SetServerChooser(&fixedChooser{uri: "tcp://127.0.0.1:1/amps/json"})
	if err := ha.ConnectAndLogon(); err == nil {
		t.Fatalf("expected ConnectAndLogon failure with refused endpoint")
	}

	ha.stopped.Store(true)
	if err := ha.ConnectAndLogon(); err == nil {
		t.Fatalf("expected stopped HA client error")
	}

	ha.stopped.Store(false)
	ha.SetReconnectDelayStrategy(errorReconnectStrategy{})
	if err := ha.ConnectAndLogon(); err == nil {
		t.Fatalf("expected reconnect strategy error")
	}

	if err := ha.connectAndLogonOnce("", nil, LogonParams{}, false); err == nil {
		t.Fatalf("expected connectAndLogonOnce empty uri error")
	}
}

func TestHAHandleDisconnectGuards(t *testing.T) {
	ha := NewHAClient("ha-handle-disconnect")
	ha.stopped.Store(true)
	ha.handleDisconnect(NewError(ConnectionError, "stopped"))

	ha.stopped.Store(false)
	ha.reconnecting.Store(true)
	ha.handleDisconnect(NewError(ConnectionError, "already reconnecting"))

	ha.reconnecting.Store(false)
	ha.SetServerChooser(&fixedChooser{uri: "tcp://127.0.0.1:1/amps/json"})
	ha.SetReconnectDelay(0)
	ha.SetTimeout(10 * time.Millisecond)
	ha.handleDisconnect(NewError(ConnectionError, "trigger reconnect"))
	time.Sleep(30 * time.Millisecond)
	reconnecting := ha.reconnecting.Load()
	if reconnecting {
		t.Fatalf("expected reconnecting flag to clear after reconnect attempt")
	}
}

func TestCreateFileBackedAliases(t *testing.T) {
	tempDir := t.TempDir()
	publishPath := filepath.Join(tempDir, "publish.json")
	bookmarkPath := filepath.Join(tempDir, "bookmark.json")
	ha := CreateFileBackedHAClient(publishPath, bookmarkPath, "ha-file")
	if ha == nil || ha.Client() == nil {
		t.Fatalf("expected file-backed HA client")
	}
	if ha.Client().PublishStore() == nil || ha.Client().BookmarkStore() == nil {
		t.Fatalf("expected file-backed stores to be configured")
	}

	if alias := CreateMemoryBacked("ha-memory-alias"); alias == nil || alias.Client() == nil {
		t.Fatalf("expected memory-backed alias client")
	}
	if alias := CreateFileBacked(publishPath, bookmarkPath, "ha-file-alias"); alias == nil || alias.Client() == nil {
		t.Fatalf("expected file-backed alias client")
	}
}
