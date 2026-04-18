package amps

import (
	"net/url"
	"testing"
	"time"
)

type legacyPublishStore struct{}

func (*legacyPublishStore) Store(*Command) (uint64, error)    { return 0, nil }
func (*legacyPublishStore) DiscardUpTo(uint64) error          { return nil }
func (*legacyPublishStore) Replay(func(*Command) error) error { return nil }
func (*legacyPublishStore) ReplaySingle(func(*Command) error, uint64) (bool, error) {
	return false, nil
}
func (*legacyPublishStore) UnpersistedCount() int        { return 0 }
func (*legacyPublishStore) Flush(time.Duration) error    { return nil }
func (*legacyPublishStore) GetLowestUnpersisted() uint64 { return 0 }
func (*legacyPublishStore) GetLastPersisted() uint64     { return 0 }
func (*legacyPublishStore) SetErrorOnPublishGap(bool)    {}
func (*legacyPublishStore) ErrorOnPublishGap() bool      { return false }

var _ PublishStore = (*legacyPublishStore)(nil)

func TestClientLogonWithLegacyPublishStore(t *testing.T) {
	client := NewClient("legacy-publish-store")
	client.connected.Store(true)
	client.resetDisconnectSignal()
	client.connection = newTestConn()
	client.url, _ = url.Parse("tcp://localhost:9007/amps/json")
	client.SetErrorHandler(func(error) {})
	client.SetPublishStore(&legacyPublishStore{})

	result := make(chan error, 1)
	go func() {
		result <- client.Logon()
	}()

	_, handler := waitForAnyRouteHandler(t, client)
	ack := AckTypeProcessed
	seq := uint64(42)
	_ = handler(&Message{header: &_Header{
		command:    CommandAck,
		ackType:    &ack,
		status:     []byte("success"),
		version:    []byte("5.3.5.1"),
		clientName: []byte("12345"),
		sequenceID: &seq,
	}})

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("expected legacy publish store logon success, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected legacy publish store logon to complete")
	}
}
