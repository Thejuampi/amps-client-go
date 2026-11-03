package amps

import (
	"errors"
	"net/url"
	"sync"
	"testing"
	"time"
)

func auditConnectedClient(t *testing.T) (*Client, *testConn) {
	t.Helper()
	var client = NewClient("behavior-audit")
	var connection = newTestConn()
	client.connection = connection
	client.connected.Store(true)
	t.Cleanup(func() { _ = client.Close() })
	return client, connection
}

func auditBookmarkMessage(bookmark string) *Message {
	return commandToMessage(NewCommand("publish").SetTopic("orders").SetSubID("sub-1").SetBookmark(bookmark))
}

type auditFailedSubscriptionManager struct{ *DefaultSubscriptionManager }

func (manager *auditFailedSubscriptionManager) Resubscribe(*Client) error {
	return NewError(NotEntitledError, "resubscribe denied")
}

func auditLogon(t *testing.T, client *Client, params LogonParams, ack *Message) error {
	t.Helper()
	if client.url == nil {
		client.url, _ = url.Parse("tcp://localhost:9000/amps/json")
	}
	var result = make(chan error, 1)
	go func() { result <- client.Logon(params) }()
	var id, _ = waitForAnyRouteHandler(t, client)
	ack.header.commandID = []byte(id)
	_ = client.onMessage(ack)
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("logon did not return")
		return nil
	}
}

type auditRetryFailureAuthenticator struct{}

func (auditRetryFailureAuthenticator) Authenticate(string, string) (string, error) {
	return "token", nil
}

func (auditRetryFailureAuthenticator) Retry(string, string) (string, error) {
	return "", errors.New("retry failed")
}

func (auditRetryFailureAuthenticator) Completed(string, string, string) {}

type auditSnapshotBookmarkStore struct {
	*MemoryBookmarkStore
	snapshot chan struct{}
	once     sync.Once
}

func (store *auditSnapshotBookmarkStore) GetMostRecent(string) string {
	store.once.Do(func() { close(store.snapshot) })
	return "1|1|"
}
