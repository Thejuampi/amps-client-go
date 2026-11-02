package amps

import (
	"context"
	"net/url"
	"testing"
	"time"
)

type deepAuditPublishingAuthenticator struct {
	client *Client
	called chan error
}

func (auth *deepAuditPublishingAuthenticator) Authenticate(string, string) (string, error) {
	return "token", nil
}

func (auth *deepAuditPublishingAuthenticator) Retry(string, string) (string, error) {
	var err = auth.client.Publish("orders", "")
	auth.called <- err
	return "retry", err
}

func (*deepAuditPublishingAuthenticator) Completed(string, string, string) {}

type deepAuditPublishingChallenge struct {
	*deepAuditPublishingAuthenticator
}

func (*deepAuditPublishingChallenge) Begin(string, string) (string, error) { return "token", nil }

func (auth *deepAuditPublishingChallenge) Continue(string, string) (string, error) {
	var err = auth.client.Publish("orders", "")
	auth.called <- err
	return "retry", err
}

func TestDeepAuditAuthenticatorRetryMayRetainPublish(t *testing.T) {
	for _, challenge := range []bool{false, true} {
		var name = "retry"
		if challenge {
			name = "challenge"
		}
		t.Run(name, func(t *testing.T) {
			var client, _ = auditConnectedClient(t)
			client.url, _ = url.Parse("tcp://localhost:9000/amps/json")
			var store = newMemoryPublishStore(1)
			client.SetPublishStore(store)
			var base = &deepAuditPublishingAuthenticator{client: client, called: make(chan error, 1)}
			var authenticator Authenticator = base
			if challenge {
				authenticator = &deepAuditPublishingChallenge{base}
			}
			var ctx, cancel = context.WithCancel(context.Background())
			defer cancel()
			var loggedOn = make(chan error, 1)
			go func() {
				loggedOn <- client.logonWithContext(ctx, LogonParams{Timeout: 1000, Authenticator: authenticator})
			}()
			var id, _ = waitForAnyRouteHandler(t, client)
			var retry = commandToMessage(NewCommand("ack").SetCommandID(id).SetAckType(AckTypeProcessed))
			retry.header.status = []byte("retry")
			retry.header.reason = []byte("challenge")
			var delivered = make(chan error, 1)
			go func() { delivered <- client.onMessage(retry) }()
			select {
			case err := <-delivered:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(300 * time.Millisecond):
				cancel()
				<-loggedOn
				<-delivered
				t.Fatal("authenticator publish deadlocked logon retry")
			}
			if err := <-base.called; err != nil {
				t.Fatal(err)
			}
			var success = commandToMessage(NewCommand("ack").SetCommandID(id).SetAckType(AckTypeProcessed))
			success.header.status = []byte("success")
			if err := client.onMessage(success); err != nil {
				t.Fatal(err)
			}
			if err := <-loggedOn; err != nil || store.UnpersistedCount() != 1 {
				t.Fatalf("logon error=%v retained=%d", err, store.UnpersistedCount())
			}
		})
	}
}
