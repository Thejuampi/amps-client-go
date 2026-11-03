package amps

import (
	"errors"
	"strings"
	"testing"
	"time"
)

type failedChallengeAuthenticator struct{ auditRetryFailureAuthenticator }

func (failedChallengeAuthenticator) Begin(string, string) (string, error) { return "token", nil }
func (failedChallengeAuthenticator) Continue(string, string) (string, error) {
	return "", errors.New("challenge failed")
}

func TestAuthenticatorErrorsEndLogonWithoutTimeout(t *testing.T) {
	var cases = []struct {
		name          string
		authenticator Authenticator
		reason        string
	}{
		{"retry", auditRetryFailureAuthenticator{}, "retry failed"},
		{"challenge", failedChallengeAuthenticator{}, "challenge failed"},
	}
	for _, entry := range cases {
		t.Run(entry.name, func(t *testing.T) {
			var client, _ = auditConnectedClient(t)
			var ack = commandToMessage(NewCommand("ack").SetAckType(AckTypeProcessed))
			ack.header.status = []byte("retry")
			var err = auditLogon(t, client, LogonParams{Authenticator: entry.authenticator}, ack)
			if !IsErrorKind(err, AuthenticationError) || !strings.Contains(err.Error(), entry.reason) {
				t.Fatalf("auth error did not end unbounded logon: %v", err)
			}
		})
	}
}

func TestRecoveryFailurePreservesDeferredWork(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	var failure = errors.New("replay failed")
	var store = &replayFailurePublishStore{MemoryPublishStore: NewMemoryPublishStore(), replayErr: failure}
	client.SetPublishStore(store)
	var manager = &recordingSubscriptionManager{}
	client.SetSubscriptionManager(manager)
	var calls int
	client.DeferredExecution(func(*Client, any) { calls++ }, nil)
	if err := client.postLogonRecovery(); !errors.Is(err, failure) {
		t.Fatalf("recovery failure lost: %v", err)
	}
	var state = ensureClientState(client)
	if calls != 0 || manager.resubscribeCalls != 0 || len(state.deferredExecutions) != 1 || state.recoveryInProgress {
		t.Fatal("failed recovery advanced or forgot pending work")
	}
	store.replayErr = nil
	if err := client.postLogonRecovery(); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || manager.resubscribeCalls != 1 || len(state.deferredExecutions) != 0 || state.recoveryInProgress {
		t.Fatal("retry did not finish pending recovery exactly once")
	}
}

func TestSubscriptionGenerationHistoryHasSubscriptionLifetime(t *testing.T) {
	for _, action := range []string{"one", "all", "clear", "failed"} {
		t.Run(action, func(t *testing.T) {
			var manager = NewDefaultSubscriptionManager()
			var command = NewCommand("subscribe").SetTopic("orders").SetSubID("sub-1")
			command.generation = 1
			manager.Subscribe(nil, command, 0)
			switch action {
			case "one":
				manager.Unsubscribe("sub-1")
			case "all":
				manager.Unsubscribe("all")
			case "clear":
				manager.Clear()
			case "failed":
				manager.lock.Lock()
				manager.removeTrackedSubscription("sub-1")
				manager.lock.Unlock()
			}
			if len(manager.generations) != 0 || manager.hasGeneration("sub-1", 1) {
				t.Fatal("removed subscription retained its generation history")
			}
		})
	}
}

func TestFailedResubscribeRemainsRetryableInSameSession(t *testing.T) {
	var client, _ = auditConnectedClient(t)
	client.SetRetryOnDisconnect(true)
	var manager = NewDefaultSubscriptionManager()
	client.SetSubscriptionManager(manager)
	manager.Subscribe(nil, NewCommand("subscribe").SetSubID("sub-1").SetTopic("orders"), 0)
	var sent = make(chan struct{}, 2)
	client.SetTransportFilter(func(direction TransportFilterDirection, data []byte) []byte {
		if direction == TransportFilterOutbound && strings.Contains(string(data), `"c":"subscribe"`) {
			sent <- struct{}{}
		}
		return data
	})
	var done = make(chan error, 1)
	go func() { done <- manager.Resubscribe(client) }()
	<-sent
	var id, _ = waitForAnyRouteHandler(t, client)
	var failed = commandToMessage(NewCommand("ack").SetCommandID(id).SetAckType(AckTypeProcessed))
	failed.header.status = []byte("failure")
	failed.header.reason = []byte("not entitled")
	_ = client.onMessage(failed)
	if err := <-done; !IsErrorKind(err, NotEntitledError) {
		t.Fatalf("wrong recovery failure: %v", err)
	}
	go func() { done <- manager.Resubscribe(client) }()
	select {
	case <-sent:
	case err := <-done:
		t.Fatalf("failed subscription was incorrectly considered current: %v", err)
	case <-time.After(time.Second):
		t.Fatal("retry did not send")
	}
	id, _ = waitForAnyRouteHandler(t, client)
	var success = commandToMessage(NewCommand("ack").SetCommandID(id).SetAckType(AckTypeProcessed))
	success.header.status = []byte("success")
	_ = client.onMessage(success)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestFailedOldAttemptDoesNotInvalidateReplacementGeneration(t *testing.T) {
	var manager = NewDefaultSubscriptionManager()
	var command = NewCommand("subscribe").SetSubID("sub-1").SetTopic("orders").SetCommandID("replacement")
	command.generation = 2
	manager.Subscribe(nil, command, 0)
	manager.forgetGeneration("sub-1", "old-attempt")
	if !manager.hasGeneration("sub-1", 2) {
		t.Fatal("an older failure invalidated the replacement")
	}
	manager.forgetGeneration("sub-1", "replacement")
	if manager.hasGeneration("sub-1", 2) {
		t.Fatal("failed current attempt remained marked as sent")
	}
}
