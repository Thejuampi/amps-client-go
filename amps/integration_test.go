package amps

import (
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func integrationURI(t *testing.T) string {
	t.Helper()
	uri := strings.TrimSpace(os.Getenv("AMPS_TEST_URI"))
	if uri == "" {
		t.Skip("integration test skipped: AMPS_TEST_URI is not set")
	}

	user := strings.TrimSpace(os.Getenv("AMPS_TEST_USER"))
	password := strings.TrimSpace(os.Getenv("AMPS_TEST_PASSWORD"))
	if user == "" {
		return applyIntegrationProtocol(uri)
	}

	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatalf("invalid AMPS_TEST_URI: %v", err)
	}
	if parsed.User == nil {
		parsed.User = url.UserPassword(user, password)
	}
	return applyIntegrationProtocol(parsed.String())
}

func applyIntegrationProtocol(uri string) string {
	protocol := strings.TrimSpace(os.Getenv("AMPS_TEST_PROTOCOL"))
	if protocol == "" {
		return uri
	}

	parsed, err := url.Parse(uri)
	if err != nil {
		return uri
	}
	if strings.Contains(parsed.Path, "/amps/") {
		return uri
	}

	parsed.Path = "/amps/" + protocol
	return parsed.String()
}

func TestIntegrationConnectLogonPublishSubscribe(t *testing.T) {
	uri := integrationURI(t)

	client := NewClient("integration-connect-logon")
	defer func() { _ = client.Close() }()

	if err := client.Connect(uri); err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	logonResult := make(chan error, 1)
	go func() {
		logonResult <- client.Logon()
	}()
	select {
	case err := <-logonResult:
		if err != nil {
			t.Fatalf("logon failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for logon ack")
	}

	topic := "amps.integration." + time.Now().UTC().Format("20060102150405.000000")
	received := make(chan struct{}, 1)
	type subscribeResult struct {
		id  string
		err error
	}
	subscribeDone := make(chan subscribeResult, 1)
	go func() {
		subID, err := client.SubscribeAsync(func(message *Message) error {
			if messageTopic, hasTopic := message.Topic(); hasTopic && messageTopic == topic {
				select {
				case received <- struct{}{}:
				default:
				}
			}
			return nil
		}, topic)
		subscribeDone <- subscribeResult{id: subID, err: err}
	}()

	var subID string
	select {
	case result := <-subscribeDone:
		if result.err != nil {
			t.Fatalf("subscribe failed: %v", result.err)
		}
		subID = result.id
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for subscribe ack")
	}
	defer func() { _ = client.Unsubscribe(subID) }()

	if err := client.Publish(topic, `{"integration":true}`); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for subscribed message")
	}
}

func TestIntegrationFailoverEnvContract(t *testing.T) {
	value := strings.TrimSpace(os.Getenv("AMPS_TEST_FAILOVER_URIS"))
	if value == "" {
		t.Skip("integration test skipped: AMPS_TEST_FAILOVER_URIS is not set")
	}

	parts := strings.Split(value, ",")
	usable := make([]string, 0, len(parts))
	for _, part := range parts {
		uri := strings.TrimSpace(part)
		if uri != "" {
			usable = append(usable, uri)
		}
	}
	if len(usable) == 0 {
		t.Fatalf("AMPS_TEST_FAILOVER_URIS is set but contains no usable URIs")
	}
}
