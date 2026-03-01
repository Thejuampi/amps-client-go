package amps

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func perfAPIIntegrationURI() string {
	var uri = strings.TrimSpace(os.Getenv("PERF_FAKEAMPS_URI"))
	if uri == "" {
		uri = "tcp://127.0.0.1:19000/amps/json"
	}
	return uri
}

func perfAPIRequireIntegrationEndpoint(b *testing.B, uri string) {
	b.Helper()

	var probe = NewClient("perf-api-integration-probe")
	var connectErr = probe.Connect(uri)
	if connectErr != nil {
		b.Skipf("integration benchmark skipped: fake AMPS endpoint unavailable at %s (%v)", uri, connectErr)
	}
	_ = probe.Close()
}

func perfAPIConnectAndLogon(b *testing.B, clientName string, uri string) *Client {
	b.Helper()

	var client = NewClient(clientName)
	var connectErr = client.Connect(uri)
	if connectErr != nil {
		b.Fatalf("connect failed: %v", connectErr)
	}

	var logonErr = client.Logon()
	if logonErr != nil {
		_ = client.Close()
		b.Fatalf("logon failed: %v", logonErr)
	}

	return client
}

func perfAPIProcessedAckResultHandler(result chan<- error) func(*Message) error {
	return func(message *Message) error {
		var commandType, hasCommandType = message.Command()
		if !hasCommandType || commandType != CommandAck {
			return nil
		}

		var ackType, hasAckType = message.AckType()
		if !hasAckType || ackType != AckTypeProcessed {
			return nil
		}

		var status, _ = message.Status()
		if status == "success" {
			select {
			case result <- nil:
			default:
			}
			return nil
		}

		var reason, hasReason = message.Reason()
		if hasReason && strings.TrimSpace(reason) != "" {
			select {
			case result <- errors.New(reason):
			default:
			}
			return nil
		}

		select {
		case result <- errors.New("processed ack failure"):
		default:
		}
		return nil
	}
}

func perfAPIWaitForProcessedAck(b *testing.B, result <-chan error, operation string) {
	b.Helper()

	select {
	case ackErr := <-result:
		if ackErr != nil {
			b.Fatalf("%s failed: %v", operation, ackErr)
		}
	case <-time.After(2 * time.Second):
		b.Fatalf("timed out waiting for %s processed ack", operation)
	}
}

func BenchmarkAPIIntegrationClientConnectLogon(b *testing.B) {
	var uri = perfAPIIntegrationURI()
	perfAPIRequireIntegrationEndpoint(b, uri)

	b.ReportAllocs()
	b.ResetTimer()

	var index int
	for index = 0; index < b.N; index++ {
		var clientName = fmt.Sprintf("perf-api-connect-logon-%d", index)
		var client = perfAPIConnectAndLogon(b, clientName, uri)
		var closeErr = client.Close()
		if closeErr != nil {
			b.Fatalf("close failed: %v", closeErr)
		}
	}
}

func BenchmarkAPIIntegrationClientPublish(b *testing.B) {
	var uri = perfAPIIntegrationURI()
	perfAPIRequireIntegrationEndpoint(b, uri)

	var client = perfAPIConnectAndLogon(b, "perf-api-publish", uri)
	defer func() {
		_ = client.Close()
	}()

	var topic = "amps.perf.integration.publish"
	var payload = []byte(`{"integration":true}`)

	b.ReportAllocs()
	b.ResetTimer()

	var index int
	for index = 0; index < b.N; index++ {
		var result = make(chan error, 1)
		var command = NewCommand("publish").SetTopic(topic).SetData(payload).AddAckType(AckTypeProcessed)
		var _, executeErr = client.ExecuteAsync(command, perfAPIProcessedAckResultHandler(result))
		if executeErr != nil {
			b.Fatalf("publish execute failed: %v", executeErr)
		}

		perfAPIWaitForProcessedAck(b, result, "publish")
	}
}

func BenchmarkAPIIntegrationClientSubscribe(b *testing.B) {
	var uri = perfAPIIntegrationURI()
	perfAPIRequireIntegrationEndpoint(b, uri)

	var client = perfAPIConnectAndLogon(b, "perf-api-subscribe", uri)
	defer func() {
		_ = client.Close()
	}()

	var topic = "amps.perf.integration.subscribe"

	b.ReportAllocs()
	b.ResetTimer()

	var index int
	for index = 0; index < b.N; index++ {
		var result = make(chan error, 1)
		var command = NewCommand("subscribe").SetTopic(topic).AddAckType(AckTypeProcessed)
		var subID, executeErr = client.ExecuteAsync(command, perfAPIProcessedAckResultHandler(result))
		if executeErr != nil {
			b.Fatalf("subscribe execute failed: %v", executeErr)
		}

		perfAPIWaitForProcessedAck(b, result, "subscribe")

		var unsubscribeErr = client.Unsubscribe(subID)
		if unsubscribeErr != nil {
			b.Fatalf("unsubscribe failed: %v", unsubscribeErr)
		}
	}
}

func BenchmarkAPIIntegrationHAConnectAndLogon(b *testing.B) {
	var uri = perfAPIIntegrationURI()
	perfAPIRequireIntegrationEndpoint(b, uri)

	var badURI = "tcp://127.0.0.1:1/amps/json"

	b.ReportAllocs()
	b.ResetTimer()

	var index int
	for index = 0; index < b.N; index++ {
		var ha = NewHAClient("perf-api-ha")
		ha.SetTimeout(2 * time.Second)
		ha.SetReconnectDelay(0)
		ha.SetReconnectDelayStrategy(NewFixedDelayStrategy(0))
		ha.SetServerChooser(NewDefaultServerChooser(badURI, uri))

		var connectErr = ha.ConnectAndLogon()
		if connectErr != nil {
			b.Fatalf("HA connect and logon failed: %v", connectErr)
		}

		var disconnectErr = ha.Disconnect()
		if disconnectErr != nil {
			b.Fatalf("HA disconnect failed: %v", disconnectErr)
		}
	}
}
