package amps

import (
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func perfAPIRequireIntegrationEndpointQuick(b *testing.B, uri string) {
	b.Helper()

	var parsedURI, parseErr = url.Parse(uri)
	if parseErr != nil {
		b.Skipf("integration benchmark skipped: invalid uri %s (%v)", uri, parseErr)
	}

	var conn, dialErr = net.DialTimeout("tcp", parsedURI.Host, 500*time.Millisecond)
	if dialErr != nil {
		b.Skipf("integration benchmark skipped: fake AMPS endpoint unavailable at %s (%v)", uri, dialErr)
	}
	_ = conn.Close()
}

func perfAPIIntegrationFailoverURIs() []string {
	var raw = strings.TrimSpace(os.Getenv("PERF_FAKEAMPS_FAILOVER_URIS"))
	if raw == "" {
		return nil
	}

	var uris []string
	for _, part := range strings.Split(raw, ",") {
		var uri = strings.TrimSpace(part)
		if uri == "" {
			continue
		}
		uris = append(uris, uri)
	}

	return uris
}

func BenchmarkIntegrationClientQueueAckRoundtrip(b *testing.B) {
	var uri = perfAPIIntegrationURI()
	perfAPIRequireIntegrationEndpointQuick(b, uri)

	var publisher = perfAPIConnectAndLogon(b, "perf-integration-queue-pub", uri)
	defer func() {
		_ = publisher.Close()
	}()

	var consumer = perfAPIConnectAndLogon(b, "perf-integration-queue-consumer", uri)
	defer func() {
		_ = consumer.Close()
	}()

	var topic = "queue://amps.perf.integration.ack"
	var deliveries = make(chan string, 64)
	var _, subscribeErr = consumer.ExecuteAsync(
		NewCommand("subscribe").
			SetTopic(topic).
			SetSubID("perf-integration-queue-sub").
			AddAckType(AckTypeProcessed),
		func(message *Message) error {
			if message == nil {
				return nil
			}

			var command, hasCommand = message.Command()
			if hasCommand && command == CommandAck {
				return nil
			}

			if command == CommandPublish {
				var sowKey, hasSowKey = message.SowKey()
				if hasSowKey {
					select {
					case deliveries <- sowKey:
					default:
					}
				}
			}

			return nil
		},
	)
	if subscribeErr != nil {
		b.Fatalf("queue subscribe failed: %v", subscribeErr)
	}

	time.Sleep(20 * time.Millisecond)

	var publishWaiter = newPerfAPIAckWaiter()
	var payload = []byte(`{"integration":true,"queue_ack":true}`)

	b.ReportAllocs()
	b.ResetTimer()

	var index int
	for index = 0; index < b.N; index++ {
		publishWaiter.reset()
		var _, publishErr = publisher.ExecuteAsync(
			NewCommand("publish").
				SetTopic(topic).
				SetData(payload).
				SetSowKey("ack-"+strconv.Itoa(index)).
				AddAckType(AckTypeProcessed),
			publishWaiter.handler,
		)
		if publishErr != nil {
			b.Fatalf("queue publish failed: %v", publishErr)
		}
		perfAPIWaitForProcessedAck(b, publishWaiter, "queue publish")

		var deliveryKey string
		select {
		case deliveryKey = <-deliveries:
		case <-time.After(2 * time.Second):
			b.Fatalf("timed out waiting for queue delivery")
		}

		var _, ackErr = consumer.ExecuteAsync(
			NewCommand("ack").
				SetSowKey(deliveryKey),
			nil,
		)
		if ackErr != nil {
			b.Fatalf("queue ack failed: %v", ackErr)
		}
	}

	b.StopTimer()
	_ = consumer.Unsubscribe("perf-integration-queue-sub")
}

func BenchmarkIntegrationHAFailoverConnectAndLogon(b *testing.B) {
	var uri = perfAPIIntegrationURI()
	perfAPIRequireIntegrationEndpointQuick(b, uri)

	var chooserURIs = perfAPIIntegrationFailoverURIs()
	if len(chooserURIs) == 0 {
		chooserURIs = []string{"tcp://127.0.0.1:1/amps/json", uri}
	}

	b.ReportAllocs()
	b.ResetTimer()

	var index int
	for index = 0; index < b.N; index++ {
		b.StopTimer()
		var ha = NewHAClient("perf-integration-ha-failover")
		ha.SetTimeout(2 * time.Second)
		ha.SetReconnectDelay(0)
		ha.SetReconnectDelayStrategy(NewFixedDelayStrategy(0))
		ha.SetServerChooser(NewDefaultServerChooser(chooserURIs...))
		b.StartTimer()

		var connectErr = ha.ConnectAndLogon()
		if connectErr != nil {
			b.Fatalf("HA failover connect and logon failed: %v", connectErr)
		}

		b.StopTimer()
		var disconnectErr = ha.Disconnect()
		if disconnectErr != nil {
			b.Fatalf("HA disconnect failed: %v", disconnectErr)
		}
		b.StartTimer()
	}

	b.StopTimer()
}
