package amps

import (
	"os"
	"runtime"
	"strings"
	"sync/atomic"
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

type perfAPIAckWaiter struct {
	state atomic.Int32
}

func newPerfAPIAckWaiter() *perfAPIAckWaiter {
	var waiter = &perfAPIAckWaiter{}
	return waiter
}

func (waiter *perfAPIAckWaiter) reset() {
	if waiter == nil {
		return
	}
	waiter.state.Store(0)
}

func (waiter *perfAPIAckWaiter) handler(message *Message) error {
	if waiter == nil || message == nil {
		return nil
	}

	var status, hasStatus = message.Status()
	if !hasStatus {
		return nil
	}

	if status == "success" {
		waiter.state.CompareAndSwap(0, 1)
		return nil
	}

	waiter.state.CompareAndSwap(0, -1)
	return nil
}

func perfAPIWaitForProcessedAck(b *testing.B, waiter *perfAPIAckWaiter, operation string) {
	b.Helper()
	if waiter == nil {
		b.Fatalf("nil ack waiter for %s", operation)
	}

	var deadline = time.Now().Add(2 * time.Second)
	for {
		var state = waiter.state.Load()
		if state == 1 {
			return
		}
		if state == -1 {
			b.Fatalf("%s failed", operation)
		}
		if time.Now().After(deadline) {
			b.Fatalf("timed out waiting for %s processed ack", operation)
		}
		runtime.Gosched()
	}
}

func BenchmarkAPIIntegrationClientConnectLogon(b *testing.B) {
	var uri = perfAPIIntegrationURI()
	perfAPIRequireIntegrationEndpoint(b, uri)

	b.ReportAllocs()

	b.ResetTimer()

	var index int
	for index = 0; index < b.N; index++ {
		b.StopTimer()
		var clientName = "perf-api-connect-logon"
		var client = NewClient(clientName)
		b.StartTimer()

		var connectErr = client.Connect(uri)
		if connectErr != nil {
			b.Fatalf("connect failed: %v", connectErr)
		}

		var logonErr = client.Logon()
		if logonErr != nil {
			_ = client.Close()
			b.Fatalf("logon failed: %v", logonErr)
		}

		b.StopTimer()
		var closeErr = client.Close()
		if closeErr != nil {
			b.Fatalf("close failed: %v", closeErr)
		}
		b.StartTimer()
	}
	b.StopTimer()
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
	var waiter = newPerfAPIAckWaiter()
	var command = NewCommand("publish").SetTopic(topic).SetData(payload).AddAckType(AckTypeProcessed)
	var handler = waiter.handler

	b.ReportAllocs()
	b.ResetTimer()

	var index int
	for index = 0; index < b.N; index++ {
		waiter.reset()
		var _, executeErr = client.ExecuteAsync(command, handler)
		if executeErr != nil {
			b.Fatalf("publish execute failed: %v", executeErr)
		}
		perfAPIWaitForProcessedAck(b, waiter, "publish")
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
	var waiter = newPerfAPIAckWaiter()
	var command = NewCommand("subscribe").SetTopic(topic).AddAckType(AckTypeProcessed)
	var handler = waiter.handler

	b.ReportAllocs()
	b.ResetTimer()

	var index int
	for index = 0; index < b.N; index++ {
		waiter.reset()
		// ExecuteAsync auto-populates sub_id from command_id when sub_id is nil.
		// Reset between iterations so reused command values keep route correlation.
		command.header.subID = nil
		var subID, executeErr = client.ExecuteAsync(command, handler)
		if executeErr != nil {
			b.Fatalf("subscribe execute failed: %v", executeErr)
		}
		perfAPIWaitForProcessedAck(b, waiter, "subscribe")

		b.StopTimer()
		var unsubscribeErr = client.Unsubscribe(subID)
		if unsubscribeErr != nil {
			b.Fatalf("unsubscribe failed: %v", unsubscribeErr)
		}
		b.StartTimer()
	}
	b.StopTimer()
}

func BenchmarkAPIIntegrationHAConnectAndLogon(b *testing.B) {
	var uri = perfAPIIntegrationURI()
	perfAPIRequireIntegrationEndpoint(b, uri)

	b.ReportAllocs()
	b.ResetTimer()

	var index int
	for index = 0; index < b.N; index++ {
		b.StopTimer()
		var ha = NewHAClient("perf-api-ha")
		ha.SetTimeout(2 * time.Second)
		ha.SetReconnectDelay(0)
		ha.SetReconnectDelayStrategy(NewFixedDelayStrategy(0))
		ha.SetServerChooser(NewDefaultServerChooser(uri))
		b.StartTimer()

		var connectErr = ha.ConnectAndLogon()
		if connectErr != nil {
			b.Fatalf("HA connect and logon failed: %v", connectErr)
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
