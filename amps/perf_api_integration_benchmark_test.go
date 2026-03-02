package amps

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

const perfAPIConnectIterationCooldown = 5 * time.Millisecond

func perfAPIIntegrationURI() string {
	var uri = strings.TrimSpace(os.Getenv("PERF_FAKEAMPS_URI"))
	if uri == "" {
		uri = "tcp://127.0.0.1:19000/amps/json"
	}
	return uri
}

func perfAPIRequireIntegrationEndpoint(b *testing.B, uri string) {
	b.Helper()
	var connectErr = perfAPIAwaitEndpoint(uri, 20, 100*time.Millisecond, perfAPIDefaultProbe)
	if connectErr == nil {
		return
	}
	b.Skipf("integration benchmark skipped: fake AMPS endpoint unavailable at %s (%v)", uri, connectErr)
}

func perfAPIDefaultProbe(uri string) error {
	var probe = NewClient("perf-api-integration-probe")
	var connectErr = probe.Connect(uri)
	if connectErr != nil {
		return connectErr
	}
	_ = probe.Close()
	return nil
}

func perfAPIAwaitEndpoint(uri string, attempts int, delay time.Duration, probe func(string) error) error {
	if probe == nil {
		return errors.New("nil probe")
	}
	if attempts <= 0 {
		attempts = 1
	}

	var err error
	var attempt int
	for attempt = 0; attempt < attempts; attempt++ {
		err = probe(uri)
		if err == nil {
			return nil
		}
		if attempt+1 < attempts && delay > 0 {
			time.Sleep(delay)
		}
	}

	return err
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
	result chan int32
	timer  *time.Timer
}

func newPerfAPIAckWaiter() *perfAPIAckWaiter {
	var timer = time.NewTimer(time.Hour)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	var waiter = &perfAPIAckWaiter{
		result: make(chan int32, 1),
		timer:  timer,
	}
	return waiter
}

func (waiter *perfAPIAckWaiter) reset() {
	if waiter == nil {
		return
	}
	for {
		select {
		case <-waiter.result:
		default:
			return
		}
	}
}

func (waiter *perfAPIAckWaiter) signal(state int32) {
	if waiter == nil {
		return
	}

	select {
	case waiter.result <- state:
	default:
	}
}

func (waiter *perfAPIAckWaiter) await(timeout time.Duration) (int32, bool) {
	if waiter == nil {
		return 0, false
	}

	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	if waiter.timer == nil {
		waiter.timer = time.NewTimer(timeout)
	} else {
		if !waiter.timer.Stop() {
			select {
			case <-waiter.timer.C:
			default:
			}
		}
		waiter.timer.Reset(timeout)
	}

	select {
	case state := <-waiter.result:
		if !waiter.timer.Stop() {
			select {
			case <-waiter.timer.C:
			default:
			}
		}
		return state, true
	case <-waiter.timer.C:
		return 0, false
	}
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
		waiter.signal(1)
		return nil
	}

	waiter.signal(-1)
	return nil
}

func perfAPIWaitForProcessedAck(b *testing.B, waiter *perfAPIAckWaiter, operation string) {
	b.Helper()
	if waiter == nil {
		b.Fatalf("nil ack waiter for %s", operation)
	}

	var state, ok = waiter.await(2 * time.Second)
	if !ok {
		b.Fatalf("timed out waiting for %s processed ack", operation)
	}
	if state == 1 {
		return
	}
	b.Fatalf("%s failed", operation)
}

func TestPerfAPIAwaitEndpointSucceedsAfterRetry(t *testing.T) {
	var calls int
	var err = perfAPIAwaitEndpoint("tcp://127.0.0.1:19000/amps/json", 3, 0, func(uri string) error {
		_ = uri
		calls++
		if calls < 3 {
			return errors.New("connect failed")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected endpoint retry to succeed: %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected three probe attempts, got %d", calls)
	}
}

func TestPerfAPIAwaitEndpointReturnsLastError(t *testing.T) {
	var calls int
	var err = perfAPIAwaitEndpoint("tcp://127.0.0.1:19000/amps/json", 2, 0, func(uri string) error {
		_ = uri
		calls++
		return errors.New("connect failed")
	})
	if err == nil {
		t.Fatalf("expected endpoint retry failure")
	}
	if calls != 2 {
		t.Fatalf("expected two probe attempts, got %d", calls)
	}
}

func TestPerfAPIAwaitEndpointNilProbe(t *testing.T) {
	var err = perfAPIAwaitEndpoint("tcp://127.0.0.1:19000/amps/json", 1, 0, nil)
	if err == nil {
		t.Fatalf("expected nil probe error")
	}
}

func TestPerfAPIAckWaiterCoverage(t *testing.T) {
	var waiter = newPerfAPIAckWaiter()

	t.Run("success signal", func(t *testing.T) {
		waiter.reset()
		var err = waiter.handler(&Message{header: &_Header{status: []byte("success")}})
		if err != nil {
			t.Fatalf("unexpected waiter handler error: %v", err)
		}

		select {
		case state := <-waiter.result:
			if state != 1 {
				t.Fatalf("expected success state 1, got %d", state)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("timed out waiting for success state")
		}
	})

	t.Run("failure signal", func(t *testing.T) {
		waiter.reset()
		var err = waiter.handler(&Message{header: &_Header{status: []byte("failure")}})
		if err != nil {
			t.Fatalf("unexpected waiter handler error: %v", err)
		}

		select {
		case state := <-waiter.result:
			if state != -1 {
				t.Fatalf("expected failure state -1, got %d", state)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("timed out waiting for failure state")
		}
	})

	t.Run("reset clears pending state", func(t *testing.T) {
		waiter.reset()
		_ = waiter.handler(&Message{header: &_Header{status: []byte("success")}})
		waiter.reset()

		select {
		case <-waiter.result:
			t.Fatalf("expected reset to clear pending state")
		default:
		}
	})

	t.Run("await timeout", func(t *testing.T) {
		waiter.reset()
		var _, ok = waiter.await(5 * time.Millisecond)
		if ok {
			t.Fatalf("expected await timeout with no signal")
		}
	})
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
		time.Sleep(perfAPIConnectIterationCooldown)
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
	var unsubscribeCommand = NewCommand("unsubscribe")
	var handler = waiter.handler

	b.ReportAllocs()
	b.ResetTimer()

	var index int
	for index = 0; index < b.N; index++ {
		waiter.reset()
		command.header.subID = nil
		var subID, executeErr = client.ExecuteAsync(command, handler)
		if executeErr != nil {
			b.Fatalf("subscribe execute failed: %v", executeErr)
		}
		perfAPIWaitForProcessedAck(b, waiter, "subscribe")

		b.StopTimer()
		unsubscribeCommand.SetSubID(subID)
		var _, unsubscribeErr = client.ExecuteAsync(unsubscribeCommand, nil)
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
		time.Sleep(perfAPIConnectIterationCooldown)
		b.StartTimer()
	}
	b.StopTimer()
}
