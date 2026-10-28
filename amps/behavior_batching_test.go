package amps

import (
	"net"
	"strings"
	"testing"
	"time"
)

func TestPublishBatchFlushesAtCPPThreshold(t *testing.T) {
	var direct, probe = auditConnectedClient(t)
	if err := direct.Publish("orders", "payload"); err != nil {
		t.Fatal(err)
	}
	var frameSize = len(probe.WrittenBytes())
	var client, connection = auditConnectedClient(t)
	client.SetPublishBatching(uint64(frameSize*2+32), 60000)
	if err := client.Publish("orders", "payload"); err != nil {
		t.Fatal(err)
	}
	if len(connection.WrittenBytes()) != 0 {
		t.Fatal("batch flushed below threshold")
	}
	if err := client.Publish("orders", "payload"); err != nil {
		t.Fatal(err)
	}
	if got := len(connection.WrittenBytes()); got != frameSize*2 {
		t.Fatalf("batch boundary: wrote %d, want %d", got, frameSize*2)
	}
}

type notifiedWriteConn struct {
	*testConn
	written chan struct{}
}

func (connection *notifiedWriteConn) Write(data []byte) (int, error) {
	var count, err = connection.testConn.Write(data)
	select {
	case connection.written <- struct{}{}:
	default:
	}
	return count, err
}

func TestPublishBatchTimeoutFlushesWithoutAnotherSend(t *testing.T) {
	var client, connection = auditConnectedClient(t)
	var notified = &notifiedWriteConn{testConn: connection, written: make(chan struct{}, 2)}
	client.connection = notified
	client.SetPublishBatching(1<<20, 10)
	if err := client.Publish("orders", "timed"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-notified.written:
	case <-time.After(time.Second):
		t.Fatal("publish batch timer did not flush")
	}
	if !strings.Contains(connection.WrittenPayload(), "timed") {
		t.Fatal("timer flushed the wrong buffer")
	}
}

func TestPublishBatchControlCommandAndDisableFlushInOrder(t *testing.T) {
	for _, disable := range []bool{false, true} {
		t.Run(map[bool]string{false: "control", true: "disable"}[disable], func(t *testing.T) {
			var client, connection = auditConnectedClient(t)
			client.SetPublishBatching(1<<20, 60000)
			if err := client.Publish("orders", "pending"); err != nil {
				t.Fatal(err)
			}
			if len(connection.WrittenBytes()) != 0 {
				t.Fatal("batch sent immediately")
			}
			if disable {
				client.SetPublishBatching(0, 0)
			} else if err := client.Ack("orders", "1|1|"); err != nil {
				t.Fatal(err)
			}
			var wire = string(connection.WrittenBytes())
			if !strings.Contains(wire, "pending") || (!disable && strings.Index(wire, "publish") >= strings.Index(wire, "sow_delete")) {
				t.Fatalf("control overtook publish: %q", wire)
			}
		})
	}
}

func TestPublishBatchNeverReusesAnOldConnectionBuffer(t *testing.T) {
	var client, first = auditConnectedClient(t)
	client.SetPublishBatching(1<<20, 60000)
	if err := client.Publish("orders", "old-session"); err != nil {
		t.Fatal(err)
	}
	var replacement net.Conn = newTestConn()
	client.lock.Lock()
	client.connection = replacement
	client.generation.Add(1)
	var err = client.send(NewCommand("logon"))
	client.lock.Unlock()
	_ = first.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(first.WrittenBytes()) != 0 {
		t.Fatal("old batch was sent after changing sessions")
	}
	if got := replacement.(*testConn).WrittenPayload(); strings.Contains(got, "old-session") || !strings.Contains(got, `"c":"logon"`) {
		t.Fatalf("old batch leaked into new session: %q", got)
	}
}

func BenchmarkPublishBatching(b *testing.B) {
	for _, limit := range []uint64{0, 4096} {
		var name = "immediate"
		if limit != 0 {
			name = "batch4096"
		}
		b.Run(name, func(b *testing.B) {
			var client = NewClient("batch-benchmark")
			client.connected.Store(true)
			client.connection = &noProgressConn{}
			client.SetPublishBatching(limit, 60000)
			b.Cleanup(func() { _ = client.Close() })
			var payload = []byte(`{"id":1}`)
			b.ReportAllocs()
			for b.Loop() {
				if err := client.PublishBytes("orders", payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestPublishBatchTimeoutDoesNotOverflow(t *testing.T) {
	var cases = []struct {
		millis   uint64
		expected time.Duration
	}{
		{0, 0}, {1, time.Millisecond}, {^uint64(0), 9223372036854000000},
	}
	for _, entry := range cases {
		var client = NewClient("batch-timeout-boundary")
		client.SetPublishBatching(0, entry.millis)
		if _, timeout := client.PublishBatching(); timeout != entry.expected {
			t.Fatalf("timeout overflow for %d: %s", entry.millis, timeout)
		}
	}
}
