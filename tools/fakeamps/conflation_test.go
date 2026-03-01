package main

import (
	"net"
	"testing"
	"time"
)

func TestParseConflationInterval(t *testing.T) {
	cases := []struct {
		options string
		want    time.Duration
	}{
		{"conflation=100ms", 100 * time.Millisecond},
		{"x=1,conflation=1s", 1 * time.Second},
		{"conflation=2m", 2 * time.Minute},
		{"conflation=bad", 0},
		{"other=1", 0},
	}

	for _, c := range cases {
		got := parseConflationInterval(c.options)
		if got != c.want {
			t.Fatalf("parseConflationInterval(%q)=%s want %s", c.options, got, c.want)
		}
	}
}

func TestParseConflationKey(t *testing.T) {
	cases := []struct {
		options string
		want    string
	}{
		{"conflation_key=id", "id"},
		{"conflation_key=/id", "id"},
		{"x=1,conflation_key=/sym,y=2", "sym"},
		{"other=1", ""},
	}

	for _, c := range cases {
		got := parseConflationKey(c.options)
		if got != c.want {
			t.Fatalf("parseConflationKey(%q)=%q want %q", c.options, got, c.want)
		}
	}
}

func TestConflationBufferAddMergesBySowKey(t *testing.T) {
	connA, connB := net.Pipe()
	defer connA.Close()
	defer connB.Close()

	sub := &subscription{conn: connA, subID: "s1", writer: newConnWriter(connA, &connStats{})}
	defer sub.writer.close()
	cb := newConflationBuffer(sub, time.Hour)
	defer cb.stop()

	cb.add("orders", "s1", []byte(`{"id":1,"v":1}`), "bm1", "ts1", "k1", "json", false)
	cb.add("orders", "s1", []byte(`{"id":1,"v":2}`), "bm2", "ts2", "k1", "json", false)

	cb.mu.Lock()
	defer cb.mu.Unlock()
	if len(cb.pending) != 1 {
		t.Fatalf("expected one merged pending entry got %d", len(cb.pending))
	}
}

func TestConflationBufferAddUsesConfiguredKey(t *testing.T) {
	connA, connB := net.Pipe()
	defer connA.Close()
	defer connB.Close()

	sub := &subscription{conn: connA, subID: "s1", writer: newConnWriter(connA, &connStats{})}
	defer sub.writer.close()
	cb := newConflationBufferWithKey(sub, time.Hour, "sym")
	defer cb.stop()

	cb.add("orders", "s1", []byte(`{"sym":"AAPL","v":1}`), "bm1", "ts1", "k1", "json", false)
	cb.add("orders", "s1", []byte(`{"sym":"AAPL","v":2}`), "bm2", "ts2", "k2", "json", false)

	cb.mu.Lock()
	defer cb.mu.Unlock()
	if len(cb.pending) != 1 {
		t.Fatalf("expected one merged entry by conflation key got %d", len(cb.pending))
	}
}

func TestConflationBufferStop(t *testing.T) {
	connA, connB := net.Pipe()
	defer connA.Close()
	defer connB.Close()

	sub := &subscription{conn: connA, subID: "s1", writer: newConnWriter(connA, &connStats{})}
	defer sub.writer.close()
	cb := newConflationBuffer(sub, time.Hour)
	cb.stop()

	cb.mu.Lock()
	stopped := cb.stopped
	cb.mu.Unlock()
	if !stopped {
		t.Fatalf("expected stopped=true")
	}
}

func TestConflationBufferFlush(t *testing.T) {
	conn := &testConn{}
	stats := &connStats{}
	cw := newConnWriter(conn, stats)
	defer cw.close()

	sub := &subscription{subID: "s1", writer: cw}
	cb := newConflationBuffer(sub, time.Hour)
	defer cb.stop()

	cb.add("orders", "s1", []byte(`{"id":1}`), "bm1", "ts1", "k1", "json", false)
	cb.flush()
	time.Sleep(20 * time.Millisecond)

	if stats.messagesOut.Load() == 0 {
		t.Fatalf("expected flush to deliver at least one message")
	}
}
