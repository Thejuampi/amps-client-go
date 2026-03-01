package main

import (
	"bytes"
	"compress/zlib"
	"io"
	"sync"
	"testing"
	"time"
)

type testConn struct {
	mu     sync.Mutex
	writes [][]byte
}

func (c *testConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make([]byte, len(p))
	copy(cp, p)
	c.writes = append(c.writes, cp)
	return len(p), nil
}

func (c *testConn) Bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]byte, 0)
	for _, w := range c.writes {
		out = append(out, w...)
	}
	return out
}

func TestConnWriterCompressFrame(t *testing.T) {
	cw := &connWriter{}
	input := bytes.Repeat([]byte("a"), 128)
	compressed := cw.compressFrame(input)

	if len(compressed) <= 1 || compressed[0] != 'z' {
		t.Fatalf("expected compressed frame prefixed with z")
	}

	r, err := zlib.NewReader(bytes.NewReader(compressed[1:]))
	if err != nil {
		t.Fatalf("failed to create zlib reader: %v", err)
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("failed reading decompressed bytes: %v", err)
	}
	if !bytes.Equal(out, input) {
		t.Fatalf("unexpected decompressed output")
	}
}

func TestConnWriterEnableAndQueueSend(t *testing.T) {
	cw := &connWriter{ch: make(chan []byte, 1)}
	cw.EnableCompression()
	if !cw.compress {
		t.Fatalf("expected compression enabled")
	}

	cw.send([]byte("a"))
	if len(cw.ch) != 1 {
		t.Fatalf("expected one queued frame")
	}

	// queue full: this send should be dropped
	cw.send([]byte("b"))
	if len(cw.ch) != 1 {
		t.Fatalf("expected queue size unchanged on overflow")
	}
}

func TestConnWriterRunAndClose(t *testing.T) {
	conn := &testConn{}
	stats := &connStats{}
	cw := newConnWriter(conn, stats)

	cw.send([]byte("frame1"))
	cw.sendDirect([]byte("frame2"))
	time.Sleep(20 * time.Millisecond)
	cw.close()

	out := string(conn.Bytes())
	if out == "" || !bytes.Contains([]byte(out), []byte("frame")) {
		t.Fatalf("expected written frames in output: %q", out)
	}
	if stats.messagesOut.Load() == 0 {
		t.Fatalf("expected messagesOut stats to increment")
	}
}

func TestStatsLogger(t *testing.T) {
	var stats = &connStats{}
	stats.messagesIn.Store(10)
	stats.messagesOut.Store(20)
	stats.publishIn.Store(5)
	stats.publishOut.Store(7)

	var done = make(chan struct{})
	go statsLoggerWithInterval("test", stats, done, 5*time.Millisecond)
	time.Sleep(15 * time.Millisecond)
	close(done)
	time.Sleep(10 * time.Millisecond)
	// no panic means success
}
