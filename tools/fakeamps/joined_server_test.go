package main

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// startJoinedTestServer returns a stop function that must run before restoring
// globals used by handlers. Stopping acceptance alone does not join handlers.
func startJoinedTestServer(t *testing.T, handler func(net.Conn)) (net.Listener, func()) {
	t.Helper()
	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	var accepted []net.Conn
	var handlers sync.WaitGroup
	var acceptDone = make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			var conn, acceptErr = listener.Accept()
			if acceptErr != nil {
				return
			}
			accepted = append(accepted, conn)
			handlers.Add(1)
			go func() {
				defer handlers.Done()
				defer conn.Close()
				handler(conn)
			}()
		}
	}()
	var once sync.Once
	return listener, func() {
		once.Do(func() {
			_ = listener.Close()
			// Join acceptance before reading accepted or waiting for handlers:
			// no append or WaitGroup.Add may race with shutdown.
			<-acceptDone
			for _, conn := range accepted {
				_ = conn.Close()
			}
			handlers.Wait()
		})
	}
}

func TestJoinedTestServerStopsWithoutConnections(t *testing.T) {
	var listener, stop = startJoinedTestServer(t, func(conn net.Conn) {
		t.Error("unexpected connection")
	})
	stop()
	stop()
	var conn, err = net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err == nil {
		_ = conn.Close()
		t.Fatal("stopped listener still accepts connections")
	}
}

func TestJoinedTestServerWaitsForEveryHandlerCleanup(t *testing.T) {
	var entered = make(chan struct{}, 2)
	var closed = make(chan struct{}, 2)
	var release = make(chan struct{})
	var releaseOnce sync.Once
	var allowCleanup = func() { releaseOnce.Do(func() { close(release) }) }
	var completed atomic.Int32
	var listener, stop = startJoinedTestServer(t, func(conn net.Conn) {
		defer completed.Add(1)
		entered <- struct{}{}
		var one [1]byte
		_, _ = io.ReadFull(conn, one[:])
		closed <- struct{}{}
		<-release
	})
	defer stop()
	defer allowCleanup()
	for i := 0; i < 2; i++ {
		var conn, err = net.DialTimeout("tcp", listener.Addr().String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("handler did not start")
		}
	}
	var stopped = make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-closed:
		case <-time.After(2 * time.Second):
			t.Fatal("shutdown did not close every accepted socket")
		}
	}
	select {
	case <-stopped:
		t.Fatal("shutdown returned before handler cleanup")
	default:
	}
	allowCleanup()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not join handlers")
	}
	if got := completed.Load(); got != 2 {
		t.Fatalf("completed handler cleanups = %d, want 2", got)
	}
}
