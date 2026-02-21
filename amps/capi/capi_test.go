package capi

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientAttemptReconnectInvalidHandle(t *testing.T) {
	if result := ClientAttemptReconnect(Handle(0), 5); result != ENotFound {
		t.Fatalf("expected ENotFound, got %d", result)
	}
}

func TestRouteFunctionBridge(t *testing.T) {
	called := false
	SetWaitingFunction(func() {
		called = true
	})
	InvokeWaitingFunction()
	if !called {
		t.Fatalf("expected waiting callback to be invoked")
	}
}

func TestSSLCompatibilityFlow(t *testing.T) {
	SSLLibraryInit()
	SSLLoadErrorStrings()
	method := SSLv23ClientMethod()
	context := SSLCTXNew(method)
	if context == 0 {
		t.Fatalf("expected context handle")
	}
	handle := SSLNew(context)
	if handle == 0 {
		t.Fatalf("expected ssl handle")
	}
	if result := SSLSetFD(handle, 1); result != 1 {
		t.Fatalf("expected SSLSetFD result 1, got %d", result)
	}
	if result := SSLConnect(handle); result != 1 {
		t.Fatalf("expected SSLConnect result 1, got %d", result)
	}
	if result := SSLWrite(handle, []byte("payload")); result != len("payload") {
		t.Fatalf("expected SSLWrite length, got %d", result)
	}
	if result := SSLShutdown(handle); result != 1 {
		t.Fatalf("expected SSLShutdown result 1, got %d", result)
	}
	SSLFreeHandle(handle)
	SSLCTXFree(context)
}

func TestZlibCompatibilityRoundtrip(t *testing.T) {
	if result := ZlibInit(""); result != EOK {
		t.Fatalf("expected ZlibInit EOK, got %d", result)
	}
	if loaded := ZlibIsLoaded(); loaded != 1 {
		t.Fatalf("expected zlib loaded state")
	}
	input := []byte("hello amps parity")
	stream := &ZStream{NextIn: input}
	if result := DeflateInit2(stream, 0, 0, 0, 0, 0); result != EOK {
		t.Fatalf("expected DeflateInit2 EOK, got %d", result)
	}
	if result := Deflate(stream, 0); result != EOK {
		t.Fatalf("expected Deflate EOK, got %d", result)
	}
	compressed := append([]byte(nil), stream.NextOut...)
	decoded := &ZStream{NextIn: compressed}
	if result := InflateInit2(decoded, 0); result != EOK {
		t.Fatalf("expected InflateInit2 EOK, got %d", result)
	}
	if result := Inflate(decoded, 0); result != EOK {
		t.Fatalf("expected Inflate EOK, got %d", result)
	}
	if !bytes.Equal(decoded.NextOut, input) {
		t.Fatalf("decoded payload mismatch")
	}
}

type lifecycleTestServer struct {
	listener net.Listener
	lock     sync.Mutex
	conn     net.Conn
}

func createLifecycleTestServer() (*lifecycleTestServer, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	server := &lifecycleTestServer{listener: listener}
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		server.lock.Lock()
		server.conn = conn
		server.lock.Unlock()
		_, _ = io.Copy(io.Discard, conn)
		_ = conn.Close()
	}()
	return server, nil
}

func newLifecycleTestServer(t *testing.T) *lifecycleTestServer {
	t.Helper()
	server, err := createLifecycleTestServer()
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	return server
}

func (server *lifecycleTestServer) uri() string {
	return "tcp://" + server.listener.Addr().String() + "/amps/json"
}

func (server *lifecycleTestServer) closeConn() {
	server.lock.Lock()
	conn := server.conn
	server.conn = nil
	server.lock.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (server *lifecycleTestServer) close() {
	server.closeConn()
	_ = server.listener.Close()
}

func waitForCondition(timeout time.Duration, check func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return check()
}

func setShortJoinTimeout(handle Handle) {
	object, ok := getClientObject(handle)
	if !ok {
		return
	}
	object.lock.Lock()
	object.joinTimeout = 500 * time.Millisecond
	object.lock.Unlock()
}

func waitForReceiveActive(handle Handle, timeout time.Duration) bool {
	return waitForCondition(timeout, func() bool {
		object, ok := getClientObject(handle)
		if !ok {
			return false
		}
		object.lock.Lock()
		active := object.receiveActive
		object.lock.Unlock()
		return active
	})
}

func TestThreadCountersJoinOnDisconnect(t *testing.T) {
	server := newLifecycleTestServer(t)
	defer server.close()

	createBefore := GetThreadCreateCount()
	joinBefore := GetThreadJoinCount()
	detachBefore := GetThreadDetachCount()

	handle := ClientCreate("thread-join")
	defer ClientDestroy(handle)
	setShortJoinTimeout(handle)

	if result := ClientConnect(handle, server.uri()); result != EOK {
		t.Fatalf("connect failed: %d", result)
	}
	if !waitForCondition(2*time.Second, func() bool {
		return GetThreadCreateCount() >= createBefore+1
	}) {
		t.Fatalf("expected create counter to increment")
	}

	ClientDisconnect(handle)

	if !waitForCondition(2*time.Second, func() bool {
		return GetThreadJoinCount() >= joinBefore+1
	}) {
		t.Fatalf("expected join counter to increment")
	}
	if !waitForCondition(200*time.Millisecond, func() bool {
		return GetThreadDetachCount() == detachBefore
	}) {
		t.Fatalf("expected detach counter to remain unchanged for join-consumed disconnect")
	}
}

func TestThreadCountersDetachOnRemoteClose(t *testing.T) {
	server := newLifecycleTestServer(t)
	defer server.close()

	createBefore := GetThreadCreateCount()
	joinBefore := GetThreadJoinCount()
	detachBefore := GetThreadDetachCount()

	handle := ClientCreate("thread-detach")
	defer ClientDestroy(handle)

	if result := ClientConnect(handle, server.uri()); result != EOK {
		t.Fatalf("connect failed: %d", result)
	}
	if !waitForCondition(2*time.Second, func() bool {
		return GetThreadCreateCount() >= createBefore+1
	}) {
		t.Fatalf("expected create counter to increment")
	}

	server.closeConn()

	if !waitForCondition(2*time.Second, func() bool {
		return GetThreadDetachCount() >= detachBefore+1
	}) {
		t.Fatalf("expected detach counter to increment on remote close")
	}
	if GetThreadJoinCount() != joinBefore {
		t.Fatalf("expected join counter to remain unchanged on remote close")
	}
}

func TestThreadLifecycleCallbacks(t *testing.T) {
	server := newLifecycleTestServer(t)
	defer server.close()

	handle := ClientCreate("thread-callbacks")
	defer ClientDestroy(handle)
	setShortJoinTimeout(handle)

	var createdCount atomic.Uint64
	var exitedCount atomic.Uint64
	var createdID atomic.Uint64
	var exitedID atomic.Uint64
	var callbackErr atomic.Value

	if result := ClientSetThreadCreatedCallback(handle, func(threadID uint64, userData any) int {
		if userData != "create" {
			callbackErr.Store(fmt.Errorf("unexpected created callback userData: %v", userData))
		}
		if threadID == 0 {
			callbackErr.Store(fmt.Errorf("expected non-zero created thread id"))
		}
		createdID.Store(threadID)
		createdCount.Add(1)
		return EOK
	}, "create"); result != EOK {
		t.Fatalf("set created callback failed: %d", result)
	}

	if result := ClientSetThreadExitCallback(handle, func(threadID uint64, userData any) int {
		if userData != "exit" {
			callbackErr.Store(fmt.Errorf("unexpected exit callback userData: %v", userData))
		}
		if threadID == 0 {
			callbackErr.Store(fmt.Errorf("expected non-zero exit thread id"))
		}
		exitedID.Store(threadID)
		exitedCount.Add(1)
		return EOK
	}, "exit"); result != EOK {
		t.Fatalf("set exit callback failed: %d", result)
	}

	if result := ClientConnect(handle, server.uri()); result != EOK {
		t.Fatalf("connect failed: %d", result)
	}
	if !waitForCondition(2*time.Second, func() bool {
		return createdCount.Load() == 1
	}) {
		t.Fatalf("expected created callback to run before disconnect")
	}
	ClientDisconnect(handle)

	if !waitForCondition(2*time.Second, func() bool {
		return createdCount.Load() == 1 && exitedCount.Load() == 1
	}) {
		t.Fatalf("expected exactly one created and one exit callback, got created=%d exited=%d", createdCount.Load(), exitedCount.Load())
	}
	if createdID.Load() != exitedID.Load() {
		t.Fatalf("expected matching lifecycle ids, got created=%d exited=%d", createdID.Load(), exitedID.Load())
	}
	if err, ok := callbackErr.Load().(error); ok && err != nil {
		t.Fatal(err)
	}
}

func TestThreadCountersConcurrentConnectDisconnect(t *testing.T) {
	createBefore := GetThreadCreateCount()
	joinBefore := GetThreadJoinCount()
	detachBefore := GetThreadDetachCount()
	_ = detachBefore

	const workers = 6
	var waitGroup sync.WaitGroup
	errs := make(chan error, workers)

	for idx := 0; idx < workers; idx++ {
		waitGroup.Add(1)
		go func(worker int) {
			defer waitGroup.Done()
			server, err := createLifecycleTestServer()
			if err != nil {
				errs <- fmt.Errorf("worker %d listen failed: %v", worker, err)
				return
			}
			defer server.close()

			handle := ClientCreate(fmt.Sprintf("thread-worker-%d", worker))
			defer ClientDestroy(handle)
			setShortJoinTimeout(handle)

			if result := ClientConnect(handle, server.uri()); result != EOK {
				errs <- fmt.Errorf("worker %d connect failed: %d", worker, result)
				return
			}
			if !waitForReceiveActive(handle, 2*time.Second) {
				errs <- fmt.Errorf("worker %d receive routine did not become active", worker)
				return
			}
			ClientDisconnect(handle)
		}(idx)
	}

	waitGroup.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	if !waitForCondition(3*time.Second, func() bool {
		return GetThreadCreateCount() >= createBefore+workers
	}) {
		t.Fatalf("expected create counter >= +%d", workers)
	}
	if !waitForCondition(3*time.Second, func() bool {
		return GetThreadJoinCount() >= joinBefore+workers
	}) {
		t.Fatalf("expected join counter >= +%d", workers)
	}
	if GetThreadDetachCount() < detachBefore {
		t.Fatalf("detach counter must be monotonic")
	}
}
