package capi

import (
	"bytes"
	"testing"
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
