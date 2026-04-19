package amps

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func websocketTestURL(serverURL string, scheme string) *url.URL {
	parsedURL, _ := url.Parse(serverURL)
	parsedURL.Scheme = scheme
	return parsedURL
}

func TestDialWebSocketWithPreflightHeadersAndNetConnCoverage(t *testing.T) {
	var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	headerCh := make(chan string, 1)
	writeCh := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headerCh <- r.Header.Get("X-Test")
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		_ = conn.WriteMessage(websocket.BinaryMessage, []byte("hello"))
		_, payload, err := conn.ReadMessage()
		if err == nil {
			writeCh <- string(payload)
		}
	}))
	defer server.Close()

	client := NewClient("websocket-coverage")
	state := ensureClientState(client)
	state.lock.Lock()
	state.httpPreflightHeaders = []string{"X-Test: value", "BrokenHeader"}
	state.lock.Unlock()

	conn, err := client.dialWebSocket(context.Background(), websocketTestURL(server.URL, "ws"))
	if err != nil {
		t.Fatalf("dialWebSocket() error = %v", err)
	}
	defer conn.Close()

	buffer := make([]byte, 2)
	n, err := conn.Read(buffer)
	if err != nil {
		t.Fatalf("Read() first chunk error = %v", err)
	}
	if got := string(buffer[:n]); got != "he" {
		t.Fatalf("Read() first chunk = %q, want he", got)
	}

	buffer = make([]byte, 8)
	n, err = conn.Read(buffer)
	if err != nil {
		t.Fatalf("Read() second chunk error = %v", err)
	}
	if got := string(buffer[:n]); got != "llo" {
		t.Fatalf("Read() second chunk = %q, want llo", got)
	}

	if _, err = conn.Write([]byte("ping")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	deadline := time.Now().Add(time.Second)
	if err = conn.SetDeadline(deadline); err != nil {
		t.Fatalf("SetDeadline() error = %v", err)
	}
	if err = conn.SetReadDeadline(deadline); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	if err = conn.SetWriteDeadline(deadline); err != nil {
		t.Fatalf("SetWriteDeadline() error = %v", err)
	}
	if conn.LocalAddr() == nil {
		t.Fatalf("LocalAddr() returned nil")
	}
	if conn.RemoteAddr() == nil {
		t.Fatalf("RemoteAddr() returned nil")
	}

	if got := <-headerCh; got != "value" {
		t.Fatalf("preflight header = %q, want value", got)
	}
	if got := <-writeCh; got != "ping" {
		t.Fatalf("server received payload = %q, want ping", got)
	}
	if _, ok := conn.(*websocketNetConn); !ok {
		t.Fatalf("expected websocketNetConn, got %T", conn)
	}
}

func TestDialWebSocketWSSCoverage(t *testing.T) {
	var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		_ = conn.Close()
	}))
	defer server.Close()

	client := NewClient("websocket-wss")
	if _, err := client.dialWebSocket(context.Background(), websocketTestURL(server.URL, "wss")); err == nil {
		t.Fatalf("expected default WSS dial to fail with self-signed server")
	}

	client.tlsConfig = &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}
	conn, err := client.dialWebSocket(context.Background(), websocketTestURL(server.URL, "wss"))
	if err != nil {
		t.Fatalf("dialWebSocket() with explicit TLS config error = %v", err)
	}
	defer conn.Close()

	if !strings.HasPrefix(websocketTestURL(server.URL, "wss").String(), "wss://") {
		t.Fatalf("expected WSS URL prefix")
	}
}