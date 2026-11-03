package amps

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type trackingReadCloser struct {
	closed bool
}

func (reader *trackingReadCloser) Read(buffer []byte) (int, error) {
	return 0, io.EOF
}

func (reader *trackingReadCloser) Close() error {
	reader.closed = true
	return nil
}

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

		_, _, _ = conn.ReadMessage() // transport preface
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"c":"ack"}`))
		_, _, err = conn.ReadMessage() // part count
		if err == nil {
			_, payload, readErr := conn.ReadMessage()
			if readErr == nil {
				writeCh <- string(payload)
			}
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
	if got := buffer[:n]; !bytes.Equal(got, []byte{0, 0}) {
		t.Fatalf("Read() first chunk = %x, want initial length bytes", got)
	}

	buffer = make([]byte, 32)
	n, err = conn.Read(buffer)
	if err != nil {
		t.Fatalf("Read() second chunk error = %v", err)
	}
	if got := buffer[:n]; len(got) != 2+len(`{"c":"ack"}`) || binary.BigEndian.Uint16(got[:2]) != uint16(len(`{"c":"ack"}`)) || string(got[2:]) != `{"c":"ack"}` {
		t.Fatalf("Read() second chunk = %x, want remaining length and ack", got)
	}

	if _, err = conn.Write(websocketTCPFrame(`{"c":"flush"}`, nil)); err != nil {
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
	if got := <-writeCh; got != `{"c":"flush"}` {
		t.Fatalf("server received header = %q, want flush", got)
	}
	if _, ok := conn.(*websocketNetConn); !ok {
		t.Fatalf("expected websocketNetConn, got %T", conn)
	}
}

func TestWebSocketNetConnSetDeadlineAppliesToWrites(t *testing.T) {
	var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		select {
		case <-done:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer close(done)

	client := NewClient("websocket-deadline")
	conn, err := client.dialWebSocket(context.Background(), websocketTestURL(server.URL, "ws"))
	if err != nil {
		t.Fatalf("dialWebSocket() error = %v", err)
	}
	defer conn.Close()

	if err = conn.SetDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("SetDeadline() error = %v", err)
	}
	if _, err = conn.Write(websocketTCPFrame(`{"c":"flush"}`, nil)); err == nil {
		t.Fatalf("Write() after expired SetDeadline succeeded")
	}
}

func TestDialWebSocketWSSCoverage(t *testing.T) {
	var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		_, _, _ = conn.ReadMessage()
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

func TestCloseHTTPResponseBodyCoverage(t *testing.T) {
	closeHTTPResponseBody(nil)
	closeHTTPResponseBody(&http.Response{})

	body := &trackingReadCloser{}
	closeHTTPResponseBody(&http.Response{Body: body})
	if !body.closed {
		t.Fatalf("expected response body to be closed")
	}
}
