package amps

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

func closeHTTPResponseBody(response *http.Response) {
	if response == nil || response.Body == nil {
		return
	}
	_ = response.Body.Close()
}

func (client *Client) dialWebSocket(ctx context.Context, parsedURI *url.URL) (net.Conn, error) {
	var targetURL = "ws://" + parsedURI.Host + parsedURI.Path
	if parsedURI.Scheme == "wss" {
		targetURL = "wss://" + parsedURI.Host + parsedURI.Path
	}
	if parsedURI.RawQuery != "" {
		targetURL += "?" + parsedURI.RawQuery
	}

	var dialer = websocket.DefaultDialer
	if parsedURI.Scheme == "wss" {
		tlsConfig := client.tlsConfig
		if tlsConfig == nil {
			tlsConfig = &tls.Config{
				MinVersion: tls.VersionTLS12,
				ServerName: parsedURI.Hostname(),
			}
		}
		dialer = &websocket.Dialer{
			TLSClientConfig: tlsConfig,
		}
	}

	state := ensureClientState(client)
	var requestHeader http.Header
	if state != nil {
		state.lock.Lock()
		if len(state.httpPreflightHeaders) > 0 {
			requestHeader = make(http.Header, len(state.httpPreflightHeaders))
			for _, header := range state.httpPreflightHeaders {
				if parts := strings.SplitN(header, ":", 2); len(parts) == 2 {
					requestHeader.Add(strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
				}
			}
		}
		state.lock.Unlock()
	}

	wsConn, response, err := dialer.DialContext(ctx, targetURL, requestHeader)
	if err != nil {
		closeHTTPResponseBody(response)
		return nil, err
	}

	return &websocketNetConn{conn: wsConn}, nil
}

type websocketNetConn struct {
	conn *websocket.Conn
	rbuf []byte
	rpos int
}

func (wc *websocketNetConn) Read(b []byte) (int, error) {
	if wc.rpos < len(wc.rbuf) {
		var n = copy(b, wc.rbuf[wc.rpos:])
		wc.rpos += n
		return n, nil
	}

	_, message, err := wc.conn.ReadMessage()
	if err != nil {
		return 0, err
	}
	wc.rbuf = message
	wc.rpos = 0
	var n = copy(b, wc.rbuf)
	wc.rpos = n
	return n, nil
}

func (wc *websocketNetConn) Write(b []byte) (int, error) {
	err := wc.conn.WriteMessage(websocket.BinaryMessage, b)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func (wc *websocketNetConn) Close() error {
	return wc.conn.Close()
}

func (wc *websocketNetConn) LocalAddr() net.Addr {
	return wc.conn.LocalAddr()
}

func (wc *websocketNetConn) RemoteAddr() net.Addr {
	return wc.conn.RemoteAddr()
}

func (wc *websocketNetConn) SetDeadline(t time.Time) error {
	return wc.conn.SetReadDeadline(t)
}

func (wc *websocketNetConn) SetReadDeadline(t time.Time) error {
	return wc.conn.SetReadDeadline(t)
}

func (wc *websocketNetConn) SetWriteDeadline(t time.Time) error {
	return wc.conn.SetWriteDeadline(t)
}
