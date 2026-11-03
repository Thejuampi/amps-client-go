package amps

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

func closeHTTPResponseBody(response *http.Response) {
	if response == nil || response.Body == nil {
		return
	}
	_ = response.Body.Close()
}

func (client *Client) dialWebSocket(ctx context.Context, parsedURI *url.URL, options ...socketOptions) (net.Conn, error) {
	var targetURL = "ws://" + parsedURI.Host + parsedURI.Path
	if parsedURI.Scheme == "wss" {
		targetURL = "wss://" + parsedURI.Host + parsedURI.Path
	}
	if parsedURI.RawQuery != "" {
		targetURL += "?" + parsedURI.RawQuery
	}

	var dialerCopy = *websocket.DefaultDialer
	var dialer = &dialerCopy
	if parsedURI.Scheme == "wss" {
		client.configLock.RLock()
		var baseTLSConfig = client.tlsConfig
		client.configLock.RUnlock()
		var tlsConfig = clientTLSConfigForHost(baseTLSConfig, parsedURI.Hostname())
		dialer = &websocket.Dialer{
			TLSClientConfig: tlsConfig,
		}
	}

	var baseDialContext = dialer.NetDialContext
	var baseDial = dialer.NetDial
	if len(options) > 0 && options[0].ipPreference != "" {
		baseDialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			return dialTCPPreference(ctx, network, address, options[0].ipPreference, clientNetDialContext)
		}
	} else if baseDialContext == nil && baseDial != nil {
		baseDialContext = func(_ context.Context, network, address string) (net.Conn, error) {
			return baseDial(network, address)
		}
	} else if baseDialContext == nil {
		var networkDialer net.Dialer
		baseDialContext = networkDialer.DialContext
	}
	var deadlineConn *websocketDeadlineConn
	dialer.NetDialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		var connection, err = baseDialContext(ctx, network, address)
		if err != nil {
			return connection, err
		}
		deadlineConn = &websocketDeadlineConn{Conn: connection}
		return deadlineConn, nil
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

	var preface = "binary"
	var payloadType = websocket.BinaryMessage
	var path = strings.TrimPrefix(parsedURI.Path, "/")
	if strings.SplitN(path, "/", 3)[0] == "amps" {
		var parts = strings.SplitN(path, "/", 3)
		if len(parts) > 1 && parts[1] == "json" {
			preface = "text"
			payloadType = websocket.TextMessage
		}
	}
	wsConn.SetReadLimit(maxInboundFrameLength)
	var cancelled = make(chan struct{})
	var stop = context.AfterFunc(ctx, func() {
		defer close(cancelled)
		_ = wsConn.Close()
	})
	err = wsConn.WriteMessage(websocket.TextMessage, []byte(preface))
	if !stop() {
		<-cancelled
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		_ = wsConn.Close()
		return nil, ctxErr
	}
	if err != nil {
		_ = wsConn.Close()
		return nil, err
	}
	return &websocketNetConn{conn: wsConn, deadlines: deadlineConn, payloadType: payloadType}, nil
}

// Gorilla sets its own write deadline before each frame; preserve the caller's
// independently configured deadline underneath both WebSocket and TLS layers.
type websocketDeadlineConn struct {
	net.Conn
	lock                  sync.Mutex
	internalWriteDeadline time.Time
	externalWriteDeadline time.Time
}

func (connection *websocketDeadlineConn) applyWriteDeadline() error {
	var deadline = connection.internalWriteDeadline
	if !connection.externalWriteDeadline.IsZero() && (deadline.IsZero() || connection.externalWriteDeadline.Before(deadline)) {
		deadline = connection.externalWriteDeadline
	}
	return connection.Conn.SetWriteDeadline(deadline)
}

func (connection *websocketDeadlineConn) SetDeadline(deadline time.Time) error {
	connection.lock.Lock()
	defer connection.lock.Unlock()
	var readErr = connection.Conn.SetReadDeadline(deadline)
	connection.internalWriteDeadline = deadline
	var writeErr = connection.applyWriteDeadline()
	if readErr != nil {
		return readErr
	}
	return writeErr
}

func (connection *websocketDeadlineConn) SetWriteDeadline(deadline time.Time) error {
	connection.lock.Lock()
	defer connection.lock.Unlock()
	connection.internalWriteDeadline = deadline
	return connection.applyWriteDeadline()
}

func (connection *websocketDeadlineConn) setExternalWriteDeadline(deadline time.Time) error {
	connection.lock.Lock()
	defer connection.lock.Unlock()
	connection.externalWriteDeadline = deadline
	return connection.applyWriteDeadline()
}

type websocketNetConn struct {
	conn         *websocket.Conn
	deadlines    *websocketDeadlineConn
	payloadType  int
	readLock     sync.Mutex
	writeLock    sync.Mutex
	rbuf         []byte
	rpos         int
	readErr      error
	sowHeader    []byte
	sowRemaining uint
	wbuf         []byte
	writeErr     error
}

func (wc *websocketNetConn) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	wc.readLock.Lock()
	defer wc.readLock.Unlock()
	if wc.readErr != nil {
		return 0, wc.readErr
	}
	if wc.rpos == len(wc.rbuf) {
		var frame, err = wc.readFrame()
		if err != nil {
			wc.readErr = err
			return 0, err
		}
		wc.rbuf = frame
		wc.rpos = 0
	}
	var n = copy(b, wc.rbuf[wc.rpos:])
	wc.rpos += n
	if wc.rpos == len(wc.rbuf) {
		wc.rbuf = nil
		wc.rpos = 0
	}
	return n, nil
}

func (wc *websocketNetConn) Write(b []byte) (int, error) {
	wc.writeLock.Lock()
	defer wc.writeLock.Unlock()
	if wc.writeErr != nil {
		return 0, wc.writeErr
	}
	var consumed int
	for consumed < len(b) {
		var frameStart = consumed
		if len(wc.wbuf) == 0 && len(b)-consumed >= 4 {
			var frameSize, err = websocketFrameSize(b[consumed : consumed+4])
			if err != nil {
				wc.writeErr = err
				return consumed, err
			}
			if frameSize <= len(b)-consumed {
				if err = wc.writeFrame(b[consumed+4 : consumed+frameSize]); err != nil {
					wc.writeErr = err
					return consumed, err
				}
				consumed += frameSize
				continue
			}
		}
		if len(wc.wbuf) < 4 {
			var need = min(4-len(wc.wbuf), len(b)-consumed)
			wc.wbuf = append(wc.wbuf, b[consumed:consumed+need]...)
			consumed += need
			if len(wc.wbuf) < 4 {
				break
			}
		}
		var frameSize, err = websocketFrameSize(wc.wbuf[:4])
		if err != nil {
			wc.writeErr = err
			return frameStart, err
		}
		var need = min(frameSize-len(wc.wbuf), len(b)-consumed)
		wc.wbuf = append(wc.wbuf, b[consumed:consumed+need]...)
		consumed += need
		if len(wc.wbuf) == frameSize {
			if err = wc.writeFrame(wc.wbuf[4:]); err != nil {
				wc.writeErr = err
				return frameStart, err
			}
			wc.wbuf = nil
		}
	}
	return consumed, nil
}

func websocketFrameSize(prefix []byte) (int, error) {
	var size = uint64(binary.BigEndian.Uint32(prefix)) + 4
	if size > uint64(int(^uint(0)>>1)) || size <= 4 {
		return 0, NewError(ProtocolError, "invalid outbound WebSocket frame length")
	}
	return int(size), nil
}

func websocketHeader(data []byte) (*Message, error) {
	if !utf8.Valid(data) || !json.Valid(data) || len(data) == 0 || data[0] != '{' {
		return nil, NewError(ProtocolError, "invalid AMPS WebSocket header")
	}
	var message = &Message{}
	var remaining, err = parseHeader(message, true, data)
	if err != nil || len(remaining) != 0 {
		return nil, NewError(ProtocolError, "invalid AMPS WebSocket header")
	}
	return message, nil
}

func (wc *websocketNetConn) writeFrame(frame []byte) error {
	var message = &Message{}
	var payload, err = parseHeader(message, true, frame)
	if err != nil {
		return NewError(ProtocolError, fmt.Errorf("invalid outbound WebSocket header: %w", err))
	}
	var header = frame[:len(frame)-len(payload)]
	if _, err = websocketHeader(header); err != nil {
		return err
	}
	if len(payload) > 0 && wc.payloadType == websocket.TextMessage && !utf8.Valid(payload) {
		return NewError(ProtocolError, "invalid UTF-8 WebSocket text payload")
	}
	var count uint32 = 1
	if len(payload) > 0 {
		count++
	}
	var partCount [4]byte
	binary.LittleEndian.PutUint32(partCount[:], count)
	if err = wc.conn.WriteMessage(websocket.BinaryMessage, partCount[:]); err != nil {
		return err
	}
	if err = wc.conn.WriteMessage(websocket.TextMessage, header); err != nil {
		return err
	}
	if len(payload) > 0 {
		return wc.conn.WriteMessage(wc.payloadType, payload)
	}
	return nil
}

func (wc *websocketNetConn) readHeader() ([]byte, *Message, error) {
	var kind, data, err = wc.conn.ReadMessage()
	if err != nil {
		return nil, nil, err
	}
	if kind != websocket.TextMessage {
		return nil, nil, NewError(ProtocolError, "expected AMPS WebSocket header as text")
	}
	data = bytes.TrimSpace(data)
	var message *Message
	message, err = websocketHeader(data)
	return data, message, err
}

func (wc *websocketNetConn) readBody() ([]byte, error) {
	var kind, data, err = wc.conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	if kind != websocket.TextMessage && kind != websocket.BinaryMessage {
		return nil, NewError(ProtocolError, "invalid AMPS WebSocket payload type")
	}
	return data, nil
}

func appendWebSocketFramePart(frame []byte, part []byte) ([]byte, error) {
	if len(part) > maxInboundFrameLength-(len(frame)-4) {
		return nil, NewError(ProtocolError, "AMPS WebSocket frame exceeds maximum length")
	}
	return append(frame, part...), nil
}

func (wc *websocketNetConn) readFrame() ([]byte, error) {
	if wc.sowRemaining != 0 {
		return wc.readSOWRecord()
	}
	var header, message, err = wc.readHeader()
	if err != nil {
		return nil, err
	}
	var frame = make([]byte, 4, len(header)+4)
	frame, err = appendWebSocketFramePart(frame, header)
	if err != nil {
		return nil, err
	}
	switch message.header.command {
	case CommandPublish, CommandOOF:
		if message.header.messageLength == nil || *message.header.messageLength != 0 {
			var body []byte
			body, err = wc.readBody()
			if err == nil {
				frame, err = appendWebSocketFramePart(frame, body)
			}
		}
	case CommandSOW:
		if message.header.batchSize == nil || *message.header.batchSize > maxInboundFrameLength/2 {
			return nil, NewError(ProtocolError, "invalid AMPS WebSocket SOW batch size")
		}
		if *message.header.batchSize > 0 {
			wc.sowHeader = header
			wc.sowRemaining = *message.header.batchSize
			return wc.readSOWRecord()
		}
	}
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint32(frame[:4], uint32(len(frame)-4))
	return frame, nil
}

func (wc *websocketNetConn) readSOWRecord() ([]byte, error) {
	var record, recordMessage, err = wc.readHeader()
	if err != nil {
		return nil, err
	}
	var body []byte
	body, err = wc.readBody()
	if err != nil {
		return nil, err
	}
	if recordMessage.header.messageLength == nil {
		var comma = len(bytes.TrimSpace(record[1:len(record)-1])) > 0
		record = record[:len(record)-1]
		if comma {
			record = append(record, ',')
		}
		record = append(record, `"l":`...)
		record = strconv.AppendInt(record, int64(len(body)), 10)
		record = append(record, '}')
	} else if uint64(*recordMessage.header.messageLength) != uint64(len(body)) {
		return nil, NewError(ProtocolError, "AMPS WebSocket SOW record length mismatch")
	}
	var frame = make([]byte, 4, len(wc.sowHeader)+4)
	frame, err = appendWebSocketFramePart(frame, wc.sowHeader)
	if err == nil {
		frame, err = appendWebSocketFramePart(frame, record)
	}
	if err == nil {
		frame, err = appendWebSocketFramePart(frame, body)
	}
	if err != nil {
		return nil, err
	}
	wc.sowRemaining--
	if wc.sowRemaining == 0 {
		wc.sowHeader = nil
	}
	binary.BigEndian.PutUint32(frame[:4], uint32(len(frame)-4))
	return frame, nil
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
	var readErr = wc.conn.SetReadDeadline(t)
	var writeErr = wc.deadlines.setExternalWriteDeadline(t)
	if readErr != nil {
		return readErr
	}
	return writeErr
}

func (wc *websocketNetConn) SetReadDeadline(t time.Time) error {
	return wc.conn.SetReadDeadline(t)
}

func (wc *websocketNetConn) SetWriteDeadline(t time.Time) error {
	return wc.deadlines.setExternalWriteDeadline(t)
}
