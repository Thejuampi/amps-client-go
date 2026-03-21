package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

const websocketAcceptGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

const (
	workspaceWriteQueueDepth = 128
	websocketWriteTimeout    = 250 * time.Millisecond
	websocketMaxPayloadSize  = 256 * 1024 * 1024
)

type websocketQueuedFrame struct {
	opcode  byte
	payload []byte
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

type websocketConn struct {
	net.Conn
	reader  *bufio.Reader
	writeMu sync.Mutex

	readBuf []byte
	readPos int

	workspaceMu     sync.Mutex
	workspaceQueue  chan websocketQueuedFrame
	workspaceDone   chan struct{}
	workspaceClosed bool
}

func (c *websocketConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	for {
		if c.readPos < len(c.readBuf) {
			var n = copy(p, c.readBuf[c.readPos:])
			c.readPos += n
			if c.readPos >= len(c.readBuf) {
				c.readBuf = nil
				c.readPos = 0
			}
			return n, nil
		}

		var payload, opcode, err = c.readFrame()
		if err != nil {
			return 0, err
		}

		switch opcode {
		case 0x8:
			return 0, io.EOF
		case 0x9:
			_ = c.writeControlFrame(0xA, payload)
			continue
		case 0xA:
			continue
		case 0x0, 0x1, 0x2:
			c.readBuf = payload
			c.readPos = 0
		default:
			continue
		}
	}
}

func (c *websocketConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeDataFrameLocked(0x2, p)
}

func (c *websocketConn) WriteText(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeDataFrameLocked(0x1, p)
}

func (c *websocketConn) QueueText(p []byte) error {
	if len(p) == 0 {
		return nil
	}

	var frame = append([]byte(nil), p...)

	c.workspaceMu.Lock()
	if c.workspaceClosed {
		c.workspaceMu.Unlock()
		return net.ErrClosed
	}
	if c.workspaceQueue == nil {
		c.workspaceQueue = make(chan websocketQueuedFrame, workspaceWriteQueueDepth)
		c.workspaceDone = make(chan struct{})
		go c.runWorkspaceWriter(c.workspaceQueue, c.workspaceDone)
	}

	select {
	case c.workspaceQueue <- websocketQueuedFrame{opcode: 0x1, payload: frame}:
		c.workspaceMu.Unlock()
		return nil
	default:
		c.workspaceMu.Unlock()
		_ = c.Close()
		return errors.New("workspace websocket queue full")
	}
}

func (c *websocketConn) Close() error {
	if c == nil {
		return nil
	}

	c.workspaceMu.Lock()
	var queue = c.workspaceQueue
	var done = c.workspaceDone
	var alreadyClosed = c.workspaceClosed
	c.workspaceClosed = true
	c.workspaceQueue = nil
	c.workspaceDone = nil
	c.workspaceMu.Unlock()

	var err error
	if !alreadyClosed && c.Conn != nil {
		err = c.Conn.Close()
	}
	if queue != nil {
		close(queue)
		if done != nil {
			<-done
		}
	}
	return err
}

func (c *websocketConn) writeDataFrameLocked(opcode byte, p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if err := c.setWriteDeadline(); err != nil {
		return 0, err
	}
	defer func() {
		_ = c.clearWriteDeadline()
	}()

	var header []byte
	var length = len(p)
	if length <= 125 {
		header = []byte{0x80 | opcode, byte(length)}
	} else if length <= 65535 {
		header = []byte{0x80 | opcode, 126, byte(length >> 8), byte(length)}
	} else {
		header = make([]byte, 10)
		header[0] = 0x80 | opcode
		header[1] = 127
		binary.BigEndian.PutUint64(header[2:], uint64(length))
	}

	if _, err := c.Conn.Write(header); err != nil {
		return 0, err
	}
	if _, err := c.Conn.Write(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *websocketConn) writeControlFrame(opcode byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := c.setWriteDeadline(); err != nil {
		return err
	}
	defer func() {
		_ = c.clearWriteDeadline()
	}()

	var header []byte
	var length = len(payload)
	if length <= 125 {
		header = []byte{0x80 | opcode, byte(length)}
	} else {
		header = []byte{0x80 | opcode, 126, byte(length >> 8), byte(length)}
	}

	if _, err := c.Conn.Write(header); err != nil {
		return err
	}
	if length > 0 {
		if _, err := c.Conn.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func (c *websocketConn) setWriteDeadline() error {
	if c == nil || c.Conn == nil {
		return nil
	}
	return c.Conn.SetWriteDeadline(time.Now().Add(websocketWriteTimeout))
}

func (c *websocketConn) clearWriteDeadline() error {
	if c == nil || c.Conn == nil {
		return nil
	}
	return c.Conn.SetWriteDeadline(time.Time{})
}

func (c *websocketConn) runWorkspaceWriter(queue <-chan websocketQueuedFrame, done chan struct{}) {
	defer close(done)

	for frame := range queue {
		c.writeMu.Lock()
		var _, err = c.writeDataFrameLocked(frame.opcode, frame.payload)
		c.writeMu.Unlock()
		if err != nil {
			c.workspaceMu.Lock()
			c.workspaceClosed = true
			c.workspaceQueue = nil
			c.workspaceMu.Unlock()
			_ = c.Conn.Close()
			return
		}
	}
}

func (c *websocketConn) readFrame() ([]byte, byte, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(c.reader, hdr[:]); err != nil {
		return nil, 0, err
	}

	var opcode = hdr[0] & 0x0f
	var masked = (hdr[1] & 0x80) != 0
	var length = uint64(hdr[1] & 0x7f)

	if length == 126 {
		var ext [2]byte
		if _, err := io.ReadFull(c.reader, ext[:]); err != nil {
			return nil, 0, err
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	} else if length == 127 {
		var ext [8]byte
		if _, err := io.ReadFull(c.reader, ext[:]); err != nil {
			return nil, 0, err
		}
		length = binary.BigEndian.Uint64(ext[:])
	}
	if length > websocketMaxPayloadSize || length > uint64(^uint(0)>>1) {
		return nil, 0, errors.New("websocket frame too large")
	}

	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(c.reader, mask[:]); err != nil {
			return nil, 0, err
		}
	}

	var payload = make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(c.reader, payload); err != nil {
			return nil, 0, err
		}
	}

	if masked {
		var index int
		for index = 0; index < len(payload); index++ {
			payload[index] ^= mask[index%4]
		}
	}

	return payload, opcode, nil
}

func prepareProtocolConn(conn net.Conn) (net.Conn, error) {
	var reader = bufio.NewReader(conn)
	var first, err = reader.Peek(1)
	if err != nil {
		return nil, err
	}

	if first[0] != 'G' {
		return &bufferedConn{Conn: conn, reader: reader}, nil
	}

	if err := performWebSocketHandshake(conn, reader); err != nil {
		return nil, err
	}

	return &websocketConn{Conn: conn, reader: reader}, nil
}

func connectionProtocolLabel(conn net.Conn) string {
	switch conn.(type) {
	case *websocketConn:
		return "websocket"
	default:
		return "tcp"
	}
}

func performWebSocketHandshake(conn net.Conn, reader *bufio.Reader) error {
	var requestLine, err = reader.ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.HasPrefix(requestLine, "GET ") {
		return errors.New("unsupported protocol preface")
	}

	var websocketKey string
	for {
		var line string
		line, err = reader.ReadString('\n')
		if err != nil {
			return err
		}

		line = strings.TrimSpace(line)
		if line == "" {
			break
		}

		var lower = strings.ToLower(line)
		if strings.HasPrefix(lower, "sec-websocket-key:") {
			websocketKey = strings.TrimSpace(line[len("Sec-WebSocket-Key:"):])
		}
	}

	if websocketKey == "" {
		return errors.New("missing sec-websocket-key")
	}

	var acceptRaw = sha1.Sum([]byte(websocketKey + websocketAcceptGUID))
	var accept = base64.StdEncoding.EncodeToString(acceptRaw[:])

	var response = "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"

	_, err = conn.Write([]byte(response))
	return err
}
