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
)

const websocketAcceptGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

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
	if len(p) == 0 {
		return 0, nil
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	var header []byte
	var length = len(p)
	if length <= 125 {
		header = []byte{0x82, byte(length)}
	} else if length <= 65535 {
		header = []byte{0x82, 126, byte(length >> 8), byte(length)}
	} else {
		header = make([]byte, 10)
		header[0] = 0x82
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
