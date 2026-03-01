package main

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func writeWebSocketBinaryFrame(t *testing.T, conn net.Conn, payload []byte) {
	t.Helper()

	var header []byte
	var length = len(payload)
	if length <= 125 {
		header = []byte{0x82, byte(0x80 | length)}
	} else if length <= 65535 {
		header = []byte{0x82, 0x80 | 126, byte(length >> 8), byte(length)}
	} else {
		header = make([]byte, 10)
		header[0] = 0x82
		header[1] = 0x80 | 127
		binary.BigEndian.PutUint64(header[2:], uint64(length))
	}

	var mask = []byte{0x01, 0x02, 0x03, 0x04}
	var masked = make([]byte, len(payload))
	var index int
	for index = 0; index < len(payload); index++ {
		masked[index] = payload[index] ^ mask[index%4]
	}

	_, err := conn.Write(header)
	if err != nil {
		t.Fatalf("failed to write ws header: %v", err)
	}
	_, err = conn.Write(mask)
	if err != nil {
		t.Fatalf("failed to write ws mask: %v", err)
	}
	_, err = conn.Write(masked)
	if err != nil {
		t.Fatalf("failed to write ws payload: %v", err)
	}
}

func readWebSocketPayload(t *testing.T, conn net.Conn, timeout time.Duration) []byte {
	t.Helper()

	var err = conn.SetReadDeadline(time.Now().Add(timeout))
	if err != nil {
		t.Fatalf("failed to set read deadline: %v", err)
	}
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
	}()

	var hdr [2]byte
	_, err = io.ReadFull(conn, hdr[:])
	if err != nil {
		t.Fatalf("failed to read ws frame header: %v", err)
	}

	var length = int(hdr[1] & 0x7f)
	if length == 126 {
		var ext [2]byte
		_, err = io.ReadFull(conn, ext[:])
		if err != nil {
			t.Fatalf("failed to read ws frame ext16: %v", err)
		}
		length = int(binary.BigEndian.Uint16(ext[:]))
	} else if length == 127 {
		var ext [8]byte
		_, err = io.ReadFull(conn, ext[:])
		if err != nil {
			t.Fatalf("failed to read ws frame ext64: %v", err)
		}
		length = int(binary.BigEndian.Uint64(ext[:]))
	}

	var masked = (hdr[1] & 0x80) != 0
	var mask [4]byte
	if masked {
		_, err = io.ReadFull(conn, mask[:])
		if err != nil {
			t.Fatalf("failed to read ws frame mask: %v", err)
		}
	}

	var payload = make([]byte, length)
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		t.Fatalf("failed to read ws frame payload: %v", err)
	}

	if masked {
		var index int
		for index = 0; index < len(payload); index++ {
			payload[index] ^= mask[index%4]
		}
	}

	return payload
}

func TestWebSocketUpgradeAndLogonCommandFlow(t *testing.T) {
	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	var done = make(chan struct{})
	go func() {
		var conn, acceptErr = listener.Accept()
		if acceptErr != nil {
			close(done)
			return
		}
		handleConnection(conn)
		close(done)
	}()

	var client, dialErr = net.Dial("tcp", listener.Addr().String())
	if dialErr != nil {
		t.Fatalf("failed to dial test server: %v", dialErr)
	}

	var request = strings.Join([]string{
		"GET /amps/json HTTP/1.1",
		"Host: 127.0.0.1",
		"Upgrade: websocket",
		"Connection: Upgrade",
		"Sec-WebSocket-Version: 13",
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==",
		"X-Preflight-Token: parity",
		"",
		"",
	}, "\r\n")

	_, err = client.Write([]byte(request))
	if err != nil {
		t.Fatalf("failed to write websocket handshake: %v", err)
	}

	err = client.SetReadDeadline(time.Now().Add(750 * time.Millisecond))
	if err != nil {
		t.Fatalf("failed setting handshake read deadline: %v", err)
	}

	var reader = bufio.NewReader(client)
	var response, readErr = reader.ReadString('\n')
	if readErr != nil {
		t.Fatalf("failed to read websocket status line: %v", readErr)
	}
	if !strings.Contains(response, "101") {
		t.Fatalf("expected websocket 101 response, got %q", response)
	}

	var sawAcceptHeader = false
	for {
		var line, lineErr = reader.ReadString('\n')
		if lineErr != nil {
			t.Fatalf("failed reading websocket headers: %v", lineErr)
		}
		if strings.TrimSpace(line) == "" {
			break
		}
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(line)), "sec-websocket-accept:") {
			sawAcceptHeader = true
		}
	}
	if !sawAcceptHeader {
		t.Fatalf("expected sec-websocket-accept header in handshake response")
	}

	err = client.SetReadDeadline(time.Time{})
	if err != nil {
		t.Fatalf("failed clearing read deadline: %v", err)
	}

	var logonFrame = buildCommandFrame(`{"c":"logon","cid":"log-1","a":"processed","client_name":"ws-client"}`, nil)
	writeWebSocketBinaryFrame(t, client, logonFrame)

	var payload = readWebSocketPayload(t, client, 500*time.Millisecond)
	if !strings.Contains(string(payload), `"c":"ack"`) || !strings.Contains(string(payload), `"a":"processed"`) {
		t.Fatalf("expected websocket logon ack payload, got %s", string(payload))
	}

	_ = client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("handleConnection did not exit in time")
	}
}
