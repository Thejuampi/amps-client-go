package main

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
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

func dialAMPSWebSocket(t *testing.T) net.Conn {
	t.Helper()
	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
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
	t.Cleanup(func() {
		_ = client.Close()
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("handleConnection did not exit")
		}
	})

	var request = strings.Join([]string{
		"GET /amps/json HTTP/1.1",
		"Host: 127.0.0.1",
		"Upgrade: websocket",
		"Connection: Upgrade",
		"Sec-WebSocket-Version: 13",
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==", // gitleaks:allow
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
	return client
}

func TestWebSocketUpgradeAndLogonCommandFlow(t *testing.T) {
	var client = dialAMPSWebSocket(t)
	writeTextWebSocketFrame(t, client, []byte("text"))
	writeWebSocketBinaryFrame(t, client, []byte{1, 0, 0, 0})
	writeTextWebSocketFrame(t, client, []byte(`{"c":"logon","cid":"log-1","a":"processed","client_name":"ws-client"}`))

	var opcode, payload = readWebSocketFrame(t, client, 500*time.Millisecond)
	if opcode != 0x1 || !strings.HasPrefix(string(payload), `{"c":"ack"`) || !strings.Contains(string(payload), `"a":"processed"`) {
		t.Fatalf("expected text websocket logon ack header, got opcode=%d payload=%q", opcode, payload)
	}

}

func TestWebSocketRejectsLegacyAMPSFrame(t *testing.T) {
	var client = dialAMPSWebSocket(t)
	writeWebSocketBinaryFrame(t, client, buildCommandFrame(`{"c":"logon","cid":"legacy","a":"processed"}`, nil))
	var opcode, body, err = tryReadWebSocketFrame(client, time.Second)
	if err == nil {
		t.Fatalf("legacy frame accepted: opcode=%d body=%q", opcode, body)
	}
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatalf("legacy frame was not rejected: %v", err)
	}
}

func TestWebSocketAMPSMultipartPublishAndSOW(t *testing.T) {
	var oldSOW, oldJournal = sow, journal
	sow, journal = newSOWCache(), newMessageJournal(100)
	t.Cleanup(func() { sow, journal = oldSOW, oldJournal })
	for _, mode := range []string{"text", "binary"} {
		t.Run(mode, func(t *testing.T) {
			var client = dialAMPSWebSocket(t)
			writeTextWebSocketFrame(t, client, []byte(mode))
			writeWebSocketBinaryFrame(t, client, []byte{1, 0, 0, 0})
			writeTextWebSocketFrame(t, client, []byte(`{"c":"logon","cid":"logon","a":"processed","client_name":"ws-test"}`))
			var opcode, response = readWebSocketFrame(t, client, time.Second)
			if opcode != 1 || !strings.Contains(string(response), `"cid":"logon"`) {
				t.Fatalf("logon ack opcode=%d data=%q", opcode, response)
			}

			var topic = "ws-multipart-" + mode
			writeWebSocketBinaryFrame(t, client, []byte{3, 0, 0, 0})
			writeTextWebSocketFrame(t, client, []byte(`{"c":"publish","cid":"pub","t":"`+topic+`","k":"key-1","a":"processed"}`))
			writeTextWebSocketFrame(t, client, []byte(`{"id":`))
			writeWebSocketBinaryFrame(t, client, []byte(`1}`))
			opcode, response = readWebSocketFrame(t, client, time.Second)
			if opcode != 1 || !strings.Contains(string(response), `"cid":"pub"`) {
				t.Fatalf("publish ack opcode=%d data=%q", opcode, response)
			}

			writeWebSocketBinaryFrame(t, client, []byte{1, 0, 0, 0})
			writeTextWebSocketFrame(t, client, []byte(`{"c":"sow","cid":"query","query_id":"q","t":"`+topic+`","a":"processed,completed"}`))
			var want = []string{`"a":"processed"`, `"c":"group_begin"`, `"c":"sow"`, `"k":"key-1"`, `{"id":1}`, `"c":"group_end"`, `"a":"completed"`}
			for i, fragment := range want {
				opcode, response = readWebSocketFrame(t, client, time.Second)
				var wantOpcode byte = 1
				if i == 4 && mode == "binary" {
					wantOpcode = 2
				}
				if opcode != wantOpcode || !strings.Contains(string(response), fragment) {
					t.Fatalf("SOW frame %d opcode=%d data=%q, want opcode=%d containing %q", i, opcode, response, wantOpcode, fragment)
				}
				if i == 2 && !strings.Contains(string(response), `"bs":1`) {
					t.Fatalf("SOW batch has no record count: %q", response)
				}
			}
		})
	}
}

func TestIntegrationGoClientWebSocketPublishSubscribeAndSOW(t *testing.T) {
	var oldSOW, oldJournal, oldEcho = sow, journal, *flagEcho
	sow, journal, *flagEcho = newSOWCache(), newMessageJournal(100), true
	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var done = make(chan struct{})
	go func() {
		defer close(done)
		var conn, acceptErr = listener.Accept()
		if acceptErr == nil {
			handleConnection(conn)
		}
	}()
	var client = amps.NewClient("go-websocket-integration")
	t.Cleanup(func() {
		_ = client.Close()
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("fakeamps handler did not exit")
		}
		sow, journal, *flagEcho = oldSOW, oldJournal, oldEcho
	})
	var errors = make(chan error, 1)
	client.SetErrorHandler(func(err error) {
		select {
		case errors <- err:
		default:
		}
	})
	if err := client.Connect("ws://" + listener.Addr().String() + "/amps/json"); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := client.Logon(); err != nil {
		t.Fatalf("logon: %v", err)
	}
	var live = make(chan string, 1)
	if _, err := client.SubscribeAsync(func(message *amps.Message) error {
		if command, _ := message.Command(); command == amps.CommandPublish {
			select {
			case live <- string(message.Data()):
			default:
			}
		}
		return nil
	}, "go-ws-orders"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := client.Publish("go-ws-orders", `{"id":1}`); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := client.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	select {
	case payload := <-live:
		if payload != `{"id":1}` {
			t.Fatalf("live payload = %q", payload)
		}
	case err := <-errors:
		t.Fatalf("client error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("live delivery timed out")
	}
	var rows = make(chan string, 1)
	var completed = make(chan struct{}, 1)
	if _, err := client.ExecuteAsync(amps.NewCommand("sow").SetTopic("go-ws-orders").AddAckType(amps.AckTypeCompleted), func(message *amps.Message) error {
		switch command, _ := message.Command(); command {
		case amps.CommandSOW:
			select {
			case rows <- string(message.Data()):
			default:
			}
		case amps.CommandAck:
			if ack, _ := message.AckType(); ack == amps.AckTypeCompleted {
				select {
				case completed <- struct{}{}:
				default:
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("SOW query: %v", err)
	}
	select {
	case row := <-rows:
		if row != `{"id":1}` {
			t.Fatalf("SOW row = %q", row)
		}
	case err := <-errors:
		t.Fatalf("client error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("SOW row timed out")
	}
	select {
	case <-completed:
	case err := <-errors:
		t.Fatalf("client error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("SOW completed ack timed out")
	}
}
