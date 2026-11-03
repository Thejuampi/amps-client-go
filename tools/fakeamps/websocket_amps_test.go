package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

func ampsMaskedWebSocketFrame(opcode byte, final bool, payload []byte) []byte {
	var first = opcode
	if final {
		first |= 0x80
	}
	var result = []byte{first, 0x80 | byte(len(payload)), 1, 2, 3, 4}
	for i, b := range payload {
		result = append(result, b^result[2+i%4])
	}
	return result
}

func TestAMPSWebSocketReadMultipartAndFragmented(t *testing.T) {
	var input []byte
	input = append(input, ampsMaskedWebSocketFrame(1, true, []byte("text"))...)
	input = append(input, ampsMaskedWebSocketFrame(2, true, []byte{3, 0, 0, 0})...)
	input = append(input, ampsMaskedWebSocketFrame(1, false, []byte(`  {"c":"p",`))...)
	input = append(input, ampsMaskedWebSocketFrame(9, true, []byte("ping"))...)
	input = append(input, ampsMaskedWebSocketFrame(0, true, []byte("\"t\":\"x\"} \n"))...)
	input = append(input, ampsMaskedWebSocketFrame(1, true, []byte(`{"id":`))...)
	input = append(input, ampsMaskedWebSocketFrame(2, true, []byte(`1}`))...)
	var raw = &recordingWebsocketConn{}
	var ws = &websocketConn{Conn: raw, reader: bufio.NewReader(bytes.NewReader(input))}
	var conn = newAMPSWebsocketConn(ws)
	var want = buildCommandFrame(`{"c":"p","t":"x"}`, []byte(`{"id":1}`))
	var got = make([]byte, len(want))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read reconstructed command: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("command = %q, want %q", got, want)
	}
	if got := raw.Bytes(); !bytes.Equal(got, []byte{0x8a, 4, 'p', 'i', 'n', 'g'}) {
		t.Fatalf("ping response = %v, want pong", got)
	}
}

func TestAMPSWebSocketReadRejectsInvalidHandshakeAndCounts(t *testing.T) {
	var goodPreface = ampsMaskedWebSocketFrame(1, true, []byte("text"))
	var countOne = ampsMaskedWebSocketFrame(2, true, []byte{1, 0, 0, 0})
	var tests = []struct {
		name  string
		input []byte
	}{
		{"legacy binary frame", ampsMaskedWebSocketFrame(2, true, buildCommandFrame(`{"c":"logon"}`, nil))},
		{"unknown preface", ampsMaskedWebSocketFrame(1, true, []byte("other"))},
		{"binary preface", ampsMaskedWebSocketFrame(2, true, []byte("text"))},
		{"text count", append(append([]byte(nil), goodPreface...), ampsMaskedWebSocketFrame(1, true, []byte{1, 0, 0, 0})...)},
		{"short count", append(append([]byte(nil), goodPreface...), ampsMaskedWebSocketFrame(2, true, []byte{1, 0, 0})...)},
		{"zero count", append(append([]byte(nil), goodPreface...), ampsMaskedWebSocketFrame(2, true, []byte{0, 0, 0, 0})...)},
		{"huge count", append(append([]byte(nil), goodPreface...), ampsMaskedWebSocketFrame(2, true, []byte{255, 255, 255, 255})...)},
		{"binary header", append(append(append([]byte(nil), goodPreface...), countOne...), ampsMaskedWebSocketFrame(2, true, []byte(`{"c":"logon"}`))...)},
		{"invalid header", append(append(append([]byte(nil), goodPreface...), countOne...), ampsMaskedWebSocketFrame(1, true, []byte(`{broken}`))...)},
		{"unexpected continuation", append(append([]byte(nil), goodPreface...), ampsMaskedWebSocketFrame(0, true, []byte{1, 0, 0, 0})...)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var ws = &websocketConn{Conn: &recordingWebsocketConn{}, reader: bufio.NewReader(bytes.NewReader(test.input))}
			var conn = newAMPSWebsocketConn(ws)
			var output [1]byte
			if _, err := conn.Read(output[:]); err == nil || err == io.EOF {
				t.Fatalf("invalid input accepted or ended without protocol error: %v", err)
			}
		})
	}
}

func TestAMPSWebSocketWriteServerFrames(t *testing.T) {
	for _, mode := range []string{"text", "binary"} {
		t.Run(mode, func(t *testing.T) {
			var raw = &recordingWebsocketConn{}
			var ws = &websocketConn{Conn: raw, reader: bufio.NewReader(bytes.NewReader(nil))}
			var conn = newAMPSWebsocketConn(ws)
			conn.mode = mode

			var stream []byte
			stream = append(stream, buildCommandFrame(`{"c":"ack","a":"processed"}`, nil)...)
			stream = append(stream, buildCommandFrame(`{"c":"p","sids":"s"}`, []byte("abc"))...)
			stream = append(stream, buildCommandFrame(`{"c":"oof","sids":"s"}`, nil)...)
			stream = append(stream, buildCommandFrame(`{"c":"sow","query_id":"q"}`, []byte(`{"k":"a","l":3}abc{"k":"b","l":0}`))...)
			for _, part := range [][]byte{stream[:2], stream[2:7], stream[7:]} {
				if n, err := conn.Write(part); err != nil || n != len(part) {
					t.Fatalf("write stream fragment: n=%d err=%v", n, err)
				}
			}

			var payloadOpcode byte = 1
			if mode == "binary" {
				payloadOpcode = 2
			}
			var expected = []struct {
				opcode byte
				data   string
			}{
				{1, `{"c":"ack","a":"processed"}`},
				{1, `{"c":"p","sids":"s","l":3}`},
				{payloadOpcode, "abc"},
				{1, `{"c":"oof","sids":"s","l":0}`},
				{1, `{"c":"sow","query_id":"q","bs":2}`},
				{1, `{"k":"a","l":3}`},
				{payloadOpcode, "abc"},
				{1, `{"k":"b","l":0}`},
				{payloadOpcode, ""},
			}
			var decoder = &websocketConn{reader: bufio.NewReader(bytes.NewReader(raw.Bytes()))}
			for i, want := range expected {
				var data, opcode, err = decoder.readFrame()
				if err != nil || opcode != want.opcode || string(data) != want.data {
					t.Fatalf("frame %d = opcode=%d data=%q err=%v, want opcode=%d data=%q", i, opcode, data, err, want.opcode, want.data)
				}
			}
			if _, _, err := decoder.readFrame(); err != io.EOF {
				t.Fatalf("unexpected extra websocket frame: %v", err)
			}
			if raw.DeadlineCount() < len(expected)*2 {
				t.Fatalf("websocket writes did not set and clear deadlines: %d", raw.DeadlineCount())
			}
		})
	}
}

func TestAMPSWebSocketWriteRejectsInvalidFrames(t *testing.T) {
	var tooLarge [4]byte
	binary.BigEndian.PutUint32(tooLarge[:], websocketMaxPayloadSize+1)
	var tests = []struct {
		name  string
		frame []byte
	}{
		{"too large", tooLarge[:]},
		{"invalid header", buildCommandFrame(`{invalid}`, nil)},
		{"missing record length", buildCommandFrame(`{"c":"sow"}`, []byte(`{"k":"a"}abc`))},
		{"short record", buildCommandFrame(`{"c":"sow"}`, []byte(`{"k":"a","l":4}abc`))},
		{"mismatched batch count", buildCommandFrame(`{"c":"sow","bs":2}`, []byte(`{"k":"a","l":3}abc`))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var raw = &recordingWebsocketConn{}
			var conn = newAMPSWebsocketConn(&websocketConn{Conn: raw, reader: bufio.NewReader(strings.NewReader(""))})
			conn.mode = "text"
			if _, err := conn.Write(test.frame); err == nil {
				t.Fatal("accepted invalid frame")
			}
			if len(raw.Bytes()) != 0 {
				t.Fatalf("wrote partial invalid frame: %q", raw.Bytes())
			}
		})
	}
}

func TestAMPSWebSocketNumberFieldBoundaries(t *testing.T) {
	var cases = []struct {
		name      string
		header    string
		value     int
		want      string
		wantError bool
	}{
		{"negative absent", `{"c":"p"}`, -1, "", true},
		{"negative present", `{"c":"p","l":0}`, -1, "", true},
		{"zero absent", `{"c":"p"}`, 0, `{"c":"p","l":0}`, false},
		{"zero present", `{"c":"p","l":0}`, 0, `{"c":"p","l":0}`, false},
		{"positive absent", `{"c":"p"}`, 3, `{"c":"p","l":3}`, false},
		{"positive present", `{"c":"p","l":3}`, 3, `{"c":"p","l":3}`, false},
		{"mismatch", `{"c":"p","l":2}`, 3, "", true},
	}
	for _, entry := range cases {
		t.Run(entry.name, func(t *testing.T) {
			var header, _, fields, err = ampsWebSocketObject([]byte(entry.header))
			if err != nil {
				t.Fatal(err)
			}
			var got []byte
			got, err = ampsWebSocketNumberField(header, fields, "l", entry.value)
			if entry.wantError {
				if err == nil || got != nil {
					t.Fatalf("got %q, %v; want error", got, err)
				}
				return
			}
			if err != nil || string(got) != entry.want {
				t.Fatalf("got %q, %v; want %q", got, err, entry.want)
			}
		})
	}
}

type failingOnceAMPSWebSocketConn struct {
	written  bytes.Buffer
	failAt   int
	attempts int
	failure  error
	closes   int
}

func (c *failingOnceAMPSWebSocketConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c *failingOnceAMPSWebSocketConn) Write(p []byte) (int, error) {
	c.attempts++
	if c.attempts == c.failAt {
		return 0, c.failure
	}
	return c.written.Write(p)
}
func (c *failingOnceAMPSWebSocketConn) Close() error                     { c.closes++; return nil }
func (c *failingOnceAMPSWebSocketConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *failingOnceAMPSWebSocketConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *failingOnceAMPSWebSocketConn) SetDeadline(time.Time) error      { return nil }
func (c *failingOnceAMPSWebSocketConn) SetReadDeadline(time.Time) error  { return nil }
func (c *failingOnceAMPSWebSocketConn) SetWriteDeadline(time.Time) error { return nil }

func TestAMPSWebSocketWriteFailureIsTerminal(t *testing.T) {
	var ack = buildCommandFrame(`{"c":"ack","a":"processed"}`, nil)
	var publish = buildCommandFrame(`{"c":"p","sids":"s"}`, []byte(`{"id":1}`))
	var tests = []struct {
		name        string
		prefix      []byte
		input       []byte
		failAt      int
		wantWritten int
	}{
		{name: "standalone", input: publish, failAt: 3},
		{name: "after completed frame", input: append(append([]byte(nil), ack...), publish...), failAt: 5, wantWritten: len(ack)},
		{name: "frame buffered across writes", prefix: publish[:len(publish)-1], input: publish[len(publish)-1:], failAt: 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var failure = &net.OpError{Op: "write", Err: os.ErrDeadlineExceeded}
			var raw = &failingOnceAMPSWebSocketConn{failAt: test.failAt, failure: failure}
			var conn = newAMPSWebsocketConn(&websocketConn{Conn: raw, reader: bufio.NewReader(strings.NewReader(""))})
			conn.mode = "text"
			if len(test.prefix) > 0 {
				if n, err := conn.Write(test.prefix); n != len(test.prefix) || err != nil {
					t.Fatalf("buffer prefix: n=%d, err=%v", n, err)
				}
			}
			var n, err = conn.Write(test.input)
			if n != test.wantWritten || err != failure {
				t.Fatalf("failed write = (%d, %v), want (%d, original timeout)", n, err, test.wantWritten)
			}
			if raw.closes != 1 || raw.written.Len() == 0 {
				t.Fatalf("expected one close after partial output, closes=%d wire=%q", raw.closes, raw.written.Bytes())
			}
			var wireBefore = append([]byte(nil), raw.written.Bytes()...)
			var attemptsBefore = raw.attempts
			n, err = conn.Write(ack)
			if n != 0 || err != failure || raw.attempts != attemptsBefore || !bytes.Equal(raw.written.Bytes(), wireBefore) {
				t.Fatalf("write after failure = (%d, %v), attempts=%d/%d, wire=%q", n, err, raw.attempts, attemptsBefore, raw.written.Bytes())
			}
			type outcome struct {
				n   int
				err error
			}
			var outcomes = make(chan outcome, 8)
			for i := 0; i < cap(outcomes); i++ {
				go func() {
					var n, err = conn.Write(ack)
					outcomes <- outcome{n, err}
				}()
			}
			for i := 0; i < cap(outcomes); i++ {
				if got := <-outcomes; got.n != 0 || got.err != failure {
					t.Errorf("concurrent write after failure = (%d, %v)", got.n, got.err)
				}
			}
			if raw.closes != 1 || raw.attempts != attemptsBefore || !bytes.Equal(raw.written.Bytes(), wireBefore) {
				t.Fatalf("concurrent writes changed wire or close count: closes=%d attempts=%d wire=%q", raw.closes, raw.attempts, raw.written.Bytes())
			}
			_ = conn.Close()
			if raw.closes != 1 {
				t.Fatalf("explicit Close called underlying connection %d times", raw.closes)
			}
		})
	}
}
