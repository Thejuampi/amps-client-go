package amps

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func compressTestBytes(t *testing.T, payload []byte) []byte {
	t.Helper()

	var buffer bytes.Buffer
	writer := zlib.NewWriter(&buffer)
	if _, err := writer.Write(payload); err != nil {
		t.Fatalf("compress write failed: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("compress flush failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("compress close failed: %v", err)
	}

	return buffer.Bytes()
}

func inflateTestBytes(t *testing.T, payload []byte, expectedLength int) []byte {
	t.Helper()

	reader, err := zlib.NewReader(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("inflate reader failed: %v", err)
	}
	defer func() { _ = reader.Close() }()

	var inflated = make([]byte, expectedLength)
	if _, err := io.ReadFull(reader, inflated); err != nil {
		t.Fatalf("inflate read failed: %v", err)
	}
	return inflated
}

func acceptConnection(t *testing.T, listener net.Listener) chan net.Conn {
	t.Helper()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			accepted <- nil
			return
		}
		accepted <- conn
	}()
	return accepted
}

func TestCompressedNetConnWriteCompressesOutboundBytes(t *testing.T) {
	frame := buildFrameFromCommand(t, NewCommand("publish").SetTopic("orders").SetData([]byte("payload")))
	rawConn := newTestConn()
	compressedConn := newCompressedNetConn(rawConn)

	written, err := compressedConn.Write(frame)
	if err != nil {
		t.Fatalf("Write returned error: %v", err)
	}
	if written != len(frame) {
		t.Fatalf("Write()=%d want %d", written, len(frame))
	}

	rawBytes := rawConn.WrittenBytes()
	if bytes.Equal(rawBytes, frame) {
		t.Fatalf("expected compressed bytes to differ from raw frame")
	}
	if inflated := inflateTestBytes(t, rawBytes, len(frame)); !bytes.Equal(inflated, frame) {
		t.Fatalf("inflated bytes did not match original frame")
	}
}

func TestCompressedNetConnReadInflatesInboundBytes(t *testing.T) {
	frame := buildFrameFromCommand(t, NewCommand("publish").SetTopic("orders").SetData([]byte("payload")))
	rawConn := newTestConn()
	rawConn.enqueueRead(compressTestBytes(t, frame))

	compressedConn := newCompressedNetConn(rawConn)
	readBuffer := make([]byte, len(frame))
	if _, err := io.ReadFull(compressedConn, readBuffer); err != nil {
		t.Fatalf("ReadFull returned error: %v", err)
	}
	if !bytes.Equal(readBuffer, frame) {
		t.Fatalf("inflated read bytes did not match original frame")
	}
}

func TestConnectUsesCompressionFromURIAndClientDefaults(t *testing.T) {
	tests := []struct {
		name               string
		defaultCompression bool
		uriQuery           string
		wantCompressed     bool
	}{
		{
			name:               "default-disabled",
			defaultCompression: false,
			uriQuery:           "",
			wantCompressed:     false,
		},
		{
			name:               "default-enabled",
			defaultCompression: true,
			uriQuery:           "",
			wantCompressed:     true,
		},
		{
			name:               "explicit-uri-overrides-default",
			defaultCompression: false,
			uriQuery:           "?compression=zlib",
			wantCompressed:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			defer listener.Close()

			accepted := acceptConnection(t, listener)
			client := NewClient("compression-connect-" + tc.name)
			client.SetErrorHandler(func(error) {})
			client.SetCompression(tc.defaultCompression)

			uri := fmt.Sprintf("tcp://%s/amps%s", listener.Addr().String(), tc.uriQuery)
			if err := client.Connect(uri); err != nil {
				t.Fatalf("Connect returned error: %v", err)
			}
			defer func() { _ = client.Close() }()

			serverConn := <-accepted
			if serverConn == nil {
				t.Fatalf("listener did not accept connection")
			}
			defer func() { _ = serverConn.Close() }()

			_, isCompressed := client.connection.(*compressedNetConn)
			if isCompressed != tc.wantCompressed {
				t.Fatalf("internal connection compressed=%v want %v", isCompressed, tc.wantCompressed)
			}
			if tc.wantCompressed && client.RawConnection() == nil {
				t.Fatalf("expected RawConnection to expose underlying socket")
			}
		})
	}
}

func TestConnectRejectsUnsupportedCompressionConfigurations(t *testing.T) {
	tests := []struct {
		name string
		uri  string
	}{
		{
			name: "ws-uri-option",
			uri:  "ws://127.0.0.1:9000/amps?compression=zlib",
		},
		{
			name: "unix-client-default",
			uri:  "unix:///tmp/amps.sock/amps/json",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := NewClient("compression-invalid-" + tc.name)
			client.SetErrorHandler(func(error) {})
			if tc.name == "unix-client-default" {
				client.SetCompression(true)
			}

			err := client.Connect(tc.uri)
			if err == nil {
				t.Fatalf("expected Connect to reject unsupported compression configuration")
			}
			if !strings.Contains(strings.ToLower(err.Error()), "compression") {
				t.Fatalf("expected compression-related error, got %v", err)
			}
		})
	}
}

func TestConnectWithCompressionPreservesHAReconnectCompatibility(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	type serverResult struct {
		raw []byte
		err error
	}
	serverDone := make(chan serverResult, 1)
	releaseServer := make(chan struct{})
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverDone <- serverResult{err: acceptErr}
			return
		}

		if deadlineErr := conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); deadlineErr != nil {
			_ = conn.Close()
			serverDone <- serverResult{err: deadlineErr}
			return
		}
		var rawInbound []byte
		var readBuffer = make([]byte, 1024)
		for {
			bytesRead, readErr := conn.Read(readBuffer)
			if bytesRead > 0 {
				rawInbound = append(rawInbound, readBuffer[:bytesRead]...)
			}
			if readErr != nil {
				if netErr, ok := readErr.(net.Error); ok && netErr.Timeout() && len(rawInbound) > 0 {
					break
				}
				if readErr == io.EOF && len(rawInbound) > 0 {
					break
				}
				_ = conn.Close()
				serverDone <- serverResult{err: readErr}
				return
			}
		}
		_ = conn.SetReadDeadline(time.Time{})
		serverDone <- serverResult{raw: rawInbound}
		<-releaseServer
		_ = conn.Close()
	}()

	ha := NewHAClient("ha-compression")
	ha.Client().SetErrorHandler(func(error) {})
	ha.Client().SetCompression(true)
	ha.SetServerChooser(NewDefaultServerChooser(fmt.Sprintf("tcp://%s/amps", listener.Addr().String())))
	ha.SetTimeout(250 * time.Millisecond)
	ha.SetReconnectDelay(0)
	ha.SetLogonOptions(LogonParams{Timeout: 25})

	if err := ha.ConnectAndLogon(); err == nil {
		t.Fatalf("expected logon timeout without server response")
	}
	defer close(releaseServer)
	_ = ha.Disconnect()

	result := <-serverDone
	if result.err != nil {
		t.Fatalf("server exchange failed: %v", result.err)
	}

	reader, readErr := zlib.NewReader(bytes.NewReader(result.raw))
	if readErr != nil {
		t.Fatalf("expected HA logon bytes to be zlib-compressed: %v", readErr)
	}
	defer func() { _ = reader.Close() }()

	var lengthBytes [4]byte
	if _, err := io.ReadFull(reader, lengthBytes[:]); err != nil {
		t.Fatalf("failed to read decompressed frame length: %v", err)
	}
	var frameLength = binary.BigEndian.Uint32(lengthBytes[:])
	var framePayload = make([]byte, frameLength)
	if _, err := io.ReadFull(reader, framePayload); err != nil {
		t.Fatalf("failed to read decompressed frame payload: %v", err)
	}

	message := &Message{header: newHeader()}
	if _, err := parseHeader(message, true, framePayload); err != nil {
		t.Fatalf("failed to parse decompressed HA logon frame: %v", err)
	}

	command, ok := message.Command()
	if !ok || command != commandLogon {
		t.Fatalf("expected decompressed HA frame to be logon, got %d ok=%v", command, ok)
	}
}
