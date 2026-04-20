package amps

import (
	"fmt"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestParseSocketOptionsRecognizesSupportedKeys(t *testing.T) {
	values := url.Values{
		"tcp_nodelay": {"true"},
		"tcp_sndbuf":  {"8192"},
		"tcp_rcvbuf":  {"4096"},
		"compression": {"zlib"},
	}

	options, err := parseSocketOptions(values)
	if err != nil {
		t.Fatalf("parseSocketOptions returned error: %v", err)
	}
	if !options.noDelaySet || !options.noDelay {
		t.Fatalf("expected tcp_nodelay to be enabled")
	}
	if !options.sendBufferSet || options.sendBuffer != 8192 {
		t.Fatalf("sendBuffer = %d, want 8192", options.sendBuffer)
	}
	if !options.receiveBufferSet || options.receiveBuffer != 4096 {
		t.Fatalf("receiveBuffer = %d, want 4096", options.receiveBuffer)
	}
	if options.compression != "zlib" {
		t.Fatalf("compression = %q, want zlib", options.compression)
	}
}

func TestParseSocketOptionsRejectsUnsupportedKey(t *testing.T) {
	_, err := parseSocketOptions(url.Values{"mystery": {"1"}})
	if err == nil {
		t.Fatalf("parseSocketOptions should reject unsupported keys")
	}
}

func TestParseSocketOptionsRejectsInvalidValues(t *testing.T) {
	_, err := parseSocketOptions(url.Values{"tcp_nodelay": {"maybe"}})
	if err == nil {
		t.Fatalf("parseSocketOptions should reject invalid boolean values")
	}

	_, err = parseSocketOptions(url.Values{"tcp_sndbuf": {"bad"}})
	if err == nil {
		t.Fatalf("parseSocketOptions should reject invalid integer values")
	}

	_, err = parseSocketOptions(url.Values{"compression": {"snappy"}})
	if err == nil {
		t.Fatalf("parseSocketOptions should reject unsupported compression values")
	}
}

func TestConnectRejectsUnsupportedURIOptions(t *testing.T) {
	client := NewClient("uriopts-invalid")

	err := client.Connect("tcp://127.0.0.1:1/amps?mystery=1")
	if err == nil {
		t.Fatalf("expected Connect to reject unsupported URI options")
	}
	if !strings.Contains(err.Error(), "InvalidURIError") {
		t.Fatalf("Connect error = %q, want InvalidURIError", err)
	}
}

func TestConnectAcceptsSupportedURIOptions(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	accepted := make(chan struct{}, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			_ = conn.Close()
		}
		accepted <- struct{}{}
	}()

	client := NewClient("uriopts-valid")
	uri := fmt.Sprintf("tcp://%s/amps?tcp_nodelay=true&tcp_sndbuf=8192&tcp_rcvbuf=4096", listener.Addr().String())
	if err := client.Connect(uri); err != nil {
		t.Fatalf("Connect returned error: %v", err)
	}
	defer func() { _ = client.Close() }()

	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatalf("listener did not observe the connection")
	}
}

func TestConnectRejectsCompressionOnNonTCPTransportSchemes(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	client := NewClient("uriopts-compression-nontcp")
	uri := fmt.Sprintf("http://%s/amps?compression=zlib", listener.Addr().String())
	err = client.Connect(uri)
	if err == nil {
		t.Fatalf("expected Connect to reject compression on non-tcp transport schemes")
	}
	if !strings.Contains(err.Error(), "InvalidURIError") {
		t.Fatalf("Connect error = %q, want InvalidURIError", err)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "compression") {
		t.Fatalf("Connect error = %q, want compression-related failure", err)
	}
}
