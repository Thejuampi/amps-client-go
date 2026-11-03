package amps

import (
	"context"
	"errors"
	"net"
	"net/url"
	"strings"
	"testing"
)

func TestCurrentSocketOptionsValidateValues(t *testing.T) {
	for _, query := range []string{
		"tcp_connecttimeout=0", "tcp_connecttimeout=1", "tcp_keepalive=false", "tcp_keepalive=true", "tcp_linger=0", "tcp_linger=2", "http_preflight=0", "http_preflight=1", "ip_protocol_prefer=ipv4", "ip_protocol_prefer=ipv6", "pretty=true",
	} {
		t.Run(query, func(t *testing.T) {
			var values, _ = url.ParseQuery(query)
			if _, err := parseSocketOptions(values); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, query := range []string{"tcp_connecttimeout=-1", "tcp_connecttimeout=9223372036854775807", "tcp_keepalive=invalid", "http_preflight=invalid", "ip_protocol_prefer=ipv9", "tcp_linger=-1", "pretty=invalid"} {
		t.Run(query, func(t *testing.T) {
			var values, _ = url.ParseQuery(query)
			if _, err := parseSocketOptions(values); err == nil {
				t.Fatal("invalid option accepted")
			}
		})
	}
}

func TestConnectTimeoutUsesDialContext(t *testing.T) {
	var previous = clientNetDialContext
	t.Cleanup(func() { clientNetDialContext = previous })
	clientNetDialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		if _, present := ctx.Deadline(); !present {
			t.Error("connect timeout did not reach dialer")
			return nil, errors.New("no deadline")
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	var client = NewClient("timeout")
	if err := client.Connect("tcp://localhost:9000/amps/json?tcp_connecttimeout=10"); !IsErrorKind(err, TimedOutError) {
		t.Fatalf("wrong timeout result: %v", err)
	}
}

type paritySocketConn struct {
	*testConn
	keepAlive bool
	linger    int
}

func (connection *paritySocketConn) SetKeepAlive(value bool) error {
	connection.keepAlive = value
	return nil
}
func (connection *paritySocketConn) SetLinger(value int) error { connection.linger = value; return nil }

func TestTCPPreferenceFallsBackAndAppliesOptions(t *testing.T) {
	var previous = clientNetDialContext
	t.Cleanup(func() { clientNetDialContext = previous })
	var connection = &paritySocketConn{testConn: newTestConn(), keepAlive: true}
	var networks []string
	clientNetDialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		networks = append(networks, network)
		if network == "tcp6" {
			return nil, errors.New("IPv6 unavailable")
		}
		return connection, nil
	}
	var client = NewClient("prefer")
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect("tcp://localhost:9000/amps/json?ip_protocol_prefer=ipv6&tcp_keepalive=false&tcp_linger=2&http_preflight=0"); err != nil {
		t.Fatal(err)
	}
	if strings.Join(networks, ",") != "tcp6,tcp4" || connection.keepAlive || connection.linger != 2 {
		t.Fatalf("options not applied: networks=%v keepalive=%t linger=%d", networks, connection.keepAlive, connection.linger)
	}
}

func TestSocketLingerZeroDisablesAbortiveClose(t *testing.T) {
	var values, _ = url.ParseQuery("tcp_linger=0")
	var options, err = parseSocketOptions(values)
	if err != nil {
		t.Fatal(err)
	}
	var connection = &paritySocketConn{testConn: newTestConn()}
	if err := applySocketOptions(connection, options); err != nil {
		t.Fatal(err)
	}
	if connection.linger != -1 {
		t.Fatalf("C++ disables linger at zero; Go would abort with linger=%d", connection.linger)
	}
}

func TestHTTPPreflightURIOption(t *testing.T) {
	for _, enabled := range []string{"0", "1"} {
		t.Run(enabled, func(t *testing.T) {
			var previous = clientNetDialContext
			t.Cleanup(func() { clientNetDialContext = previous })
			var connection = newTestConn()
			connection.enqueueRead([]byte("HTTP/1.1 200 OK\r\n\r\n"))
			clientNetDialContext = func(context.Context, string, string) (net.Conn, error) { return connection, nil }
			var client = NewClient("preflight")
			t.Cleanup(func() { _ = client.Close() })
			if err := client.Connect("tcp://localhost:9000/amps/json?http_preflight=" + enabled); err != nil {
				t.Fatal(err)
			}
			if got := strings.HasPrefix(string(connection.WrittenBytes()), "GET /amps HTTP/1.1"); got != (enabled == "1") {
				t.Fatal("HTTP preflight option ignored")
			}
		})
	}
}

func TestLogonURIRequestsPretty(t *testing.T) {
	for _, value := range []string{"0", "1"} {
		t.Run(value, func(t *testing.T) {
			var client, connection = auditConnectedClient(t)
			client.url, _ = url.Parse("tcp://localhost:9000/amps/json?pretty=" + value)
			var ack = commandToMessage(NewCommand("ack").SetAckType(AckTypeProcessed))
			ack.header.status = []byte("success")
			if err := auditLogon(t, client, LogonParams{Timeout: 100}, ack); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(connection.WrittenPayload(), `"opts":"pretty"`) != (value == "1") {
				t.Fatal("URI pretty setting did not reach logon")
			}
		})
	}
}
