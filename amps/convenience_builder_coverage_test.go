package amps

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestConvenienceHelpersCoverage(t *testing.T) {
	var oauth OAuthAuthenticator
	if token, err := oauth.Authenticate("user", "secret"); err != nil || token != "secret" {
		t.Fatalf("unexpected OAuth authenticate result token=%q err=%v", token, err)
	}
	if token, err := oauth.Retry("user", "secret"); err != nil || token != "secret" {
		t.Fatalf("unexpected OAuth retry result token=%q err=%v", token, err)
	}
	oauth.Completed("user", "secret", "done")

	var ldap LDAPAuthenticator
	if token, err := ldap.Authenticate("user", "secret"); err != nil || token != "secret" {
		t.Fatalf("unexpected LDAP authenticate result token=%q err=%v", token, err)
	}
	if token, err := ldap.Retry("user", "secret"); err != nil || token != "secret" {
		t.Fatalf("unexpected LDAP retry result token=%q err=%v", token, err)
	}
	ldap.Completed("user", "secret", "done")

	var nilClient *Client
	if nilClient.SetTLSCiphers([]uint16{0x1301}) != nil {
		t.Fatalf("expected nil SetTLSCiphers to return nil")
	}
	if nilClient.SetTLSMinVersion(0x0304) != nil {
		t.Fatalf("expected nil SetTLSMinVersion to return nil")
	}
	if nilClient.SetTLSCAPath("missing.pem") != nil {
		t.Fatalf("expected nil SetTLSCAPath to return nil")
	}
	if nilClient.SetTLSInsecureSkipVerify(true) != nil {
		t.Fatalf("expected nil SetTLSInsecureSkipVerify to return nil")
	}
	if nilClient.SetSlowClientPolicy(NewSlowClientPolicy(1, time.Second)) != nil {
		t.Fatalf("expected nil SetSlowClientPolicy to return nil")
	}
	if nilClient.SetCompression(true) != nil {
		t.Fatalf("expected nil SetCompression to return nil")
	}
	if nilClient.SetReplicationGroup("rg-nil") != nil {
		t.Fatalf("expected nil SetReplicationGroup to return nil")
	}
	if nilClient.ReplicationGroup() != "" {
		t.Fatalf("expected nil ReplicationGroup to be empty")
	}
	if nilClient.LogonAckSequence() != 0 {
		t.Fatalf("expected nil LogonAckSequence to be zero")
	}

	client := NewClient("convenience")
	client.SetTLSCiphers([]uint16{0x1301})
	if client.tlsConfig == nil || len(client.tlsConfig.CipherSuites) != 1 || client.tlsConfig.CipherSuites[0] != 0x1301 {
		t.Fatalf("unexpected TLS cipher suites: %+v", client.tlsConfig)
	}
	client.SetTLSMinVersion(0x0304)
	client.SetTLSInsecureSkipVerify(true)
	if client.tlsConfig.MinVersion != 0x0304 || !client.tlsConfig.InsecureSkipVerify {
		t.Fatalf("unexpected TLS config after existing-config setters: %+v", client.tlsConfig)
	}

	if !IsEntitlementReadError(errors.New("wrapped entitlement_read failure")) || IsEntitlementReadError(nil) {
		t.Fatalf("unexpected entitlement read helper behavior")
	}
	if !IsEntitlementWriteError(errors.New("wrapped entitlement_write failure")) || IsEntitlementWriteError(nil) {
		t.Fatalf("unexpected entitlement write helper behavior")
	}

	policy := NewSlowClientPolicy(128, 3*time.Second)
	if policy.MaxBacklogBytes != 128 || policy.CheckInterval != 3*time.Second {
		t.Fatalf("unexpected slow client policy: %+v", policy)
	}
	if client.SetSlowClientPolicy(policy) != client {
		t.Fatalf("expected SetSlowClientPolicy to be chainable")
	}
	state := ensureClientState(client)
	state.lock.Lock()
	if state.slowClientPolicy != policy {
		state.lock.Unlock()
		t.Fatalf("expected slow client policy to be stored")
	}
	state.lock.Unlock()

	if client.SetCompression(true) != client {
		t.Fatalf("expected SetCompression to be chainable")
	}
	state.lock.Lock()
	if !state.compressionEnabled {
		state.lock.Unlock()
		t.Fatalf("expected compression flag to be stored")
	}
	state.lock.Unlock()

	if client.SetReplicationGroup("rg-a") != client || client.ReplicationGroup() != "rg-a" {
		t.Fatalf("unexpected replication group state")
	}
	client.logonAckBookmark = "1|9|"
	client.logonAckSequence.Store(42)
	if bookmark, ok := client.LogonAckBookmark(); !ok || bookmark != "1|9|" {
		t.Fatalf("unexpected logon ack bookmark %q ok=%v", bookmark, ok)
	}
	if client.LogonAckSequence() != 42 {
		t.Fatalf("unexpected logon ack sequence: %d", client.LogonAckSequence())
	}

	if _, err := ParseTimestampBookmark("not-a-bookmark"); err == nil {
		t.Fatalf("expected invalid timestamp bookmark error")
	}

	payload := []byte{
		0, 0, 0, 3, 'a', 'b', 'c',
		0, 0, 0, 1, 'd',
		'x', 'y',
	}
	message := &Message{data: payload}
	if got := string(message.UnparsedCompositePayload()); got != "xy" {
		t.Fatalf("unexpected unparsed composite payload: %q", got)
	}
	if got := string((&Message{data: []byte{0, 0, 0}}).UnparsedCompositePayload()); got != string([]byte{0, 0, 0}) {
		t.Fatalf("expected truncated composite payload to preserve remaining bytes, got %v", []byte(got))
	}
	if (*Message)(nil).UnparsedCompositePayload() != nil {
		t.Fatalf("expected nil composite payload to return nil")
	}

	globalBuilder := NewCompositeGlobalBuilder()
	localBuilder := NewCompositeLocalBuilder()
	if globalBuilder == nil || localBuilder == nil {
		t.Fatalf("expected composite builders")
	}
	globalBuilder.SetCompositeMessageType(CompositeMessageTypeGlobal)
	localBuilder.SetCompositeMessageType(CompositeMessageTypeLocal)

	convenienceCases := []struct {
		name string
		run  func(*Client) error
	}{
		{name: "historical", run: func(c *Client) error { _, err := c.SOWHistoricalQuery("orders", "/id > 10", 5); return err }},
		{name: "aggregate", run: func(c *Client) error { _, err := c.SOWAggregateQuery("orders", "/id > 10", "/desk", 3); return err }},
		{name: "paginated", run: func(c *Client) error { _, err := c.SOWPaginatedQuery("orders", "/id > 10", 3, 2); return err }},
		{name: "queue-cancel", run: func(c *Client) error { return c.QueueCancel("orders", "sub-1") }},
		{name: "queue-expire", run: func(c *Client) error { return c.QueueExpire("orders", "1|1|") }},
		{name: "queue-backlog", run: func(c *Client) error { return c.QueueSetMaxBacklog("orders", 25) }},
		{name: "monitor-sow", run: func(c *Client) error { _, err := c.MonitorSOWStats(func(*Message) error { return nil }); return err }},
		{name: "monitor-client", run: func(c *Client) error {
			_, err := c.MonitorClientStatus(func(*Message) error { return nil })
			return err
		}},
		{name: "monitor-connection", run: func(c *Client) error {
			_, err := c.MonitorConnectionStats(func(*Message) error { return nil })
			return err
		}},
		{name: "monitor-txlog", run: func(c *Client) error {
			_, err := c.MonitorTransactionLogStats(func(*Message) error { return nil })
			return err
		}},
		{name: "request-stats", run: func(c *Client) error { _, err := c.RequestStats("/AMPS/SOWStats"); return err }},
		{name: "bookmark-options", run: func(c *Client) error {
			_, err := c.BookmarkSubscribeWithOptions("orders", "1|1|", []string{OptionPause, OptionResume}, "/id > 10")
			return err
		}},
		{name: "fully-durable", run: func(c *Client) error {
			_, err := c.FullyDurableSubscribe("orders", BookmarksRECENT, "/id > 10")
			return err
		}},
	}
	for _, tc := range convenienceCases {
		if err := tc.run(client); err == nil {
			t.Fatalf("expected disconnected error for %s convenience helper", tc.name)
		}
	}
}

func TestMessageTypeBuildersCoverage(t *testing.T) {
	jsonBuilder := NewJSONBuilder()
	jsonBuilder.Append("name", "alice")
	jsonBuilder.AppendInt("count", 5)
	jsonBuilder.AppendFloat("ratio", 1.25)
	if got := jsonBuilder.Data(); got != `{"name":"alice","count":"5","ratio":"1.25"}` {
		t.Fatalf("unexpected JSON builder data: %q", got)
	}
	jsonBuilder.Reset()
	if got := jsonBuilder.Data(); got != "{}" {
		t.Fatalf("expected reset JSON builder to be empty, got %q", got)
	}

	bflatBuilder := NewBFlatBuilder()
	bflatBuilder.Append("id", "7")
	if got := bflatBuilder.Data(); got != "2:id=1:7," {
		t.Fatalf("unexpected BFlat builder data: %q", got)
	}
	bflatBuilder.Reset()
	if got := bflatBuilder.Data(); got != "" {
		t.Fatalf("expected reset BFlat builder to be empty, got %q", got)
	}

	xmlBuilder := NewXMLBuilder()
	xmlBuilder.SetRoot("row")
	xmlBuilder.Append("value", `<"&'>`)
	if got := xmlBuilder.Data(); got != "<row><value>&lt;&quot;&amp;&apos;&gt;</value></row>" {
		t.Fatalf("unexpected XML builder data: %q", got)
	}
	xmlBuilder.Reset()
	if got := xmlBuilder.Data(); got != "<row></row>" {
		t.Fatalf("expected reset XML builder to preserve root, got %q", got)
	}
	defaultXMLBuilder := NewXMLBuilder()
	defaultXMLBuilder.Append("id", "1")
	if got := defaultXMLBuilder.Data(); got != "<message><id>1</id></message>" {
		t.Fatalf("unexpected default-root XML builder data: %q", got)
	}

	messagePackBuilder := NewMessagePackBuilder()
	messagePackBuilder.Append("id", "7")
	if data := messagePackBuilder.Data(); len(data) == 0 {
		t.Fatalf("expected MessagePack builder data")
	}
	messagePackBuilder.Reset()
	if got := string(messagePackBuilder.Bytes()); got != string([]byte{0x80}) {
		t.Fatalf("expected empty MessagePack fixmap, got %v", []byte(got))
	}
	if encoded := appendMsgPackString(nil, strings.Repeat("a", 5)); len(encoded) == 0 || encoded[0] != 0xa5 {
		t.Fatalf("expected fixstr encoding, got %v", encoded)
	}
	if encoded := appendMsgPackString(nil, strings.Repeat("b", 40)); len(encoded) == 0 || encoded[0] != 0xd9 {
		t.Fatalf("expected str8 encoding, got %v", encoded[:1])
	}
	if encoded := appendMsgPackString(nil, strings.Repeat("c", 300)); len(encoded) == 0 || encoded[0] != 0xda {
		t.Fatalf("expected str16 encoding, got %v", encoded[:1])
	}
	tooLargeMessagePack := NewMessagePackBuilder()
	for i := 0; i < 16; i++ {
		tooLargeMessagePack.Append("k", "v")
	}
	if tooLargeMessagePack.Bytes() != nil {
		t.Fatalf("expected MessagePack builder with >15 fields to return nil")
	}

	bsonBuilder := NewBSONBuilder()
	bsonBuilder.Append("name", "alice")
	if data := bsonBuilder.Data(); len(data) == 0 {
		t.Fatalf("expected BSON Data to return encoded content")
	}
	bsonBytes := bsonBuilder.Bytes()
	if len(bsonBytes) < 6 || bsonBytes[len(bsonBytes)-1] != 0x00 {
		t.Fatalf("unexpected BSON bytes: %v", bsonBytes)
	}
	bsonBuilder.Reset()
	if got := bsonBuilder.Bytes(); len(got) != 5 || got[0] != 5 {
		t.Fatalf("expected empty BSON document, got %v", got)
	}
}
