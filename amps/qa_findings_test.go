package amps

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestResolveBookmarkReturnsStoredBookmarkForRECENT(t *testing.T) {
	client := NewClient("resolve-bookmark")
	store := NewMemoryBookmarkStore()
	client.SetBookmarkStore(store)
	client.command = NewCommand("subscribe").SetSubID("sub-wrong").SetTopic("orders")

	sub := NewCommand("subscribe").SetSubID("sub-1").SetTopic("orders")

	msg := &Message{
		header: &_Header{
			command:  CommandPublish,
			subID:    []byte("sub-1"),
			topic:    []byte("orders"),
			bookmark: []byte("1|42|"),
		},
	}
	store.Log(msg)
	store.Discard("sub-1", 1)

	resolved := client.resolveBookmark(sub, BookmarksRECENT)
	if resolved == BookmarksRECENT {
		t.Fatalf("expected MOST_RECENT to resolve to stored bookmark, got %q", resolved)
	}
	if resolved != "1|42|" {
		t.Fatalf("expected resolved bookmark to be 1|42|, got %q", resolved)
	}
}

func TestResolveBookmarkReturnsOriginalWhenNoStore(t *testing.T) {
	client := NewClient("resolve-no-store")
	resolved := client.resolveBookmark(NewCommand("subscribe").SetSubID("sub-1"), BookmarksRECENT)
	if resolved != BookmarksRECENT {
		t.Fatalf("expected original bookmark when no store, got %q", resolved)
	}
}

func TestResolveBookmarkIgnoresUnrelatedScratchCommandState(t *testing.T) {
	client := NewClient("resolve-ignores-scratch")
	store := NewMemoryBookmarkStore()
	client.SetBookmarkStore(store)
	client.command = NewCommand("subscribe").SetSubID("sub-wrong").SetTopic("orders")

	store.Log(&Message{
		header: &_Header{
			command:  CommandPublish,
			subID:    []byte("sub-wrong"),
			topic:    []byte("orders"),
			bookmark: []byte("1|99|"),
		},
	})
	store.Discard("sub-wrong", 1)

	resolved := client.resolveBookmark(NewCommand("subscribe").SetTopic("orders"), BookmarksRECENT)
	if resolved != BookmarksRECENT {
		t.Fatalf("expected RECENT without an outgoing subscription id to remain unresolved, got %q", resolved)
	}
}

func TestResolveBookmarkLeavesNOWUnchangedWhenStorePresent(t *testing.T) {
	client := NewClient("resolve-now")
	store := NewMemoryBookmarkStore()
	client.SetBookmarkStore(store)
	client.command = NewCommand("subscribe").SetSubID("sub-now").SetTopic("orders")

	store.Log(&Message{
		header: &_Header{
			command:  CommandPublish,
			subID:    []byte("sub-now"),
			topic:    []byte("orders"),
			bookmark: []byte("1|77|"),
		},
	})
	store.Discard("sub-now", 1)

	resolved := client.resolveBookmark(NewCommand("subscribe").SetSubID("sub-now").SetTopic("orders"), BookmarksNOW)
	if resolved != BookmarksNOW {
		t.Fatalf("expected NOW bookmark to remain %q, got %q", BookmarksNOW, resolved)
	}
}

func TestResolveBookmarkReturnsOriginalForLiteralBookmark(t *testing.T) {
	client := NewClient("resolve-literal")
	resolved := client.resolveBookmark(NewCommand("subscribe").SetSubID("sub-1"), "1|100|")
	if resolved != "1|100|" {
		t.Fatalf("expected literal bookmark to pass through, got %q", resolved)
	}
}

func TestConnectionStateReconnectingEmitted(t *testing.T) {
	ha := NewHAClient("reconnecting-state")
	ha.SetServerChooser(&fixedChooser{uri: "tcp://127.0.0.1:1/amps/json"})
	ha.SetReconnectDelay(0)
	ha.SetTimeout(10 * time.Millisecond)

	var observed ConnectionState
	ha.Client().AddConnectionStateListener(ConnectionStateListenerFunc(func(state ConnectionState) {
		if state == ConnectionStateReconnecting {
			observed = state
		}
	}))

	ha.handleDisconnect(NewError(ConnectionError, "trigger"))
	time.Sleep(50 * time.Millisecond)

	_ = ha.Disconnect()

	if observed != ConnectionStateReconnecting {
		t.Fatalf("expected ConnectionStateReconnecting to be emitted, got %d", observed)
	}
}

func TestConnectionStateAuthenticatingEmitted(t *testing.T) {
	client := NewClient("authenticating-state")

	var observed ConnectionState
	client.AddConnectionStateListener(ConnectionStateListenerFunc(func(state ConnectionState) {
		if state == ConnectionStateAuthenticating {
			observed = state
		}
	}))

	client.notifyConnectionState(ConnectionStateAuthenticating)

	if observed != ConnectionStateAuthenticating {
		t.Fatalf("expected ConnectionStateAuthenticating to be receivable, got %d", observed)
	}
}

func TestRingBookmarkStoreEviction(t *testing.T) {
	store := NewRingBookmarkStoreWithSize(3)

	for i := 0; i < 5; i++ {
		msg := &Message{
			header: &_Header{
				command:  CommandPublish,
				subID:    []byte("sub-ring"),
				topic:    []byte("orders"),
				bookmark: []byte("1|" + string(rune('0'+i)) + "|"),
			},
		}
		store.Log(msg)
	}

	records := store.MemoryBookmarkStore.records["sub-ring"]
	if records == nil {
		t.Fatalf("expected records to exist for sub-ring")
	}
	if uint(len(records)) > store.ringSize {
		t.Fatalf("expected at most %d records after eviction, got %d", store.ringSize, len(records))
	}
}

func TestRingBookmarkStoreEvictsDiscardedEntriesFirst(t *testing.T) {
	store := NewRingBookmarkStoreWithSize(3)

	for i := 0; i < 3; i++ {
		store.Log(&Message{
			header: &_Header{
				command:  CommandPublish,
				subID:    []byte("sub-ring-discarded"),
				topic:    []byte("orders"),
				bookmark: []byte("1|" + string(rune('0'+i)) + "|"),
			},
		})
	}

	store.DiscardMessage(&Message{
		header: &_Header{
			command:  CommandPublish,
			subID:    []byte("sub-ring-discarded"),
			topic:    []byte("orders"),
			bookmark: []byte("1|0|"),
		},
	})
	store.DiscardMessage(&Message{
		header: &_Header{
			command:  CommandPublish,
			subID:    []byte("sub-ring-discarded"),
			topic:    []byte("orders"),
			bookmark: []byte("1|1|"),
		},
	})

	store.Log(&Message{
		header: &_Header{
			command:  CommandPublish,
			subID:    []byte("sub-ring-discarded"),
			topic:    []byte("orders"),
			bookmark: []byte("1|3|"),
		},
	})

	records := store.MemoryBookmarkStore.records["sub-ring-discarded"]
	if records == nil {
		t.Fatalf("expected records to exist for sub-ring-discarded")
	}
	if _, exists := records["1|3|"]; !exists {
		t.Fatalf("expected newest live bookmark to remain after eviction")
	}
}

func TestRingBookmarkStoreDefaultSize(t *testing.T) {
	store := NewRingBookmarkStore()
	if store.RingSize() != 10000 {
		t.Fatalf("expected default ring size 10000, got %d", store.RingSize())
	}
}

func TestRingBookmarkStoreNilReceiver(t *testing.T) {
	var store *RingBookmarkStore
	if store.RingSize() != 0 {
		t.Fatalf("expected nil ring size 0")
	}
	store.Log(nil)
}

func TestJSONBuilderOutput(t *testing.T) {
	b := NewJSONBuilder()
	b.Append("name", "test")
	b.AppendInt("count", 42)
	data := b.Data()
	if !strings.Contains(data, `"name":"test"`) {
		t.Fatalf("expected name field in JSON, got %q", data)
	}
	if !strings.Contains(data, `"count":"42"`) {
		t.Fatalf("expected count field in JSON, got %q", data)
	}
	b.Reset()
	if len(b.Data()) != 2 {
		t.Fatalf("expected empty JSON object after reset, got %q", b.Data())
	}
}

func TestXMLBuilderOutput(t *testing.T) {
	b := NewXMLBuilder()
	b.Append("name", "test")
	b.SetRoot("order")
	data := b.Data()
	if !strings.Contains(data, "<order>") {
		t.Fatalf("expected root element, got %q", data)
	}
	if !strings.Contains(data, "<name>test</name>") {
		t.Fatalf("expected name element, got %q", data)
	}
}

func TestMessagePackBuilderOutput(t *testing.T) {
	b := NewMessagePackBuilder()
	b.Append("key", "val")
	data := b.Bytes()
	if len(data) == 0 {
		t.Fatalf("expected non-empty MessagePack output")
	}
	if data[0]&0xf0 != 0x80 {
		t.Fatalf("expected fixmap header byte, got 0x%02x", data[0])
	}
}

func TestBSONBuilderOutput(t *testing.T) {
	b := NewBSONBuilder()
	b.Append("key", "val")
	data := b.Bytes()
	if len(data) < 4 {
		t.Fatalf("expected at least 4 bytes for BSON document size")
	}
	docSize := int(data[0]) | int(data[1])<<8 | int(data[2])<<16 | int(data[3])<<24
	if docSize != len(data) {
		t.Fatalf("expected BSON document size %d to match actual size %d", docSize, len(data))
	}
}

func TestBFlatBuilderOutput(t *testing.T) {
	b := NewBFlatBuilder()
	b.Append("key", "val")
	data := b.Data()
	if !strings.Contains(data, "3:key=3:val") {
		t.Fatalf("expected BFlat key-val format in output, got %q", data)
	}
}

func TestWebSocketNetConnImplementsNetConn(t *testing.T) {
	var _ net.Conn = (*websocketNetConn)(nil)
}

func TestUnixSocketSchemeHandled(t *testing.T) {
	client := NewClient("unix-test")
	err := client.Connect("unix:///tmp/amps.sock/amps/json")
	if err == nil {
		t.Fatalf("expected connection error for non-existent unix socket")
	}
}

func TestCommandSetMessageType(t *testing.T) {
	cmd := NewCommand("publish").SetMessageType("json").SetTopic("orders")
	if string(cmd.header.messageType) != "json" {
		t.Fatalf("expected message type json, got %q", string(cmd.header.messageType))
	}
}

func TestClientSetHeartbeatWithGraceStoresSeconds(t *testing.T) {
	client := NewClient("heartbeat-grace")
	if client.SetHeartbeatWithGrace(3*time.Second, 7*time.Second, 2*time.Second) != client {
		t.Fatalf("expected SetHeartbeatWithGrace to be chainable")
	}
	if got := client.heartbeatInterval.Load(); got != 3 {
		t.Fatalf("expected heartbeatInterval=3 seconds, got %d", got)
	}
	if got := client.heartbeatTimeout.Load(); got != 7 {
		t.Fatalf("expected heartbeatTimeout=7 seconds, got %d", got)
	}
	if got := client.heartbeatGracePeriod; got != 2 {
		t.Fatalf("expected heartbeatGracePeriod=2 seconds, got %d", got)
	}
}

func TestClientSetHeartbeatClearsGracePeriod(t *testing.T) {
	client := NewClient("heartbeat-clears-grace")
	client.SetHeartbeatWithGrace(3*time.Second, 7*time.Second, 2*time.Second)
	client.SetHeartbeat(5, 9)

	if got := client.heartbeatInterval.Load(); got != 5 {
		t.Fatalf("expected heartbeatInterval=5 seconds, got %d", got)
	}
	if got := client.heartbeatTimeout.Load(); got != 9 {
		t.Fatalf("expected heartbeatTimeout=9 seconds, got %d", got)
	}
	if got := client.heartbeatGracePeriod; got != 0 {
		t.Fatalf("expected SetHeartbeat to clear grace period, got %d", got)
	}
}

func TestTimestampBookmarkHelper(t *testing.T) {
	now := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	bm := NewTimestampBookmark(now)
	if bm == "" {
		t.Fatalf("expected non-empty timestamp bookmark")
	}
	parsed, err := ParseTimestampBookmark(bm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	diff := parsed.Sub(now)
	if diff < -time.Second || diff > time.Second {
		t.Fatalf("expected parsed time within 1s of original, got diff %v", diff)
	}
}

func TestMessagePasswordGetter(t *testing.T) {
	msg := &Message{
		header: &_Header{
			password: []byte("secret"),
		},
	}
	pwd, hasPwd := msg.Password()
	if !hasPwd || pwd != "secret" {
		t.Fatalf("expected password 'secret', got %q has=%v", pwd, hasPwd)
	}
}

func TestOAuthAuthenticatorImplementsInterface(t *testing.T) {
	var _ Authenticator = &OAuthAuthenticator{}
	var _ Authenticator = &LDAPAuthenticator{}
}

func TestTLSConvenienceAPI(t *testing.T) {
	client := NewClient("tls-test")
	result := client.SetTLSMinVersion(0x0304)
	if result != client {
		t.Fatalf("expected chainable return")
	}
	client.SetTLSCiphers(nil)
	client.SetTLSInsecureSkipVerify(true)
	if client.tlsConfig == nil || !client.tlsConfig.InsecureSkipVerify {
		t.Fatalf("expected insecure skip verify set")
	}
}

func TestSetTLSCAPathLoadsPool(t *testing.T) {
	client := NewClient("tls-ca-path")
	certPath := filepath.Join(t.TempDir(), "ca.pem")
	certData := []byte("-----BEGIN CERTIFICATE-----\nMIIBszCCAVmgAwIBAgIUeQ4X2I6wMquYpVqnGP0w0jarruUwCgYIKoZIzj0EAwIw\nEjEQMA4GA1UEAwwHVGVzdCBDQTAeFw0yNjA0MTcxMjAwMDBaFw0zNjA0MTQxMjAw\nMDBaMBIxEDAOBgNVBAMMB1Rlc3QgQ0EwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNC\nAAR0x3Y5N0TqzM7gc3KJ2YMBX1AHzrc4c3OJbGdv8ImZ/Sc7VcRbP5t6dv3Vv4Vo\n20N6l0e6QF6UVx/F4WRzE7vdo1MwUTAdBgNVHQ4EFgQUf7N0eZZ+j9RnBbTK1ZBO\nVakiobgwHwYDVR0jBBgwFoAUf7N0eZZ+j9RnBbTK1ZBOVakiobgwDwYDVR0TAQH/\nBAUwAwEB/zAKBggqhkjOPQQDAgNJADBGAiEAjJYG1ChB+FAxo0xO+ogzAm8h1Hn0\nRyhokW2N7DbStOQCIQDQYB9H9n1tFoZT3zh0+BTtPlqvGjufH6G+jD/adJzi10g==\n-----END CERTIFICATE-----\n")
	if err := os.WriteFile(certPath, certData, 0o600); err != nil {
		t.Fatalf("WriteFile(certPath): %v", err)
	}

	result := client.SetTLSCAPath(certPath)
	if result != client {
		t.Fatalf("expected chainable return")
	}
	if client.tlsConfig == nil || client.tlsConfig.RootCAs == nil {
		t.Fatalf("expected RootCAs to be configured")
	}
}

func TestSetTLSCAPathReportsReadError(t *testing.T) {
	client := NewClient("tls-ca-path-missing")
	var reported error
	client.SetErrorHandler(func(err error) {
		reported = err
	})

	client.SetTLSCAPath(filepath.Join(t.TempDir(), "missing.pem"))

	if reported == nil || !strings.Contains(reported.Error(), "failed to read CA file") {
		t.Fatalf("expected CA read failure to be reported, got %v", reported)
	}
	if client.tlsConfig == nil || client.tlsConfig.RootCAs != nil {
		t.Fatalf("expected failed CA read to leave RootCAs unset")
	}
}

func TestEntitlementErrorHelpers(t *testing.T) {
	if IsEntitlementReadError(NewError(CommandError, EntitlementReadError)) != true {
		t.Fatalf("expected read entitlement error")
	}
	if IsEntitlementWriteError(NewError(CommandError, EntitlementWriteError)) != true {
		t.Fatalf("expected write entitlement error")
	}
	if IsEntitlementReadError(NewError(CommandError, "other")) != false {
		t.Fatalf("expected false for non-entitlement error")
	}
}

func TestSlowClientPolicyCreation(t *testing.T) {
	policy := NewSlowClientPolicy(1024*1024, 5*time.Second)
	if policy.MaxBacklogBytes != 1024*1024 {
		t.Fatalf("expected 1MB max backlog")
	}
	client := NewClient("slow-client")
	client.SetSlowClientPolicy(policy)
}

func TestSOWConvenienceMethodsExist(t *testing.T) {
	client := NewClient("sow-test")
	client.connected.Store(false)
	if _, err := client.SOWHistoricalQuery("orders", "", 10); err == nil {
		t.Fatalf("expected error for disconnected client")
	}
	if _, err := client.SOWAggregateQuery("orders", "", "field", 10); err == nil {
		t.Fatalf("expected error for disconnected client")
	}
	if _, err := client.SOWPaginatedQuery("orders", "", 10, 5); err == nil {
		t.Fatalf("expected error for disconnected client")
	}
}

func TestMonitoringConvenience(t *testing.T) {
	client := NewClient("monitor-test")
	client.connected.Store(false)
	if _, err := client.MonitorSOWStats(func(m *Message) error { return nil }); err == nil {
		t.Fatalf("expected error for disconnected client")
	}
	if _, err := client.RequestStats("/AMPS/SOWStats"); err == nil {
		t.Fatalf("expected error for disconnected client")
	}
}

func TestUnparsedCompositePayloadEmpty(t *testing.T) {
	msg := &Message{data: nil}
	if payload := msg.UnparsedCompositePayload(); payload != nil {
		t.Fatalf("expected nil for nil data")
	}
}

func TestUnparsedCompositePayloadNoRemainderWhenFullyParsed(t *testing.T) {
	data := []byte{
		0, 0, 0, 3, 'a', 'b', 'c',
	}
	msg := &Message{data: data}
	rem := msg.UnparsedCompositePayload()
	if rem != nil {
		t.Fatalf("expected nil when all data is length-prefixed parts, got %q", string(rem))
	}
}

func TestUnparsedCompositePayloadWithTrailingZeroLength(t *testing.T) {
	data := []byte{
		0, 0, 0, 3, 'a', 'b', 'c',
		0, 0, 0, 0,
	}
	msg := &Message{data: data}
	rem := msg.UnparsedCompositePayload()
	if rem != nil {
		t.Fatalf("expected nil when zero-length part is last, got %q", string(rem))
	}
}

func TestBookmarkStoreServerVersion(t *testing.T) {
	store := NewMemoryBookmarkStore()
	if store.ServerVersion() != "" {
		t.Fatalf("expected empty initial version")
	}
	store.SetServerVersion("5.3.5")
	if store.ServerVersion() != "5.3.5" {
		t.Fatalf("expected 5.3.5")
	}
	if !store.SupportsTimestampBookmarks() {
		t.Fatalf("expected 5.3.5 to support timestamp bookmarks")
	}
}

func TestBookmarkStoreSupportsTimestampBookmarksForDoubleDigitMajor(t *testing.T) {
	store := NewMemoryBookmarkStore()
	store.SetServerVersion("10.0")

	if !store.SupportsTimestampBookmarks() {
		t.Fatalf("expected 10.0 to support timestamp bookmarks")
	}
}

func TestCompressionConfig(t *testing.T) {
	client := NewClient("compression-test")
	client.SetCompression(true)
}

func TestCompositeBuilderTypes(t *testing.T) {
	if NewCompositeGlobalBuilder() == nil {
		t.Fatalf("expected non-nil composite global builder")
	}
	if NewCompositeLocalBuilder() == nil {
		t.Fatalf("expected non-nil composite local builder")
	}
}
