package amps

import (
	"path/filepath"
	"testing"
	"time"
)

func TestClientConnectClearsStaleMessageTypeWhenURIHasNoPath(t *testing.T) {
	client := NewClient("stale-message-type")
	client.messageType = []byte("json")

	if err := client.Connect("tcp://127.0.0.1:1"); err == nil {
		t.Fatalf("expected connect error against unopened port")
	}
	if client.messageType != nil {
		t.Fatalf("expected bare URI connect to clear stale message type, got %q", string(client.messageType))
	}
}

func TestFileBookmarkStoreWithoutWALPersistsMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bookmark_store.json")
	options := FileStoreOptions{
		UseWAL:             false,
		SyncOnWrite:        false,
		CheckpointInterval: 1,
	}
	store := NewFileBookmarkStoreWithOptions(path, options)

	seqNo, err := store.LogWithError(bookmarkMessage("sub-no-wal", "10|1|"))
	if err != nil {
		t.Fatalf("LogWithError returned error: %v", err)
	}
	if seqNo == 0 {
		t.Fatalf("expected non-zero bookmark sequence")
	}

	reloaded := NewFileBookmarkStoreWithOptions(path, options)
	if got := reloaded.GetMostRecent("sub-no-wal"); got != "10|1|" {
		t.Fatalf("GetMostRecent() = %q, want %q", got, "10|1|")
	}
}

func TestFilePublishStoreWithoutWALPersistsMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "publish_store.json")
	options := FileStoreOptions{
		UseWAL:             false,
		SyncOnWrite:        false,
		CheckpointInterval: 1,
	}
	store := NewFilePublishStoreWithOptions(path, options)

	sequence, err := store.Store(NewCommand("publish").SetTopic("orders").SetData([]byte(`{"id":1}`)))
	if err != nil {
		t.Fatalf("Store returned error: %v", err)
	}
	if sequence == 0 {
		t.Fatalf("expected non-zero publish sequence")
	}

	reloaded := NewFilePublishStoreWithOptions(path, options)
	if got := reloaded.UnpersistedCount(); got != 1 {
		t.Fatalf("UnpersistedCount() = %d, want 1", got)
	}
}

func TestZeroValueFIXBuilderAppendUsesDefaultSeparator(t *testing.T) {
	var builder FixMessageBuilder
	done := make(chan error, 1)

	go func() {
		done <- builder.Append(8, "FIX.4.4")
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Append returned error: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("Append hung for zero-value FIX builder")
	}

	if got := string(builder.Bytes()); got != "8=FIX.4.4\x01" {
		t.Fatalf("Bytes() = %q, want %q", got, "8=FIX.4.4\x01")
	}
}

func TestZeroValueNVFIXBuilderAppendUsesDefaultSeparator(t *testing.T) {
	var builder NvfixMessageBuilder
	done := make(chan error, 1)

	go func() {
		done <- builder.AppendStrings("symbol", "AAPL")
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("AppendStrings returned error: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("AppendStrings hung for zero-value NVFIX builder")
	}

	if got := string(builder.Bytes()); got != "symbol=AAPL\x01" {
		t.Fatalf("Bytes() = %q, want %q", got, "symbol=AAPL\x01")
	}
}
