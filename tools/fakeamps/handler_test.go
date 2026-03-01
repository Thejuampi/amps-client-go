package main

import (
	"bytes"
	"compress/zlib"
	"io"
	"testing"
)

func TestDecompressFrame(t *testing.T) {
	plain := []byte(`{"c":"publish"}{"id":1}`)

	var compressed bytes.Buffer
	zw := zlib.NewWriter(&compressed)
	_, err := zw.Write(plain)
	if err != nil {
		t.Fatalf("failed to compress test payload: %v", err)
	}
	_ = zw.Close()

	out, err := decompressFrame(compressed.Bytes())
	if err != nil {
		t.Fatalf("decompressFrame returned error: %v", err)
	}
	if !bytes.Equal(out, plain) {
		t.Fatalf("unexpected decompressed payload: %s", string(out))
	}

	_, err = decompressFrame([]byte("not-zlib"))
	if err == nil {
		t.Fatalf("expected error for invalid compressed payload")
	}
}

func TestTopicMessageTypeHelpers(t *testing.T) {
	topicConfigsMu.Lock()
	topicConfigs = make(map[string]*topicConfig)
	topicConfigsMu.Unlock()

	mt := getOrSetTopicMessageType("orders", "json")
	if mt != "json" {
		t.Fatalf("expected json message type got %q", mt)
	}

	mt2 := getOrSetTopicMessageType("orders", "xml")
	if mt2 != "json" {
		t.Fatalf("expected original topic message type to remain json got %q", mt2)
	}

	defaultMT := getTopicMessageType("unknown")
	if defaultMT != "json" {
		t.Fatalf("expected default message type json got %q", defaultMT)
	}
}

func TestDecompressFrameRoundTripReader(t *testing.T) {
	payload := []byte("hello world")

	var buf bytes.Buffer
	writer := zlib.NewWriter(&buf)
	_, _ = writer.Write(payload)
	_ = writer.Close()

	out, err := decompressFrame(buf.Bytes())
	if err != nil {
		t.Fatalf("expected successful round-trip decompression: %v", err)
	}

	reader := bytes.NewReader(out)
	readBack, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed reading decompressed bytes: %v", err)
	}
	if string(readBack) != "hello world" {
		t.Fatalf("unexpected decompressed content: %s", string(readBack))
	}
}
