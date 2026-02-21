package wal

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestReadWriteCoverage(t *testing.T) {
	if err := Write("", []byte("data")); err == nil {
		t.Fatalf("expected empty path write error")
	}
	if _, err := Read(""); err == nil {
		t.Fatalf("expected empty path read error")
	}

	path := filepath.Join(t.TempDir(), "wal.bin")
	input := []byte("wal-data")
	if err := Write(path, input); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	output, err := Read(path)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(output) != string(input) {
		t.Fatalf("unexpected wal output: %q", string(output))
	}

	// ReadFile error branch.
	if _, err := Read(filepath.Join(t.TempDir(), "missing.bin")); err == nil {
		t.Fatalf("expected missing file read error")
	}

	// WriteFile error branch (directory path).
	dirPath := filepath.Join(t.TempDir(), "subdir")
	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	if err := Write(dirPath, []byte("x")); err == nil {
		t.Fatalf("expected directory write error")
	}
}

func TestWALAppendReplayAndTruncateCoverage(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "ops.wal")
	checkpointPath := filepath.Join(tempDir, "checkpoint.json")

	if err := Append("", []byte("x"), false); err == nil {
		t.Fatalf("expected append path error")
	}
	if err := Truncate(""); err == nil {
		t.Fatalf("expected truncate path error")
	}
	if err := WriteAtomic("", []byte("x"), 0o600); err == nil {
		t.Fatalf("expected writeatomic path error")
	}
	if err := Replay("", func([]byte) error { return nil }); err == nil {
		t.Fatalf("expected replay path error")
	}
	if err := Replay(logPath, nil); err != nil {
		t.Fatalf("expected nil replayer noop: %v", err)
	}
	if err := ReplayNoCopy(logPath, nil); err != nil {
		t.Fatalf("expected nil no-copy replayer noop: %v", err)
	}
	if err := Replay(filepath.Join(tempDir, "missing.wal"), func([]byte) error { return nil }); err != nil {
		t.Fatalf("expected missing wal replay to noop: %v", err)
	}
	if err := ReplayNoCopy(filepath.Join(tempDir, "missing-nocopy.wal"), func([]byte) error { return nil }); err != nil {
		t.Fatalf("expected missing wal no-copy replay to noop: %v", err)
	}
	if err := Append(logPath, nil, false); err != nil {
		t.Fatalf("expected empty append to noop: %v", err)
	}

	if err := Append(logPath, []byte("first\n"), true); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	type op struct {
		Type string `json:"type"`
		ID   int    `json:"id"`
	}
	if err := AppendJSON(logPath, op{Type: "second", ID: 2}, true); err != nil {
		t.Fatalf("append json failed: %v", err)
	}

	collected := make([]string, 0, 2)
	if err := Replay(logPath, func(line []byte) error {
		collected = append(collected, string(line))
		return nil
	}); err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if len(collected) != 2 {
		t.Fatalf("expected two replay lines, got %d", len(collected))
	}

	decoded := op{}
	if err := json.Unmarshal([]byte(collected[1]), &decoded); err != nil {
		t.Fatalf("unmarshal replayed json failed: %v", err)
	}
	if decoded.Type != "second" || decoded.ID != 2 {
		t.Fatalf("unexpected replayed record: %+v", decoded)
	}
	if err := Replay(logPath, func([]byte) error { return errors.New("stop") }); err == nil {
		t.Fatalf("expected replay apply error")
	}
	if err := ReplayNoCopy(logPath, func([]byte) error { return errors.New("stop") }); err == nil {
		t.Fatalf("expected no-copy replay apply error")
	}
	if err := AppendJSON(logPath, map[string]any{"bad": make(chan int)}, false); err == nil {
		t.Fatalf("expected append json marshal error")
	}

	if err := WriteAtomic(checkpointPath, []byte("checkpoint"), 0o600); err != nil {
		t.Fatalf("writeatomic checkpoint failed: %v", err)
	}
	if data, err := os.ReadFile(checkpointPath); err != nil || string(data) != "checkpoint" {
		t.Fatalf("unexpected checkpoint write: %q err=%v", string(data), err)
	}

	if err := Truncate(logPath); err != nil {
		t.Fatalf("truncate failed: %v", err)
	}
	emptyCount := 0
	if err := Replay(logPath, func([]byte) error {
		emptyCount++
		return nil
	}); err != nil {
		t.Fatalf("replay after truncate failed: %v", err)
	}
	if emptyCount != 0 {
		t.Fatalf("expected no replay lines after truncate")
	}
	if err := Append(tempDir, []byte("x"), false); err == nil {
		t.Fatalf("expected append error when path is a directory")
	}
	if err := Truncate(tempDir); err == nil {
		t.Fatalf("expected truncate error when path is a directory")
	}
	if err := ReplayNoCopy("", func([]byte) error { return nil }); err == nil {
		t.Fatalf("expected no-copy replay path error")
	}
}

func TestReplayHandlesLargeLine(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "large.wal")
	largeLine := bytes.Repeat([]byte("a"), (17*1024*1024)+7)
	record := append(append([]byte(nil), largeLine...), '\n')

	if err := Append(logPath, record, false); err != nil {
		t.Fatalf("append large line failed: %v", err)
	}

	lines := 0
	if err := Replay(logPath, func(line []byte) error {
		lines++
		if len(line) != len(largeLine) {
			t.Fatalf("unexpected replay line length: got %d want %d", len(line), len(largeLine))
		}
		return nil
	}); err != nil {
		t.Fatalf("replay large line failed: %v", err)
	}
	if lines != 1 {
		t.Fatalf("expected one replayed line, got %d", lines)
	}
}
