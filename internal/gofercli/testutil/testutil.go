package testutil

import (
	"archive/zip"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func RepoRoot(t *testing.T) string {
	t.Helper()

	var dir, err = os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}
		var parent = filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find repo root from %s", dir)
		}
		dir = parent
	}
}

func WriteTempFile(t *testing.T, name string, data []byte) string {
	t.Helper()

	var path = filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func WriteTempZIP(t *testing.T, name string, files map[string]string) string {
	t.Helper()

	var buffer bytes.Buffer
	var archive = zip.NewWriter(&buffer)
	for fileName, body := range files {
		var handle, err = archive.Create(fileName)
		if err != nil {
			t.Fatalf("zip create %s: %v", fileName, err)
		}
		if _, err := io.WriteString(handle, body); err != nil {
			t.Fatalf("zip write %s: %v", fileName, err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return WriteTempFile(t, name, buffer.Bytes())
}
