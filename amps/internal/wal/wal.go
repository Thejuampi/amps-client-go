package wal

import (
	"errors"
	"os"
)

// Write persists WAL bytes to disk.
func Write(path string, data []byte) error {
	if path == "" {
		return errors.New("wal path is required")
	}
	return os.WriteFile(path, data, 0600)
}

// Read loads WAL bytes from disk.
func Read(path string) ([]byte, error) {
	if path == "" {
		return nil, errors.New("wal path is required")
	}
	return os.ReadFile(path)
}
