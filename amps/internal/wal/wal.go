package wal

import (
	"errors"
	"os"
)

func Write(path string, data []byte) error {
	if path == "" {
		return errors.New("wal path is required")
	}
	return os.WriteFile(path, data, 0600)
}

func Read(path string) ([]byte, error) {
	if path == "" {
		return nil, errors.New("wal path is required")
	}
	return os.ReadFile(path)
}
