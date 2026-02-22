//go:build windows

package amps

import (
	"errors"
	"os"
	"path/filepath"
)

func mmapReadFile(path string) ([]byte, error) {
	return os.ReadFile(path) // #nosec G304 -- path is configured by trusted caller
}

func mmapWriteFile(path string, data []byte, perm os.FileMode, initialSize int64) error {
	_ = initialSize
	if path == "" {
		return errors.New("mmap path is required")
	}
	directory := filepath.Dir(path)
	if directory != "" && directory != "." {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return err
		}
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, perm); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
