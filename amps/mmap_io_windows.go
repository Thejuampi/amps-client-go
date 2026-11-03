//go:build windows

package amps

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/Thejuampi/amps-client-go/amps/internal/wal"
)

var mmapSyncDirectory = syncDirectoryPath
var mmapSyncFile = func(file *os.File) error { return file.Sync() }

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
	var file, err = os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm) // #nosec G304 -- path is configured by trusted caller
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err = mmapSyncFile(file); err != nil {
		_ = file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	if err := mmapSyncDirectory(directory); err != nil {
		return &wal.CommitError{Err: err}
	}
	return nil
}
