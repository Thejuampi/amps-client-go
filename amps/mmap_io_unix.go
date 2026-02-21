//go:build !windows

package amps

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

func mmapReadFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size == 0 {
		return []byte{}, nil
	}
	if size < 0 || size > int64(^uint(0)>>1) {
		return nil, errors.New("mmap file size is out of bounds")
	}

	mapped, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = syscall.Munmap(mapped)
	}()

	output := make([]byte, len(mapped))
	copy(output, mapped)
	return output, nil
}

func mmapWriteFile(path string, data []byte, perm os.FileMode, initialSize int64) error {
	if path == "" {
		return errors.New("mmap path is required")
	}
	if initialSize < int64(len(data)) {
		initialSize = int64(len(data))
	}
	if initialSize <= 0 {
		initialSize = int64(len(data))
	}

	directory := filepath.Dir(path)
	if directory != "" && directory != "." {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return err
		}
	}

	tmpPath := path + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, perm)
	if err != nil {
		return err
	}

	if err := file.Truncate(initialSize); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return err
	}

	mapped, err := syscall.Mmap(int(file.Fd()), 0, int(initialSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return err
	}

	for index := range mapped {
		mapped[index] = 0
	}
	copy(mapped, data)
	if err = syscall.Munmap(mapped); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return err
	}

	if err = file.Truncate(int64(len(data))); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err = file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err = file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, path)
}
