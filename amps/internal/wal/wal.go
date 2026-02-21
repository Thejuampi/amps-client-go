package wal

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

func Write(path string, data []byte) error {
	if path == "" {
		return errors.New("wal path is required")
	}
	return WriteAtomic(path, data, 0600)
}

func Read(path string) ([]byte, error) {
	if path == "" {
		return nil, errors.New("wal path is required")
	}
	return os.ReadFile(path)
}

func WriteAtomic(path string, data []byte, mode os.FileMode) error {
	if path == "" {
		return errors.New("wal path is required")
	}
	directory := filepath.Dir(path)
	if directory != "" && directory != "." {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return err
		}
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, mode); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func Append(path string, data []byte, syncWrite bool) error {
	if path == "" {
		return errors.New("wal path is required")
	}
	if len(data) == 0 {
		return nil
	}
	directory := filepath.Dir(path)
	if directory != "" && directory != "." {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return err
		}
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err = file.Write(data); err != nil {
		return err
	}
	if syncWrite {
		return file.Sync()
	}
	return nil
}

func AppendJSON(path string, value any, syncWrite bool) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return Append(path, data, syncWrite)
}

// Replay replays each non-empty WAL line to apply.
// The provided line slice is a defensive copy, and may be retained by apply.
func Replay(path string, apply func([]byte) error) error {
	return replay(path, apply, true)
}

// ReplayNoCopy replays each non-empty WAL line to apply without allocating per line.
// The provided slice is backed by scanner memory and must not be retained after apply returns.
func ReplayNoCopy(path string, apply func([]byte) error) error {
	return replay(path, apply, false)
}

func replay(path string, apply func([]byte) error, copyLine bool) error {
	if path == "" {
		return errors.New("wal path is required")
	}
	if apply == nil {
		return nil
	}

	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		var line []byte
		if copyLine {
			line = append([]byte(nil), scanner.Bytes()...)
		} else {
			line = scanner.Bytes()
		}
		if len(line) == 0 {
			continue
		}
		if err = apply(line); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func Truncate(path string) error {
	if path == "" {
		return errors.New("wal path is required")
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	return file.Close()
}
