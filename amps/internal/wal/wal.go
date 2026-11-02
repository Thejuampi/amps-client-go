package wal

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

var syncFile = func(file *os.File) error {
	return file.Sync()
}

var syncDirectory = syncDirectoryPath

// CommitError means the replacement was renamed into place, but a later
// durability operation failed. The new contents may already be visible.
type CommitError struct{ Err error }

func (err *CommitError) Error() string { return err.Err.Error() }
func (err *CommitError) Unwrap() error { return err.Err }

// AppendOutcomeError indicates that a failed append could not be durably rolled
// back; the record may be present after recovery.
type AppendOutcomeError struct {
	Err         error
	RollbackErr error
}

func (err *AppendOutcomeError) Error() string {
	return "WAL append outcome unknown: " + err.Err.Error() + "; rollback: " + err.RollbackErr.Error()
}
func (err *AppendOutcomeError) Unwrap() error { return errors.Join(err.Err, err.RollbackErr) }

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
	return os.ReadFile(path) // #nosec G304 -- path is provided by store configuration
}

func WriteAtomic(path string, data []byte, mode os.FileMode) error {
	if path == "" {
		return errors.New("wal path is required")
	}
	directory := filepath.Dir(path)
	if directory != "" && directory != "." {
		if mkdirErr := os.MkdirAll(directory, 0o700); mkdirErr != nil {
			return mkdirErr
		}
	}
	tmpPath := path + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode) // #nosec G304 -- path is provided by store configuration
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err = syncFile(file); err != nil {
		_ = file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpPath, path); err != nil {
		return err
	}
	if err = syncDirectory(directory); err != nil {
		return &CommitError{Err: err}
	}
	return nil
}

func Append(path string, data []byte, syncWrite bool) (err error) {
	if path == "" {
		return errors.New("wal path is required")
	}
	if len(data) == 0 {
		return nil
	}
	directory := filepath.Dir(path)
	if directory != "" && directory != "." {
		if mkdirErr := os.MkdirAll(directory, 0o700); mkdirErr != nil {
			return mkdirErr
		}
	}
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600) // #nosec G304 -- path is provided by store configuration
	if err != nil {
		return err
	}
	var info os.FileInfo
	info, err = file.Stat()
	if err != nil {
		_ = file.Close()
		return err
	}
	var originalSize = info.Size()
	var closed bool
	var rollback = func(cause error) error {
		if !closed {
			if closeErr := file.Close(); closeErr != nil {
				closed = true
				return &AppendOutcomeError{Err: cause, RollbackErr: closeErr}
			}
			closed = true
		}
		file, err = os.OpenFile(path, os.O_RDWR, 0) // #nosec G304 -- path is provided by store configuration
		if err != nil {
			return &AppendOutcomeError{Err: cause, RollbackErr: err}
		}
		var rollbackErr = file.Truncate(originalSize)
		if rollbackErr == nil {
			rollbackErr = syncFile(file)
		}
		if closeErr := file.Close(); rollbackErr == nil {
			rollbackErr = closeErr
		}
		closed = true
		if rollbackErr != nil {
			return &AppendOutcomeError{Err: cause, RollbackErr: rollbackErr}
		}
		return cause
	}
	var written int
	written, err = file.Write(data)
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return rollback(err)
	}
	if syncWrite {
		if err = syncFile(file); err != nil {
			return rollback(err)
		}
	}
	err = file.Close()
	closed = true
	if err != nil {
		return rollback(err)
	}
	if syncWrite && created {
		if err = syncDirectory(directory); err != nil {
			return rollback(err)
		}
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
	return replay(path, apply, true, false, false)
}

// ReplayNoCopy replays each non-empty WAL line to apply without allocating per line.
// The provided slice is backed by scanner memory and must not be retained after apply returns.
func ReplayNoCopy(path string, apply func([]byte) error) error {
	return replay(path, apply, false, false, false)
}

// ReplayAndRepair removes an incomplete trailing record before a store appends.
// The line slice is valid only until apply returns.
func ReplayAndRepair(path string, apply func([]byte) error, syncWrite bool) error {
	return replay(path, apply, false, true, syncWrite)
}

func replay(path string, apply func([]byte) error, copyLine, repair, syncWrite bool) (err error) {
	if path == "" {
		return errors.New("wal path is required")
	}
	if apply == nil {
		return nil
	}

	var file *os.File
	if repair {
		file, err = os.OpenFile(path, os.O_RDWR, 0) // #nosec G304 -- path is provided by store configuration
	} else {
		file, err = os.Open(path) // #nosec G304 -- path is provided by store configuration
	}
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer func() {
		closeErr := file.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	reader := bufio.NewReader(file)
	var committed int64
	for {
		line, readErr := reader.ReadBytes('\n')
		hasTerminator := len(line) > 0 && line[len(line)-1] == '\n'
		if hasTerminator {
			committed += int64(len(line))
			if line[len(line)-1] == '\n' {
				line = line[:len(line)-1]
			}
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
		}
		if !hasTerminator {
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					if repair && len(line) > 0 {
						if err = file.Truncate(committed); err != nil {
							return err
						}
						if syncWrite {
							return syncFile(file)
						}
					}
					return nil
				}
				return readErr
			}
			continue
		}
		if len(line) == 0 {
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					return nil
				}
				return readErr
			}
		} else {
			if copyLine {
				line = append([]byte(nil), line...)
			}
			if err = apply(line); err != nil {
				return err
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return readErr
		}
	}
}

func Truncate(path string) error {
	if path == "" {
		return errors.New("wal path is required")
	}
	directory := filepath.Dir(path)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600) // #nosec G304 -- path is provided by store configuration
	if err != nil {
		return err
	}
	if err = syncFile(file); err != nil {
		_ = file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	return syncDirectory(directory)
}
