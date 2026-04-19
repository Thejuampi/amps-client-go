package amps

import (
	"errors"
	"testing"
)

type syncDirectoryFileStub struct {
	closeCalls int
	closeErr   error
	syncErr    error
}

func (stub *syncDirectoryFileStub) Close() error {
	stub.closeCalls++
	return stub.closeErr
}

func (stub *syncDirectoryFileStub) Sync() error {
	return stub.syncErr
}

func TestSyncOpenedDirectoryReturnsCloseError(t *testing.T) {
	var expected = errors.New("close failed")
	var file = &syncDirectoryFileStub{closeErr: expected}

	var err = syncOpenedDirectory(file)

	if !errors.Is(err, expected) {
		t.Fatalf("expected close error, got %v", err)
	}
}

func TestSyncOpenedDirectoryPreservesSyncError(t *testing.T) {
	var syncErr = errors.New("sync failed")
	var file = &syncDirectoryFileStub{
		closeErr: errors.New("close failed"),
		syncErr:  syncErr,
	}

	var err = syncOpenedDirectory(file)

	if !errors.Is(err, syncErr) {
		t.Fatalf("expected sync error, got %v", err)
	}
}

func TestSyncOpenedDirectoryClosesFile(t *testing.T) {
	var file = &syncDirectoryFileStub{}

	_ = syncOpenedDirectory(file)

	if file.closeCalls != 1 {
		t.Fatalf("expected close to be called once, got %d", file.closeCalls)
	}
}

func TestNormalizeSyncDirectoryPath(t *testing.T) {
	var got = normalizeSyncDirectoryPath("./foo/../bar")

	if got != "bar" {
		t.Fatalf("expected cleaned sync directory path, got %q", got)
	}
}
