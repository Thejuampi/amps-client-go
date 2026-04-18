//go:build !windows

package wal

import "os"

var openSyncDirectory = func(path string) (syncDirectoryFile, error) {
	// #nosec G304 -- sync-directory paths are internal filesystem locations normalized before open.
	return os.Open(path)
}

func syncDirectoryPath(path string) error {
	file, err := openSyncDirectory(normalizeSyncDirectoryPath(path))
	if err != nil {
		return err
	}

	return syncOpenedDirectory(file)
}
