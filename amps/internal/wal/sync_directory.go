package wal

import "path/filepath"

type syncDirectoryFile interface {
	Close() error
	Sync() error
}

func normalizeSyncDirectoryPath(path string) string {
	return filepath.Clean(path)
}

func syncOpenedDirectory(file syncDirectoryFile) (err error) {
	defer func() {
		var closeErr = file.Close()
		if err == nil {
			err = closeErr
		}
	}()

	err = file.Sync()
	return
}
