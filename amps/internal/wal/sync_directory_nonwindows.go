//go:build !windows

package wal

import "os"

func syncDirectoryPath(path string) error {
	var file, err = os.Open(path)
	if err != nil {
		return err
	}
	return syncOpenedDirectory(file)
}
