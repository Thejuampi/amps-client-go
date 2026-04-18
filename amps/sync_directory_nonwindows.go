//go:build !windows

package amps

import "os"

func syncDirectoryPath(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
