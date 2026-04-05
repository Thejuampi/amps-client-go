package amps

type syncDirectoryFile interface {
	Close() error
	Sync() error
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
