package amps

type fileStoreTestHooks struct {
	bookmarkCheckpointBeforeCommit func()
	bookmarkWalBeforeAppend        func()
	bookmarkWalAppendError         func() error
	publishCheckpointBeforeCommit  func()
	publishWalBeforeAppend         func()
	publishWalAppendError          func() error
}

var currentFileStoreTestHooks fileStoreTestHooks
