package amps

type fileStoreTestHooks struct {
	bookmarkCheckpointBeforeCommit func()
	bookmarkWalBeforeAppend        func()
	publishCheckpointBeforeCommit  func()
	publishWalBeforeAppend         func()
}

var currentFileStoreTestHooks fileStoreTestHooks
