package amps

// MMapOptions configures optional memory-mapped file behavior.
type MMapOptions struct {
	Enabled     bool
	InitialSize int64
}

// FileStoreOptions configures file-backed store durability behavior.
type FileStoreOptions struct {
	UseWAL             bool
	SyncOnWrite        bool
	CheckpointInterval uint64
	MMap               MMapOptions
}

func defaultFileStoreOptions() FileStoreOptions {
	return FileStoreOptions{
		UseWAL:             true,
		SyncOnWrite:        true,
		CheckpointInterval: 1,
		MMap: MMapOptions{
			Enabled:     false,
			InitialSize: 64 * 1024,
		},
	}
}

func normalizeFileStoreOptions(options FileStoreOptions) FileStoreOptions {
	normalized := options
	defaults := defaultFileStoreOptions()
	if normalized.CheckpointInterval == 0 {
		normalized.CheckpointInterval = defaults.CheckpointInterval
	}
	if normalized.MMap.InitialSize <= 0 {
		normalized.MMap.InitialSize = defaults.MMap.InitialSize
	}
	return normalized
}
