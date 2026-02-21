package amps

import (
	"encoding/json"
	"errors"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/Thejuampi/amps-client-go/amps/internal/wal"
)

type publishStoreRecord struct {
	Sequence uint64          `json:"sequence"`
	Command  commandSnapshot `json:"command"`
}

type publishStoreFileState struct {
	LastPersisted uint64               `json:"last_persisted"`
	NextSequence  uint64               `json:"next_sequence"`
	ErrorOnGap    bool                 `json:"error_on_gap"`
	Records       []publishStoreRecord `json:"records"`
}

type publishStoreWalRecord struct {
	Type       string          `json:"type"`
	Sequence   uint64          `json:"sequence,omitempty"`
	Command    commandSnapshot `json:"command,omitempty"`
	ErrorOnGap *bool           `json:"error_on_gap,omitempty"`
}

// MemoryPublishStore stores replay or bookmark state for recovery-oriented workflows.
type MemoryPublishStore struct {
	lock              sync.Mutex
	entries           map[uint64]*Command
	lastPersisted     uint64
	nextSequence      uint64
	errorOnPublishGap bool
}

// NewMemoryPublishStore returns a new MemoryPublishStore.
func NewMemoryPublishStore() *MemoryPublishStore {
	return &MemoryPublishStore{
		entries:       make(map[uint64]*Command),
		nextSequence:  1,
		lastPersisted: 0,
	}
}

// Store returns the configured store instance used by the receiver.
func (store *MemoryPublishStore) Store(command *Command) (uint64, error) {
	if store == nil {
		return 0, errors.New("nil publish store")
	}
	if command == nil {
		return 0, errors.New("nil command")
	}

	store.lock.Lock()
	defer store.lock.Unlock()

	sequence := uint64(0)
	if command.header != nil && command.header.sequenceID != nil && *command.header.sequenceID > 0 {
		sequence = *command.header.sequenceID
	} else {
		sequence = store.nextSequence
		store.nextSequence++
	}

	cloned := cloneCommand(command)
	cloned.SetSequenceID(sequence)
	store.entries[sequence] = cloned
	if sequence >= store.nextSequence {
		store.nextSequence = sequence + 1
	}
	return sequence, nil
}

// DiscardUpTo executes the exported discardupto operation.
func (store *MemoryPublishStore) DiscardUpTo(sequence uint64) error {
	if store == nil {
		return errors.New("nil publish store")
	}

	store.lock.Lock()
	defer store.lock.Unlock()

	if store.errorOnPublishGap && sequence < store.lastPersisted {
		return errors.New("publish store gap detected")
	}

	for key := range store.entries {
		if key <= sequence {
			delete(store.entries, key)
		}
	}

	if sequence > store.lastPersisted {
		store.lastPersisted = sequence
	}
	return nil
}

// Replay executes the exported replay operation.
func (store *MemoryPublishStore) Replay(replayer func(*Command) error) error {
	if store == nil {
		return errors.New("nil publish store")
	}
	if replayer == nil {
		return nil
	}

	store.lock.Lock()
	sequences := make([]uint64, 0, len(store.entries))
	for sequence := range store.entries {
		if sequence > store.lastPersisted {
			sequences = append(sequences, sequence)
		}
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	commands := make([]*Command, 0, len(sequences))
	for _, sequence := range sequences {
		commands = append(commands, cloneCommand(store.entries[sequence]))
	}
	store.lock.Unlock()

	for _, command := range commands {
		if err := replayer(command); err != nil {
			return err
		}
	}
	return nil
}

// ReplaySingle executes the exported replaysingle operation.
func (store *MemoryPublishStore) ReplaySingle(replayer func(*Command) error, sequence uint64) (bool, error) {
	if store == nil {
		return false, errors.New("nil publish store")
	}
	if replayer == nil {
		return false, nil
	}

	store.lock.Lock()
	command, ok := store.entries[sequence]
	if ok {
		command = cloneCommand(command)
	}
	store.lock.Unlock()

	if !ok {
		return false, nil
	}
	return true, replayer(command)
}

// UnpersistedCount executes the exported unpersistedcount operation.
func (store *MemoryPublishStore) UnpersistedCount() int {
	if store == nil {
		return 0
	}
	store.lock.Lock()
	defer store.lock.Unlock()
	return len(store.entries)
}

// Flush executes the exported flush operation.
func (store *MemoryPublishStore) Flush(timeout time.Duration) error {
	if store == nil {
		return errors.New("nil publish store")
	}

	if timeout <= 0 {
		for {
			if store.UnpersistedCount() == 0 {
				return nil
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	deadline := time.Now().Add(timeout)
	for {
		if store.UnpersistedCount() == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return NewError(TimedOutError, "publish store flush timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// GetLowestUnpersisted returns the current lowest unpersisted value.
func (store *MemoryPublishStore) GetLowestUnpersisted() uint64 {
	if store == nil {
		return 0
	}
	store.lock.Lock()
	defer store.lock.Unlock()
	if len(store.entries) == 0 {
		return 0
	}
	lowest := uint64(0)
	for sequence := range store.entries {
		if lowest == 0 || sequence < lowest {
			lowest = sequence
		}
	}
	return lowest
}

// GetLastPersisted returns the current last persisted value.
func (store *MemoryPublishStore) GetLastPersisted() uint64 {
	if store == nil {
		return 0
	}
	store.lock.Lock()
	defer store.lock.Unlock()
	return store.lastPersisted
}

// SetErrorOnPublishGap sets error on publish gap on the receiver.
func (store *MemoryPublishStore) SetErrorOnPublishGap(enabled bool) {
	if store == nil {
		return
	}
	store.lock.Lock()
	store.errorOnPublishGap = enabled
	store.lock.Unlock()
}

// ErrorOnPublishGap executes the exported erroronpublishgap operation.
func (store *MemoryPublishStore) ErrorOnPublishGap() bool {
	if store == nil {
		return false
	}
	store.lock.Lock()
	defer store.lock.Unlock()
	return store.errorOnPublishGap
}

// FilePublishStore stores replay or bookmark state for recovery-oriented workflows.
type FilePublishStore struct {
	*MemoryPublishStore
	path               string
	walPath            string
	options            FileStoreOptions
	opsSinceCheckpoint uint64
}

// NewFilePublishStore returns a new FilePublishStore.
func NewFilePublishStore(path string) *FilePublishStore {
	return NewFilePublishStoreWithOptions(path, defaultFileStoreOptions())
}

// NewFilePublishStoreWithOptions returns a new FilePublishStore with explicit options.
func NewFilePublishStoreWithOptions(path string, options FileStoreOptions) *FilePublishStore {
	fileStore := &FilePublishStore{
		MemoryPublishStore: NewMemoryPublishStore(),
		path:               path,
		walPath:            path + ".wal",
		options:            normalizeFileStoreOptions(options),
	}
	_ = fileStore.load()
	return fileStore
}

func (store *FilePublishStore) appendWal(record publishStoreWalRecord) error {
	if store == nil || store.walPath == "" || !store.options.UseWAL {
		return nil
	}
	store.lock.Lock()
	defer store.lock.Unlock()
	return wal.AppendJSON(store.walPath, record, store.options.SyncOnWrite)
}

func (store *FilePublishStore) bumpMutationAndMaybeCheckpoint() error {
	if store == nil {
		return nil
	}

	store.lock.Lock()
	store.opsSinceCheckpoint++
	ops := store.opsSinceCheckpoint
	checkpointInterval := store.options.CheckpointInterval
	useWAL := store.options.UseWAL
	store.lock.Unlock()

	if !useWAL || ops >= checkpointInterval {
		return store.saveCheckpoint()
	}
	return nil
}

func (store *FilePublishStore) saveCheckpoint() error {
	if store == nil || store.path == "" {
		return nil
	}

	store.lock.Lock()
	defer store.lock.Unlock()
	sequences := make([]uint64, 0, len(store.entries))
	for sequence := range store.entries {
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })

	records := make([]publishStoreRecord, 0, len(sequences))
	for _, sequence := range sequences {
		records = append(records, publishStoreRecord{
			Sequence: sequence,
			Command:  snapshotFromCommand(store.entries[sequence]),
		})
	}

	state := publishStoreFileState{
		LastPersisted: store.lastPersisted,
		NextSequence:  store.nextSequence,
		ErrorOnGap:    store.errorOnPublishGap,
		Records:       records,
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	if store.options.MMap.Enabled {
		if err = mmapWriteFile(store.path, data, 0600, store.options.MMap.InitialSize); err != nil {
			return err
		}
	} else {
		if err = wal.WriteAtomic(store.path, data, 0600); err != nil {
			return err
		}
	}
	if store.options.UseWAL {
		if err = wal.Truncate(store.walPath); err != nil {
			return err
		}
	}
	store.opsSinceCheckpoint = 0
	return nil
}

func (store *FilePublishStore) loadCheckpoint() error {
	if store == nil || store.path == "" {
		return nil
	}

	var (
		data []byte
		err  error
	)
	if store.options.MMap.Enabled {
		data, err = mmapReadFile(store.path)
	} else {
		data, err = os.ReadFile(store.path)
	}
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if len(data) == 0 {
		return nil
	}

	state := publishStoreFileState{}
	if err = json.Unmarshal(data, &state); err != nil {
		return err
	}
	if state.NextSequence == 0 {
		state.NextSequence = 1
	}

	store.lock.Lock()
	store.entries = make(map[uint64]*Command)
	for _, record := range state.Records {
		command := commandFromSnapshot(record.Command)
		command.SetSequenceID(record.Sequence)
		store.entries[record.Sequence] = command
	}
	store.lastPersisted = state.LastPersisted
	store.nextSequence = state.NextSequence
	store.errorOnPublishGap = state.ErrorOnGap
	store.lock.Unlock()
	return nil
}

func (store *FilePublishStore) applyWalRecord(record publishStoreWalRecord) {
	store.lock.Lock()
	defer store.lock.Unlock()

	switch record.Type {
	case "store":
		sequence := record.Sequence
		if sequence == 0 && record.Command.SequenceID != nil {
			sequence = *record.Command.SequenceID
		}
		if sequence == 0 {
			return
		}
		command := commandFromSnapshot(record.Command)
		command.SetSequenceID(sequence)
		store.entries[sequence] = command
		if sequence >= store.nextSequence {
			store.nextSequence = sequence + 1
		}
	case "discard":
		if store.errorOnPublishGap && record.Sequence < store.lastPersisted {
			return
		}
		for key := range store.entries {
			if key <= record.Sequence {
				delete(store.entries, key)
			}
		}
		if record.Sequence > store.lastPersisted {
			store.lastPersisted = record.Sequence
		}
	case "error_on_gap":
		if record.ErrorOnGap != nil {
			store.errorOnPublishGap = *record.ErrorOnGap
		}
	}
}

func (store *FilePublishStore) replayWal() error {
	if store == nil || store.walPath == "" || !store.options.UseWAL {
		return nil
	}
	return wal.ReplayNoCopy(store.walPath, func(line []byte) error {
		record := publishStoreWalRecord{}
		if err := json.Unmarshal(line, &record); err != nil {
			return err
		}
		store.applyWalRecord(record)
		return nil
	})
}

func (store *FilePublishStore) load() error {
	if store == nil {
		return nil
	}
	if err := store.loadCheckpoint(); err != nil {
		return err
	}
	return store.replayWal()
}

// Store returns the configured store instance used by the receiver.
func (store *FilePublishStore) Store(command *Command) (uint64, error) {
	sequence, err := store.MemoryPublishStore.Store(command)
	if err != nil {
		return 0, err
	}

	snapshot := snapshotFromCommand(command)
	snapshot.SequenceID = &sequence
	if err = store.appendWal(publishStoreWalRecord{
		Type:     "store",
		Sequence: sequence,
		Command:  snapshot,
	}); err != nil {
		return sequence, err
	}
	if err = store.bumpMutationAndMaybeCheckpoint(); err != nil {
		return sequence, err
	}
	return sequence, nil
}

// DiscardUpTo executes the exported discardupto operation.
func (store *FilePublishStore) DiscardUpTo(sequence uint64) error {
	if err := store.MemoryPublishStore.DiscardUpTo(sequence); err != nil {
		return err
	}
	if err := store.appendWal(publishStoreWalRecord{
		Type:     "discard",
		Sequence: sequence,
	}); err != nil {
		return err
	}
	return store.bumpMutationAndMaybeCheckpoint()
}

// SetErrorOnPublishGap sets error on publish gap on the receiver.
func (store *FilePublishStore) SetErrorOnPublishGap(enabled bool) {
	store.MemoryPublishStore.SetErrorOnPublishGap(enabled)
	if err := store.appendWal(publishStoreWalRecord{
		Type:       "error_on_gap",
		ErrorOnGap: &enabled,
	}); err != nil {
		return
	}
	_ = store.bumpMutationAndMaybeCheckpoint()
}
