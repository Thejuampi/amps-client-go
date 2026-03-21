package amps

import (
	"encoding/json"
	"errors"
	"os"
	"sort"
	"sync"
	"sync/atomic"
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
	lock              sync.RWMutex
	entries           map[uint64]*Command
	lastPersisted     uint64
	nextSequence      uint64
	errorOnPublishGap bool
	drainedCh         chan struct{}
}

func newPublishStoreSignal(closed bool) chan struct{} {
	var signal = make(chan struct{})
	if closed {
		close(signal)
	}
	return signal
}

func publishStoreDrainSignalClosed(signal <-chan struct{}) bool {
	if signal == nil {
		return false
	}
	select {
	case <-signal:
		return true
	default:
		return false
	}
}

func (store *MemoryPublishStore) reopenDrainSignalLocked() {
	if publishStoreDrainSignalClosed(store.drainedCh) {
		store.drainedCh = newPublishStoreSignal(false)
	}
}

func (store *MemoryPublishStore) closeDrainSignalLocked() {
	if store.drainedCh == nil {
		store.drainedCh = newPublishStoreSignal(true)
		return
	}
	if !publishStoreDrainSignalClosed(store.drainedCh) {
		close(store.drainedCh)
	}
}

func (store *MemoryPublishStore) rebuildDrainSignalLocked() {
	if len(store.entries) == 0 {
		store.drainedCh = newPublishStoreSignal(true)
		return
	}
	store.drainedCh = newPublishStoreSignal(false)
}

// NewMemoryPublishStore returns a new MemoryPublishStore.
func NewMemoryPublishStore() *MemoryPublishStore {
	return &MemoryPublishStore{
		entries:       make(map[uint64]*Command),
		nextSequence:  1,
		lastPersisted: 0,
		drainedCh:     newPublishStoreSignal(true),
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
	var wasEmpty = len(store.entries) == 0

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
	if wasEmpty {
		store.reopenDrainSignalLocked()
	}
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
	if len(store.entries) == 0 {
		store.closeDrainSignalLocked()
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
	store.lock.RLock()
	defer store.lock.RUnlock()
	return len(store.entries)
}

// Flush executes the exported flush operation.
func (store *MemoryPublishStore) Flush(timeout time.Duration) error {
	if store == nil {
		return errors.New("nil publish store")
	}
	store.lock.Lock()
	if len(store.entries) == 0 {
		store.lock.Unlock()
		return nil
	}
	var drainedCh = store.drainedCh
	store.lock.Unlock()

	if timeout <= 0 {
		<-drainedCh
		return nil
	}

	var timer = time.NewTimer(timeout)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	select {
	case <-drainedCh:
		return nil
	case <-timer.C:
		return NewError(TimedOutError, "publish store flush timed out")
	}
}

// GetLowestUnpersisted returns the current lowest unpersisted value.
func (store *MemoryPublishStore) GetLowestUnpersisted() uint64 {
	if store == nil {
		return 0
	}
	store.lock.RLock()
	defer store.lock.RUnlock()
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
	loadErr            error
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
	fileStore.loadErr = fileStore.load()
	return fileStore
}

// LoadError returns the most recent constructor-time load error, if any.
func (store *FilePublishStore) LoadError() error {
	if store == nil {
		return nil
	}
	store.lock.RLock()
	defer store.lock.RUnlock()
	return store.loadErr
}

// appendWalNoLock writes a WAL record for the given store operation.
// It never acquires store.lock; callers may invoke it with or without the
// store mutex held.
func (store *FilePublishStore) appendWalNoLock(record publishStoreWalRecord) error {
	if store == nil || store.walPath == "" || !store.options.UseWAL {
		return nil
	}
	if currentFileStoreTestHooks.publishWalBeforeAppend != nil {
		currentFileStoreTestHooks.publishWalBeforeAppend()
	}
	return wal.AppendJSON(store.walPath, record, store.options.SyncOnWrite)
}

func (store *FilePublishStore) bumpMutationAndMaybeCheckpoint() error {
	if store == nil {
		return nil
	}

	ops := atomic.AddUint64(&store.opsSinceCheckpoint, 1)
	if store.options.UseWAL && ops >= store.options.CheckpointInterval {
		return store.saveCheckpoint()
	}
	return nil
}

func (store *FilePublishStore) saveCheckpoint() error {
	if store == nil || store.path == "" {
		return nil
	}

	store.lock.Lock()
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
	if currentFileStoreTestHooks.publishCheckpointBeforeCommit != nil {
		currentFileStoreTestHooks.publishCheckpointBeforeCommit()
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		store.lock.Unlock()
		return err
	}
	if store.options.MMap.Enabled {
		if err = mmapWriteFile(store.path, data, 0600, store.options.MMap.InitialSize); err != nil {
			store.lock.Unlock()
			return err
		}
	} else {
		if err = wal.WriteAtomic(store.path, data, 0600); err != nil {
			store.lock.Unlock()
			return err
		}
	}
	if store.options.UseWAL {
		if err = wal.Truncate(store.walPath); err != nil {
			store.lock.Unlock()
			return err
		}
	}

	store.opsSinceCheckpoint = 0
	store.lock.Unlock()
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

func (store *FilePublishStore) applyWalRecordLocked(record publishStoreWalRecord) {
	switch record.Type {
	case "store":
		wasEmpty := len(store.entries) == 0
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
		if wasEmpty {
			store.reopenDrainSignalLocked()
		}
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
		if len(store.entries) == 0 {
			store.closeDrainSignalLocked()
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

func (store *FilePublishStore) applyWalRecord(record publishStoreWalRecord) {
	store.lock.Lock()
	store.applyWalRecordLocked(record)
	store.lock.Unlock()
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
	if err := store.replayWal(); err != nil {
		return err
	}
	store.lock.Lock()
	store.loadErr = nil
	store.rebuildDrainSignalLocked()
	store.lock.Unlock()
	return nil
}

// Store returns the configured store instance used by the receiver.
func (store *FilePublishStore) Store(command *Command) (uint64, error) {
	if store == nil {
		return 0, errors.New("nil publish store")
	}
	if command == nil {
		return 0, errors.New("nil command")
	}

	store.lock.Lock()
	if store.loadErr != nil {
		err := store.loadErr
		store.lock.Unlock()
		return 0, err
	}

	sequence := uint64(0)
	if command.header != nil && command.header.sequenceID != nil && *command.header.sequenceID > 0 {
		sequence = *command.header.sequenceID
	} else {
		sequence = store.nextSequence
	}

	snapshot := snapshotFromCommand(command)
	snapshot.SequenceID = &sequence
	record := publishStoreWalRecord{
		Type:     "store",
		Sequence: sequence,
		Command:  snapshot,
	}
	if err := store.appendWalNoLock(record); err != nil {
		store.lock.Unlock()
		return 0, err
	}
	store.applyWalRecordLocked(record)
	store.lock.Unlock()
	if err := store.bumpMutationAndMaybeCheckpoint(); err != nil {
		return sequence, err
	}
	return sequence, nil
}

// DiscardUpTo executes the exported discardupto operation.
func (store *FilePublishStore) DiscardUpTo(sequence uint64) error {
	if store == nil {
		return errors.New("nil publish store")
	}

	store.lock.Lock()
	if store.loadErr != nil {
		err := store.loadErr
		store.lock.Unlock()
		return err
	}
	if store.errorOnPublishGap && sequence < store.lastPersisted {
		store.lock.Unlock()
		return errors.New("publish store gap detected")
	}
	record := publishStoreWalRecord{
		Type:     "discard",
		Sequence: sequence,
	}
	if err := store.appendWalNoLock(record); err != nil {
		store.lock.Unlock()
		return err
	}
	store.applyWalRecordLocked(record)
	store.lock.Unlock()
	return store.bumpMutationAndMaybeCheckpoint()
}

// SetErrorOnPublishGap sets error on publish gap on the receiver.
func (store *FilePublishStore) SetErrorOnPublishGap(enabled bool) {
	if store == nil {
		return
	}

	store.lock.Lock()
	if store.loadErr != nil {
		store.lock.Unlock()
		return
	}
	record := publishStoreWalRecord{
		Type:       "error_on_gap",
		ErrorOnGap: &enabled,
	}
	if err := store.appendWalNoLock(record); err != nil {
		store.lock.Unlock()
		return
	}
	store.applyWalRecordLocked(record)
	store.lock.Unlock()
	_ = store.bumpMutationAndMaybeCheckpoint()
}
