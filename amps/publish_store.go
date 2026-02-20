package amps

import (
	"encoding/json"
	"errors"
	"os"
	"sort"
	"sync"
	"time"
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

type MemoryPublishStore struct {
	lock              sync.Mutex
	entries           map[uint64]*Command
	lastPersisted     uint64
	nextSequence      uint64
	errorOnPublishGap bool
}

func NewMemoryPublishStore() *MemoryPublishStore {
	return &MemoryPublishStore{
		entries:       make(map[uint64]*Command),
		nextSequence:  1,
		lastPersisted: 0,
	}
}

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

func (store *MemoryPublishStore) UnpersistedCount() int {
	if store == nil {
		return 0
	}
	store.lock.Lock()
	defer store.lock.Unlock()
	return len(store.entries)
}

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

func (store *MemoryPublishStore) GetLastPersisted() uint64 {
	if store == nil {
		return 0
	}
	store.lock.Lock()
	defer store.lock.Unlock()
	return store.lastPersisted
}

func (store *MemoryPublishStore) SetErrorOnPublishGap(enabled bool) {
	if store == nil {
		return
	}
	store.lock.Lock()
	store.errorOnPublishGap = enabled
	store.lock.Unlock()
}

func (store *MemoryPublishStore) ErrorOnPublishGap() bool {
	if store == nil {
		return false
	}
	store.lock.Lock()
	defer store.lock.Unlock()
	return store.errorOnPublishGap
}

type FilePublishStore struct {
	*MemoryPublishStore
	path string
}

func NewFilePublishStore(path string) *FilePublishStore {
	fileStore := &FilePublishStore{
		MemoryPublishStore: NewMemoryPublishStore(),
		path:               path,
	}
	_ = fileStore.load()
	return fileStore
}

func (store *FilePublishStore) Store(command *Command) (uint64, error) {
	sequence, err := store.MemoryPublishStore.Store(command)
	if err != nil {
		return 0, err
	}
	if err := store.save(); err != nil {
		return sequence, err
	}
	return sequence, nil
}

func (store *FilePublishStore) DiscardUpTo(sequence uint64) error {
	if err := store.MemoryPublishStore.DiscardUpTo(sequence); err != nil {
		return err
	}
	return store.save()
}

func (store *FilePublishStore) SetErrorOnPublishGap(enabled bool) {
	store.MemoryPublishStore.SetErrorOnPublishGap(enabled)
	_ = store.save()
}

func (store *FilePublishStore) save() error {
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
	return os.WriteFile(store.path, data, 0600)
}

func (store *FilePublishStore) load() error {
	if store == nil || store.path == "" {
		return nil
	}

	data, err := os.ReadFile(store.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	state := publishStoreFileState{}
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	store.lock.Lock()
	defer store.lock.Unlock()

	store.entries = make(map[uint64]*Command)
	for _, record := range state.Records {
		store.entries[record.Sequence] = commandFromSnapshot(record.Command)
	}
	store.lastPersisted = state.LastPersisted
	if state.NextSequence == 0 {
		state.NextSequence = 1
	}
	store.nextSequence = state.NextSequence
	store.errorOnPublishGap = state.ErrorOnGap
	return nil
}
