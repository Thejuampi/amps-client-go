package amps

import (
	"encoding/json"
	"errors"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Thejuampi/amps-client-go/amps/internal/wal"
)

type bookmarkRecord struct {
	SeqNo     uint64 `json:"seq_no"`
	Count     uint64 `json:"count"`
	Discarded bool   `json:"discarded"`
}

type bookmarkWalRecord struct {
	Type          string          `json:"type"`
	SubID         string          `json:"sub_id,omitempty"`
	Bookmark      string          `json:"bookmark,omitempty"`
	Record        *bookmarkRecord `json:"record,omitempty"`
	DiscardedUpTo uint64          `json:"discarded_up_to,omitempty"`
	SubIDs        []string        `json:"sub_ids,omitempty"`
	ServerVersion string          `json:"server_version,omitempty"`
	NextSeqNo     uint64          `json:"next_seq_no,omitempty"`
}

// MemoryBookmarkStore stores replay or bookmark state for recovery-oriented workflows.
type MemoryBookmarkStore struct {
	lock          sync.RWMutex
	nextSeqNo     uint64
	records       map[string]map[string]*bookmarkRecord
	mostRecent    map[string]string
	discardedUpTo map[string]uint64
	dirty         map[string]bool
	serverVersion string
}

// NewMemoryBookmarkStore returns a new MemoryBookmarkStore.
func NewMemoryBookmarkStore() *MemoryBookmarkStore {
	return &MemoryBookmarkStore{
		nextSeqNo:     1,
		records:       make(map[string]map[string]*bookmarkRecord),
		mostRecent:    make(map[string]string),
		discardedUpTo: make(map[string]uint64),
		dirty:         make(map[string]bool),
	}
}

func bookmarkStoreKey(message *Message) (string, string, bool) {
	if message == nil {
		return "", "", false
	}

	bookmark, hasBookmark := message.Bookmark()
	if !hasBookmark || bookmark == "" {
		return "", "", false
	}

	subID, hasSubID := message.SubID()
	if !hasSubID || subID == "" {
		subID, hasSubID = message.QueryID()
	}
	if !hasSubID || subID == "" {
		subIDs, hasSubIDs := message.SubIDs()
		if hasSubIDs && subIDs != "" {
			subID = subIDs
		}
	}
	if subID == "" {
		subID = "_default"
	}

	return subID, bookmark, true
}

func (store *MemoryBookmarkStore) ensureSubID(subID string) map[string]*bookmarkRecord {
	records := store.records[subID]
	if records == nil {
		records = make(map[string]*bookmarkRecord)
		store.records[subID] = records
	}
	return records
}

func (store *MemoryBookmarkStore) computeMostRecentLocked(subID string) string {
	var records = store.records[subID]
	if records == nil {
		return ""
	}

	var publishers = make(map[uint64]uint64)
	var publisherBookmarks = make(map[uint64]string)
	var foundPublisher bool
	for bookmark := range records {
		publisher, sequence, ok := parseBookmarkToken(bookmark)
		if !ok {
			continue
		}
		foundPublisher = true
		if current, exists := publishers[publisher]; !exists || current < sequence {
			publishers[publisher] = sequence
			publisherBookmarks[publisher] = bookmark
		}
	}

	if !foundPublisher {
		return store.mostRecent[subID]
	}

	var publisherIDs = make([]uint64, 0, len(publishers))
	for publisherID := range publishers {
		publisherIDs = append(publisherIDs, publisherID)
	}
	sort.Slice(publisherIDs, func(left int, right int) bool {
		return publisherIDs[left] < publisherIDs[right]
	})

	var parts = make([]string, 0, len(publisherIDs))
	for _, publisherID := range publisherIDs {
		parts = append(parts, publisherBookmarks[publisherID])
	}
	return strings.Join(parts, ",")
}

// Log executes the exported log operation.
func (store *MemoryBookmarkStore) Log(message *Message) uint64 {
	if store == nil {
		return 0
	}

	subID, bookmark, ok := bookmarkStoreKey(message)
	if !ok {
		return 0
	}

	store.lock.Lock()
	defer store.lock.Unlock()

	records := store.ensureSubID(subID)
	record := records[bookmark]
	if record == nil {
		record = &bookmarkRecord{SeqNo: store.nextSeqNo, Count: 1}
		records[bookmark] = record
		store.nextSeqNo++
	} else {
		record.Count++
	}
	store.dirty[subID] = true
	return record.SeqNo
}

// Discard executes the exported discard operation.
func (store *MemoryBookmarkStore) Discard(subID string, bookmarkSeqNo uint64) {
	if store == nil || subID == "" {
		return
	}

	store.lock.Lock()
	defer store.lock.Unlock()

	if bookmarkSeqNo > store.discardedUpTo[subID] {
		store.discardedUpTo[subID] = bookmarkSeqNo
	}
}

// DiscardMessage executes the exported discardmessage operation.
func (store *MemoryBookmarkStore) DiscardMessage(message *Message) {
	if store == nil {
		return
	}

	subID, bookmark, ok := bookmarkStoreKey(message)
	if !ok {
		return
	}

	store.lock.Lock()
	defer store.lock.Unlock()

	records := store.ensureSubID(subID)
	record := records[bookmark]
	if record != nil {
		record.Discarded = true
		if record.SeqNo > store.discardedUpTo[subID] {
			store.discardedUpTo[subID] = record.SeqNo
		}
	}
}

// GetMostRecent returns the current most recent value.
func (store *MemoryBookmarkStore) GetMostRecent(subID string) string {
	if store == nil {
		return ""
	}
	store.lock.RLock()
	if !store.dirty[subID] {
		var result = store.mostRecent[subID]
		store.lock.RUnlock()
		return result
	}
	store.lock.RUnlock()

	store.lock.Lock()
	if store.dirty[subID] {
		store.mostRecent[subID] = store.computeMostRecentLocked(subID)
		delete(store.dirty, subID)
	}
	var result = store.mostRecent[subID]
	store.lock.Unlock()
	return result
}

// IsDiscarded reports whether discarded is true for the receiver.
func (store *MemoryBookmarkStore) IsDiscarded(message *Message) bool {
	if store == nil {
		return false
	}

	subID, bookmark, ok := bookmarkStoreKey(message)
	if !ok {
		return false
	}

	store.lock.RLock()
	records := store.records[subID]
	if records == nil {
		store.lock.RUnlock()
		return false
	}
	record := records[bookmark]
	if record == nil {
		store.lock.RUnlock()
		return false
	}
	var discarded = record.Discarded || record.SeqNo <= store.discardedUpTo[subID] || record.Count > 1
	store.lock.RUnlock()
	return discarded
}

// Purge executes the exported purge operation.
func (store *MemoryBookmarkStore) Purge(subID ...string) {
	if store == nil {
		return
	}

	store.lock.Lock()
	defer store.lock.Unlock()

	if len(subID) == 0 {
		store.records = make(map[string]map[string]*bookmarkRecord)
		store.mostRecent = make(map[string]string)
		store.discardedUpTo = make(map[string]uint64)
		store.nextSeqNo = 1
		return
	}

	for _, value := range subID {
		delete(store.records, value)
		delete(store.mostRecent, value)
		delete(store.discardedUpTo, value)
	}
}

// GetOldestBookmarkSeq returns the current oldest bookmark seq value.
func (store *MemoryBookmarkStore) GetOldestBookmarkSeq(subID string) uint64 {
	if store == nil || subID == "" {
		return 0
	}
	store.lock.RLock()
	defer store.lock.RUnlock()

	records := store.records[subID]
	if records == nil {
		return 0
	}

	oldest := uint64(0)
	discardedUpTo := store.discardedUpTo[subID]
	for _, record := range records {
		if record == nil || record.Discarded || record.SeqNo <= discardedUpTo {
			continue
		}
		if oldest == 0 || record.SeqNo < oldest {
			oldest = record.SeqNo
		}
	}
	return oldest
}

// Persisted executes the exported persisted operation.
func (store *MemoryBookmarkStore) Persisted(subID string, bookmark string) string {
	if store == nil || subID == "" || bookmark == "" {
		return ""
	}
	store.lock.Lock()
	defer store.lock.Unlock()

	records := store.ensureSubID(subID)
	record := records[bookmark]
	if record == nil {
		record = &bookmarkRecord{SeqNo: store.nextSeqNo, Count: 1, Discarded: true}
		records[bookmark] = record
		store.nextSeqNo++
	} else {
		record.Discarded = true
	}

	if record.SeqNo > store.discardedUpTo[subID] {
		store.discardedUpTo[subID] = record.SeqNo
	}
	store.mostRecent[subID] = store.computeMostRecentLocked(subID)
	return bookmark
}

// SetServerVersion sets server version on the receiver.
func (store *MemoryBookmarkStore) SetServerVersion(version string) {
	if store == nil {
		return
	}
	store.lock.Lock()
	store.serverVersion = version
	store.lock.Unlock()
}

func (store *MemoryBookmarkStore) LogWithError(message *Message) (uint64, error) {
	return store.Log(message), nil
}

func (store *MemoryBookmarkStore) DiscardWithError(subID string, bookmarkSeqNo uint64) error {
	store.Discard(subID, bookmarkSeqNo)
	return nil
}

func (store *MemoryBookmarkStore) DiscardMessageWithError(message *Message) error {
	store.DiscardMessage(message)
	return nil
}

func (store *MemoryBookmarkStore) PurgeWithError(subID ...string) error {
	store.Purge(subID...)
	return nil
}

func (store *MemoryBookmarkStore) PersistedWithError(subID string, bookmark string) (string, error) {
	return store.Persisted(subID, bookmark), nil
}

func (store *MemoryBookmarkStore) SetServerVersionWithError(version string) error {
	store.SetServerVersion(version)
	return nil
}

type bookmarkFileEntry struct {
	SubID    string         `json:"sub_id"`
	Bookmark string         `json:"bookmark"`
	Record   bookmarkRecord `json:"record"`
}

type bookmarkFileState struct {
	NextSeqNo     uint64              `json:"next_seq_no"`
	MostRecent    map[string]string   `json:"most_recent"`
	DiscardedUpTo map[string]uint64   `json:"discarded_up_to"`
	ServerVersion string              `json:"server_version"`
	Entries       []bookmarkFileEntry `json:"entries"`
}

// FileBookmarkStore stores replay or bookmark state for recovery-oriented workflows.
type FileBookmarkStore struct {
	*MemoryBookmarkStore
	path               string
	walPath            string
	options            FileStoreOptions
	opsSinceCheckpoint uint64
	loadErr            error
	sequenceExhausted  bool
}

// NewFileBookmarkStore returns a new FileBookmarkStore.
func NewFileBookmarkStore(path string) *FileBookmarkStore {
	return NewFileBookmarkStoreWithOptions(path, defaultFileStoreOptions())
}

// NewFileBookmarkStoreWithOptions returns a new FileBookmarkStore with explicit options.
func NewFileBookmarkStoreWithOptions(path string, options FileStoreOptions) *FileBookmarkStore {
	store := &FileBookmarkStore{
		MemoryBookmarkStore: NewMemoryBookmarkStore(),
		path:                path,
		walPath:             path + ".wal",
		options:             normalizeFileStoreOptions(options),
	}
	store.loadErr = store.load()
	return store
}

// LoadError returns the most recent constructor-time load error, if any.
func (store *FileBookmarkStore) LoadError() error {
	if store == nil {
		return nil
	}
	store.lock.RLock()
	defer store.lock.RUnlock()
	return store.loadErr
}

// GetMostRecent returns the current most recent value.
func (store *FileBookmarkStore) GetMostRecent(subID string) string {
	if store == nil || subID == "" {
		return ""
	}
	store.lock.RLock()
	var loadErr = store.loadErr
	store.lock.RUnlock()
	if loadErr != nil {
		return ""
	}
	return store.MemoryBookmarkStore.GetMostRecent(subID)
}

// IsDiscarded reports whether discarded is true for the receiver.
func (store *FileBookmarkStore) IsDiscarded(message *Message) bool {
	if store == nil {
		return false
	}
	store.lock.RLock()
	var loadErr = store.loadErr
	store.lock.RUnlock()
	if loadErr != nil {
		return false
	}
	return store.MemoryBookmarkStore.IsDiscarded(message)
}

// GetOldestBookmarkSeq returns the current oldest bookmark seq value.
func (store *FileBookmarkStore) GetOldestBookmarkSeq(subID string) uint64 {
	if store == nil || subID == "" {
		return 0
	}
	store.lock.RLock()
	var loadErr = store.loadErr
	store.lock.RUnlock()
	if loadErr != nil {
		return 0
	}
	return store.MemoryBookmarkStore.GetOldestBookmarkSeq(subID)
}

func (store *FileBookmarkStore) loadCheckpoint() error {
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

	state := bookmarkFileState{}
	if err = json.Unmarshal(data, &state); err != nil {
		return err
	}

	maxSeqNo := uint64(0)
	for _, entry := range state.Entries {
		if entry.Record.SeqNo > maxSeqNo {
			maxSeqNo = entry.Record.SeqNo
		}
	}
	for _, value := range state.DiscardedUpTo {
		if value > maxSeqNo {
			maxSeqNo = value
		}
	}

	store.lock.Lock()
	defer store.lock.Unlock()

	state.NextSeqNo, store.sequenceExhausted = deriveNextStoreSequence(maxSeqNo, state.NextSeqNo)

	store.nextSeqNo = state.NextSeqNo
	store.records = make(map[string]map[string]*bookmarkRecord)
	for _, entry := range state.Entries {
		records := store.records[entry.SubID]
		if records == nil {
			records = make(map[string]*bookmarkRecord)
			store.records[entry.SubID] = records
		}
		record := entry.Record
		records[entry.Bookmark] = &record
	}
	store.mostRecent = make(map[string]string)
	for key, value := range state.MostRecent {
		store.mostRecent[key] = value
	}
	store.dirty = make(map[string]bool)
	for subID := range store.records {
		store.dirty[subID] = true
	}
	store.discardedUpTo = make(map[string]uint64)
	for key, value := range state.DiscardedUpTo {
		store.discardedUpTo[key] = value
	}
	store.serverVersion = state.ServerVersion
	return nil
}

func (store *FileBookmarkStore) saveCheckpoint() error {
	if store == nil || store.path == "" {
		return nil
	}

	store.lock.Lock()
	subIDs := make([]string, 0, len(store.records))
	for subID := range store.records {
		subIDs = append(subIDs, subID)
	}
	sort.Strings(subIDs)

	entries := make([]bookmarkFileEntry, 0)
	for _, subID := range subIDs {
		bookmarks := make([]string, 0, len(store.records[subID]))
		for bookmark := range store.records[subID] {
			bookmarks = append(bookmarks, bookmark)
		}
		sort.Strings(bookmarks)
		for _, bookmark := range bookmarks {
			entries = append(entries, bookmarkFileEntry{
				SubID:    subID,
				Bookmark: bookmark,
				Record:   *store.records[subID][bookmark],
			})
		}
	}

	state := bookmarkFileState{
		NextSeqNo:     store.nextSeqNo,
		MostRecent:    map[string]string{},
		DiscardedUpTo: map[string]uint64{},
		ServerVersion: store.serverVersion,
		Entries:       entries,
	}

	for key, value := range store.mostRecent {
		state.MostRecent[key] = value
	}
	for key, value := range store.discardedUpTo {
		state.DiscardedUpTo[key] = value
	}
	if currentFileStoreTestHooks.bookmarkCheckpointBeforeCommit != nil {
		currentFileStoreTestHooks.bookmarkCheckpointBeforeCommit()
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

	atomic.StoreUint64(&store.opsSinceCheckpoint, 0)
	store.lock.Unlock()
	return nil
}

// appendWalNoLock writes a WAL record for the given bookmark store operation.
// It never acquires store.lock; callers may invoke it with or without the
// store mutex held.
func (store *FileBookmarkStore) appendWalNoLock(record bookmarkWalRecord) error {
	if store == nil || !store.options.UseWAL || store.walPath == "" {
		return nil
	}
	if currentFileStoreTestHooks.bookmarkWalBeforeAppend != nil {
		currentFileStoreTestHooks.bookmarkWalBeforeAppend()
	}
	return wal.AppendJSON(store.walPath, record, store.options.SyncOnWrite)
}

func (store *FileBookmarkStore) bumpMutationAndMaybeCheckpoint() error {
	if store == nil {
		return nil
	}

	interval := store.options.CheckpointInterval
	if interval == 0 {
		interval = defaultFileStoreOptions().CheckpointInterval
	}
	ops := atomic.AddUint64(&store.opsSinceCheckpoint, 1)
	if ops >= interval {
		return store.saveCheckpoint()
	}
	return nil
}

func (store *FileBookmarkStore) applyUpsertLocked(subID string, bookmark string, record bookmarkRecord) {
	records := store.ensureSubID(subID)
	copied := record
	records[bookmark] = &copied
	store.mostRecent[subID] = store.computeMostRecentLocked(subID)
	if copied.SeqNo > store.discardedUpTo[subID] && copied.Discarded {
		store.discardedUpTo[subID] = copied.SeqNo
	}
	if copied.SeqNo >= store.nextSeqNo {
		store.nextSeqNo, store.sequenceExhausted = advanceStoreSequence(copied.SeqNo)
	}
}

func (store *FileBookmarkStore) applyWalRecordLocked(record bookmarkWalRecord) {
	switch record.Type {
	case "upsert":
		if record.SubID == "" || record.Bookmark == "" || record.Record == nil {
			return
		}
		store.applyUpsertLocked(record.SubID, record.Bookmark, *record.Record)
	case "discard_upto":
		if record.SubID == "" {
			return
		}
		if record.DiscardedUpTo > store.discardedUpTo[record.SubID] {
			store.discardedUpTo[record.SubID] = record.DiscardedUpTo
		}
	case "purge":
		if len(record.SubIDs) == 0 {
			store.records = make(map[string]map[string]*bookmarkRecord)
			store.mostRecent = make(map[string]string)
			store.discardedUpTo = make(map[string]uint64)
			store.nextSeqNo = 1
			store.sequenceExhausted = false
			return
		}
		for _, subID := range record.SubIDs {
			delete(store.records, subID)
			delete(store.mostRecent, subID)
			delete(store.discardedUpTo, subID)
		}
	case "server_version":
		store.serverVersion = record.ServerVersion
	case "next_seq":
		if record.NextSeqNo > store.nextSeqNo {
			store.nextSeqNo = record.NextSeqNo
		}
	}
}

func (store *FileBookmarkStore) applyWalRecord(record bookmarkWalRecord) {
	store.lock.Lock()
	store.applyWalRecordLocked(record)
	store.lock.Unlock()
}

func (store *FileBookmarkStore) replayWal() error {
	if store == nil || !store.options.UseWAL || store.walPath == "" {
		return nil
	}
	return wal.ReplayNoCopy(store.walPath, func(line []byte) error {
		record := bookmarkWalRecord{}
		if err := json.Unmarshal(line, &record); err != nil {
			return err
		}
		store.applyWalRecord(record)
		return nil
	})
}

func (store *FileBookmarkStore) load() error {
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
	store.lock.Unlock()
	return nil
}

func (store *FileBookmarkStore) appendUpsertFor(subID string, bookmark string) error {
	if store == nil || subID == "" || bookmark == "" {
		return nil
	}
	store.lock.Lock()
	var recordCopy *bookmarkRecord
	if records := store.records[subID]; records != nil && records[bookmark] != nil {
		value := *records[bookmark]
		recordCopy = &value
	}
	store.lock.Unlock()
	if recordCopy == nil {
		return nil
	}
	return store.appendWalNoLock(bookmarkWalRecord{
		Type:     "upsert",
		SubID:    subID,
		Bookmark: bookmark,
		Record:   recordCopy,
	})
}

// Log executes the exported log operation.
func (store *FileBookmarkStore) Log(message *Message) uint64 {
	seqNo, _ := store.LogWithError(message)
	return seqNo
}

func (store *FileBookmarkStore) LogWithError(message *Message) (uint64, error) {
	if store == nil {
		return 0, nil
	}
	subID, bookmark, ok := bookmarkStoreKey(message)
	if !ok {
		return 0, nil
	}

	store.lock.Lock()
	if store.loadErr != nil {
		err := store.loadErr
		store.lock.Unlock()
		return 0, err
	}

	record := bookmarkRecord{}
	if records := store.records[subID]; records != nil && records[bookmark] != nil {
		record = *records[bookmark]
		record.Count++
	} else {
		if store.sequenceExhausted {
			store.lock.Unlock()
			return 0, NewError(CommandError, "bookmark sequence space exhausted")
		}
		record = bookmarkRecord{SeqNo: store.nextSeqNo, Count: 1}
	}

	walRecord := bookmarkWalRecord{
		Type:     "upsert",
		SubID:    subID,
		Bookmark: bookmark,
		Record:   &record,
	}
	if err := store.appendWalNoLock(walRecord); err != nil {
		store.lock.Unlock()
		return 0, err
	}
	store.applyWalRecordLocked(walRecord)
	store.lock.Unlock()
	if err := store.bumpMutationAndMaybeCheckpoint(); err != nil {
		return record.SeqNo, err
	}
	return record.SeqNo, nil
}

// Discard executes the exported discard operation.
func (store *FileBookmarkStore) Discard(subID string, bookmarkSeqNo uint64) {
	_ = store.DiscardWithError(subID, bookmarkSeqNo)
}

func (store *FileBookmarkStore) DiscardWithError(subID string, bookmarkSeqNo uint64) error {
	if store == nil || subID == "" {
		return nil
	}

	store.lock.Lock()
	if store.loadErr != nil {
		err := store.loadErr
		store.lock.Unlock()
		return err
	}
	walRecord := bookmarkWalRecord{
		Type:          "discard_upto",
		SubID:         subID,
		DiscardedUpTo: bookmarkSeqNo,
	}
	if err := store.appendWalNoLock(walRecord); err != nil {
		store.lock.Unlock()
		return err
	}
	store.applyWalRecordLocked(walRecord)
	store.lock.Unlock()
	return store.bumpMutationAndMaybeCheckpoint()
}

// DiscardMessage executes the exported discardmessage operation.
func (store *FileBookmarkStore) DiscardMessage(message *Message) {
	_ = store.DiscardMessageWithError(message)
}

func (store *FileBookmarkStore) DiscardMessageWithError(message *Message) error {
	if store == nil {
		return nil
	}
	subID, bookmark, ok := bookmarkStoreKey(message)
	if !ok {
		return nil
	}

	store.lock.Lock()
	if store.loadErr != nil {
		err := store.loadErr
		store.lock.Unlock()
		return err
	}
	records := store.records[subID]
	if records == nil || records[bookmark] == nil {
		store.lock.Unlock()
		return nil
	}
	record := *records[bookmark]
	record.Discarded = true
	walRecord := bookmarkWalRecord{
		Type:     "upsert",
		SubID:    subID,
		Bookmark: bookmark,
		Record:   &record,
	}
	if err := store.appendWalNoLock(walRecord); err != nil {
		store.lock.Unlock()
		return err
	}
	store.applyWalRecordLocked(walRecord)
	store.lock.Unlock()
	return store.bumpMutationAndMaybeCheckpoint()
}

// Purge executes the exported purge operation.
func (store *FileBookmarkStore) Purge(subID ...string) {
	_ = store.PurgeWithError(subID...)
}

func (store *FileBookmarkStore) PurgeWithError(subID ...string) error {
	if store == nil {
		return nil
	}
	copied := append([]string(nil), subID...)
	store.lock.Lock()
	if store.loadErr != nil {
		err := store.loadErr
		store.lock.Unlock()
		return err
	}
	walRecord := bookmarkWalRecord{
		Type:   "purge",
		SubIDs: copied,
	}
	if err := store.appendWalNoLock(walRecord); err != nil {
		store.lock.Unlock()
		return err
	}
	store.applyWalRecordLocked(walRecord)
	store.lock.Unlock()
	return store.bumpMutationAndMaybeCheckpoint()
}

// Persisted executes the exported persisted operation.
func (store *FileBookmarkStore) Persisted(subID string, bookmark string) string {
	value, _ := store.PersistedWithError(subID, bookmark)
	return value
}

func (store *FileBookmarkStore) PersistedWithError(subID string, bookmark string) (string, error) {
	if store == nil || subID == "" || bookmark == "" {
		return "", nil
	}

	store.lock.Lock()
	if store.loadErr != nil {
		err := store.loadErr
		store.lock.Unlock()
		return "", err
	}
	record := bookmarkRecord{SeqNo: store.nextSeqNo, Count: 1, Discarded: true}
	if records := store.records[subID]; records != nil && records[bookmark] != nil {
		record = *records[bookmark]
		record.Discarded = true
	} else if store.sequenceExhausted {
		store.lock.Unlock()
		return bookmark, NewError(CommandError, "bookmark sequence space exhausted")
	}
	walRecord := bookmarkWalRecord{
		Type:     "upsert",
		SubID:    subID,
		Bookmark: bookmark,
		Record:   &record,
	}
	if err := store.appendWalNoLock(walRecord); err != nil {
		store.lock.Unlock()
		return bookmark, err
	}
	store.applyWalRecordLocked(walRecord)
	store.lock.Unlock()
	if err := store.bumpMutationAndMaybeCheckpoint(); err != nil {
		return bookmark, err
	}
	return bookmark, nil
}

// SetServerVersion sets server version on the receiver.
func (store *FileBookmarkStore) SetServerVersion(version string) {
	_ = store.SetServerVersionWithError(version)
}

func (store *FileBookmarkStore) SetServerVersionWithError(version string) error {
	if store == nil {
		return nil
	}

	store.lock.Lock()
	if store.loadErr != nil {
		err := store.loadErr
		store.lock.Unlock()
		return err
	}
	walRecord := bookmarkWalRecord{
		Type:          "server_version",
		ServerVersion: version,
	}
	if err := store.appendWalNoLock(walRecord); err != nil {
		store.lock.Unlock()
		return err
	}
	store.applyWalRecordLocked(walRecord)
	store.lock.Unlock()
	return store.bumpMutationAndMaybeCheckpoint()
}

// MMapBookmarkStore stores replay or bookmark state for recovery-oriented workflows.
type MMapBookmarkStore struct {
	*FileBookmarkStore
}

// NewMMapBookmarkStore returns a new MMapBookmarkStore.
func NewMMapBookmarkStore(path string) *MMapBookmarkStore {
	options := defaultFileStoreOptions()
	options.MMap.Enabled = true
	return &MMapBookmarkStore{FileBookmarkStore: NewFileBookmarkStoreWithOptions(path, options)}
}

// RingBookmarkStore stores replay or bookmark state for recovery-oriented workflows.
type RingBookmarkStore struct {
	*MemoryBookmarkStore
}

// NewRingBookmarkStore returns a new RingBookmarkStore.
func NewRingBookmarkStore() *RingBookmarkStore {
	return &RingBookmarkStore{MemoryBookmarkStore: NewMemoryBookmarkStore()}
}
