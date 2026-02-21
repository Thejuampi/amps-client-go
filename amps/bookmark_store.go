package amps

import (
	"encoding/json"
	"errors"
	"os"
	"sort"
	"sync"
)

type bookmarkRecord struct {
	SeqNo     uint64 `json:"seq_no"`
	Count     uint64 `json:"count"`
	Discarded bool   `json:"discarded"`
}

// MemoryBookmarkStore stores replay or bookmark state for recovery-oriented workflows.
type MemoryBookmarkStore struct {
	lock          sync.Mutex
	nextSeqNo     uint64
	records       map[string]map[string]*bookmarkRecord
	mostRecent    map[string]string
	discardedUpTo map[string]uint64
	serverVersion string
}

// NewMemoryBookmarkStore returns a new MemoryBookmarkStore.
func NewMemoryBookmarkStore() *MemoryBookmarkStore {
	return &MemoryBookmarkStore{
		nextSeqNo:     1,
		records:       make(map[string]map[string]*bookmarkRecord),
		mostRecent:    make(map[string]string),
		discardedUpTo: make(map[string]uint64),
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
	store.mostRecent[subID] = bookmark
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
	store.lock.Lock()
	defer store.lock.Unlock()
	return store.mostRecent[subID]
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

	store.lock.Lock()
	defer store.lock.Unlock()

	records := store.records[subID]
	if records == nil {
		return false
	}

	record := records[bookmark]
	if record == nil {
		return false
	}

	if record.Discarded {
		return true
	}
	if record.SeqNo <= store.discardedUpTo[subID] {
		return true
	}
	return record.Count > 1
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
	store.lock.Lock()
	defer store.lock.Unlock()

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
	store.mostRecent[subID] = bookmark
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
	path string
}

// NewFileBookmarkStore returns a new FileBookmarkStore.
func NewFileBookmarkStore(path string) *FileBookmarkStore {
	store := &FileBookmarkStore{
		MemoryBookmarkStore: NewMemoryBookmarkStore(),
		path:                path,
	}
	_ = store.load()
	return store
}

func (store *FileBookmarkStore) save() error {
	if store == nil || store.path == "" {
		return nil
	}

	store.lock.Lock()
	defer store.lock.Unlock()

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

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(store.path, data, 0600)
}

func (store *FileBookmarkStore) load() error {
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

	state := bookmarkFileState{}
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	store.lock.Lock()
	defer store.lock.Unlock()

	if state.NextSeqNo == 0 {
		state.NextSeqNo = 1
	}

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
	store.discardedUpTo = make(map[string]uint64)
	for key, value := range state.DiscardedUpTo {
		store.discardedUpTo[key] = value
	}
	store.serverVersion = state.ServerVersion

	return nil
}

// Log executes the exported log operation.
func (store *FileBookmarkStore) Log(message *Message) uint64 {
	seqNo := store.MemoryBookmarkStore.Log(message)
	_ = store.save()
	return seqNo
}

// Discard executes the exported discard operation.
func (store *FileBookmarkStore) Discard(subID string, bookmarkSeqNo uint64) {
	store.MemoryBookmarkStore.Discard(subID, bookmarkSeqNo)
	_ = store.save()
}

// DiscardMessage executes the exported discardmessage operation.
func (store *FileBookmarkStore) DiscardMessage(message *Message) {
	store.MemoryBookmarkStore.DiscardMessage(message)
	_ = store.save()
}

// Purge executes the exported purge operation.
func (store *FileBookmarkStore) Purge(subID ...string) {
	store.MemoryBookmarkStore.Purge(subID...)
	_ = store.save()
}

// Persisted executes the exported persisted operation.
func (store *FileBookmarkStore) Persisted(subID string, bookmark string) string {
	value := store.MemoryBookmarkStore.Persisted(subID, bookmark)
	_ = store.save()
	return value
}

// SetServerVersion sets server version on the receiver.
func (store *FileBookmarkStore) SetServerVersion(version string) {
	store.MemoryBookmarkStore.SetServerVersion(version)
	_ = store.save()
}

// MMapBookmarkStore stores replay or bookmark state for recovery-oriented workflows.
type MMapBookmarkStore struct {
	*FileBookmarkStore
}

// NewMMapBookmarkStore returns a new MMapBookmarkStore.
func NewMMapBookmarkStore(path string) *MMapBookmarkStore {
	return &MMapBookmarkStore{FileBookmarkStore: NewFileBookmarkStore(path)}
}

// RingBookmarkStore stores replay or bookmark state for recovery-oriented workflows.
type RingBookmarkStore struct {
	*MemoryBookmarkStore
}

// NewRingBookmarkStore returns a new RingBookmarkStore.
func NewRingBookmarkStore() *RingBookmarkStore {
	return &RingBookmarkStore{MemoryBookmarkStore: NewMemoryBookmarkStore()}
}
