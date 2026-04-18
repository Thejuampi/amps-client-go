package cppcompat

import (
	"bytes"
	"sync"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

// Buffer is a mutable byte buffer abstraction.
type Buffer interface {
	Bytes() []byte
	Write([]byte) (int, error)
	Reset()
	Len() int
}

// MemoryStoreBuffer stores bytes in memory.
type MemoryStoreBuffer struct {
	buffer bytes.Buffer
}

// NewMemoryStoreBuffer creates a memory-backed buffer.
func NewMemoryStoreBuffer() *MemoryStoreBuffer {
	return &MemoryStoreBuffer{}
}

// Bytes returns buffered bytes.
func (buffer *MemoryStoreBuffer) Bytes() []byte {
	if buffer == nil {
		return nil
	}
	copied := make([]byte, buffer.buffer.Len())
	copy(copied, buffer.buffer.Bytes())
	return copied
}

// Write appends data to buffer.
func (buffer *MemoryStoreBuffer) Write(data []byte) (int, error) {
	if buffer == nil {
		return 0, nil
	}
	return buffer.buffer.Write(data)
}

// Reset clears buffered bytes.
func (buffer *MemoryStoreBuffer) Reset() {
	if buffer == nil {
		return
	}
	buffer.buffer.Reset()
}

// Len returns current buffer length.
func (buffer *MemoryStoreBuffer) Len() int {
	if buffer == nil {
		return 0
	}
	return buffer.buffer.Len()
}

// MMapStoreBuffer is a compatibility alias for memory-backed buffering.
type MMapStoreBuffer struct {
	*MemoryStoreBuffer
}

// NewMMapStoreBuffer creates an mmap-compat buffer wrapper.
func NewMMapStoreBuffer() *MMapStoreBuffer {
	return &MMapStoreBuffer{MemoryStoreBuffer: NewMemoryStoreBuffer()}
}

// BlockStore stores byte blocks by sequence id.
type BlockStore struct {
	lock   sync.Mutex
	blocks map[uint64][]byte
}

// NewBlockStore creates a new block store.
func NewBlockStore() *BlockStore {
	return &BlockStore{blocks: make(map[uint64][]byte)}
}

// Put stores a block for sequence.
func (store *BlockStore) Put(sequence uint64, block []byte) {
	if store == nil {
		return
	}
	store.lock.Lock()
	store.blocks[sequence] = append([]byte(nil), block...)
	store.lock.Unlock()
}

// Get returns a block for sequence.
func (store *BlockStore) Get(sequence uint64) ([]byte, bool) {
	if store == nil {
		return nil, false
	}
	store.lock.Lock()
	defer store.lock.Unlock()
	block, ok := store.blocks[sequence]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), block...), true
}

// DeleteUpTo removes blocks up to sequence inclusive.
func (store *BlockStore) DeleteUpTo(sequence uint64) {
	if store == nil {
		return
	}
	store.lock.Lock()
	for key := range store.blocks {
		if key <= sequence {
			delete(store.blocks, key)
		}
	}
	store.lock.Unlock()
}

// StoreReplayer replays commands from durable storage.
type StoreReplayer interface {
	Replay(command *amps.Command) error
}

// Store adapts publish-store behavior.
type Store interface {
	Store(command *amps.Command) (uint64, error)
	DiscardUpTo(sequence uint64) error
	Replay(replayer func(*amps.Command) error) error
}

// BlockPublishStore provides a publish-store adapter around the Go publish store.
type BlockPublishStore struct {
	store amps.PublishStore
}

// NewBlockPublishStore creates a block publish-store adapter.
func NewBlockPublishStore(store amps.PublishStore) *BlockPublishStore {
	if store == nil {
		store = amps.NewMemoryPublishStore()
	}
	return &BlockPublishStore{store: store}
}

// Store stores a command and returns sequence id.
func (store *BlockPublishStore) Store(command *amps.Command) (uint64, error) {
	if store == nil || store.store == nil {
		return 0, nil
	}
	return store.store.Store(command)
}

// DiscardUpTo discards commands up to sequence.
func (store *BlockPublishStore) DiscardUpTo(sequence uint64) error {
	if store == nil || store.store == nil {
		return nil
	}
	return store.store.DiscardUpTo(sequence)
}

// Replay replays unpersisted commands.
func (store *BlockPublishStore) Replay(replayer func(*amps.Command) error) error {
	if store == nil || store.store == nil {
		return nil
	}
	return store.store.Replay(replayer)
}

// HybridPublishStore writes to primary and fallback publish stores.
type HybridPublishStore struct {
	primary   amps.PublishStore
	secondary amps.PublishStore
}

// NewHybridPublishStore creates a hybrid publish store.
func NewHybridPublishStore(primary amps.PublishStore, secondary amps.PublishStore) *HybridPublishStore {
	if primary == nil {
		primary = amps.NewMemoryPublishStore()
	}
	if secondary == nil {
		secondary = amps.NewMemoryPublishStore()
	}
	return &HybridPublishStore{primary: primary, secondary: secondary}
}

// Store stores command in primary, then fallback on error.
func (store *HybridPublishStore) Store(command *amps.Command) (uint64, error) {
	if store == nil {
		return 0, nil
	}
	sequence, err := store.primary.Store(command)
	if err == nil {
		return sequence, nil
	}
	return store.secondary.Store(command)
}

// DiscardUpTo discards in both stores.
func (store *HybridPublishStore) DiscardUpTo(sequence uint64) error {
	if store == nil {
		return nil
	}
	if err := store.primary.DiscardUpTo(sequence); err != nil {
		return err
	}
	return store.secondary.DiscardUpTo(sequence)
}

// Replay replays from primary then secondary.
func (store *HybridPublishStore) Replay(replayer func(*amps.Command) error) error {
	if store == nil {
		return nil
	}
	if err := store.primary.Replay(replayer); err != nil {
		return err
	}
	return store.secondary.Replay(replayer)
}

// LoggedBookmarkStore wraps a bookmark store with access logging.
type LoggedBookmarkStore struct {
	store amps.BookmarkStore
	lock  sync.Mutex
	logs  []string
}

// NewLoggedBookmarkStore creates a logged bookmark store.
func NewLoggedBookmarkStore(store amps.BookmarkStore) *LoggedBookmarkStore {
	if store == nil {
		store = amps.NewMemoryBookmarkStore()
	}
	return &LoggedBookmarkStore{store: store, logs: []string{}}
}

func (store *LoggedBookmarkStore) appendLog(event string) {
	store.lock.Lock()
	store.logs = append(store.logs, event)
	store.lock.Unlock()
}

// Logs returns store operation logs.
func (store *LoggedBookmarkStore) Logs() []string {
	store.lock.Lock()
	defer store.lock.Unlock()
	return append([]string(nil), store.logs...)
}

// Log stores bookmark and logs the operation.
func (store *LoggedBookmarkStore) Log(message *amps.Message) uint64 {
	store.appendLog("log")
	return store.store.Log(message)
}

// Discard marks bookmark sequence discarded.
func (store *LoggedBookmarkStore) Discard(subID string, bookmarkSeqNo uint64) {
	store.appendLog("discard")
	store.store.Discard(subID, bookmarkSeqNo)
}

// DiscardMessage discards message bookmark.
func (store *LoggedBookmarkStore) DiscardMessage(message *amps.Message) {
	store.appendLog("discard_message")
	store.store.DiscardMessage(message)
}

// GetMostRecent returns most recent bookmark for sub id.
func (store *LoggedBookmarkStore) GetMostRecent(subID string) string {
	store.appendLog("get_most_recent")
	return store.store.GetMostRecent(subID)
}

// IsDiscarded reports whether message bookmark was discarded.
func (store *LoggedBookmarkStore) IsDiscarded(message *amps.Message) bool {
	store.appendLog("is_discarded")
	return store.store.IsDiscarded(message)
}

// Purge clears state for subscriptions.
func (store *LoggedBookmarkStore) Purge(subID ...string) {
	store.appendLog("purge")
	store.store.Purge(subID...)
}

// GetOldestBookmarkSeq returns oldest sequence for sub id.
func (store *LoggedBookmarkStore) GetOldestBookmarkSeq(subID string) uint64 {
	store.appendLog("get_oldest")
	return store.store.GetOldestBookmarkSeq(subID)
}

// Persisted marks bookmark persisted.
func (store *LoggedBookmarkStore) Persisted(subID string, bookmark string) string {
	store.appendLog("persisted")
	return store.store.Persisted(subID, bookmark)
}

// SetServerVersion sets server version metadata.
func (store *LoggedBookmarkStore) SetServerVersion(version string) {
	store.appendLog("set_server_version")
	store.store.SetServerVersion(version)
}

// LogWithError stores bookmark and returns mutation errors when supported.
func (store *LoggedBookmarkStore) LogWithError(message *amps.Message) (uint64, error) {
	store.appendLog("log")
	if errorStore, ok := store.store.(amps.BookmarkStoreWithErrors); ok {
		return errorStore.LogWithError(message)
	}
	return store.store.Log(message), nil
}

// DiscardWithError marks bookmark sequence discarded and returns mutation errors when supported.
func (store *LoggedBookmarkStore) DiscardWithError(subID string, bookmarkSeqNo uint64) error {
	store.appendLog("discard")
	if errorStore, ok := store.store.(amps.BookmarkStoreWithErrors); ok {
		return errorStore.DiscardWithError(subID, bookmarkSeqNo)
	}
	store.store.Discard(subID, bookmarkSeqNo)
	return nil
}

// DiscardMessageWithError discards message bookmark and returns mutation errors when supported.
func (store *LoggedBookmarkStore) DiscardMessageWithError(message *amps.Message) error {
	store.appendLog("discard_message")
	if errorStore, ok := store.store.(amps.BookmarkStoreWithErrors); ok {
		return errorStore.DiscardMessageWithError(message)
	}
	store.store.DiscardMessage(message)
	return nil
}

// PurgeWithError clears state for subscriptions and returns mutation errors when supported.
func (store *LoggedBookmarkStore) PurgeWithError(subID ...string) error {
	store.appendLog("purge")
	if errorStore, ok := store.store.(amps.BookmarkStoreWithErrors); ok {
		return errorStore.PurgeWithError(subID...)
	}
	store.store.Purge(subID...)
	return nil
}

// PersistedWithError marks bookmark persisted and returns mutation errors when supported.
func (store *LoggedBookmarkStore) PersistedWithError(subID string, bookmark string) (string, error) {
	store.appendLog("persisted")
	if errorStore, ok := store.store.(amps.BookmarkStoreWithErrors); ok {
		return errorStore.PersistedWithError(subID, bookmark)
	}
	return store.store.Persisted(subID, bookmark), nil
}

// SetServerVersionWithError sets server version metadata and returns mutation errors when supported.
func (store *LoggedBookmarkStore) SetServerVersionWithError(version string) error {
	store.appendLog("set_server_version")
	if errorStore, ok := store.store.(amps.BookmarkStoreWithErrors); ok {
		return errorStore.SetServerVersionWithError(version)
	}
	store.store.SetServerVersion(version)
	return nil
}

// MemorySubscriptionManager adapts default Go subscription manager with C++ naming.
type MemorySubscriptionManager struct {
	inner *amps.DefaultSubscriptionManager
}

// NewMemorySubscriptionManager creates a memory subscription manager.
func NewMemorySubscriptionManager() *MemorySubscriptionManager {
	return &MemorySubscriptionManager{inner: amps.NewDefaultSubscriptionManager()}
}

// SetDefaultResubscriptionTimeout sets the default timeout in milliseconds for new memory subscription managers.
func SetDefaultResubscriptionTimeout(timeout int) int {
	return amps.SetDefaultResubscriptionTimeout(timeout)
}

// GetDefaultResubscriptionTimeout returns the default timeout in milliseconds for new memory subscription managers.
func GetDefaultResubscriptionTimeout() int {
	return amps.GetDefaultResubscriptionTimeout()
}

// Subscribe tracks a subscription command.
func (manager *MemorySubscriptionManager) Subscribe(messageHandler func(*amps.Message) error, command *amps.Command, requestedAckTypes int) {
	if manager == nil || manager.inner == nil {
		return
	}
	manager.inner.Subscribe(messageHandler, command, requestedAckTypes)
}

// Unsubscribe removes a tracked subscription.
func (manager *MemorySubscriptionManager) Unsubscribe(subID string) {
	if manager == nil || manager.inner == nil {
		return
	}
	manager.inner.Unsubscribe(subID)
}

// Clear removes all tracked subscriptions.
func (manager *MemorySubscriptionManager) Clear() {
	if manager == nil || manager.inner == nil {
		return
	}
	manager.inner.Clear()
}

// Resubscribe re-establishes tracked subscriptions on client.
func (manager *MemorySubscriptionManager) Resubscribe(client *amps.Client) error {
	if manager == nil || manager.inner == nil {
		return nil
	}
	return manager.inner.Resubscribe(client)
}

// SetFailedResubscribeHandler sets failed resubscribe callback.
func (manager *MemorySubscriptionManager) SetFailedResubscribeHandler(handler amps.FailedResubscribeHandler) {
	if manager == nil || manager.inner == nil {
		return
	}
	manager.inner.SetFailedResubscribeHandler(handler)
}

// SetResubscriptionTimeout sets the timeout used for replayed resubscribe commands in milliseconds.
func (manager *MemorySubscriptionManager) SetResubscriptionTimeout(timeout int) {
	if manager == nil || manager.inner == nil {
		return
	}
	manager.inner.SetResubscriptionTimeout(timeout)
}

// GetResubscriptionTimeout returns the timeout used for replayed resubscribe commands in milliseconds.
func (manager *MemorySubscriptionManager) GetResubscriptionTimeout() int {
	if manager == nil || manager.inner == nil {
		return 0
	}
	return manager.inner.GetResubscriptionTimeout()
}

// Flush waits for primary store to flush when supported.
func (store *HybridPublishStore) Flush(timeout time.Duration) error {
	if store == nil {
		return nil
	}
	deadline := time.Time{}
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}

	flushStore := func(target amps.PublishStore) error {
		flushable, ok := target.(interface{ Flush(time.Duration) error })
		if !ok {
			return nil
		}
		if deadline.IsZero() {
			return flushable.Flush(timeout)
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return amps.NewError(amps.TimedOutError, "publish store flush timed out")
		}
		return flushable.Flush(remaining)
	}

	var firstErr error
	if err := flushStore(store.primary); err != nil {
		firstErr = err
	}
	if err := flushStore(store.secondary); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (store *HybridPublishStore) ReplaySingle(replayer func(*amps.Command) error, sequence uint64) (bool, error) {
	if store == nil {
		return false, nil
	}
	if found, err := store.primary.ReplaySingle(replayer, sequence); found || err != nil {
		return found, err
	}
	return store.secondary.ReplaySingle(replayer, sequence)
}

func (store *HybridPublishStore) UnpersistedCount() int {
	if store == nil {
		return 0
	}
	return store.primary.UnpersistedCount() + store.secondary.UnpersistedCount()
}

func (store *HybridPublishStore) GetLowestUnpersisted() uint64 {
	if store == nil {
		return 0
	}
	primary := store.primary.GetLowestUnpersisted()
	secondary := store.secondary.GetLowestUnpersisted()
	if primary == 0 {
		return secondary
	}
	if secondary == 0 {
		return primary
	}
	if primary < secondary {
		return primary
	}
	return secondary
}

func (store *HybridPublishStore) GetLastPersisted() uint64 {
	if store == nil {
		return 0
	}
	primary := store.primary.GetLastPersisted()
	secondary := store.secondary.GetLastPersisted()
	if primary > secondary {
		return primary
	}
	return secondary
}

func (store *HybridPublishStore) SetErrorOnPublishGap(enabled bool) {
	if store == nil {
		return
	}
	store.primary.SetErrorOnPublishGap(enabled)
	store.secondary.SetErrorOnPublishGap(enabled)
}

func (store *HybridPublishStore) ErrorOnPublishGap() bool {
	if store == nil {
		return false
	}
	return store.primary.ErrorOnPublishGap()
}

func (store *HybridPublishStore) SetInitialSequence(sequence uint64) {
	if store == nil {
		return
	}
	if primary, ok := store.primary.(interface{ SetInitialSequence(uint64) }); ok {
		primary.SetInitialSequence(sequence)
	}
	if secondary, ok := store.secondary.(interface{ SetInitialSequence(uint64) }); ok {
		secondary.SetInitialSequence(sequence)
	}
}
