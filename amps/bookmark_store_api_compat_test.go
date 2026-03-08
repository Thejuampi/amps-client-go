package amps

import (
	"errors"
	"testing"
)

type legacyBookmarkStore struct{}

func (store *legacyBookmarkStore) Log(message *Message) uint64 { return 1 }

func (store *legacyBookmarkStore) Discard(subID string, bookmarkSeqNo uint64) {}

func (store *legacyBookmarkStore) DiscardMessage(message *Message) {}

func (store *legacyBookmarkStore) GetMostRecent(subID string) string { return "" }

func (store *legacyBookmarkStore) IsDiscarded(message *Message) bool { return false }

func (store *legacyBookmarkStore) Purge(subID ...string) {}

func (store *legacyBookmarkStore) GetOldestBookmarkSeq(subID string) uint64 { return 0 }

func (store *legacyBookmarkStore) Persisted(subID string, bookmark string) string { return bookmark }

func (store *legacyBookmarkStore) SetServerVersion(version string) {}

var _ BookmarkStore = (*legacyBookmarkStore)(nil)
var _ BookmarkStoreWithErrors = (*FileBookmarkStore)(nil)

type errorCapableBookmarkStore struct {
	legacyBookmarkStore
	logErr              error
	persistedErr        error
	setServerVersionErr error
}

func (store *errorCapableBookmarkStore) LogWithError(message *Message) (uint64, error) {
	return 2, store.logErr
}

func (store *errorCapableBookmarkStore) DiscardWithError(subID string, bookmarkSeqNo uint64) error {
	return nil
}

func (store *errorCapableBookmarkStore) DiscardMessageWithError(message *Message) error {
	return nil
}

func (store *errorCapableBookmarkStore) PurgeWithError(subID ...string) error {
	return nil
}

func (store *errorCapableBookmarkStore) PersistedWithError(subID string, bookmark string) (string, error) {
	return bookmark, store.persistedErr
}

func (store *errorCapableBookmarkStore) SetServerVersionWithError(version string) error {
	return store.setServerVersionErr
}

func TestBookmarkStoreErrorHelpersPreserveLegacyCompatibility(t *testing.T) {
	message := bookmarkMessage("sub-1", "1|1|")

	legacy := &legacyBookmarkStore{}
	if seqNo, err := bookmarkStoreLog(legacy, message); err != nil || seqNo != 1 {
		t.Fatalf("expected legacy bookmark store path to preserve old behavior, seq=%d err=%v", seqNo, err)
	}
	if value, err := bookmarkStorePersisted(legacy, "sub-1", "1|1|"); err != nil || value != "1|1|" {
		t.Fatalf("expected legacy persisted path to preserve old behavior, value=%q err=%v", value, err)
	}
	if err := bookmarkStoreSetServerVersion(legacy, "6.0.0.0"); err != nil {
		t.Fatalf("expected legacy server version path to ignore errors, got %v", err)
	}

	errorStore := &errorCapableBookmarkStore{
		logErr:              errors.New("log failed"),
		persistedErr:        errors.New("persisted failed"),
		setServerVersionErr: errors.New("set server version failed"),
	}
	if _, err := bookmarkStoreLog(errorStore, message); !errors.Is(err, errorStore.logErr) {
		t.Fatalf("expected LogWithError to be used, got %v", err)
	}
	if _, err := bookmarkStorePersisted(errorStore, "sub-1", "1|1|"); !errors.Is(err, errorStore.persistedErr) {
		t.Fatalf("expected PersistedWithError to be used, got %v", err)
	}
	if err := bookmarkStoreSetServerVersion(errorStore, "6.0.0.0"); !errors.Is(err, errorStore.setServerVersionErr) {
		t.Fatalf("expected SetServerVersionWithError to be used, got %v", err)
	}
}
