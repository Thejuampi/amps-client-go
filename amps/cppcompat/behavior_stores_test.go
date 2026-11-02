package cppcompat

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Thejuampi/amps-client-go/amps"
)

type switchablePublishStore struct {
	amps.PublishStore
	fail bool
}

type savedThenFailedStore struct{ amps.PublishStore }

type uncertainPublishStore struct {
	amps.PublishStore
	sequence uint64
	err      error
	calls    int
}

func (store *uncertainPublishStore) Store(*amps.Command) (uint64, error) {
	store.calls++
	return store.sequence, store.err
}

func (store savedThenFailedStore) Store(command *amps.Command) (uint64, error) {
	var sequence, err = store.PublishStore.Store(command)
	if err != nil {
		return sequence, err
	}
	return sequence, errors.New("checkpoint failed after saving")
}

func TestHybridStorePreservesSequenceAfterPartialStoreFailure(t *testing.T) {
	var primary = amps.NewMemoryPublishStore()
	primary.SetInitialSequence(1 << 63)
	var secondary = amps.NewMemoryPublishStore()
	var hybrid = NewHybridPublishStore(savedThenFailedStore{primary}, secondary)
	var sequence, err = hybrid.Store(amps.NewCommand("publish").SetTopic("orders"))
	if err != nil {
		t.Fatal(err)
	}
	var deliveries int
	if err := hybrid.Replay(func(command *amps.Command) error {
		var replayed, _ = command.SequenceID()
		if replayed != sequence {
			t.Errorf("one publish has two sequences: %d != %d", replayed, sequence)
		}
		deliveries++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if deliveries != 1 {
		t.Fatalf("partially saved publish replayed %d times", deliveries)
	}
}

func TestHybridInitialSequenceProtectsRecoveredPublisher(t *testing.T) {
	var hybrid = NewHybridPublishStore(amps.NewMemoryPublishStore(), amps.NewMemoryPublishStore())
	if _, err := hybrid.Store(amps.NewCommand("publish").SetTopic("before-recovery")); err != nil {
		t.Fatal(err)
	}
	hybrid.SetInitialSequence(1 << 63)
	var sequence, err = hybrid.Store(amps.NewCommand("publish").SetTopic("orders"))
	if err != nil || sequence != (1<<63)+1 {
		t.Fatalf("hybrid ignored server sequence: %d %v", sequence, err)
	}
}

func TestHybridStoreRejectsFailuresAndSequenceCollisions(t *testing.T) {
	var command = amps.NewCommand("publish").SetTopic("orders")
	var failure = errors.New("injected failure")
	var cases = []struct {
		name    string
		store   *HybridPublishStore
		command *amps.Command
	}{
		{"nil-command", NewHybridPublishStore(nil, nil), nil},
		{"primary-recovery", NewHybridPublishStore(&fakePublishStore{replayErr: failure}, nil), command},
		{"secondary-recovery", NewHybridPublishStore(nil, &fakePublishStore{replayErr: failure}), command},
		{"both-stores", NewHybridPublishStore(&fakePublishStore{storeErr: failure}, &fakePublishStore{storeErr: failure}), command},
		{"exhausted", &HybridPublishStore{initialized: true, lastSequence: ^uint64(0)}, command},
		{"reused-by-backend", &HybridPublishStore{initialized: true, lastSequence: 90, primary: &fakePublishStore{}}, command},
		{"explicit-collision", &HybridPublishStore{initialized: true, lastSequence: 90}, command.Clone().SetSequenceID(90)},
	}
	for _, entry := range cases {
		t.Run(entry.name, func(t *testing.T) {
			if _, err := entry.store.Store(entry.command); err == nil {
				t.Fatal("failed store or collision was accepted")
			}
		})
	}
}

func TestHybridReplayRecoveryAndCallbackErrors(t *testing.T) {
	var backend = amps.NewMemoryPublishStore()
	if _, err := backend.Store(amps.NewCommand("publish").SetSequenceID(10)); err != nil {
		t.Fatal(err)
	}
	var secondary = amps.NewMemoryPublishStore()
	var backendFloor, _ = backend.NextPublishSequence()
	var secondaryFloor, _ = secondary.NextPublishSequence()
	var hybrid = NewHybridPublishStore(backend, secondary)
	if sequence, err := hybrid.Store(amps.NewCommand("publish")); err != nil || sequence != max(backendFloor, secondaryFloor) {
		t.Fatalf("recovery reused sequence: %d %v", sequence, err)
	}
	var callbackError = errors.New("callback failed")
	if err := hybrid.Replay(func(*amps.Command) error { return callbackError }); !errors.Is(err, callbackError) {
		t.Fatalf("callback error lost: %v", err)
	}
	if err := hybrid.Replay(nil); err != nil {
		t.Fatal(err)
	}
}

func TestHybridPreservesBackingFloorsAcrossFallback(t *testing.T) {
	var primary = &switchablePublishStore{PublishStore: amps.NewMemoryPublishStore(), fail: true}
	var secondary = amps.NewMemoryPublishStore()
	secondary.SetInitialSequence(1 << 63)
	var hybrid = NewHybridPublishStore(primary, secondary)
	hybrid.SetInitialSequence(42)
	var first, err = hybrid.Store(amps.NewCommand("publish"))
	if err != nil || first != (1<<63)+1 {
		t.Fatalf("fallback sequence = %d, error %v", first, err)
	}
	primary.fail = false
	var second, err2 = hybrid.Store(amps.NewCommand("publish"))
	if err2 != nil || second != first+1 {
		t.Fatalf("primary after fallback sequence = %d, error %v", second, err2)
	}
}

func TestHybridBackingFloorExhaustion(t *testing.T) {
	var primary = amps.NewMemoryPublishStore()
	primary.SetInitialSequence(^uint64(0))
	var hybrid = NewHybridPublishStore(primary, amps.NewMemoryPublishStore())
	if _, err := hybrid.Store(amps.NewCommand("publish")); err == nil {
		t.Fatal("exhausted backing sequence accepted")
	}
}

func TestHybridCheckpointMaintenanceDoesNotFallbackOrReuseSequence(t *testing.T) {
	for _, failPrimary := range []bool{false, true} {
		var name = "primary-accepted"
		if failPrimary {
			name = "secondary-accepted"
		}
		t.Run(name, func(t *testing.T) {
			var path = filepath.Join(t.TempDir(), "publish")
			var fileStore = amps.NewFilePublishStore(path)
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
			var memory = &switchablePublishStore{PublishStore: amps.NewMemoryPublishStore(), fail: failPrimary}
			var hybrid *HybridPublishStore
			if failPrimary {
				hybrid = NewHybridPublishStore(memory, fileStore)
			} else {
				hybrid = NewHybridPublishStore(fileStore, memory)
			}
			var first, err = hybrid.Store(amps.NewCommand("publish").SetTopic("orders"))
			var accepted *amps.PublishStoreAcceptedError
			if !errors.As(err, &accepted) || first == 0 || fileStore.UnpersistedCount() != 1 || memory.UnpersistedCount() != 0 {
				t.Fatalf("first sequence=%d error=%v file=%d memory=%d", first, err, fileStore.UnpersistedCount(), memory.UnpersistedCount())
			}
			if err = os.Remove(path); err != nil {
				t.Fatal(err)
			}
			var second, secondErr = hybrid.Store(amps.NewCommand("publish").SetTopic("orders"))
			if secondErr != nil || second != first+1 {
				t.Fatalf("next sequence=%d first=%d error=%v", second, first, secondErr)
			}
		})
	}
}

func TestHybridUnknownStoreOutcomeBlocksFallbackAndFurtherWrites(t *testing.T) {
	for _, failPrimary := range []bool{false, true} {
		var name = "primary-uncertain"
		if failPrimary {
			name = "secondary-uncertain"
		}
		t.Run(name, func(t *testing.T) {
			var failure = errors.New("injected uncertain WAL outcome")
			var outcome = &amps.PublishStoreUncertainError{Sequence: 123, Cause: failure}
			var uncertain = &uncertainPublishStore{PublishStore: amps.NewMemoryPublishStore(), sequence: 123, err: outcome}
			var memory = &switchablePublishStore{PublishStore: amps.NewMemoryPublishStore(), fail: failPrimary}
			var hybrid *HybridPublishStore
			if failPrimary {
				hybrid = NewHybridPublishStore(memory, uncertain)
			} else {
				hybrid = NewHybridPublishStore(uncertain, memory)
			}
			var first, err = hybrid.Store(amps.NewCommand("publish"))
			if first != 123 || !errors.Is(err, failure) || uncertain.calls != 1 || memory.UnpersistedCount() != 0 {
				t.Fatalf("uncertain sequence=%d error=%v calls=%d other=%d", first, err, uncertain.calls, memory.UnpersistedCount())
			}
			var _, retryErr = hybrid.Store(amps.NewCommand("publish"))
			if !errors.Is(retryErr, failure) || uncertain.calls != 1 {
				t.Fatalf("hybrid continued after uncertainty: error=%v calls=%d", retryErr, uncertain.calls)
			}
		})
	}
}

func (store *switchablePublishStore) Store(command *amps.Command) (uint64, error) {
	if store.fail {
		return 0, errors.New("injected store failure")
	}
	return store.PublishStore.Store(command)
}

func TestHybridStoreMaintainsOrderAcrossFallbackAndRecovery(t *testing.T) {
	var primary = &switchablePublishStore{PublishStore: amps.NewMemoryPublishStore()}
	var secondary = amps.NewMemoryPublishStore()
	var hybrid = NewHybridPublishStore(primary, secondary)
	var first, err = hybrid.Store(amps.NewCommand("publish").SetTopic("first").SetSequenceID(1000))
	if err != nil {
		t.Fatal(err)
	}
	primary.fail = true
	var second, secondErr = hybrid.Store(amps.NewCommand("publish").SetTopic("second"))
	primary.fail = false
	var third, thirdErr = hybrid.Store(amps.NewCommand("publish").SetTopic("third"))
	if secondErr != nil || thirdErr != nil || !(first < second && second < third) {
		t.Fatalf("sequence order changed across stores: %d %d %d, errors %v %v", first, second, third, secondErr, thirdErr)
	}
	var sequences []uint64
	if err := hybrid.Replay(func(command *amps.Command) error {
		var sequence, _ = command.SequenceID()
		sequences = append(sequences, sequence)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(sequences) != 3 || sequences[0] != first || sequences[1] != second || sequences[2] != third {
		t.Fatalf("replay order differs: %v", sequences)
	}
	if err := hybrid.DiscardUpTo(second); err != nil {
		t.Fatal(err)
	}
	if primary.UnpersistedCount() != 1 || secondary.UnpersistedCount() != 0 {
		t.Fatal("persisted ack discarded an unrelated message")
	}
}
