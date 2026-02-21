package cppcompat

import (
	"errors"
	"hash/crc32"
	"testing"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

type fakePublishStore struct {
	nextSequence uint64
	storeErr     error
	discardErr   error
	replayErr    error
	replayData   []*amps.Command
	flushErr     error
}

func (store *fakePublishStore) Store(command *amps.Command) (uint64, error) {
	if store.storeErr != nil {
		return 0, store.storeErr
	}
	store.nextSequence++
	return store.nextSequence, nil
}

func (store *fakePublishStore) DiscardUpTo(sequence uint64) error {
	_ = sequence
	return store.discardErr
}

func (store *fakePublishStore) Replay(replayer func(*amps.Command) error) error {
	if store.replayErr != nil {
		return store.replayErr
	}
	if replayer == nil {
		return nil
	}
	for _, command := range store.replayData {
		if err := replayer(command); err != nil {
			return err
		}
	}
	return nil
}

func (store *fakePublishStore) ReplaySingle(replayer func(*amps.Command) error, sequence uint64) (bool, error) {
	_ = sequence
	if len(store.replayData) == 0 || replayer == nil {
		return false, nil
	}
	return true, replayer(store.replayData[0])
}

func (store *fakePublishStore) UnpersistedCount() int { return len(store.replayData) }
func (store *fakePublishStore) Flush(timeout time.Duration) error {
	_ = timeout
	return store.flushErr
}
func (store *fakePublishStore) GetLowestUnpersisted() uint64 { return 0 }
func (store *fakePublishStore) GetLastPersisted() uint64     { return 0 }
func (store *fakePublishStore) SetErrorOnPublishGap(enabled bool) {
	_ = enabled
}
func (store *fakePublishStore) ErrorOnPublishGap() bool { return false }

type testRecoveryPoint struct {
	values []string
	index  int
}

func (point *testRecoveryPoint) Next() string {
	if point == nil || point.index >= len(point.values) {
		return ""
	}
	value := point.values[point.index]
	point.index++
	return value
}

func (point *testRecoveryPoint) Reset() {
	if point == nil {
		return
	}
	point.index = 0
}

func TestTypesCoverage(t *testing.T) {
	if parsed := ParseVersionInfo("5.3.5.1"); parsed.Hotfix != 1 {
		t.Fatalf("unexpected parse version info: %+v", parsed)
	}

	uri := ParseURI("tcp://user:pass@localhost:9007/amps/json")
	if uri.Transport != "tcp" || uri.User != "user" || uri.Password != "pass" || uri.Host != "localhost" || uri.Port != "9007" || uri.Protocol != "json" {
		t.Fatalf("unexpected URI parse: %+v", uri)
	}
	invalidURI := ParseURI(":// bad")
	if invalidURI.Raw == "" {
		t.Fatalf("expected raw to be preserved for invalid URI")
	}

	var reason Reason
	if reason.Duplicate() == "" || reason.BadFilter() == "" || reason.BadRegexTopic() == "" || reason.SubscriptionAlreadyExists() == "" || reason.NameInUse() == "" || reason.AuthFailure() == "" || reason.NotEntitled() == "" || reason.AuthDisabled() == "" || reason.SubIDInUse() == "" || reason.NoTopic() == "" {
		t.Fatalf("expected reason constants")
	}

	field := NewField([]byte(" 123 "))
	if field.Empty() {
		t.Fatalf("field should not be empty")
	}
	if !field.Contains("23") || field.Uint64() != 123 {
		t.Fatalf("unexpected field contents")
	}
	copied := field.Bytes()
	copied[0] = 'X'
	if field.String()[0] == 'X' {
		t.Fatalf("field bytes should be copied")
	}
	if field.Len() != len(" 123 ") {
		t.Fatalf("unexpected field length")
	}
	if NewField(nil).Len() != 0 || !NewField(nil).Empty() {
		t.Fatalf("expected empty nil field")
	}
	if NewFieldString("abc").String() != "abc" {
		t.Fatalf("unexpected NewFieldString")
	}
	if NewFieldString("abc").Uint64() != 0 {
		t.Fatalf("invalid uint parse should return zero")
	}

	var crc CRC
	data := []byte("crc-data")
	if got := crc.Compute(data); got != crc32.ChecksumIEEE(data) {
		t.Fatalf("unexpected CRC: %d", got)
	}

	_ = BookmarkRange{Begin: "1", End: "2"}
}

func TestFIXCoverage(t *testing.T) {
	var nilFix *FIX
	if nilFix.Parse("8=FIX.4.4\x01") != nil {
		t.Fatalf("nil Parse should return nil")
	}
	if nilFix.Set(1, "x") != nil {
		t.Fatalf("nil Set should return nil")
	}
	if value, ok := nilFix.Get(1); ok || value != "" {
		t.Fatalf("nil Get should miss")
	}
	if nilFix.Data() != "" || nilFix.Size() != 0 {
		t.Fatalf("nil Data/Size should be zero values")
	}
	if _, _, ok := nilFix.Begin().Next(); ok {
		t.Fatalf("nil begin iterator should be exhausted")
	}
	if _, _, ok := nilFix.End().Next(); ok {
		t.Fatalf("nil end iterator should be exhausted")
	}

	fix := NewFIX()
	fix.Parse("8=FIX.4.4|35=A|invalid|badtag=|49=SENDER|", '|')
	if size := fix.Size(); size != 3 {
		t.Fatalf("unexpected parsed size: %d", size)
	}
	fix.Set(49, "BUYER").Set(56, "TARGET")
	if value, ok := fix.Get(49); !ok || value != "BUYER" {
		t.Fatalf("unexpected updated tag 49: %q", value)
	}
	if value, ok := fix.Get(56); !ok || value != "TARGET" {
		t.Fatalf("unexpected inserted tag 56: %q", value)
	}
	if value, ok := fix.Get(999); ok || value != "" {
		t.Fatalf("unexpected unknown tag result")
	}

	data := fix.Data('|')
	if data == "" || data[len(data)-1] != '|' {
		t.Fatalf("expected serialized FIX with separator")
	}

	begin := fix.Begin()
	seen := 0
	for {
		_, _, ok := begin.Next()
		if !ok {
			break
		}
		seen++
	}
	if seen != fix.Size() {
		t.Fatalf("iterator mismatch: seen=%d size=%d", seen, fix.Size())
	}
	if _, _, ok := fix.End().Next(); ok {
		t.Fatalf("end iterator should be exhausted")
	}
}

func TestRecoveryCoverage(t *testing.T) {
	fixed := NewFixedRecoveryPoint("1|2|")
	if fixed.Next() != "1|2|" {
		t.Fatalf("unexpected fixed recovery point")
	}
	var fixedInterface RecoveryPoint = fixed
	fixedInterface.Reset()
	var nilFixed *FixedRecoveryPoint
	if nilFixed.Next() != "" {
		t.Fatalf("nil fixed point should return empty")
	}
	nilFixed.Reset()

	dynamicCalls := 0
	dynamic := NewDynamicRecoveryPoint(func() string {
		dynamicCalls++
		if dynamicCalls == 1 {
			return "bm-1"
		}
		return ""
	})
	if dynamic.Next() != "bm-1" {
		t.Fatalf("unexpected dynamic next")
	}
	var dynamicInterface RecoveryPoint = dynamic
	dynamicInterface.Reset()
	var nilDynamic *DynamicRecoveryPoint
	if nilDynamic.Next() != "" {
		t.Fatalf("nil dynamic should return empty")
	}
	nilDynamic.Reset()
	nilDynamic = NewDynamicRecoveryPoint(nil)
	if nilDynamic.Next() != "" {
		t.Fatalf("nil producer should return empty")
	}

	adapter := NewRecoveryPointAdapter(nil, NewFixedRecoveryPoint(""), NewFixedRecoveryPoint("bm"))
	if next := adapter.Next(); next != "bm" {
		t.Fatalf("unexpected adapter next: %q", next)
	}
	adapter.Add(nil)
	adapter.Add(NewFixedRecoveryPoint("bm-2"))
	if next := adapter.Next(); next == "" {
		t.Fatalf("expected non-empty adapter value")
	}
	adapter.Reset()
	var nilAdapter *RecoveryPointAdapter
	nilAdapter.Add(NewFixedRecoveryPoint("x"))
	if nilAdapter.Next() != "" {
		t.Fatalf("nil adapter next should be empty")
	}
	nilAdapter.Reset()
	if next := NewRecoveryPointAdapter().Next(); next != "" {
		t.Fatalf("expected empty next from adapter with no points, got %q", next)
	}
	if value := NewRecoveryPointAdapter(NewFixedRecoveryPoint(""), NewFixedRecoveryPoint("")).Next(); value != "" {
		t.Fatalf("expected empty adapter value when all points are empty")
	}

	testPoint := &testRecoveryPoint{values: []string{"a", "", "b"}}
	conflating := NewConflatingRecoveryPointAdapter(testPoint)
	if next := conflating.Next(); next != "a" {
		t.Fatalf("unexpected conflating next: %q", next)
	}
	conflating.Reset()
	if next := conflating.Next(); next != "a" {
		t.Fatalf("unexpected conflating reset next: %q", next)
	}
	var nilConflating *ConflatingRecoveryPointAdapter
	if nilConflating.Next() != "" {
		t.Fatalf("nil conflating next should be empty")
	}
	nilConflating.Reset()

	sowAdapter := NewSOWRecoveryPointAdapter(NewFixedRecoveryPoint("sow"))
	if next := sowAdapter.Next(); next != "sow" {
		t.Fatalf("unexpected sow adapter next: %q", next)
	}
}

func TestStoresCoverage(t *testing.T) {
	buffer := NewMemoryStoreBuffer()
	if _, err := buffer.Write([]byte("abc")); err != nil {
		t.Fatalf("write buffer failed: %v", err)
	}
	if buffer.Len() != 3 || string(buffer.Bytes()) != "abc" {
		t.Fatalf("unexpected buffer state")
	}
	bytesCopy := buffer.Bytes()
	bytesCopy[0] = 'X'
	if string(buffer.Bytes()) != "abc" {
		t.Fatalf("buffer bytes should be copied")
	}
	buffer.Reset()
	if buffer.Len() != 0 {
		t.Fatalf("expected reset buffer length zero")
	}
	var nilBuffer *MemoryStoreBuffer
	if data := nilBuffer.Bytes(); data != nil {
		t.Fatalf("nil buffer bytes should be nil")
	}
	if count, err := nilBuffer.Write([]byte("x")); count != 0 || err != nil {
		t.Fatalf("nil buffer write should be noop")
	}
	nilBuffer.Reset()
	if nilBuffer.Len() != 0 {
		t.Fatalf("nil buffer len should be zero")
	}

	mmapBuffer := NewMMapStoreBuffer()
	if _, err := mmapBuffer.Write([]byte("mmap")); err != nil {
		t.Fatalf("mmap write failed: %v", err)
	}

	blockStore := NewBlockStore()
	blockStore.Put(1, []byte("one"))
	gotBlock, ok := blockStore.Get(1)
	if !ok || string(gotBlock) != "one" {
		t.Fatalf("unexpected block store value")
	}
	gotBlock[0] = 'X'
	again, _ := blockStore.Get(1)
	if string(again) != "one" {
		t.Fatalf("block store must copy values")
	}
	if _, ok := blockStore.Get(2); ok {
		t.Fatalf("unexpected missing block hit")
	}
	blockStore.Put(2, []byte("two"))
	blockStore.DeleteUpTo(1)
	if _, ok := blockStore.Get(1); ok {
		t.Fatalf("expected deleted block")
	}
	var nilBlockStore *BlockStore
	nilBlockStore.Put(1, []byte("x"))
	if _, ok := nilBlockStore.Get(1); ok {
		t.Fatalf("nil block store should miss")
	}
	nilBlockStore.DeleteUpTo(1)

	blockPublishStore := NewBlockPublishStore(nil)
	sequence, err := blockPublishStore.Store(amps.NewCommand("publish").SetTopic("orders"))
	if err != nil || sequence == 0 {
		t.Fatalf("unexpected block publish store result seq=%d err=%v", sequence, err)
	}
	if err := blockPublishStore.DiscardUpTo(sequence); err != nil {
		t.Fatalf("discard up to failed: %v", err)
	}
	replayed := 0
	if err := blockPublishStore.Replay(func(command *amps.Command) error {
		replayed++
		_ = command
		return nil
	}); err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	var nilBlockPublishStore *BlockPublishStore
	if sequence, err := nilBlockPublishStore.Store(nil); sequence != 0 || err != nil {
		t.Fatalf("nil block publish store should noop")
	}
	if err := nilBlockPublishStore.DiscardUpTo(1); err != nil {
		t.Fatalf("nil block publish discard should noop")
	}
	if err := nilBlockPublishStore.Replay(nil); err != nil {
		t.Fatalf("nil block publish replay should noop")
	}

	primary := &fakePublishStore{storeErr: errors.New("primary failed"), replayData: []*amps.Command{amps.NewCommand("publish").SetTopic("a")}}
	secondary := &fakePublishStore{replayData: []*amps.Command{amps.NewCommand("publish").SetTopic("b")}}
	hybrid := NewHybridPublishStore(primary, secondary)
	if sequence, err := hybrid.Store(amps.NewCommand("publish").SetTopic("orders")); err != nil || sequence == 0 {
		t.Fatalf("hybrid fallback store failed: seq=%d err=%v", sequence, err)
	}
	if err := hybrid.DiscardUpTo(5); err != nil {
		t.Fatalf("hybrid discard failed: %v", err)
	}
	if err := hybrid.Replay(func(command *amps.Command) error { return nil }); err != nil {
		t.Fatalf("hybrid replay failed: %v", err)
	}
	if err := hybrid.Flush(5 * time.Millisecond); err != nil {
		t.Fatalf("hybrid flush failed: %v", err)
	}
	primarySuccess := &fakePublishStore{replayData: []*amps.Command{amps.NewCommand("publish").SetTopic("ok")}}
	hybridPrimarySuccess := &HybridPublishStore{primary: primarySuccess, secondary: secondary}
	if sequence, err := hybridPrimarySuccess.Store(amps.NewCommand("publish").SetTopic("orders")); err != nil || sequence == 0 {
		t.Fatalf("expected primary hybrid store success, seq=%d err=%v", sequence, err)
	}
	var nilHybrid *HybridPublishStore
	if sequence, err := nilHybrid.Store(nil); sequence != 0 || err != nil {
		t.Fatalf("nil hybrid store should noop")
	}
	if err := nilHybrid.DiscardUpTo(1); err != nil {
		t.Fatalf("nil hybrid discard should noop")
	}
	if err := nilHybrid.Replay(nil); err != nil {
		t.Fatalf("nil hybrid replay should noop")
	}
	if err := nilHybrid.Flush(1); err != nil {
		t.Fatalf("nil hybrid flush should noop")
	}
	manualHybrid := &HybridPublishStore{}
	if err := manualHybrid.Flush(time.Millisecond); err != nil {
		t.Fatalf("manual hybrid flush should noop when primary missing")
	}
	if NewHybridPublishStore(nil, secondary) == nil || NewHybridPublishStore(primary, nil) == nil {
		t.Fatalf("expected hybrid constructor to default missing stores")
	}

	primaryDiscardError := &fakePublishStore{discardErr: errors.New("discard failed")}
	discardHybrid := &HybridPublishStore{primary: primaryDiscardError, secondary: &fakePublishStore{}}
	if err := discardHybrid.DiscardUpTo(3); err == nil {
		t.Fatalf("expected primary discard error")
	}

	primaryReplayError := &fakePublishStore{replayErr: errors.New("replay failed")}
	replayHybrid := &HybridPublishStore{primary: primaryReplayError, secondary: &fakePublishStore{}}
	if err := replayHybrid.Replay(func(*amps.Command) error { return nil }); err == nil {
		t.Fatalf("expected primary replay error")
	}

	logged := NewLoggedBookmarkStore(nil)
	message := amps.NewCommand("publish").
		SetSubID("sub-1").
		SetBookmark("1|1|").
		SetTopic("orders").
		SetData([]byte(`{"id":1}`)).
		GetMessage()
	seqNo := logged.Log(message)
	logged.Discard("sub-1", seqNo)
	logged.DiscardMessage(message)
	_ = logged.GetMostRecent("sub-1")
	_ = logged.IsDiscarded(message)
	logged.Purge("sub-1")
	_ = logged.GetOldestBookmarkSeq("sub-1")
	_ = logged.Persisted("sub-1", "1|1|")
	logged.SetServerVersion("5.3.5.1")
	if len(logged.Logs()) == 0 {
		t.Fatalf("expected logged bookmark events")
	}

	manager := NewMemorySubscriptionManager()
	manager.Subscribe(func(*amps.Message) error { return nil }, amps.NewCommand("subscribe").SetSubID("sub-1").SetTopic("orders"), amps.AckTypeProcessed)
	manager.Unsubscribe("sub-1")
	manager.Clear()
	if err := manager.Resubscribe(nil); err != nil {
		t.Fatalf("memory subscription manager nil client resubscribe should pass")
	}
	manager.SetFailedResubscribeHandler(nil)

	var nilManager *MemorySubscriptionManager
	nilManager.Subscribe(nil, nil, 0)
	nilManager.Unsubscribe("x")
	nilManager.Clear()
	if err := nilManager.Resubscribe(nil); err != nil {
		t.Fatalf("nil memory subscription manager should noop")
	}
	nilManager.SetFailedResubscribeHandler(nil)
}
