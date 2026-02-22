package amps

import (
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"
)

type testAuthenticator struct{}

func (testAuthenticator) Authenticate(username string, password string) (string, error) {
	return username + ":" + password, nil
}
func (testAuthenticator) Retry(username string, password string) (string, error) {
	return password + "-retry", nil
}
func (testAuthenticator) Completed(username string, password string, reason string) {}

type valueListener struct{}

func (valueListener) ConnectionStateChanged(ConnectionState) {}

func TestErrorHelpersCoverage(t *testing.T) {
	reasonCases := map[string]string{
		"bad filter":    "BadFilterError",
		"invalid topic": "InvalidTopicError",
		"not entitled":  "NotEntitledError",
		"auth failure":  "AuthenticationError",
		"bad regex":     "BadRegexTopicError",
		"other":         "UnknownError",
	}
	for reason, expected := range reasonCases {
		if got := reasonToError(reason); !strings.Contains(got.Error(), expected) {
			t.Fatalf("reasonToError(%q)=%q, expected %q", reason, got.Error(), expected)
		}
	}

	errorNames := map[int]string{
		AlreadyConnectedError:          "AlreadyConnectedError",
		AuthenticationError:            "AuthenticationError",
		BadFilterError:                 "BadFilterError",
		BadRegexTopicError:             "BadRegexTopicError",
		CommandError:                   "CommandError",
		ConnectionError:                "ConnectionError",
		ConnectionRefusedError:         "ConnectionRefusedError",
		DisconnectedError:              "DisconnectedError",
		ProtocolError:                  "ProtocolError",
		InvalidTopicError:              "InvalidTopicError",
		InvalidURIError:                "InvalidURIError",
		nameInUseError:                 "NameInUseError",
		NotEntitledError:               "NotEntitledError",
		RetryOperationError:            "RetryOperationError",
		SubidInUseError:                "SubidInUseError",
		SubscriptionAlreadyExistsError: "SubscriptionAlreadyExistsError",
		TimedOutError:                  "TimedOutError",
		MessageHandlerError:            "MessageHandlerError",
		UnknownError:                   "UnknownError",
	}
	for code, expected := range errorNames {
		if got := NewError(code); !strings.Contains(got.Error(), expected) {
			t.Fatalf("NewError(%d)=%q expected contains %q", code, got.Error(), expected)
		}
		if got := NewError(code, "msg"); !strings.Contains(got.Error(), expected+": msg") {
			t.Fatalf("NewError(%d,msg)=%q expected contains %q", code, got.Error(), expected+": msg")
		}
	}
}

func TestVersionHelpersCoverage(t *testing.T) {
	info := ParseVersionInfo("5.3.5.1")
	if info.Major != 5 || info.Minor != 3 || info.Maintenance != 5 || info.Hotfix != 1 {
		t.Fatalf("unexpected version info: %+v", info)
	}
	if info.Number() != 5030501 {
		t.Fatalf("unexpected version number: %d", info.Number())
	}
	if info.String() != "5.3.5.1" {
		t.Fatalf("unexpected version string: %q", info.String())
	}

	if got := ParseVersionInfo("1.bad.3.4"); got.Minor != 0 || got.Maintenance != 0 || got.Hotfix != 0 {
		t.Fatalf("expected parse to stop at invalid segment, got %+v", got)
	}

	if BOOKMARK_EPOCH() != BookmarksEPOCH || EPOCH() != BookmarksEPOCH {
		t.Fatalf("unexpected epoch aliases")
	}
	if BOOKMARK_RECENT() != BookmarksRECENT || RECENT() != BookmarksRECENT || MOST_RECENT() != BookmarksRECENT || BOOKMARK_MOST_RECENT() != BookmarksRECENT {
		t.Fatalf("unexpected recent aliases")
	}
	if BOOKMARK_NOW() != BookmarksNOW || NOW() != BookmarksNOW {
		t.Fatalf("unexpected now aliases")
	}

	if value := ConvertVersionToNumber(""); value != 0 {
		t.Fatalf("expected 0 for empty version, got %d", value)
	}
	if value := ConvertVersionToNumber("abc"); value != 0 {
		t.Fatalf("expected 0 for no-digit version, got %d", value)
	}
	if value := ConvertVersionToNumber("5.3.5.1-extra"); value != 5030501 {
		t.Fatalf("unexpected converted version: %d", value)
	}
	if value := ConvertVersionToNumber("123.150.999.1000"); value != 99999999 {
		t.Fatalf("expected clamped version, got %d", value)
	}
}

func TestStoreCodecCoverage(t *testing.T) {
	command := NewCommand("sow_and_subscribe").
		SetAckType(AckTypeProcessed | AckTypeCompleted).
		SetBatchSize(50).
		SetBookmark("1|1|").
		SetCommandID("cid").
		SetCorrelationID("corr").
		SetExpiration(33).
		SetFilter("/id > 10").
		SetOptions("replace").
		SetOrderBy("/id desc").
		SetQueryID("qid").
		SetSequenceID(99).
		SetSowKey("k1").
		SetSowKeys("k1,k2").
		SetSubID("sub").
		SetSubIDs("sub,sub2").
		SetTopic("orders").
		SetTopN(5).
		SetTimeout(2000).
		SetData([]byte("payload"))

	snapshot := snapshotFromCommand(command)
	restored := commandFromSnapshot(snapshot)
	if restored == nil {
		t.Fatalf("expected restored command")
	}
	if restored.GetTimeout() != 2000 {
		t.Fatalf("unexpected restored timeout: %d", restored.GetTimeout())
	}
	if got, _ := restored.Topic(); got != "orders" {
		t.Fatalf("unexpected restored topic: %q", got)
	}

	clone := cloneCommand(command)
	clone.SetTopic("mutated")
	topic, _ := command.Topic()
	if topic == "mutated" {
		t.Fatalf("clone mutation should not affect original")
	}

	raw, err := marshalCommandSnapshot(command)
	if err != nil {
		t.Fatalf("marshal snapshot failed: %v", err)
	}
	unmarshaled, err := unmarshalCommandSnapshot(raw)
	if err != nil {
		t.Fatalf("unmarshal snapshot failed: %v", err)
	}
	if got, _ := unmarshaled.CommandID(); got != "cid" {
		t.Fatalf("unexpected unmarshaled command id: %q", got)
	}
	if _, err := unmarshalCommandSnapshot([]byte("{not-json")); err == nil {
		t.Fatalf("expected invalid json error")
	}

	empty := snapshotFromCommand(nil)
	if empty.Command != "" || empty.Timeout != 0 {
		t.Fatalf("unexpected nil snapshot: %+v", empty)
	}
}

func TestParityTypesHelpersCoverage(t *testing.T) {
	if ensureClientState(nil) != nil {
		t.Fatalf("expected nil ensureClientState for nil client")
	}

	client := NewClient("parity-types")
	state := ensureClientState(client)
	if state == nil {
		t.Fatalf("expected client parity state")
	}
	defer forgetClientState(client)
	if same := ensureClientState(client); same != state {
		t.Fatalf("expected ensureClientState to reuse existing state")
	}

	if key := listenerKey(nil); key != 0 {
		t.Fatalf("expected zero key for nil listener, got %d", key)
	}
	funcListener := ConnectionStateListenerFunc(func(ConnectionState) {})
	if key := listenerKey(funcListener); key == 0 {
		t.Fatalf("expected non-zero key for function listener")
	}
	if key := listenerKey(valueListener{}); key == 0 {
		t.Fatalf("expected non-zero key for value listener")
	}
	var nilValueListener *valueListener
	if key := listenerKey(nilValueListener); key != 0 {
		t.Fatalf("expected nil pointer listener key to be zero")
	}
	var nilFunc ConnectionStateListenerFunc
	if key := listenerKey(nilFunc); key != 0 {
		t.Fatalf("expected nil function listener key to be zero")
	}

	var reported []error
	state.lock.Lock()
	state.exceptionListener = ExceptionListenerFunc(func(err error) { reported = append(reported, err) })
	state.connectionListeners = map[uintptr]ConnectionStateListener{
		1: ConnectionStateListenerFunc(func(ConnectionState) {}),
		2: ConnectionStateListenerFunc(func(ConnectionState) { panic("listener panic") }),
	}
	state.lock.Unlock()
	state.broadcastConnectionState(ConnectionStateConnected)
	if len(reported) == 0 {
		t.Fatalf("expected panic to be reported through exception listener")
	}
	state.lock.Lock()
	state.exceptionListener = nil
	state.connectionListeners = map[uintptr]ConnectionStateListener{
		1: ConnectionStateListenerFunc(func(ConnectionState) { panic("listener panic without exception listener") }),
	}
	state.lock.Unlock()
	state.broadcastConnectionState(ConnectionStateDisconnected)
	var nilParityState *clientParityState
	nilParityState.broadcastConnectionState(ConnectionStateConnected)

	parsed, _ := url.Parse("tcp://127.0.0.1:9007/amps/json")
	client.url = parsed
	client.serverVersion = "5.3.5.1"
	client.connection = newTestConn()
	state.lock.Lock()
	state.uri = "tcp://127.0.0.1:9007/amps/json"
	state.lock.Unlock()
	info := client.buildConnectionInfo()
	if info["uri"] == "" || info["scheme"] != "tcp" || info["server_version_number"] != "5030501" {
		t.Fatalf("unexpected connection info: %+v", info)
	}
	if emptyInfo := (*Client)(nil).buildConnectionInfo(); len(emptyInfo) != 0 {
		t.Fatalf("expected empty connection info for nil client")
	}

	payload := []byte("payload")
	state.lock.Lock()
	state.transportFilter = nil
	state.lock.Unlock()
	if filtered := client.applyTransportFilter(TransportFilterOutbound, payload); string(filtered) != "payload" {
		t.Fatalf("unexpected passthrough payload: %q", string(filtered))
	}
	state.lock.Lock()
	state.transportFilter = func(direction TransportFilterDirection, current []byte) []byte {
		_ = direction
		return append([]byte(nil), []byte("filtered:"+string(current))...)
	}
	state.lock.Unlock()
	if filtered := client.applyTransportFilter(TransportFilterInbound, payload); string(filtered) != "filtered:payload" {
		t.Fatalf("unexpected filtered payload: %q", string(filtered))
	}
	state.lock.Lock()
	state.transportFilter = func(_ TransportFilterDirection, _ []byte) []byte {
		return nil
	}
	state.lock.Unlock()
	if filtered := client.applyTransportFilter(TransportFilterInbound, payload); string(filtered) != "payload" {
		t.Fatalf("nil transport filter result should preserve payload, got %q", string(filtered))
	}
	state.lock.Lock()
	state.transportFilter = func(direction TransportFilterDirection, current []byte) []byte {
		panic("transport filter panic")
	}
	state.exceptionListener = ExceptionListenerFunc(func(err error) { reported = append(reported, err) })
	state.lock.Unlock()
	if filtered := client.applyTransportFilter(TransportFilterInbound, payload); string(filtered) != "payload" {
		t.Fatalf("expected panic fallback payload, got %q", string(filtered))
	}
	var nilClientForFilter *Client
	if filtered := nilClientForFilter.applyTransportFilter(TransportFilterInbound, payload); string(filtered) != "payload" {
		t.Fatalf("nil client should return original payload, got %q", string(filtered))
	}

	started := 0
	stopped := 0
	state.lock.Lock()
	state.exceptionListener = ExceptionListenerFunc(func(err error) { reported = append(reported, err) })
	state.receiveRoutineCallback = func() {
		started++
		panic("started panic")
	}
	state.receiveRoutineStop = func() {
		stopped++
		panic("stopped panic")
	}
	state.lock.Unlock()
	client.callReceiveRoutineStartedCallback()
	client.callReceiveRoutineStoppedCallback()
	if started != 1 || stopped != 1 {
		t.Fatalf("unexpected callback counters started=%d stopped=%d", started, stopped)
	}

	state.lock.Lock()
	state.receiveRoutineCallback = nil
	state.receiveRoutineStop = nil
	state.lock.Unlock()
	client.callReceiveRoutineStartedCallback()
	client.callReceiveRoutineStoppedCallback()

	var nilClient *Client
	nilClient.callReceiveRoutineStartedCallback()
	nilClient.callReceiveRoutineStoppedCallback()

	forgetClientState(client)
	if got := ensureClientState(client); got == nil {
		t.Fatalf("expected ensureClientState to recreate state")
	}
	forgetClientState(nil)
}

func TestReconnectAndChooserCoverage(t *testing.T) {
	fixed := NewFixedDelayStrategy(-1)
	if wait, err := fixed.GetConnectWaitDuration("uri"); err != nil || wait != 0 {
		t.Fatalf("unexpected fixed wait: %v err=%v", wait, err)
	}
	var fixedInterface ReconnectDelayStrategy = fixed
	fixedInterface.Reset()
	var nilFixed *FixedDelayStrategy
	if wait, err := nilFixed.GetConnectWaitDuration("uri"); err != nil || wait != 0 {
		t.Fatalf("unexpected nil fixed wait: %v err=%v", wait, err)
	}
	nilFixed.Reset()

	exponential := NewExponentialDelayStrategy(-1*time.Second, -1, 0.5)
	wait1, err := exponential.GetConnectWaitDuration("u")
	if err != nil || wait1 != 0 {
		t.Fatalf("unexpected first exponential wait: %v err=%v", wait1, err)
	}
	wait2, err := exponential.GetConnectWaitDuration("u")
	if err != nil || wait2 < wait1 {
		t.Fatalf("unexpected exponential progression: %v -> %v err=%v", wait1, wait2, err)
	}
	if waitDefault, err := exponential.GetConnectWaitDuration(""); err != nil || waitDefault < 0 {
		t.Fatalf("unexpected default uri wait: %v err=%v", waitDefault, err)
	}
	exponential.Reset()
	if waitAfterReset, _ := exponential.GetConnectWaitDuration("u"); waitAfterReset != wait1 {
		t.Fatalf("expected reset attempts, got %v want %v", waitAfterReset, wait1)
	}
	var nilExp *ExponentialDelayStrategy
	if wait, err := nilExp.GetConnectWaitDuration("u"); err != nil || wait != 0 {
		t.Fatalf("unexpected nil exponential wait: %v err=%v", wait, err)
	}
	nilExp.Reset()

	capped := NewExponentialDelayStrategy(100*time.Millisecond, 150*time.Millisecond, 3)
	firstCapped, _ := capped.GetConnectWaitDuration("cap")
	secondCapped, _ := capped.GetConnectWaitDuration("cap")
	if firstCapped != 100*time.Millisecond || secondCapped != 150*time.Millisecond {
		t.Fatalf("expected capped exponential delay progression, got %v then %v", firstCapped, secondCapped)
	}

	manualExp := &ExponentialDelayStrategy{
		BaseDelay: time.Second,
		MaxDelay:  -time.Second,
		Factor:    2,
		attempts:  make(map[string]uint32),
	}
	if wait, err := manualExp.GetConnectWaitDuration("u"); err != nil || wait != 0 {
		t.Fatalf("expected negative-delay clamp to zero, wait=%v err=%v", wait, err)
	}

	var chooser *DefaultServerChooser
	if chooser.CurrentURI() != "" || chooser.CurrentAuthenticator() != nil || chooser.Error() != "" {
		t.Fatalf("expected nil chooser zero values")
	}
	_ = chooser.Add("tcp://nil:1/amps/json")
	if chooser.AddWithAuthenticator("tcp://x:1/amps/json", nil) != nil {
		t.Fatalf("expected nil chooser AddWithAuthenticator to return nil")
	}
	chooser.ReportFailure(errors.New("x"), ConnectionInfo{})
	chooser.ReportSuccess(ConnectionInfo{})
	chooser.Remove("x")
	chooser = NewDefaultServerChooser()
	if chooser.CurrentURI() != "" {
		t.Fatalf("expected empty URI for chooser without endpoints")
	}
	chooser.Remove("empty")
	if chooser.CurrentAuthenticator() != nil {
		t.Fatalf("expected nil authenticator for empty chooser")
	}
	_ = chooser.Add("")
	_ = chooser.Add("tcp://c:3/amps/json")
	chooser.Remove("tcp://c:3/amps/json")
	_ = chooser.AddWithAuthenticator("", testAuthenticator{})
	chooser.index = 99
	_ = chooser.CurrentURI()
	chooser.index = -1
	_ = chooser.AddWithAuthenticator("tcp://idx:1/amps/json", testAuthenticator{})
	_ = chooser.CurrentAuthenticator()
	_ = chooser.Add("tcp://a:1/amps/json")
	_ = chooser.AddWithAuthenticator("tcp://b:2/amps/json", testAuthenticator{})
	chooser.index = 999
	_ = chooser.CurrentURI()
	if chooser.CurrentURI() == "" {
		t.Fatalf("expected chooser uri")
	}
	chooser.index = 1
	chooser.ReportFailure(errors.New("advance"), ConnectionInfo{})
	if chooser.CurrentAuthenticator() == nil {
		t.Fatalf("expected chooser authenticator on authenticated endpoint")
	}
	chooser.ReportFailure(errors.New("failed"), ConnectionInfo{"uri": "x"})
	if chooser.Error() == "" {
		t.Fatalf("expected chooser error")
	}
	chooser.ReportSuccess(ConnectionInfo{})
	if chooser.Error() != "" {
		t.Fatalf("expected chooser error reset")
	}
	chooser.Remove("tcp://a:1/amps/json")
	chooser.Remove("tcp://b:2/amps/json")
	chooser.Remove("tcp://idx:1/amps/json")
	if chooser.CurrentURI() != "" {
		t.Fatalf("expected empty chooser after remove")
	}
}

func TestSubscriptionManagerCoverage(t *testing.T) {
	if trackedSubscriptionID(nil) != "" {
		t.Fatalf("expected empty id for nil command")
	}
	if id := trackedSubscriptionID(NewCommand("subscribe").SetCommandID("cid")); id != "cid" {
		t.Fatalf("unexpected command id fallback: %q", id)
	}
	if id := trackedSubscriptionID(NewCommand("subscribe").SetQueryID("qid")); id != "qid" {
		t.Fatalf("unexpected query id fallback: %q", id)
	}
	if id := trackedSubscriptionID(NewCommand("subscribe").SetSubID("sid")); id != "sid" {
		t.Fatalf("unexpected sub id fallback: %q", id)
	}

	manager := NewDefaultSubscriptionManager()
	var nilManager *DefaultSubscriptionManager
	nilManager.Subscribe(nil, nil, 0)
	nilManager.Unsubscribe("all")
	nilManager.Clear()
	nilManager.SetFailedResubscribeHandler(nil)
	if err := nilManager.Resubscribe(nil); err != nil {
		t.Fatalf("nil manager resubscribe should be nil, got %v", err)
	}

	manager.Subscribe(nil, NewCommand("publish").SetTopic("orders"), AckTypeProcessed)
	manager.Subscribe(func(*Message) error { return nil }, NewCommand("subscribe").SetTopic("orders"), AckTypeProcessed)

	sub := NewCommand("subscribe").SetSubID("sub-1").SetTopic("orders")
	manager.Subscribe(func(*Message) error { return nil }, sub, AckTypeProcessed)
	manager.Subscribe(nil, sub, AckTypeProcessed)

	manager.Unsubscribe("sub-missing")
	manager.Unsubscribe("")
	manager.Subscribe(func(*Message) error { return nil }, sub, AckTypeProcessed)
	manager.Unsubscribe("all")
	manager.Subscribe(func(*Message) error { return nil }, sub, AckTypeProcessed)

	client := NewClient("subscription-manager")
	defer forgetClientState(client)
	if err := manager.Resubscribe(client); err == nil {
		t.Fatalf("expected disconnected resubscribe error")
	}

	state := ensureClientState(client)
	state.lock.Lock()
	state.noResubscribeRoutes["sub-1"] = struct{}{}
	state.lock.Unlock()
	if err := manager.Resubscribe(client); err != nil {
		t.Fatalf("blocked resubscribe should skip and succeed, got %v", err)
	}

	state.lock.Lock()
	delete(state.noResubscribeRoutes, "sub-1")
	state.lock.Unlock()

	failedCalled := 0
	manager.SetFailedResubscribeHandler(FailedResubscribeHandlerFunc(func(command *Command, requestedAckTypes int, err error) bool {
		_ = command
		_ = requestedAckTypes
		failedCalled++
		return true
	}))
	if err := manager.Resubscribe(client); err != nil {
		t.Fatalf("failure handler true should absorb errors, got %v", err)
	}
	if failedCalled == 0 {
		t.Fatalf("expected failure handler callback")
	}

	manager.SetFailedResubscribeHandler(FailedResubscribeHandlerFunc(func(command *Command, requestedAckTypes int, err error) bool {
		return false
	}))
	if err := manager.Resubscribe(client); err == nil {
		t.Fatalf("failure handler false should propagate error")
	}

	manager.Clear()
	if err := manager.Resubscribe(client); err != nil {
		t.Fatalf("resubscribe with empty set should succeed, got %v", err)
	}
}
