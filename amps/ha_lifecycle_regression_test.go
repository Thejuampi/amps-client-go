package amps

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func lifecycleRecoveryServer(t *testing.T, acknowledgeRecovery bool) (string, <-chan struct{}, <-chan struct{}, func()) {
	t.Helper()
	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var observed, done = make(chan struct{}), make(chan struct{})
	var lock sync.Mutex
	var stopped bool
	var active net.Conn
	go func() {
		defer close(done)
		var conn, acceptErr = listener.Accept()
		if acceptErr != nil {
			return
		}
		lock.Lock()
		if stopped {
			lock.Unlock()
			_ = conn.Close()
			return
		}
		active = conn
		lock.Unlock()
		defer conn.Close()
		var length [4]byte
		for index := 0; index < 2; index++ {
			if _, readErr := io.ReadFull(conn, length[:]); readErr != nil {
				return
			}
			var body = make([]byte, binary.BigEndian.Uint32(length[:]))
			if _, readErr := io.ReadFull(conn, body); readErr != nil {
				return
			}
			var command struct {
				CommandID string `json:"cid"`
			}
			if json.Unmarshal(body, &command) != nil {
				return
			}
			if index == 1 {
				close(observed)
				if !acknowledgeRecovery {
					break
				}
			}
			var ack, _ = json.Marshal(map[string]string{"c": "ack", "a": "processed", "cid": command.CommandID, "status": "success"})
			binary.BigEndian.PutUint32(length[:], uint32(len(ack)))
			if _, writeErr := conn.Write(append(length[:], ack...)); writeErr != nil {
				return
			}
		}
		_, _ = io.Copy(io.Discard, conn)
	}()
	var stop = func() {
		lock.Lock()
		stopped = true
		var conn = active
		lock.Unlock()
		_ = listener.Close()
		if conn != nil {
			_ = conn.Close()
		}
	}
	return "tcp://" + listener.Addr().String() + "/amps/json", observed, done, stop
}

func waitHALifecycleStopped(t *testing.T, ha *HAClient) {
	t.Helper()
	var timer = time.NewTimer(time.Second)
	defer timer.Stop()
	for ha.reconnecting.Load() {
		select {
		case <-timer.C:
			t.Error("HA reconnect goroutine did not stop during cleanup")
			return
		default:
			runtime.Gosched()
		}
	}
}

type lifecycleChooser struct {
	*fixedChooser
	attempts       atomic.Int32
	failureEntered chan struct{}
	failureRelease <-chan struct{}
	successEntered chan struct{}
	successRelease <-chan struct{}
	retryAttempted chan struct{}
	retryURI       *string
	failureOnce    sync.Once
	successOnce    sync.Once
	retryOnce      sync.Once
}

func (chooser *lifecycleChooser) CurrentURI() string {
	if chooser.attempts.Add(1) > 1 {
		if chooser.retryAttempted != nil {
			chooser.retryOnce.Do(func() { close(chooser.retryAttempted) })
		}
		if chooser.retryURI != nil {
			return *chooser.retryURI
		}
	}
	return chooser.uri
}

func (chooser *lifecycleChooser) ReportFailure(error, ConnectionInfo) {
	if chooser.failureEntered != nil {
		chooser.failureOnce.Do(func() {
			close(chooser.failureEntered)
			<-chooser.failureRelease
		})
	}
}

func (chooser *lifecycleChooser) ReportSuccess(ConnectionInfo) {
	if chooser.successEntered != nil {
		chooser.successOnce.Do(func() {
			close(chooser.successEntered)
			<-chooser.successRelease
		})
	}
}

type lifecyclePausedChooser struct {
	*lifecycleChooser
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (chooser *lifecyclePausedChooser) CurrentURI() string {
	chooser.once.Do(func() {
		close(chooser.entered)
		<-chooser.release
	})
	return chooser.lifecycleChooser.CurrentURI()
}

func TestHAInitialRecoveryFailureDoesNotStartCompetingReconnect(t *testing.T) {
	for _, cancellation := range []bool{false, true} {
		var name = "timeout"
		if cancellation {
			name = "cancel"
		}
		t.Run(name, func(t *testing.T) {
			var uri, observed, serverDone, stopServer = lifecycleRecoveryServer(t, false)
			var release = make(chan struct{})
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			var chooser = &lifecycleChooser{fixedChooser: &fixedChooser{uri: uri}, failureEntered: make(chan struct{}), failureRelease: release}
			var ha = NewHAClient("initial-recovery-failure").SetTimeout(40 * time.Millisecond)
			ha.SetServerChooser(chooser)
			ha.Client().SetHeartbeat(0)
			var manager = NewDefaultSubscriptionManager()
			manager.Subscribe(nil, NewCommand("subscribe").SetTopic("orders").SetSubID("retained"), AckTypeNone)
			ha.Client().SetSubscriptionManager(manager)
			var readerStopped = make(chan struct{})
			var readerOnce sync.Once
			ha.Client().SetReceiveRoutineStoppedCallback(func() { readerOnce.Do(func() { close(readerStopped) }) })
			var reconnectEvents atomic.Int32
			ha.Client().AddConnectionStateListener(ConnectionStateListenerFunc(func(state ConnectionState) {
				if state == ConnectionStateReconnecting {
					reconnectEvents.Add(1)
				}
			}))
			var ctx, cancel = context.WithCancel(context.Background())
			var result = make(chan error, 1)
			var resultDone = make(chan struct{})
			go func() { defer close(resultDone); result <- ha.connectAndLogon(ctx) }()
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(release) })
				cancel()
				_ = ha.Disconnect()
				stopServer()
				select {
				case <-resultDone:
				case <-time.After(time.Second):
					t.Error("initial attempt did not stop during cleanup")
				}
				select {
				case <-serverDone:
				case <-time.After(time.Second):
					t.Error("test server did not stop during cleanup")
				}
				waitHALifecycleStopped(t, ha)
			})
			select {
			case <-observed:
			case <-time.After(time.Second):
				t.Fatal("recovery subscribe did not reach server")
			}
			if cancellation {
				cancel()
			}
			select {
			case <-chooser.failureEntered:
			case <-time.After(time.Second):
				t.Fatal("failed attempt did not reach chooser")
			}
			select {
			case <-readerStopped:
			case <-time.After(time.Second):
				t.Fatal("connection reader did not finish its disconnect callback")
			}
			var earlyReconnect = ha.reconnecting.Load() || reconnectEvents.Load() != 0
			releaseOnce.Do(func() { close(release) })
			var err = <-result
			if cancellation && !IsErrorKind(err, DisconnectedError) || !cancellation && !IsErrorKind(err, TimedOutError) {
				t.Fatalf("initial failure = %v", err)
			}
			// Model a disconnect callback delayed until after the terminal result.
			ha.handleDisconnect(NewError(ConnectionError, "late callback"))
			if earlyReconnect || ha.reconnecting.Load() || reconnectEvents.Load() != 0 || chooser.attempts.Load() != 1 {
				_ = ha.Disconnect()
				t.Fatalf("terminal attempt spawned reconnect: early=%v active=%v events=%d attempts=%d", earlyReconnect, ha.reconnecting.Load(), reconnectEvents.Load(), chooser.attempts.Load())
			}
		})
	}
}

func TestHASuccessfulInitialAttemptHandsOffLateDisconnect(t *testing.T) {
	var uri, observed, serverDone, stopServer = lifecycleRecoveryServer(t, true)
	var release = make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var chooser = &lifecycleChooser{fixedChooser: &fixedChooser{uri: uri}, successEntered: make(chan struct{}), successRelease: release, retryAttempted: make(chan struct{})}
	var ha = NewHAClient("successful-recovery-handoff").SetTimeout(200 * time.Millisecond)
	ha.SetServerChooser(chooser)
	ha.SetReconnectDelayStrategy(errorReconnectStrategy{})
	ha.Client().SetHeartbeat(0)
	var manager = NewDefaultSubscriptionManager()
	manager.Subscribe(nil, NewCommand("subscribe").SetTopic("orders").SetSubID("retained"), AckTypeNone)
	ha.Client().SetSubscriptionManager(manager)
	var result = make(chan error, 1)
	var resultDone = make(chan struct{})
	go func() { defer close(resultDone); result <- ha.ConnectAndLogon() }()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		_ = ha.Disconnect()
		stopServer()
		select {
		case <-resultDone:
		case <-time.After(time.Second):
			t.Error("initial attempt did not stop during cleanup")
		}
		select {
		case <-serverDone:
		case <-time.After(time.Second):
			t.Error("test server did not stop during cleanup")
		}
		waitHALifecycleStopped(t, ha)
	})
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("recovery subscribe did not reach server")
	}
	select {
	case <-chooser.successEntered:
	case <-time.After(time.Second):
		t.Fatal("successful attempt did not reach chooser")
	}
	ha.Client().onConnectionError(NewError(ConnectionError, "disconnect before success returns"))
	var earlyReconnect = ha.reconnecting.Load()
	releaseOnce.Do(func() { close(release) })
	if err := <-result; err != nil {
		t.Fatalf("initial attempt = %v", err)
	}
	if earlyReconnect {
		_ = ha.Disconnect()
		t.Fatal("reconnect started before the successful attempt completed")
	}
	select {
	case <-chooser.retryAttempted:
	case <-time.After(time.Second):
		_ = ha.Disconnect()
		t.Fatal("lost connection was not handed off to reconnect")
	}
}

func TestHAReconnectNotificationDoesNotHoldAttemptLock(t *testing.T) {
	var uri, observed, serverDone, stopServer = lifecycleRecoveryServer(t, true)
	var release = make(chan struct{})
	var releaseOnce sync.Once
	var emptyURI string
	var chooser = &lifecycleChooser{fixedChooser: &fixedChooser{uri: uri}, successEntered: make(chan struct{}), successRelease: release, retryURI: &emptyURI}
	var ha = NewHAClient("reentrant-reconnect-listener").SetTimeout(200 * time.Millisecond)
	ha.SetServerChooser(chooser)
	ha.Client().SetHeartbeat(0)
	var manager = NewDefaultSubscriptionManager()
	manager.Subscribe(nil, NewCommand("subscribe").SetTopic("orders").SetSubID("retained"), AckTypeNone)
	ha.Client().SetSubscriptionManager(manager)
	var callbackResult = make(chan bool, 1)
	var nestedDone = make(chan struct{})
	var nestedStarted = make(chan struct{})
	var notified atomic.Bool
	ha.Client().AddConnectionStateListener(ConnectionStateListenerFunc(func(state ConnectionState) {
		if state != ConnectionStateReconnecting || !notified.CompareAndSwap(false, true) {
			return
		}
		var nested = make(chan error, 1)
		close(nestedStarted)
		go func() { defer close(nestedDone); nested <- ha.ConnectAndLogon() }()
		select {
		case <-nested:
			callbackResult <- true
		case <-time.After(300 * time.Millisecond):
			callbackResult <- false
		}
	}))
	var result = make(chan error, 1)
	var resultDone = make(chan struct{})
	go func() { defer close(resultDone); result <- ha.ConnectAndLogon() }()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		_ = ha.Disconnect()
		stopServer()
		select {
		case <-resultDone:
		case <-time.After(time.Second):
			t.Error("initial attempt did not stop during cleanup")
		}
		select {
		case <-serverDone:
		case <-time.After(time.Second):
			t.Error("test server did not stop during cleanup")
		}
		select {
		case <-nestedStarted:
			select {
			case <-nestedDone:
			case <-time.After(time.Second):
				t.Error("nested attempt did not stop during cleanup")
			}
		default:
		}
		waitHALifecycleStopped(t, ha)
	})
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("recovery subscribe did not reach server")
	}
	select {
	case <-chooser.successEntered:
	case <-time.After(time.Second):
		t.Fatal("successful attempt did not reach chooser")
	}
	ha.Client().onConnectionError(NewError(ConnectionError, "disconnect before success returns"))
	releaseOnce.Do(func() { close(release) })
	select {
	case completed := <-callbackResult:
		if !completed {
			t.Fatal("reconnect listener could not call ConnectAndLogon while notified")
		}
	case <-time.After(time.Second):
		t.Fatal("reconnect listener was not called")
	}
	if err := <-result; err != nil {
		t.Fatalf("initial attempt = %v", err)
	}
	<-nestedDone
}

func TestHAReconnectListenerCanRestoreSession(t *testing.T) {
	var uri, observed, serverDone, stopServer = lifecycleRecoveryServer(t, true)
	var chooser = &lifecycleChooser{fixedChooser: &fixedChooser{uri: uri}}
	var ha = NewHAClient("reentrant-reconnect-success").SetTimeout(200 * time.Millisecond)
	ha.SetServerChooser(chooser)
	ha.Client().SetHeartbeat(0)
	var manager = NewDefaultSubscriptionManager()
	manager.Subscribe(nil, NewCommand("subscribe").SetTopic("orders").SetSubID("retained"), AckTypeNone)
	ha.Client().SetSubscriptionManager(manager)
	var result = make(chan error, 1)
	var notified atomic.Bool
	ha.Client().AddConnectionStateListener(ConnectionStateListenerFunc(func(state ConnectionState) {
		if state == ConnectionStateReconnecting && notified.CompareAndSwap(false, true) {
			result <- ha.ConnectAndLogon()
		}
	}))
	var handlerDone = make(chan struct{})
	go func() {
		defer close(handlerDone)
		ha.handleDisconnect(NewError(ConnectionError, "connection lost"))
	}()
	t.Cleanup(func() {
		_ = ha.Disconnect()
		stopServer()
		select {
		case <-handlerDone:
		case <-time.After(time.Second):
			t.Error("disconnect handler did not stop during cleanup")
		}
		select {
		case <-serverDone:
		case <-time.After(time.Second):
			t.Error("test server did not stop during cleanup")
		}
		waitHALifecycleStopped(t, ha)
	})
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("listener did not restore the subscription")
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("listener's ConnectAndLogon = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("listener's ConnectAndLogon did not return")
	}
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("disconnect handler did not return")
	}
	if ha.reconnecting.Load() || ha.stopped.Load() || !ha.Client().connected.Load() || chooser.attempts.Load() != 1 {
		t.Fatalf("restored session disturbed by stale worker: reconnecting=%v stopped=%v connected=%v attempts=%d", ha.reconnecting.Load(), ha.stopped.Load(), ha.Client().connected.Load(), chooser.attempts.Load())
	}
}

func TestHAReconnectWorkerDoesNotReplaceConcurrentSession(t *testing.T) {
	var uri, observed, serverDone, stopServer = lifecycleRecoveryServer(t, true)
	var release = make(chan struct{})
	var releaseOnce sync.Once
	var chooser = &lifecyclePausedChooser{lifecycleChooser: &lifecycleChooser{fixedChooser: &fixedChooser{uri: uri}}, entered: make(chan struct{}), release: release}
	var ha = NewHAClient("concurrent-reconnect-success").SetTimeout(200 * time.Millisecond)
	ha.SetServerChooser(chooser)
	ha.SetReconnectDelayStrategy(errorReconnectStrategy{})
	ha.Client().SetHeartbeat(0)
	var manager = NewDefaultSubscriptionManager()
	manager.Subscribe(nil, NewCommand("subscribe").SetTopic("orders").SetSubID("retained"), AckTypeNone)
	ha.Client().SetSubscriptionManager(manager)
	var result = make(chan error, 1)
	var nestedDone = make(chan struct{})
	var nestedStarted = make(chan struct{})
	var listenerStop = make(chan struct{})
	var notified atomic.Bool
	ha.Client().AddConnectionStateListener(ConnectionStateListenerFunc(func(state ConnectionState) {
		if state == ConnectionStateReconnecting && notified.CompareAndSwap(false, true) {
			close(nestedStarted)
			go func() { defer close(nestedDone); result <- ha.ConnectAndLogon() }()
			select {
			case <-chooser.entered:
			case <-listenerStop:
			}
		}
	}))
	var handlerDone = make(chan struct{})
	go func() {
		defer close(handlerDone)
		ha.handleDisconnect(NewError(ConnectionError, "connection lost"))
	}()
	t.Cleanup(func() {
		close(listenerStop)
		releaseOnce.Do(func() { close(release) })
		_ = ha.Disconnect()
		stopServer()
		select {
		case <-handlerDone:
		case <-time.After(time.Second):
			t.Error("disconnect handler did not stop during cleanup")
		}
		select {
		case <-nestedStarted:
			select {
			case <-nestedDone:
			case <-time.After(time.Second):
				t.Error("concurrent attempt did not stop during cleanup")
			}
		default:
		}
		select {
		case <-serverDone:
		case <-time.After(time.Second):
			t.Error("test server did not stop during cleanup")
		}
		waitHALifecycleStopped(t, ha)
	})
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("disconnect handler did not return")
	}
	if ha.Client().connected.Load() || !ha.reconnecting.Load() {
		t.Fatal("background reconnect was not queued while the session was disconnected")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("concurrent session did not subscribe")
	}
	if err := <-result; err != nil {
		t.Fatalf("concurrent attempt = %v", err)
	}
	waitHALifecycleStopped(t, ha)
	if ha.stopped.Load() || !ha.Client().connected.Load() || chooser.attempts.Load() != 1 {
		t.Fatalf("restored session disturbed by queued worker: stopped=%v connected=%v attempts=%d", ha.stopped.Load(), ha.Client().connected.Load(), chooser.attempts.Load())
	}
}

func TestHACancelledOldWorkerHandsOffNewSessionDisconnect(t *testing.T) {
	for _, duringSuccess := range []bool{true, false} {
		var name = "after-success"
		if duringSuccess {
			name = "during-success"
		}
		t.Run(name, func(t *testing.T) {
			var uri, observed, serverDone, stopServer = lifecycleRecoveryServer(t, true)
			var successRelease, exceptionRelease = make(chan struct{}), make(chan struct{})
			var successOnce, exceptionOnce sync.Once
			var emptyURI string
			var chooser = &lifecycleChooser{fixedChooser: &fixedChooser{uri: uri}, successEntered: make(chan struct{}), successRelease: successRelease, retryAttempted: make(chan struct{}), retryURI: &emptyURI}
			var ha = NewHAClient("cancelled-worker-handoff").SetTimeout(200 * time.Millisecond)
			ha.SetServerChooser(&fixedChooser{})
			ha.Client().SetHeartbeat(0)
			var manager = NewDefaultSubscriptionManager()
			manager.Subscribe(nil, NewCommand("subscribe").SetTopic("orders").SetSubID("retained"), AckTypeNone)
			ha.Client().SetSubscriptionManager(manager)
			var exceptionEntered = make(chan struct{})
			var reported atomic.Bool
			ha.Client().SetExceptionListener(ExceptionListenerFunc(func(error) {
				if reported.CompareAndSwap(false, true) {
					close(exceptionEntered)
					<-exceptionRelease
				}
			}))
			var reconnectEvents atomic.Int32
			ha.Client().AddConnectionStateListener(ConnectionStateListenerFunc(func(state ConnectionState) {
				if state == ConnectionStateReconnecting {
					reconnectEvents.Add(1)
				}
			}))
			var handlerDone = make(chan struct{})
			go func() {
				defer close(handlerDone)
				ha.handleDisconnect(NewError(ConnectionError, "old session lost"))
			}()
			var result = make(chan error, 1)
			var resultDone = make(chan struct{})
			var started bool
			t.Cleanup(func() {
				successOnce.Do(func() { close(successRelease) })
				exceptionOnce.Do(func() { close(exceptionRelease) })
				_ = ha.Disconnect()
				stopServer()
				select {
				case <-handlerDone:
				case <-time.After(time.Second):
					t.Error("disconnect handler did not stop during cleanup")
				}
				if started {
					select {
					case <-resultDone:
					case <-time.After(time.Second):
						t.Error("new attempt did not stop during cleanup")
					}
				}
				select {
				case <-serverDone:
				case <-time.After(time.Second):
					t.Error("test server did not stop during cleanup")
				}
				waitHALifecycleStopped(t, ha)
			})
			select {
			case <-exceptionEntered:
			case <-time.After(time.Second):
				t.Fatal("old worker did not reach its exception callback")
			}
			_ = ha.Disconnect()
			ha.SetServerChooser(chooser)
			started = true
			go func() { defer close(resultDone); result <- ha.ConnectAndLogon() }()
			select {
			case <-observed:
			case <-time.After(time.Second):
				t.Fatal("new session did not subscribe")
			}
			select {
			case <-chooser.successEntered:
			case <-time.After(time.Second):
				t.Fatal("new session did not reach its success callback")
			}
			if duringSuccess {
				ha.Client().onConnectionError(NewError(ConnectionError, "new session lost before success returned"))
			}
			successOnce.Do(func() { close(successRelease) })
			if err := <-result; err != nil {
				t.Fatalf("new attempt = %v", err)
			}
			if !duringSuccess {
				ha.Client().onConnectionError(NewError(ConnectionError, "new session lost after success returned"))
			}
			exceptionOnce.Do(func() { close(exceptionRelease) })
			select {
			case <-chooser.retryAttempted:
			case <-time.After(time.Second):
				t.Fatal("old cancelled worker dropped the new session's disconnect")
			}
			if reconnectEvents.Load() != 2 || chooser.attempts.Load() != 2 {
				t.Fatalf("handoff events=%d, new-session attempts=%d; want 2 each", reconnectEvents.Load(), chooser.attempts.Load())
			}
		})
	}
}

func TestHAStaleQueuedAttemptCannotOverrideNewSession(t *testing.T) {
	for _, freshFirst := range []bool{false, true} {
		var name = "stale-first"
		if freshFirst {
			name = "fresh-first"
		}
		t.Run(name, func(t *testing.T) {
			var uri, observed, serverDone, stopServer = lifecycleRecoveryServer(t, true)
			var chooser = &lifecycleChooser{fixedChooser: &fixedChooser{uri: uri}}
			var ha = NewHAClient("stale-queued-attempt").SetTimeout(200 * time.Millisecond)
			ha.SetServerChooser(chooser)
			ha.Client().SetHeartbeat(0)
			var manager = NewDefaultSubscriptionManager()
			manager.Subscribe(nil, NewCommand("subscribe").SetTopic("orders").SetSubID("retained"), AckTypeNone)
			ha.Client().SetSubscriptionManager(manager)
			ha.lock.Lock()
			var staleEpoch = ha.disconnectEpoch
			ha.lock.Unlock()
			var proceed = make(chan struct{})
			var proceedOnce sync.Once
			var oldResult, freshResult = make(chan error, 1), make(chan error, 1)
			var oldDone, freshDone = make(chan struct{}), make(chan struct{})
			go func() {
				defer close(oldDone)
				<-proceed
				oldResult <- ha.connectAndLogonAttempt(context.Background(), true, staleEpoch)
			}()
			var freshStarted bool
			var startFresh = func() {
				freshStarted = true
				go func() { defer close(freshDone); freshResult <- ha.ConnectAndLogon() }()
			}
			t.Cleanup(func() {
				proceedOnce.Do(func() { close(proceed) })
				_ = ha.Disconnect()
				stopServer()
				select {
				case <-oldDone:
				case <-time.After(time.Second):
					t.Error("stale attempt did not stop during cleanup")
				}
				if freshStarted {
					select {
					case <-freshDone:
					case <-time.After(time.Second):
						t.Error("new attempt did not stop during cleanup")
					}
				}
				select {
				case <-serverDone:
				case <-time.After(time.Second):
					t.Error("test server did not stop during cleanup")
				}
				waitHALifecycleStopped(t, ha)
			})
			_ = ha.Disconnect()
			if !freshFirst {
				proceedOnce.Do(func() { close(proceed) })
				if err := <-oldResult; !IsErrorKind(err, DisconnectedError) {
					t.Fatalf("stale attempt = %v", err)
				}
			}
			startFresh()
			select {
			case <-observed:
			case <-time.After(time.Second):
				t.Fatal("new session did not subscribe")
			}
			if err := <-freshResult; err != nil {
				t.Fatalf("new attempt = %v", err)
			}
			if freshFirst {
				proceedOnce.Do(func() { close(proceed) })
				if err := <-oldResult; !IsErrorKind(err, DisconnectedError) {
					t.Fatalf("stale attempt = %v", err)
				}
			}
			if ha.stopped.Load() || !ha.Client().connected.Load() || chooser.attempts.Load() != 1 {
				t.Fatalf("stale attempt disturbed new session: stopped=%v connected=%v attempts=%d", ha.stopped.Load(), ha.Client().connected.Load(), chooser.attempts.Load())
			}
		})
	}
}
