package amps

import (
	"fmt"
	"net"
	"reflect"
	"strconv"
	"sync"
	"time"
)

// ConnectionInfo describes connection metadata similarly to the C++ client.
type ConnectionInfo map[string]string

// ConnectionState mirrors C++ connection state listener events.
type ConnectionState int

const (
	ConnectionStateDisconnected       ConnectionState = 0
	ConnectionStateShutdown           ConnectionState = 1
	ConnectionStateConnected          ConnectionState = 2
	ConnectionStateLoggedOn           ConnectionState = 4
	ConnectionStatePublishReplayed    ConnectionState = 8
	ConnectionStateHeartbeatInitiated ConnectionState = 16
	ConnectionStateResubscribed       ConnectionState = 32
	ConnectionStateUnknown            ConnectionState = 16384
)

// ConnectionStateListener receives connection state updates.
type ConnectionStateListener interface {
	ConnectionStateChanged(ConnectionState)
}

// ConnectionStateListenerFunc adapts a function to ConnectionStateListener.
type ConnectionStateListenerFunc func(ConnectionState)

func (f ConnectionStateListenerFunc) ConnectionStateChanged(state ConnectionState) { f(state) }

// ExceptionListener receives unhandled background errors.
type ExceptionListener interface {
	ExceptionThrown(error)
}

// ExceptionListenerFunc adapts a function to ExceptionListener.
type ExceptionListenerFunc func(error)

func (f ExceptionListenerFunc) ExceptionThrown(err error) { f(err) }

// FailedWriteHandler receives failed publish write reports.
type FailedWriteHandler interface {
	FailedWrite(message *Message, reason string)
}

// FailedWriteHandlerFunc adapts a function to FailedWriteHandler.
type FailedWriteHandlerFunc func(message *Message, reason string)

func (f FailedWriteHandlerFunc) FailedWrite(message *Message, reason string) { f(message, reason) }

// FailedResubscribeHandler is called when resubscribe fails.
type FailedResubscribeHandler interface {
	Failure(command *Command, requestedAckTypes int, err error) bool
}

// FailedResubscribeHandlerFunc adapts a function to FailedResubscribeHandler.
type FailedResubscribeHandlerFunc func(command *Command, requestedAckTypes int, err error) bool

func (f FailedResubscribeHandlerFunc) Failure(command *Command, requestedAckTypes int, err error) bool {
	return f(command, requestedAckTypes, err)
}

// SubscriptionManager tracks subscriptions for reconnect/resubscribe behavior.
type SubscriptionManager interface {
	Subscribe(messageHandler func(*Message) error, command *Command, requestedAckTypes int)
	Unsubscribe(subID string)
	Clear()
	Resubscribe(client *Client) error
	SetFailedResubscribeHandler(handler FailedResubscribeHandler)
}

// ServerChooser provides URI selection for HA connections.
type ServerChooser interface {
	CurrentURI() string
	CurrentAuthenticator() Authenticator
	ReportFailure(err error, info ConnectionInfo)
	ReportSuccess(info ConnectionInfo)
	Error() string
	Add(uri string) ServerChooser
	Remove(uri string)
}

// ReconnectDelayStrategy controls delay between reconnect attempts.
type ReconnectDelayStrategy interface {
	GetConnectWaitDuration(uri string) (time.Duration, error)
	Reset()
}

// PublishStore tracks outgoing publish commands for replay.
type PublishStore interface {
	Store(command *Command) (uint64, error)
	DiscardUpTo(sequence uint64) error
	Replay(replayer func(*Command) error) error
	ReplaySingle(replayer func(*Command) error, sequence uint64) (bool, error)
	UnpersistedCount() int
	Flush(timeout time.Duration) error
	GetLowestUnpersisted() uint64
	GetLastPersisted() uint64
	SetErrorOnPublishGap(enabled bool)
	ErrorOnPublishGap() bool
}

// BookmarkStore tracks processed bookmarks for bookmark subscriptions.
type BookmarkStore interface {
	Log(message *Message) uint64
	Discard(subID string, bookmarkSeqNo uint64)
	DiscardMessage(message *Message)
	GetMostRecent(subID string) string
	IsDiscarded(message *Message) bool
	Purge(subID ...string)
	GetOldestBookmarkSeq(subID string) uint64
	Persisted(subID string, bookmark string) string
	SetServerVersion(version string)
}

// TransportFilterDirection identifies data flow direction for transport filters.
type TransportFilterDirection int

const (
	TransportFilterInbound TransportFilterDirection = iota
	TransportFilterOutbound
)

// TransportFilter receives raw transport payload bytes and returns the payload to use.
type TransportFilter func(direction TransportFilterDirection, payload []byte) []byte

type retryCommand struct {
	command        *Command
	messageHandler func(*Message) error
}

type pendingAckBatch struct {
	topic     string
	subID     string
	bookmarks []string
}

type trackedSubscription struct {
	messageHandler    func(*Message) error
	command           *Command
	requestedAckTypes int
}

type clientParityState struct {
	uri                    string
	retryOnDisconnect      bool
	defaultMaxDepth        uint
	autoAck                bool
	ackBatchSize           uint
	ackTimeout             time.Duration
	pendingAcks            map[string]*pendingAckBatch
	pendingAckCount        uint
	ackTimer               *time.Timer
	bookmarkStore          BookmarkStore
	publishStore           PublishStore
	subscriptionManager    SubscriptionManager
	duplicateHandler       func(*Message) error
	exceptionListener      ExceptionListener
	failedWriteHandler     FailedWriteHandler
	unhandledHandler       func(*Message) error
	lastChanceHandler      func(*Message) error
	globalCommandHandlers  map[int]func(*Message) error
	connectionListeners    map[uintptr]ConnectionStateListener
	httpPreflightHeaders   []string
	transportFilter        TransportFilter
	receiveRoutineCallback func()
	pendingRetry           []retryCommand
	pendingPublishByCmdID  map[string]*Command
	noResubscribeRoutes    map[string]struct{}
	internalDisconnect     func(error)
	manualDisconnect       bool
	lock                   sync.Mutex
}

var clientParityStates sync.Map

func ensureClientState(client *Client) *clientParityState {
	if client == nil {
		return nil
	}

	if state, ok := clientParityStates.Load(client); ok {
		return state.(*clientParityState)
	}

	state := &clientParityState{
		retryOnDisconnect:     false,
		defaultMaxDepth:       0,
		autoAck:               false,
		ackBatchSize:          1,
		ackTimeout:            time.Second,
		pendingAcks:           make(map[string]*pendingAckBatch),
		globalCommandHandlers: make(map[int]func(*Message) error),
		connectionListeners:   make(map[uintptr]ConnectionStateListener),
		pendingPublishByCmdID: make(map[string]*Command),
		noResubscribeRoutes:   make(map[string]struct{}),
	}

	actual, _ := clientParityStates.LoadOrStore(client, state)
	return actual.(*clientParityState)
}

func forgetClientState(client *Client) {
	if client == nil {
		return
	}
	clientParityStates.Delete(client)
}

func listenerKey(listener ConnectionStateListener) uintptr {
	if listener == nil {
		return 0
	}
	val := reflect.ValueOf(listener)
	if !val.IsValid() {
		return 0
	}
	if val.Kind() == reflect.Pointer || val.Kind() == reflect.UnsafePointer || val.Kind() == reflect.Func || val.Kind() == reflect.Map || val.Kind() == reflect.Slice || val.Kind() == reflect.Chan {
		if val.Kind() != reflect.UnsafePointer && val.IsNil() {
			return 0
		}
		return val.Pointer()
	}
	return uintptr(unsafeStringHash(fmt.Sprintf("%T:%v", listener, listener)))
}

func unsafeStringHash(value string) uint64 {
	var result uint64 = 1469598103934665603
	for i := 0; i < len(value); i++ {
		result ^= uint64(value[i])
		result *= 1099511628211
	}
	return result
}

func (state *clientParityState) broadcastConnectionState(newState ConnectionState) {
	if state == nil {
		return
	}

	state.lock.Lock()
	listeners := make([]ConnectionStateListener, 0, len(state.connectionListeners))
	for _, listener := range state.connectionListeners {
		listeners = append(listeners, listener)
	}
	exceptionListener := state.exceptionListener
	state.lock.Unlock()

	for _, listener := range listeners {
		func(listener ConnectionStateListener) {
			defer func() {
				if recovered := recover(); recovered != nil && exceptionListener != nil {
					exceptionListener.ExceptionThrown(fmt.Errorf("connection state listener panic: %v", recovered))
				}
			}()
			listener.ConnectionStateChanged(newState)
		}(listener)
	}
}

func (client *Client) buildConnectionInfo() ConnectionInfo {
	info := ConnectionInfo{}
	if client == nil {
		return info
	}

	state := ensureClientState(client)
	if state != nil {
		state.lock.Lock()
		info["uri"] = state.uri
		state.lock.Unlock()
	}

	if client.url != nil {
		info["scheme"] = client.url.Scheme
		info["host"] = client.url.Host
		info["path"] = client.url.Path
		if info["uri"] == "" {
			info["uri"] = client.url.String()
		}
	}
	if client.connection != nil {
		local := client.connection.LocalAddr()
		remote := client.connection.RemoteAddr()
		if local != nil {
			info["local_addr"] = local.String()
		}
		if remote != nil {
			info["remote_addr"] = remote.String()
			if host, port, err := net.SplitHostPort(remote.String()); err == nil {
				info["remote_host"] = host
				info["remote_port"] = port
			}
		}
	}
	if client.serverVersion != "" {
		info["server_version"] = client.serverVersion
		if versionNum := ConvertVersionToNumber(client.serverVersion); versionNum > 0 {
			info["server_version_number"] = strconv.FormatUint(versionNum, 10)
		}
	}
	info["client_name"] = client.clientName
	return info
}

func (client *Client) applyTransportFilter(direction TransportFilterDirection, payload []byte) (result []byte) {
	result = payload

	state := ensureClientState(client)
	if state == nil {
		return result
	}

	state.lock.Lock()
	filter := state.transportFilter
	exceptionListener := state.exceptionListener
	state.lock.Unlock()

	if filter == nil {
		return result
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			result = payload
			if exceptionListener != nil {
				exceptionListener.ExceptionThrown(fmt.Errorf("transport filter panic: %v", recovered))
			}
		}
	}()

	filtered := filter(direction, payload)
	if filtered != nil {
		result = filtered
	}
	return result
}

func (client *Client) callReceiveRoutineStartedCallback() {
	state := ensureClientState(client)
	if state == nil {
		return
	}

	state.lock.Lock()
	callback := state.receiveRoutineCallback
	exceptionListener := state.exceptionListener
	state.lock.Unlock()

	if callback == nil {
		return
	}

	defer func() {
		if recovered := recover(); recovered != nil && exceptionListener != nil {
			exceptionListener.ExceptionThrown(fmt.Errorf("receive routine callback panic: %v", recovered))
		}
	}()

	callback()
}
