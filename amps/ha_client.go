package amps

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// HAClient wraps Client with reconnect, replay, and failover behavior.
type HAClient struct {
	lock                   sync.Mutex
	connectAndLogonLock    sync.Mutex
	client                 *Client
	timeout                time.Duration
	reconnectDelay         time.Duration
	reconnectDelayStrategy ReconnectDelayStrategy
	logonOptions           LogonParams
	hasLogonOptions        bool
	serverChooser          ServerChooser
	reconnecting           atomic.Bool
	stopped                atomic.Bool
	reconnectCancel        context.CancelFunc
}

// NewHAClient returns a new HAClient.
func NewHAClient(clientName ...string) *HAClient {
	client := NewClient(clientName...)
	client.SetRetryOnDisconnect(true)
	ha := &HAClient{
		client:                 client,
		timeout:                0,
		reconnectDelay:         time.Second,
		reconnectDelayStrategy: NewFixedDelayStrategy(time.Second),
		serverChooser:          NewDefaultServerChooser(),
	}
	client.setInternalDisconnectHandler(func(err error) {
		ha.handleDisconnect(err)
	})
	return ha
}

func (ha *HAClient) handleDisconnect(err error) {
	if ha == nil {
		return
	}

	for {
		if ha.stopped.Load() {
			return
		}
		if ha.reconnecting.CompareAndSwap(false, true) {
			break
		}
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	ha.lock.Lock()
	ha.reconnectCancel = cancel
	ha.lock.Unlock()

	go func() {
		defer func() {
			ha.reconnecting.Store(false)
			ha.lock.Lock()
			ha.reconnectCancel = nil
			ha.lock.Unlock()
		}()
		_ = ha.connectAndLogon(ctx)
		_ = err
	}()
}

func (ha *HAClient) connectAndLogonOnce(uri string, authenticator Authenticator, options LogonParams, hasLogonOptions bool) error {
	if ha == nil || ha.client == nil {
		return NewError(CommandError, "nil HAClient")
	}
	if uri == "" {
		return NewError(ConnectionError, "no URI available")
	}

	ha.client.markManualDisconnect(false)
	if err := ha.client.Connect(uri); err != nil {
		return err
	}
	if authenticator != nil {
		options.Authenticator = authenticator
	}

	var err error
	if hasLogonOptions || authenticator != nil {
		err = ha.client.Logon(options)
	} else {
		err = ha.client.Logon()
	}
	if err != nil {
		_ = ha.client.Disconnect()
	}
	return err
}

func (ha *HAClient) reconnectWait(strategy ReconnectDelayStrategy, delay time.Duration, uri string) (time.Duration, error) {
	if strategy != nil {
		return strategy.GetConnectWaitDuration(uri)
	}
	return delay, nil
}

// ConnectAndLogon loops through selected endpoints until connect and logon succeed or timeout occurs.
func (ha *HAClient) ConnectAndLogon() error {
	return ha.connectAndLogon(context.Background())
}

func (ha *HAClient) connectAndLogon(ctx context.Context) error {
	if ha == nil || ha.client == nil {
		return NewError(CommandError, "nil HAClient")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ha.lock.Lock()
	chooser := ha.serverChooser
	timeout := ha.timeout
	strategy := ha.reconnectDelayStrategy
	delay := ha.reconnectDelay
	options := ha.logonOptions
	hasLogonOptions := ha.hasLogonOptions
	ha.lock.Unlock()

	ha.connectAndLogonLock.Lock()
	defer ha.connectAndLogonLock.Unlock()

	deadline := time.Time{}
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}

	var lastErr error
	for {
		select {
		case <-ctx.Done():
			return NewError(DisconnectedError, "HAClient reconnect cancelled")
		default:
		}

		if ha.stopped.Load() {
			return NewError(DisconnectedError, "HAClient is stopped")
		}

		var uri string
		var authenticator Authenticator
		if chooser != nil {
			uri = chooser.CurrentURI()
			authenticator = chooser.CurrentAuthenticator()
		}
		if uri == "" {
			uri = ha.client.URI()
		}
		if uri == "" {
			return NewError(ConnectionError, "server chooser does not contain any URIs")
		}

		if err := ha.connectAndLogonOnce(uri, authenticator, options, hasLogonOptions); err == nil {
			if chooser != nil {
				chooser.ReportSuccess(ha.client.GetConnectionInfo())
			}
			if strategy != nil {
				strategy.Reset()
			}
			return nil
		} else {
			lastErr = err
			if chooser != nil {
				chooser.ReportFailure(err, ha.client.GetConnectionInfo())
			}
		}

		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return lastErr
		}

		wait, delayErr := ha.reconnectWait(strategy, delay, uri)
		if delayErr != nil {
			return delayErr
		}
		if wait > 0 {
			if !deadline.IsZero() {
				remaining := time.Until(deadline)
				if remaining <= 0 {
					return lastErr
				}
				if wait > remaining {
					wait = remaining
				}
			}
			select {
			case <-ctx.Done():
				return NewError(DisconnectedError, "HAClient reconnect cancelled")
			case <-time.After(wait):
			}
		}
	}
}

// Disconnected executes the exported disconnected operation.
func (ha *HAClient) Disconnected() bool {
	if ha == nil || ha.client == nil {
		return true
	}
	return !ha.client.connected.Load()
}

// SetTimeout sets timeout on the receiver.
func (ha *HAClient) SetTimeout(timeout time.Duration) *HAClient {
	if ha == nil {
		return ha
	}
	if timeout < 0 {
		timeout = 0
	}
	ha.lock.Lock()
	ha.timeout = timeout
	ha.lock.Unlock()
	return ha
}

// Timeout executes the exported timeout operation.
func (ha *HAClient) Timeout() time.Duration {
	if ha == nil {
		return 0
	}
	ha.lock.Lock()
	defer ha.lock.Unlock()
	return ha.timeout
}

// SetReconnectDelay sets reconnect delay on the receiver.
func (ha *HAClient) SetReconnectDelay(delay time.Duration) *HAClient {
	if ha == nil {
		return ha
	}
	if delay < 0 {
		delay = 0
	}
	ha.lock.Lock()
	ha.reconnectDelay = delay
	ha.reconnectDelayStrategy = NewFixedDelayStrategy(delay)
	ha.lock.Unlock()
	return ha
}

// ReconnectDelay executes the exported reconnectdelay operation.
func (ha *HAClient) ReconnectDelay() time.Duration {
	if ha == nil {
		return 0
	}
	ha.lock.Lock()
	defer ha.lock.Unlock()
	return ha.reconnectDelay
}

// SetReconnectDelayStrategy sets reconnect delay strategy on the receiver.
func (ha *HAClient) SetReconnectDelayStrategy(strategy ReconnectDelayStrategy) *HAClient {
	if ha == nil {
		return ha
	}
	ha.lock.Lock()
	ha.reconnectDelayStrategy = strategy
	ha.lock.Unlock()
	return ha
}

// ReconnectDelayStrategy executes the exported reconnectdelaystrategy operation.
func (ha *HAClient) ReconnectDelayStrategy() ReconnectDelayStrategy {
	if ha == nil {
		return nil
	}
	ha.lock.Lock()
	defer ha.lock.Unlock()
	return ha.reconnectDelayStrategy
}

// SetLogonOptions sets logon options on the receiver.
func (ha *HAClient) SetLogonOptions(options LogonParams) *HAClient {
	if ha == nil {
		return ha
	}
	ha.lock.Lock()
	ha.logonOptions = options
	ha.hasLogonOptions = true
	ha.lock.Unlock()
	return ha
}

// LogonOptions executes the exported logonoptions operation.
func (ha *HAClient) LogonOptions() LogonParams {
	if ha == nil {
		return LogonParams{}
	}
	ha.lock.Lock()
	defer ha.lock.Unlock()
	return ha.logonOptions
}

// SetServerChooser sets server chooser on the receiver.
func (ha *HAClient) SetServerChooser(chooser ServerChooser) *HAClient {
	if ha == nil {
		return ha
	}
	if chooser == nil {
		chooser = NewDefaultServerChooser()
	}
	ha.lock.Lock()
	ha.serverChooser = chooser
	ha.lock.Unlock()
	return ha
}

// ServerChooser executes the exported serverchooser operation.
func (ha *HAClient) ServerChooser() ServerChooser {
	if ha == nil {
		return nil
	}
	ha.lock.Lock()
	defer ha.lock.Unlock()
	return ha.serverChooser
}

// GetConnectionInfo returns the current connection info value.
func (ha *HAClient) GetConnectionInfo() ConnectionInfo {
	if ha == nil || ha.client == nil {
		return ConnectionInfo{}
	}
	return ha.client.GetConnectionInfo()
}

// GatherConnectionInfo executes the exported gatherconnectioninfo operation.
func (ha *HAClient) GatherConnectionInfo() ConnectionInfo {
	return ha.GetConnectionInfo()
}

// Client executes the exported client operation.
func (ha *HAClient) Client() *Client {
	if ha == nil {
		return nil
	}
	return ha.client
}

// Disconnect stops receive processing and closes the active connection.
func (ha *HAClient) Disconnect() error {
	if ha == nil || ha.client == nil {
		return nil
	}
	ha.stopped.Store(true)
	ha.lock.Lock()
	cancel := ha.reconnectCancel
	ha.reconnectCancel = nil
	ha.lock.Unlock()
	if cancel != nil {
		cancel()
	}
	return ha.client.Disconnect()
}

// SetDisconnectHandler sets disconnect handler on the receiver.
func (ha *HAClient) SetDisconnectHandler(handler func(*HAClient, error)) error {
	_ = handler
	return NewError(CommandError, "HAClient manages disconnect handling internally")
}

// CreateMemoryBackedHAClient creates a configured MemoryBackedHAClient.
func CreateMemoryBackedHAClient(clientName ...string) *HAClient {
	ha := NewHAClient(clientName...)
	if ha != nil && ha.client != nil {
		ha.client.SetPublishStore(NewMemoryPublishStore())
		ha.client.SetBookmarkStore(NewMemoryBookmarkStore())
	}
	return ha
}

// CreateMemoryBacked creates a configured memory-backed HA client.
func CreateMemoryBacked(clientName ...string) *HAClient {
	return CreateMemoryBackedHAClient(clientName...)
}

// CreateFileBackedHAClient creates a configured FileBackedHAClient.
func CreateFileBackedHAClient(args ...string) *HAClient {
	publishStorePath := "amps_publish_store.json"
	bookmarkStorePath := "amps_bookmark_store.json"
	clientName := []string{}

	if len(args) > 0 && args[0] != "" {
		publishStorePath = args[0]
	}
	if len(args) > 1 && args[1] != "" {
		bookmarkStorePath = args[1]
	}
	if len(args) > 2 && args[2] != "" {
		clientName = append(clientName, args[2])
	}

	ha := NewHAClient(clientName...)
	if ha != nil && ha.client != nil {
		ha.client.SetPublishStore(NewFilePublishStore(publishStorePath))
		ha.client.SetBookmarkStore(NewFileBookmarkStore(bookmarkStorePath))
	}
	return ha
}

// CreateFileBacked creates a configured file-backed HA client.
func CreateFileBacked(args ...string) *HAClient {
	return CreateFileBackedHAClient(args...)
}
