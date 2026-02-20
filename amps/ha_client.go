package amps

import (
	"sync"
	"time"
)

type HAClient struct {
	lock                   sync.Mutex
	client                 *Client
	timeout                time.Duration
	reconnectDelay         time.Duration
	reconnectDelayStrategy ReconnectDelayStrategy
	logonOptions           LogonParams
	hasLogonOptions        bool
	serverChooser          ServerChooser
	reconnecting           bool
	stopped                bool
}

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

	ha.lock.Lock()
	if ha.stopped || ha.reconnecting {
		ha.lock.Unlock()
		return
	}
	ha.reconnecting = true
	ha.lock.Unlock()

	go func() {
		defer func() {
			ha.lock.Lock()
			ha.reconnecting = false
			ha.lock.Unlock()
		}()
		_ = ha.ConnectAndLogon()
		_ = err
	}()
}

func (ha *HAClient) connectAndLogonOnce(uri string, authenticator Authenticator) error {
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

	ha.lock.Lock()
	options := ha.logonOptions
	hasLogonOptions := ha.hasLogonOptions
	ha.lock.Unlock()
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

func (ha *HAClient) reconnectWait(uri string) (time.Duration, error) {
	if ha == nil {
		return 0, nil
	}

	ha.lock.Lock()
	strategy := ha.reconnectDelayStrategy
	delay := ha.reconnectDelay
	ha.lock.Unlock()

	if strategy != nil {
		return strategy.GetConnectWaitDuration(uri)
	}
	return delay, nil
}

func (ha *HAClient) ConnectAndLogon() error {
	if ha == nil || ha.client == nil {
		return NewError(CommandError, "nil HAClient")
	}

	ha.lock.Lock()
	chooser := ha.serverChooser
	timeout := ha.timeout
	ha.lock.Unlock()

	deadline := time.Time{}
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}

	var lastErr error
	for {
		ha.lock.Lock()
		stopped := ha.stopped
		ha.lock.Unlock()
		if stopped {
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

		if err := ha.connectAndLogonOnce(uri, authenticator); err == nil {
			if chooser != nil {
				chooser.ReportSuccess(ha.client.GetConnectionInfo())
			}

			ha.lock.Lock()
			strategy := ha.reconnectDelayStrategy
			ha.lock.Unlock()
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

		wait, delayErr := ha.reconnectWait(uri)
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
			time.Sleep(wait)
		}
	}
}

func (ha *HAClient) Disconnected() bool {
	if ha == nil || ha.client == nil {
		return true
	}
	return !ha.client.connected
}

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

func (ha *HAClient) Timeout() time.Duration {
	if ha == nil {
		return 0
	}
	ha.lock.Lock()
	defer ha.lock.Unlock()
	return ha.timeout
}

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

func (ha *HAClient) ReconnectDelay() time.Duration {
	if ha == nil {
		return 0
	}
	ha.lock.Lock()
	defer ha.lock.Unlock()
	return ha.reconnectDelay
}

func (ha *HAClient) SetReconnectDelayStrategy(strategy ReconnectDelayStrategy) *HAClient {
	if ha == nil {
		return ha
	}
	ha.lock.Lock()
	ha.reconnectDelayStrategy = strategy
	ha.lock.Unlock()
	return ha
}

func (ha *HAClient) ReconnectDelayStrategy() ReconnectDelayStrategy {
	if ha == nil {
		return nil
	}
	ha.lock.Lock()
	defer ha.lock.Unlock()
	return ha.reconnectDelayStrategy
}

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

func (ha *HAClient) LogonOptions() LogonParams {
	if ha == nil {
		return LogonParams{}
	}
	ha.lock.Lock()
	defer ha.lock.Unlock()
	return ha.logonOptions
}

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

func (ha *HAClient) ServerChooser() ServerChooser {
	if ha == nil {
		return nil
	}
	ha.lock.Lock()
	defer ha.lock.Unlock()
	return ha.serverChooser
}

func (ha *HAClient) GetConnectionInfo() ConnectionInfo {
	if ha == nil || ha.client == nil {
		return ConnectionInfo{}
	}
	return ha.client.GetConnectionInfo()
}

func (ha *HAClient) GatherConnectionInfo() ConnectionInfo {
	return ha.GetConnectionInfo()
}

func (ha *HAClient) Client() *Client {
	if ha == nil {
		return nil
	}
	return ha.client
}

func (ha *HAClient) Disconnect() error {
	if ha == nil || ha.client == nil {
		return nil
	}
	ha.lock.Lock()
	ha.stopped = true
	ha.lock.Unlock()
	return ha.client.Disconnect()
}

func (ha *HAClient) SetDisconnectHandler(handler func(*HAClient, error)) error {
	_ = handler
	return NewError(CommandError, "HAClient manages disconnect handling internally")
}

func CreateMemoryBackedHAClient(clientName ...string) *HAClient {
	ha := NewHAClient(clientName...)
	if ha != nil && ha.client != nil {
		ha.client.SetPublishStore(NewMemoryPublishStore())
		ha.client.SetBookmarkStore(NewMemoryBookmarkStore())
	}
	return ha
}

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
