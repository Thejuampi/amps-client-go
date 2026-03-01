package amps

import "sync"

type chooserEndpoint struct {
	uri           string
	authenticator Authenticator
}

// DefaultServerChooser holds endpoint selection state for HA reconnect attempts.
type DefaultServerChooser struct {
	lock      sync.Mutex
	endpoints []chooserEndpoint
	index     int
	lastError string
}

// NewDefaultServerChooser returns a new DefaultServerChooser.
func NewDefaultServerChooser(uris ...string) *DefaultServerChooser {
	chooser := &DefaultServerChooser{
		endpoints: make([]chooserEndpoint, 0, len(uris)),
	}
	for _, uri := range uris {
		_ = chooser.Add(uri)
	}
	return chooser
}

// CurrentURI executes the exported currenturi operation.
func (chooser *DefaultServerChooser) CurrentURI() string {
	var uri, _ = chooser.CurrentEndpoint()
	return uri
}

// CurrentEndpoint returns current URI/authenticator snapshot under a single lock.
func (chooser *DefaultServerChooser) CurrentEndpoint() (string, Authenticator) {
	if chooser == nil {
		return "", nil
	}
	chooser.lock.Lock()
	defer chooser.lock.Unlock()
	if len(chooser.endpoints) == 0 {
		return "", nil
	}
	if chooser.index < 0 || chooser.index >= len(chooser.endpoints) {
		chooser.index = 0
	}
	var endpoint = chooser.endpoints[chooser.index]
	return endpoint.uri, endpoint.authenticator
}

// CurrentAuthenticator executes the exported currentauthenticator operation.
func (chooser *DefaultServerChooser) CurrentAuthenticator() Authenticator {
	var _, authenticator = chooser.CurrentEndpoint()
	return authenticator
}

// ReportFailure executes the exported reportfailure operation.
func (chooser *DefaultServerChooser) ReportFailure(err error, info ConnectionInfo) {
	if chooser == nil {
		return
	}
	chooser.lock.Lock()
	defer chooser.lock.Unlock()
	if err != nil {
		chooser.lastError = err.Error()
	}
	if len(chooser.endpoints) > 0 {
		chooser.index = (chooser.index + 1) % len(chooser.endpoints)
	}
	_ = info
}

// ReportSuccess executes the exported reportsuccess operation.
func (chooser *DefaultServerChooser) ReportSuccess(info ConnectionInfo) {
	if chooser == nil {
		return
	}
	chooser.lock.Lock()
	chooser.lastError = ""
	chooser.lock.Unlock()
	_ = info
}

// Error executes the exported error operation.
func (chooser *DefaultServerChooser) Error() string {
	if chooser == nil {
		return ""
	}
	chooser.lock.Lock()
	defer chooser.lock.Unlock()
	return chooser.lastError
}

// Add executes the exported add operation.
func (chooser *DefaultServerChooser) Add(uri string) ServerChooser {
	if chooser == nil {
		return chooser
	}
	if uri == "" {
		return chooser
	}
	chooser.lock.Lock()
	chooser.endpoints = append(chooser.endpoints, chooserEndpoint{uri: uri})
	chooser.lock.Unlock()
	return chooser
}

// AddWithAuthenticator adds with authenticator behavior on the receiver.
func (chooser *DefaultServerChooser) AddWithAuthenticator(uri string, authenticator Authenticator) *DefaultServerChooser {
	if chooser == nil || uri == "" {
		return chooser
	}
	chooser.lock.Lock()
	chooser.endpoints = append(chooser.endpoints, chooserEndpoint{uri: uri, authenticator: authenticator})
	chooser.lock.Unlock()
	return chooser
}

// Remove executes the exported remove operation.
func (chooser *DefaultServerChooser) Remove(uri string) {
	if chooser == nil || uri == "" {
		return
	}
	chooser.lock.Lock()
	defer chooser.lock.Unlock()

	if len(chooser.endpoints) == 0 {
		return
	}

	filtered := make([]chooserEndpoint, 0, len(chooser.endpoints))
	for _, endpoint := range chooser.endpoints {
		if endpoint.uri != uri {
			filtered = append(filtered, endpoint)
		}
	}
	chooser.endpoints = filtered
	if len(chooser.endpoints) == 0 {
		chooser.index = 0
		return
	}
	if chooser.index >= len(chooser.endpoints) {
		chooser.index = 0
	}
}
