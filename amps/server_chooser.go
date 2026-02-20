package amps

import "sync"

type chooserEndpoint struct {
	uri           string
	authenticator Authenticator
}

// DefaultServerChooser chooses servers in round-robin order.
type DefaultServerChooser struct {
	lock       sync.Mutex
	endpoints  []chooserEndpoint
	index      int
	lastError  string
}

// NewDefaultServerChooser creates a new chooser with optional URIs.
func NewDefaultServerChooser(uris ...string) *DefaultServerChooser {
	chooser := &DefaultServerChooser{
		endpoints: make([]chooserEndpoint, 0, len(uris)),
	}
	for _, uri := range uris {
		chooser.Add(uri)
	}
	return chooser
}

// CurrentURI returns the currently selected URI.
func (chooser *DefaultServerChooser) CurrentURI() string {
	if chooser == nil {
		return ""
	}
	chooser.lock.Lock()
	defer chooser.lock.Unlock()
	if len(chooser.endpoints) == 0 {
		return ""
	}
	if chooser.index < 0 || chooser.index >= len(chooser.endpoints) {
		chooser.index = 0
	}
	return chooser.endpoints[chooser.index].uri
}

// CurrentAuthenticator returns authenticator associated with CurrentURI.
func (chooser *DefaultServerChooser) CurrentAuthenticator() Authenticator {
	if chooser == nil {
		return nil
	}
	chooser.lock.Lock()
	defer chooser.lock.Unlock()
	if len(chooser.endpoints) == 0 {
		return nil
	}
	if chooser.index < 0 || chooser.index >= len(chooser.endpoints) {
		chooser.index = 0
	}
	return chooser.endpoints[chooser.index].authenticator
}

// ReportFailure reports a connection failure and advances chooser.
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

// ReportSuccess reports successful connection.
func (chooser *DefaultServerChooser) ReportSuccess(info ConnectionInfo) {
	if chooser == nil {
		return
	}
	chooser.lock.Lock()
	chooser.lastError = ""
	chooser.lock.Unlock()
	_ = info
}

// Error returns latest chooser error.
func (chooser *DefaultServerChooser) Error() string {
	if chooser == nil {
		return ""
	}
	chooser.lock.Lock()
	defer chooser.lock.Unlock()
	return chooser.lastError
}

// Add adds URI to chooser and returns chooser for chaining.
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

// AddWithAuthenticator adds URI with explicit authenticator.
func (chooser *DefaultServerChooser) AddWithAuthenticator(uri string, authenticator Authenticator) *DefaultServerChooser {
	if chooser == nil || uri == "" {
		return chooser
	}
	chooser.lock.Lock()
	chooser.endpoints = append(chooser.endpoints, chooserEndpoint{uri: uri, authenticator: authenticator})
	chooser.lock.Unlock()
	return chooser
}

// Remove removes URI from chooser.
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
