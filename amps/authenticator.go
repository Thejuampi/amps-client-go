package amps

// Authenticator defines behavior required by exported AMPS client components.
type Authenticator interface {
	Authenticate(username string, password string) (string, error)
	Retry(username string, password string) (string, error)
	Completed(username string, password string, reason string)
}

type _DefaultAuthenticator struct{}

// Authenticate executes the exported authenticate operation.
func (auth *_DefaultAuthenticator) Authenticate(username string, password string) (string, error) {
	return password, nil
}

// Retry executes the exported retry operation.
func (auth *_DefaultAuthenticator) Retry(username string, password string) (string, error) {
	return password, nil
}

// Completed executes the exported completed operation.
func (auth *_DefaultAuthenticator) Completed(username string, password string, reason string) {}
