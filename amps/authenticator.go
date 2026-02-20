package amps

type Authenticator interface {
	Authenticate(username string, password string) (string, error)
	Retry(username string, password string) (string, error)
	Completed(username string, password string, reason string)
}

type _DefaultAuthenticator struct{}

func (auth *_DefaultAuthenticator) Authenticate(username string, password string) (string, error) {
	return password, nil
}

func (auth *_DefaultAuthenticator) Retry(username string, password string) (string, error) {
	return password, nil
}

func (auth *_DefaultAuthenticator) Completed(username string, password string, reason string) {}
