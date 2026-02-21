package kerberos

import (
	"context"
	"errors"
	"os"
	"runtime"
	"sync/atomic"

	"github.com/Thejuampi/amps-client-go/amps"
)

// TokenProvider computes a Kerberos token for a logon step.
type TokenProvider func(ctx context.Context, username string, service string, realm string) (string, error)

// RetryProvider computes a replacement token after a failed logon.
type RetryProvider func(ctx context.Context, username string, service string, realm string, attempt uint64) (string, error)

// Config controls pure-Go Kerberos compatibility behavior.
type Config struct {
	ServicePrincipal string
	Realm            string
	StaticToken      string
	TokenProvider    TokenProvider
	RetryProvider    RetryProvider
	MaxRetries       uint64
}

type kerberosAuthenticator struct {
	config      Config
	lastReason  string
	lastToken   atomic.Value
	retryCount  atomic.Uint64
	completed   atomic.Bool
}

// NewAuthenticator creates a Kerberos compatibility authenticator.
func NewAuthenticator(config Config) amps.Authenticator {
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	return &kerberosAuthenticator{config: config}
}

func (auth *kerberosAuthenticator) resolveToken(ctx context.Context, username string, retry bool) (string, error) {
	if retry && auth.config.RetryProvider != nil {
		attempt := auth.retryCount.Add(1)
		if attempt > auth.config.MaxRetries {
			return "", errors.New("kerberos retry limit exceeded")
		}
		token, err := auth.config.RetryProvider(ctx, username, auth.config.ServicePrincipal, auth.config.Realm, attempt)
		if err != nil {
			return "", err
		}
		return token, nil
	}

	if auth.config.TokenProvider != nil {
		token, err := auth.config.TokenProvider(ctx, username, auth.config.ServicePrincipal, auth.config.Realm)
		if err != nil {
			return "", err
		}
		return token, nil
	}

	if auth.config.StaticToken != "" {
		return auth.config.StaticToken, nil
	}
	if envToken := os.Getenv("AMPS_KERBEROS_TOKEN"); envToken != "" {
		return envToken, nil
	}
	return "", errors.New("kerberos token provider is not configured")
}

// Authenticate resolves the initial Kerberos token.
func (auth *kerberosAuthenticator) Authenticate(username string, password string) (string, error) {
	token, err := auth.resolveToken(context.Background(), username, false)
	if err != nil {
		return "", err
	}
	auth.lastToken.Store(token)
	auth.retryCount.Store(0)
	_ = password
	return token, nil
}

// Retry resolves a replacement Kerberos token.
func (auth *kerberosAuthenticator) Retry(username string, password string) (string, error) {
	token, err := auth.resolveToken(context.Background(), username, true)
	if err != nil {
		return "", err
	}
	auth.lastToken.Store(token)
	_ = password
	return token, nil
}

// Completed captures terminal logon reason state.
func (auth *kerberosAuthenticator) Completed(username string, password string, reason string) {
	_ = username
	_ = password
	auth.lastReason = reason
	auth.completed.Store(true)
}

// Capabilities reports pure-Go Kerberos compatibility capabilities.
func (auth *kerberosAuthenticator) Capabilities() amps.AuthenticatorCapabilities {
	limitations := []string{
		"Pure-Go compatibility mode; no CGO SSPI/GSSAPI bridge",
		"Ticket acquisition must be provided by callback/static token/env token",
	}
	return amps.AuthenticatorCapabilities{
		Mechanism:   "kerberos",
		Platform:    runtime.GOOS,
		SupportsOS:  true,
		Limitations: limitations,
	}
}

// Begin starts optional challenge-response flow.
func (auth *kerberosAuthenticator) Begin(username string, password string) (string, error) {
	return auth.Authenticate(username, password)
}

// Continue advances optional challenge-response flow.
func (auth *kerberosAuthenticator) Continue(username string, challenge string) (string, error) {
	_ = challenge
	return auth.Retry(username, "")
}
