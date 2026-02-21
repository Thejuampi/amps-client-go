// Package amps provides a Go implementation of the AMPS client protocol with
// parity-oriented behavior for the Client and HAClient APIs.
//
// The primary lifecycle is:
//   - construct a Client with NewClient
//   - Connect to a transport URI
//   - Logon to initialize a session
//   - execute publish/query/subscribe commands
//   - Disconnect or Close when finished
//
// Most command APIs require an active connection, and data-flow operations
// typically require a successful Logon. HAClient wraps Client with reconnect,
// replay, and resubscribe behavior for failover-oriented deployments.
//
// This package is safe for concurrent use of exported client APIs that already
// synchronize internal state, but callback handlers should be written as
// thread-safe because they can execute from receive and recovery paths.
//
// Errors are reported as typed AMPS errors created with NewError and may wrap
// transport, authentication, command, protocol, timeout, or disconnect causes.
//
// Integration tests are environment-gated and use these variables:
// AMPS_TEST_URI, AMPS_TEST_FAILOVER_URIS, AMPS_TEST_PROTOCOL,
// AMPS_TEST_USER, and AMPS_TEST_PASSWORD.
package amps
