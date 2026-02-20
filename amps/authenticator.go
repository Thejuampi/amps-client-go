package amps


// The Authenticator interface is used by the client during the logging on. It provides the authentication for custom
// authentication scenarios when external actions are required before logging on.
//
// Methods:
//
//    Authenticate(username string, password string) (string, error)
//
// Authenticate() is called by the Client, just before the logon command is sent.
// Returns the value that should be placed into the Password header for the logon attempt will be passed, and 
// the error object (nil by default) with information about the error occurred while executing the method.
//
//    Retry(username string, password string) (string, error)
// Retry() is called when a logon "ack" is received with a status of "retry". AMPS will continue trying to logon as long as 
// the server returns "retry", and this method continues to succeed.
// Returns the value that should be placed into the Password header for the logon attempt will be passed, and 
// the error object (nil by default) with information about the error occurred while executing the method.
//
//    Completed(username string, password string, reason string)
// Completed() is called when a logon completes successfully. Once a logon has completed, this method is called with 
// the username and password that caused a successful logon, and optionally, with the reason for this successful completion.
type Authenticator interface {
	Authenticate(username string, password string) (string, error)
	Retry(username string, password string) (string, error)
	Completed(username string, password string, reason string)
}


// _DefaultAuthenticator the default implementation.
type _DefaultAuthenticator struct {}


func (auth *_DefaultAuthenticator) Authenticate(username string, password string) (string, error) {
	return password, nil
}


func (auth *_DefaultAuthenticator) Retry(username string, password string) (string, error) {
	return password, nil
}


func (auth *_DefaultAuthenticator) Completed(username string, password string, reason string) {}
