package amps

import (
	"errors"
	"fmt"
)

// AlreadyConnectedError and related constants define protocol and client behavior values.
const (
	AlreadyConnectedError = iota

	AuthenticationError

	BadFilterError

	BadRegexTopicError

	CommandError

	ConnectionError

	ConnectionRefusedError

	DisconnectedError

	ProtocolError

	InvalidTopicError

	InvalidURIError

	nameInUseError

	NotEntitledError

	RetryOperationError

	SubidInUseError

	SubscriptionAlreadyExistsError

	TimedOutError

	SlowClientError

	ServerShuttingDownError

	TransportDisabledError

	AlreadyExistsError

	MessageHandlerError

	UnknownError
)

func reasonToError(reason string) error {
	err := UnknownError

	switch reason {
	case "bad filter":
		err = BadFilterError
	case "invalid topic":
		err = InvalidTopicError
	case "not entitled":
		err = NotEntitledError
	case "auth failure":
		err = AuthenticationError
	case "bad regex":
		err = BadRegexTopicError
	case "already exists":
		err = AlreadyExistsError
	}

	return NewError(err)
}

// AMPSError is an AMPS client error with a machine-readable kind and an
// optional wrapped cause.
type AMPSError struct {
	Kind    int
	Message string
	cause   error
}

// Error returns the stable text representation used by earlier client releases.
func (err *AMPSError) Error() string {
	if err == nil {
		return "<nil>"
	}
	var name = errorKindName(err.Kind)
	if err.Message == "" {
		return name
	}
	return name + ": " + err.Message
}

// Unwrap returns the original cause supplied to NewError, when present.
func (err *AMPSError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.cause
}

// Is matches AMPSErrors by kind and otherwise delegates through Unwrap.
func (err *AMPSError) Is(target error) bool {
	var targetAMPSError, ok = target.(*AMPSError)
	return ok && err != nil && targetAMPSError != nil && err.Kind == targetAMPSError.Kind
}

// IsErrorKind reports whether err contains an AMPSError of the requested kind.
func IsErrorKind(err error, kind int) bool {
	var ampsErr *AMPSError
	return errors.As(err, &ampsErr) && ampsErr.Kind == kind
}

func errorKindName(errorCode int) string {
	var errorName string

	switch errorCode {
	case AlreadyConnectedError:
		errorName = "AlreadyConnectedError"
	case AuthenticationError:
		errorName = "AuthenticationError"
	case BadFilterError:
		errorName = "BadFilterError"
	case BadRegexTopicError:
		errorName = "BadRegexTopicError"
	case CommandError:
		errorName = "CommandError"
	case ConnectionError:
		errorName = "ConnectionError"
	case ConnectionRefusedError:
		errorName = "ConnectionRefusedError"
	case DisconnectedError:
		errorName = "DisconnectedError"
	case ProtocolError:
		errorName = "ProtocolError"
	case InvalidTopicError:
		errorName = "InvalidTopicError"
	case InvalidURIError:
		errorName = "InvalidURIError"
	case nameInUseError:
		errorName = "NameInUseError"
	case NotEntitledError:
		errorName = "NotEntitledError"
	case RetryOperationError:
		errorName = "RetryOperationError"
	case SubidInUseError:
		errorName = "SubidInUseError"
	case SubscriptionAlreadyExistsError:
		errorName = "SubscriptionAlreadyExistsError"
	case TimedOutError:
		errorName = "TimedOutError"
	case SlowClientError:
		errorName = "SlowClientError"
	case ServerShuttingDownError:
		errorName = "ServerShuttingDownError"
	case TransportDisabledError:
		errorName = "TransportDisabledError"
	case AlreadyExistsError:
		errorName = "AlreadyExistsError"
	case MessageHandlerError:
		errorName = "MessageHandlerError"
	default:
		errorName = "UnknownError"
	}
	return errorName
}

// NewError returns a new Error.
func NewError(errorCode int, message ...interface{}) error {
	var ampsErr = &AMPSError{Kind: errorCode}

	if len(message) > 0 {
		ampsErr.Message = fmt.Sprint(message[0])
		if cause, ok := message[0].(error); ok {
			ampsErr.cause = cause
		}
	}
	return ampsErr
}
