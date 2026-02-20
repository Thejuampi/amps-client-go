package amps

import "fmt"

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
	}

	return NewError(err)
}

func NewError(errorCode int, message ...interface{}) error {
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
	case MessageHandlerError:
		errorName = "MessageHandlerError"
	default:
		errorName = "UnknownError"
	}

	if len(message) > 0 {
		return fmt.Errorf("%s: %s", errorName, message[0])
	}

	return fmt.Errorf("%s", errorName)
}
