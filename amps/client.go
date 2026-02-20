package amps

import (
	"crypto/tls"
	"net"
	"fmt"
	"net/url"
	"time"
	"bytes"
	"sync"
	"errors"
	"math/rand"
	"strings"
	"strconv"
)


// Client constants
const (
	ClientVersion		= "0.1.0"

	// Bookmarks
	BookmarksEPOCH		= "0"
	BookmarksRECENT		= "recent"
	BookmarksNOW		= "0|1|"

	// Acks
	AckTypeNone			= 0
	AckTypeReceived		= 1
	AckTypeParsed		= 2
	AckTypeProcessed	= 4
	AckTypePersisted	= 8
	AckTypeCompleted	= 16
	AckTypeStats		= 32
)


// Client struct
type Client struct {
	// Main fields
	clientName string
	serverVersion string
	logonCorrelationID string
	nextID uint64
	messageType []byte
	errorHandler func(err error)
	disconnectHandler func(client *Client, err error)
	heartbeatInterval uint
	heartbeatTimeout uint
	heartbeatTimestamp uint
	heartbeatTimeoutID *time.Timer

	// synchronization
	lock sync.Mutex
	acksLock sync.Mutex
	ackProcessingLock sync.Mutex
	syncAckProcessing chan _Result
	routes *sync.Map
	messageStreams *sync.Map

	// Connection
	connected bool
	connection net.Conn
	logging bool
	url *url.URL
	tlsConfig *tls.Config

	// send variables
	sendBuffer *bytes.Buffer
	command *Command
	sendHb bool
	hbCommand *Command

	// receive variables
	stopped bool
	readTimeout uint
	receiveBuffer []byte
	readPosition int
	receivePosition int
	lengthBytes []byte
	message *Message
	msgRouter *MessageRouter
}



////////////////////////////////////////////////////////////////////////
//                            Internal API                            //
////////////////////////////////////////////////////////////////////////


// SystemAckProcessing helper struct
type _Result struct {
    Status bool
    Reason string
}


// Synchronous processing for methods that involve ack messages returned (sow_delete, etc).
type _Stats struct {
    Stats *Message
    Error error
}


func (client *Client) makeCommandID() string {
	commandID := strconv.FormatUint(client.nextID, 10)
	client.command.SetCommandID(commandID)
	client.nextID++

	return commandID
}

func (client *Client) send(command *Command) (err error) {
	if !client.connected { return errors.New("Client is not connected while trying to send data") }
	if client.sendBuffer == nil { return errors.New("Socket error while sending message (NullPointer)") }

	// Prepare the buffer
	client.sendBuffer.Reset()

	// Reserve space for the message length
	_, err = client.sendBuffer.WriteString("    "); if err != nil { return }

	// Write all header fields
	err = command.header.write(client.sendBuffer); if err != nil { return }

	// Write data (if any)
	if command.data != nil {
		client.sendBuffer.Write(command.data); if err != nil { return }
	}

	// Write first 4 bytes
	length := uint32(client.sendBuffer.Len()) - 4
	rawBytes := client.sendBuffer.Bytes()
	rawBytes[0] = (byte)((length & 0xFF000000) >> 24)
	rawBytes[1] = (byte)((length & 0x00FF0000) >> 16)
	rawBytes[2] = (byte)((length & 0x0000FF00) >> 8)
	rawBytes[3] = (byte)(length & 0x000000FF)

	// write the buffer to the socket (transport filter can mutate payload)
	frame := client.sendBuffer.Bytes()
	filtered := client.applyTransportFilter(TransportFilterOutbound, frame)
	if len(filtered) < 4 {
		return NewError(ProtocolError, "transport filter produced an invalid frame")
	}
	filteredLength := uint32(len(filtered) - 4)
	filtered[0] = byte((filteredLength & 0xFF000000) >> 24)
	filtered[1] = byte((filteredLength & 0x00FF0000) >> 16)
	filtered[2] = byte((filteredLength & 0x0000FF00) >> 8)
	filtered[3] = byte(filteredLength & 0x000000FF)
	_, err = client.connection.Write(filtered)

	// Connection error
	if err != nil {
		client.onConnectionError(NewError(ConnectionError, fmt.Sprintf("Socket error while sending message (%v)", err)))
	}

	return
}


func (client *Client) readRoutine() {
	if !client.connected { return }
	if client.stopped { return }
	client.callReceiveRoutineStartedCallback()

	// reset buffer variables
	client.receiveBuffer = make([]byte, 128 * 1024)
	client.lengthBytes = make([]byte, 4)
	client.readTimeout = 0
	client.readPosition = 0
	client.receivePosition = 0

	for {
		if client.stopped { return }

		for client.receivePosition - client.readPosition < 4 {
			count, err := client.connection.Read(client.receiveBuffer[client.receivePosition:])
			client.receivePosition += count
			if err != nil {
				if client.connected {
					client.onConnectionError(NewError(ConnectionError, fmt.Sprintf("Socket Read Error: (%v)", err)))
				}

				return
			}
		}

		for client.receivePosition - client.readPosition > 4 {
			messageLength := int(client.receiveBuffer[client.readPosition])     << 24 +
					  int(client.receiveBuffer[client.readPosition + 1]) << 16 +
					  int(client.receiveBuffer[client.readPosition + 2]) << 8  +
					  int(client.receiveBuffer[client.readPosition + 3])

			endByte := client.readPosition + messageLength + 4

			// if we don't have the whole message read until we do
			for endByte > client.receivePosition {
				if endByte > len(client.receiveBuffer) {
					// do we need to move the buffer
					if messageLength > len(client.receiveBuffer) {
						// this buffer's not big enough
						newBuffer := make([]byte, 2 * len(client.receiveBuffer))
						copy(newBuffer, client.receiveBuffer[client.readPosition:client.receivePosition])
						client.receiveBuffer = newBuffer
					} else {
						// buffer is big enough, but we have to relocate this message
						copy(client.receiveBuffer, client.receiveBuffer[client.readPosition:client.receivePosition])
					}

					client.receivePosition -= client.readPosition
					endByte -= client.readPosition
					client.readPosition = 0
				}

				// Now we just need to read more
				count, err := client.connection.Read(client.receiveBuffer[client.receivePosition:])
				client.receivePosition += count
				if err != nil {
					if client.connected {
						client.onConnectionError(NewError(ConnectionError, fmt.Sprintf("Socket Read Error: (%v)", err)))
					}

					return
				}
			}

			// Whole data chunk is here: let's parse it

			filteredFrame := client.applyTransportFilter(TransportFilterInbound, client.receiveBuffer[client.readPosition:endByte])
			if len(filteredFrame) < 4 {
				client.onError(NewError(ProtocolError, "invalid inbound frame"))
				return
			}

			// parse header first
			left, err := parseHeader(client.message, true, filteredFrame[4:])
			if err != nil { client.onError(NewError(ProtocolError)); return }

			// SOW case
			if client.message.header.command == CommandSOW {
				for len(left) > 0 {
					// Parsing mini-header
					left, err = parseHeader(client.message, false, left)
					if err != nil { client.onError(NewError(ProtocolError, err)); return }

					// set data
					dataLength := (*client.message.header.messageLength)
					client.message.data = left[:dataLength]

					// Deliver the message
					err = client.onMessage(client.message)
					if err != nil { client.onError(NewError(MessageHandlerError, err)) }

					// Next chunk is ready
					left = left[dataLength:]
				}
			} else {
				// Message has data
				client.message.data = left

				// Report the message
				err = client.onMessage(client.message)
				if err != nil { client.onError(NewError(MessageHandlerError, err)) }
			}

			// Keep going
			if endByte == client.receivePosition {
				client.readPosition = 0
				client.receivePosition = 0
			} else {
				client.readPosition = endByte
			}
		}
	}
}


func (client *Client) onMessage(message *Message) (err error) {
	if message == nil {
		return nil
	}

	command, _ := message.Command()
	queryID, hasQueryID := message.QueryID()
	subIDs, hasSubIDs := message.SubIDs()
	subID, hasSubID := message.SubID()
	commandID, hasCommandID := message.CommandID()
	hasHeartbeat := client.heartbeatInterval > 0
	client.applyAckBookkeeping(message)

	if command == commandHeartbeat {
		if hasHeartbeat {
			timedelta := uint(time.Now().Unix()) - client.heartbeatTimestamp
			if timedelta > client.heartbeatInterval {
				client.checkAndSendHeartbeat(true)
			}
		}
		return nil
	}

	handled := false
	var routeID string
	switch {
	case hasQueryID:
		routeID = queryID
	case hasSubID:
		routeID = subID
	case hasCommandID:
		routeID = commandID
	}

	// Route-specific handlers always run first.
	if hasSubIDs {
		for _, candidateRoute := range strings.Split(subIDs, ",") {
			candidateRoute = strings.TrimSpace(candidateRoute)
			if candidateRoute == "" {
				continue
			}
			if messageHandler, exists := client.routes.Load(candidateRoute); exists {
				handled = true
				if handlerErr := messageHandler.(func(*Message) error)(message); handlerErr != nil && err == nil {
					err = handlerErr
				}
			}
		}
	} else if routeID != "" {
		if messageHandler, exists := client.routes.Load(routeID); exists {
			handled = true
			err = messageHandler.(func(*Message) error)(message)
		}
	}

	// Global command handlers run after route handlers.
	if globalErr, globalHandled := client.callGlobalCommandTypeHandler(command, message); globalHandled {
		handled = true
		if err == nil {
			err = globalErr
		}
	}

	// Duplicate handler runs after global handlers.
	if (command == CommandPublish || command == CommandSOW || command == CommandOOF) && client.detectAndTrackDuplicate(message) {
		handled = true
		if duplicateErr := client.callDuplicateHandler(message); err == nil {
			err = duplicateErr
		}
	}

	// Last-chance chain runs only if still unhandled.
	if !handled {
		if unhandledErr := client.callUnhandledHandler(message); err == nil {
			err = unhandledErr
		}
		if lastChanceErr := client.callLastChanceHandler(message); err == nil {
			err = lastChanceErr
		}
	}

	if hasHeartbeat {
		client.checkAndSendHeartbeat(false)
	}

	if err == nil {
		client.maybeAutoAck(message)
	}

	return
}


func (client *Client) deleteRoute(routeID string) (routeErr error) {
	if messageStream, messageStreamExists := client.messageStreams.Load(routeID); messageStreamExists {
		messageStream.(*MessageStream).client = nil;
		if messageStream.(*MessageStream).state != messageStreamStateComplete {
			routeErr = messageStream.(*MessageStream).Close()
		}
		client.messageStreams.Delete(routeID)
	}

	if _, exists := client.routes.Load(routeID); exists {
		client.routes.Delete(routeID)
	}

	return
}


func (client *Client) addRoute(
	routeID string,
	messageHandler func(*Message) error,
	systemAcks int,
	requestedAcks int,
	isSubscribe bool,
	isReplace bool,
) (routeErr error)  {
	success := systemAcks == AckTypeNone

	previousMessageHandler, messageHandlerExists := client.routes.Load(routeID)

	if !isReplace {
		if messageHandlerExists {
			if isSubscribe { return NewError(SubidInUseError, "Subscription with ID '" + routeID + "' already exists") }
			return NewError(CommandError, "SubID is set to a non-Subscribe command")
		}
	} else {
		// Replace with a nil new message handler -- reuse the old one
		if messageHandler == nil { messageHandler = previousMessageHandler.(func(*Message) error) }
	}

	client.routes.Store(routeID, func(message *Message) error {
		var err error

		// Process acks first
		if message.header.command == CommandAck {
			ack, _ := message.AckType()

			// System-requested ack message
			if systemAcks & ack > 0 {
				// Delete this message from system acks
				systemAcks &^= ack

				// Processed ack -- enough to report back to the Execute()
				if ack == AckTypeProcessed && client.syncAckProcessing != nil {
					status, _ := message.Status()
					reason, _ := message.Reason()
					success = status == "success"
					client.syncAckProcessing <- _Result{success, reason}

					client.acksLock.Unlock()
				}
			}

			if !success { return nil }

			// if this ack was requested for delivery
			if requestedAcks & ack > 0 {
				// Delete this message from system acks
				requestedAcks &^= ack

				err = messageHandler(message)
				if err != nil { err = NewError(MessageHandlerError, err) }
			}

			// Delete the message handler since all the messages were delivered
			if systemAcks == AckTypeNone && requestedAcks == AckTypeNone && !isSubscribe {
				err = client.deleteRoute(routeID)
				if err != nil { err = errors.New("Error deleting route") }
			} else if systemAcks == AckTypeNone {
				// no system acks left to accept -- replace this wrapper with the original message handler
				client.routes.Store(routeID, messageHandler)
			}
		} else {
			// Non-ack -- simply pass the message to the message handler
			if messageHandler != nil {
				err = messageHandler(message)
				if err != nil { err = NewError(MessageHandlerError, err) }
			}
		}

		return err
	})

	return
}


func (client *Client) onError(err error) {
	if client.errorHandler != nil { client.errorHandler(err) }
	client.reportException(err)
}


func (client *Client) onConnectionError(err error) {
	if client.connection != nil { _ = client.connection.Close() }
	client.connection = nil

	client.connected = false
	client.stopped = true
	client.logging = false
	client.notifyConnectionState(ConnectionStateDisconnected)
	if state := ensureClientState(client); state != nil {
		state.lock.Lock()
		if state.ackTimer != nil {
			_ = state.ackTimer.Stop()
			state.ackTimer = nil
		}
		state.pendingAcks = make(map[string]*pendingAckBatch)
		state.pendingAckCount = 0
		state.lock.Unlock()
	}

	client.ackProcessingLock.Lock()
	if client.syncAckProcessing != nil {
		close(client.syncAckProcessing)
		client.syncAckProcessing = nil
	}
	client.ackProcessingLock.Unlock()

	// wipe the routes
	client.routes = new(sync.Map)
	client.messageStreams = new(sync.Map)

	client.onError(err)
	client.onInternalDisconnect(err)

	// Connection error -- report to the disconnect handler
	if client.disconnectHandler != nil { client.disconnectHandler(client, err) }
}


func (client *Client) onHeartbeatAbsence() {
	if (uint(time.Now().Unix()) - client.heartbeatTimestamp) > (client.heartbeatTimeout * 1000 ) {
		_ = client.heartbeatTimeoutID.Stop()

		client.heartbeatTimestamp = 0
		client.onError(errors.New("Heartbeat absence error"))
	}
}


func (client *Client) establishHeartbeat() (hbError error) {
	done := make(chan error)
	client.heartbeatTimestamp = uint(time.Now().Unix())

	heartbeat := NewCommand("heartbeat").AddAckType(AckTypeProcessed).SetOptions("start," + strconv.FormatUint(uint64(client.heartbeatInterval), 10))
	_, hbError = client.ExecuteAsync(heartbeat, func(message *Message) (err error) {
		status, _ := message.Status()
		command, _ := message.Command()
		ackType, _ := message.AckType()
		if  status == "success" && command == CommandAck && ackType == AckTypeProcessed {
			client.heartbeatTimeoutID = time.AfterFunc(time.Duration(client.heartbeatTimeout * 1000), client.onHeartbeatAbsence)
			client.notifyConnectionState(ConnectionStateHeartbeatInitiated)
		} else {
			err = errors.New("Heartbeat Establishment Error")
		}

		done <-err

		return
	})

	if hbError == nil {
		return <-done
	}

	return
}


func (client *Client) checkAndSendHeartbeat(force bool) {
	if client.heartbeatTimeout == 0 || (client.heartbeatTimeout != 0 && client.heartbeatTimestamp == 0) { return }

	if force || ((uint(time.Now().Unix()) - client.heartbeatTimestamp) > client.heartbeatInterval) {
		client.heartbeatTimestamp = uint(time.Now().Unix())
		_ = client.heartbeatTimeoutID.Stop()
		client.heartbeatTimeoutID = time.AfterFunc(
			time.Duration(client.heartbeatTimeout * 1000),
			client.onHeartbeatAbsence,
		)

		err := client.sendHeartbeat()
		if err != nil { client.onConnectionError(err) }
	}
}



////////////////////////////////////////////////////////////////////////
//                             Public API                             //
////////////////////////////////////////////////////////////////////////


// Params structs (since there is no overloading nor default parameters in Go)

// LogonParams is the struct that can be supplied to the Logon() method of the Client in order to supply an
// Authenticator object and/or a timeout value and Correlation id.
// If Timeout is not set or set to 0, there will be no timeout for Logon().
//
// Parameters:
//
// Authenticator [Authenticator] (optional) - The custom Authenticator object to authenticate against.
//
// Timeout [uint] (optional) - The number of milliseconds to wait for AMPS to acknowledge the command, where 0 indicates
// no timeout.
//
// CorrelationID [string] (optional) - the uninterpreted logon correlation information a client sends at logon to aid in
// searching server log files for specific clients.
type LogonParams struct {
	Timeout uint
	Authenticator Authenticator
	CorrelationID string
}



///////////////
//  Get/Set  //
///////////////


// ClientName is a getter for the client's name. If logon has not been performed yet and the name was not
// set, returns the empty string.
func (client *Client) ClientName() string { return client.clientName }


// SetClientName is a setter for the client's name.
func (client *Client) SetClientName(clientName string) *Client { client.clientName = clientName; return client }


// ErrorHandler gets the error handler for all general errors such as connection issues, exceptions, etc.
func (client *Client) ErrorHandler() func(error) { return client.errorHandler }


// SetErrorHandler sets the error handler for all general errors such as connection issues, exceptions, etc.
//
// Example:
//   client.SetErrorHandler(func(err error) {
//       fmt.Println(time.Now().Local().String() + " [" + client.ClientName() + "] >>>", err)
//   })
func (client *Client) SetErrorHandler(errorHandler func(error)) *Client {
	client.errorHandler = errorHandler
	return client
}


// DisconnectHandler gets the disconnect handler that is called in case of an unintentional disconnection.
func (client *Client) DisconnectHandler() func(*Client, error) { return client.disconnectHandler }


// SetDisconnectHandler sets the disconnect handler that is called in case of an unintentional disconnection.
//
// Example:
//   client.SetDisconnectHandler(func(cl *amps.Client, err error) {
//       fmt.Println("Switching to the next URI...")
//       connectToNextURI(client);
//   })
func (client *Client) SetDisconnectHandler(disconnectHandler func(*Client, error)) *Client {
	client.disconnectHandler = disconnectHandler
	return client
}


// SetTLSConfig is an optional setter for the secure TCP connection configuration parameters, such as Certificates.
// It must be set before the Connect() and Logon() methods are called. This configuration, is ignored if connecting
// to a standard non-secure TCP transport.
//
// Example:
//   client.SetTLSConfig(&tls.Config{
//		InsecureSkipVerify: true,
//	 })
func (client *Client) SetTLSConfig(config *tls.Config) *Client {
	client.tlsConfig = config;
	return client
}


// LogonCorrelationID gets the uninterpreted logon correlation information a client sends at logon to aid in searching
// server log files for specific clients. If it was not set, returns the empty string.
func (client *Client) LogonCorrelationID() string { return client.logonCorrelationID }


// SetLogonCorrelationID sets the uninterpreted logon correlation information a client sends at logon to aid in
// searching server log files for specific clients. Can also be set in the LogonParams object supplied to the Logon()
// method.
func (client *Client) SetLogonCorrelationID(logonCorrelationID string) *Client {
	client.logonCorrelationID = logonCorrelationID
	return client
}


// SetHeartbeat ...
func (client *Client) SetHeartbeat(interval uint, providedTimeout ...uint) *Client {
	var timeout uint
	if len(providedTimeout) > 0 {
		timeout = providedTimeout[0]
	} else {
		timeout = interval * 2
	}

	client.heartbeatTimeout = timeout
	client.heartbeatInterval = interval
	return client
}



////////////////
//  Main API  //
////////////////


// ServerVersion returns the server version returned by the AMPS server in the logon acknowledgement.
// If logon has not been performed yet, returns an empty string.
func (client *Client) ServerVersion() string { return client.serverVersion }


// Connect connects the AMPS client to the server using the URI containing all required credentials/addresses/types.
// After successful connection, the Logon() method must be called, unless the implicit logon is enabled on the server.
func (client *Client) Connect(uri string) error {
	if client.connected { return NewError(AlreadyConnectedError) }

	// Reset logon state
	client.logging = false

	// if no error handler set yet, provide a default one
	if client.errorHandler == nil {
		client.errorHandler = func(err error) {
			fmt.Println(time.Now().Local().String() + " [" + client.clientName + "] >>>", err)
		}
	}

	client.lock.Lock()
	defer client.lock.Unlock()

	// Parse the URL
	parsedURI, err := url.Parse(uri)
	if err != nil { return NewError(InvalidURIError, err) }
	state := ensureClientState(client)
	if state != nil {
		state.lock.Lock()
		state.uri = uri
		state.manualDisconnect = false
		state.lock.Unlock()
	}

	// validate the URI
	pathParts := strings.Split(parsedURI.Path, "/");
	partsLength := len(pathParts)
	if partsLength > 1 {
		if pathParts[1] != "amps" {
			return NewError(ProtocolError, "Specification of message type requires amps protocol");
		}

		// Determine message type, if any
		if partsLength > 2 {
			client.messageType = []byte(pathParts[2])
		}
	}

	client.url = parsedURI

	// Connect now
	if parsedURI.Scheme == "tcps" {
		if client.tlsConfig == nil {
			// accept self-signed certificates by default, like other clients do
			client.tlsConfig = &tls.Config{ InsecureSkipVerify: true };
		}

		connection, err := tls.Dial("tcp", parsedURI.Host, client.tlsConfig)
		client.connection = connection
		if err != nil { return NewError(ConnectionRefusedError, err) }
	} else {
		connection, err := net.Dial("tcp", parsedURI.Host)
		client.connection = connection
		if err != nil { return NewError(ConnectionRefusedError, err) }
	}

	client.connected = true
	client.notifyConnectionState(ConnectionStateConnected)

	// Start reading from the socket in the read thread
	client.stopped = false
	go client.readRoutine()

	return nil
}


// Logon logs into AMPS with the parameters provided in the connect method, if any, that is set. Optional parameters
// can be supplied using the LogonParams struct, such as Timeout, Authenticator, and logon CorrelationID.
func (client *Client) Logon(optionalParams ...LogonParams) (err error) {
	client.lock.Lock()

	hasParams := len(optionalParams) > 0
	hasAuthenticator := hasParams && (optionalParams[0].Authenticator) != nil

	// Has logon correlation id
	if hasParams && len(optionalParams[0].CorrelationID) > 0 {
		client.logonCorrelationID = optionalParams[0].CorrelationID
	}

	// Prepare the command
	client.command.reset()
	client.command.header.command = commandLogon
	ack := AckTypeProcessed
	client.command.header.ackType = &ack

	// Set private fields
	commandID := client.makeCommandID()
	client.command.header.clientName = []byte(client.clientName)
	client.command.header.version = []byte(ClientVersion)
	client.command.header.messageType = client.messageType

	// Username/password
	var username, password string
	user, hasUser := client.url.User, client.url.User != nil
	if hasUser {
		username = user.Username()

		// set the username field
		client.command.header.userID = []byte(username)

		// Detect password, if any
		localPassword, hasPassword := user.Password()
		password = localPassword

		// If the authenticator object was provided
		if hasAuthenticator {
			password, err = optionalParams[0].Authenticator.Authenticate(username, password)
			if err != nil { return NewError(AuthenticationError, err) }
			hasPassword = true
		}

		if hasPassword {
			client.command.header.password = []byte(password)
		}
	}

	// Logon correlation id
	if len(client.logonCorrelationID) > 0 {
		client.command.header.correlationID = []byte(client.logonCorrelationID)
	}

	// prepare for receiving logon acks
	doneLoggingIn := make(chan error)
	// logonSuccess := make(chan bool)
	// var doneLoggingIn, logonSuccess bool

	client.logging = true
	logonRetries := 3
	client.routes.Store(commandID, func(message *Message) (logonAckErr error) {
		if message.header.command == CommandAck {
			var loggingError error
			if ackType, hasAckType := message.AckType(); hasAckType && ackType == AckTypeProcessed {
				reason, hasReason := message.Reason()

				switch status, _ := message.Status(); status {
				case "success":
					client.serverVersion = string(message.header.version)
					doneLoggingIn <-nil

				case "failure":
					if hasReason { loggingError = reasonToError(reason) } else {
						loggingError = NewError(AuthenticationError)
					}
					doneLoggingIn <-loggingError

				default:
					logonRetries--
					if logonRetries == 0 {
						loggingError = NewError(RetryOperationError, "Exceeded the maximum of logon retry attempts (3)")
						doneLoggingIn <-loggingError
						return
					}

					// Retry case -- resend the token from authenticator
					if hasAuthenticator {
						password, logonAckErr = optionalParams[0].Authenticator.Retry(username, password)
						if logonAckErr != nil { return NewError(AuthenticationError, logonAckErr) }
						client.command.header.password = []byte(password)
					}

					logonAckErr = client.send(client.command)
				}
			}
		}

		return
	})

	err = client.send(client.command)
	if err != nil {
		client.lock.Unlock()
		return NewError(ConnectionError, err)
	}

	// waiting for the logon result
	// for client.logging && !doneLoggingIn {}
	logonFailed := <-doneLoggingIn
	client.logging = false

	// Done logging
	client.routes.Delete(commandID)

	// getting the logon ack message(s)
	if logonFailed == nil {
		client.lock.Unlock()
		client.notifyConnectionState(ConnectionStateLoggedOn)
		if bookmarkStore := client.BookmarkStore(); bookmarkStore != nil {
			bookmarkStore.SetServerVersion(client.serverVersion)
		}
		if client.heartbeatTimeout != 0 {
			// Prepare the heartbeat command
			client.hbCommand.reset()
			client.hbCommand.header.command = commandHeartbeat
			client.hbCommand.header.options = []byte("beat")

			err = client.establishHeartbeat()
			if err != nil { client.onError(err); return }
		}

		if hasAuthenticator {
			reason, _ := client.message.Reason()
			optionalParams[0].Authenticator.Completed(username, password, reason)
		}
		client.postLogonRecovery()

		return
	}

	client.lock.Unlock()
	return logonFailed
}


func (client *Client) sendHeartbeat() error {
	client.lock.Lock()
	defer client.lock.Unlock()

	// Attempting to send the command
	err := client.send(client.hbCommand)
	if err != nil { return NewError(ConnectionError, err) }

	return nil
}

// Publish method performs the publish command. The publish command is the primary way to inject messages into the AMPS
// processing stream. A publish command received by AMPS will be forwarded to other connected clients with matching
// subscriptions.
//
// Example:
//   client.Publish("topic", "{\"id\": 1}")
//
// Arguments:
//
// topic [string] - The topic to publish data.
//
// data [string] - The data to publish to a topic.
//
// expiration [uint] (optional) - An optional parameter that sets the expiration value on the message
// (for SOW topics with expiration enabled).
func (client *Client) Publish(topic string, data string, expiration ...uint) error {
	return client.PublishBytes(topic, []byte(data), expiration...)
}


// PublishBytes is the same as Publish() but is more optimized for Go's native parsing libraries which almost
// exclusively utilize slices of bytes ([]byte) as the output format.
func (client *Client) PublishBytes(topic string, data []byte, expiration ...uint) error {
	if len(topic) == 0 { return NewError(InvalidTopicError, "A topic must be specified") }
	if !client.connected { return NewError(DisconnectedError, "Client is not connected while trying to publish") }

	client.lock.Lock()
	defer client.lock.Unlock()

	// Prepare the command
	client.command.reset()
	client.command.header.command = CommandPublish
	client.command.header.topic = []byte(topic)
	client.command.data = data

	// Set expiration, if any
	if len(expiration) > 0 {
		client.command.header.expiration = &(expiration[0])
	}

	// Attempting to send the command
	err := client.send(client.command)
	if err != nil { return NewError(ConnectionError, err) }

	return nil
}


// DeltaPublish delta publishes a message to a SOW topic:
//   client.DeltaPublish("topic", "{\"id\": 1, \"text\": \"Hello, World\"}");
//
// For regular topics, DeltaPublish() behaves like regular Publish().
//
// Arguments:
//
// topic [string] - The topic to publish data.
//
// data [string] - The data to publish to a topic.
//
// expiration [uint] (optional) - An optional parameter that sets the expiration value on the message
// (for SOW topics with expiration enabled).
func (client *Client) DeltaPublish(topic string, data string, expiration ...uint) error {
	return client.DeltaPublishBytes(topic, []byte(data), expiration...)
}



// DeltaPublishBytes is the same as DeltaPublish() but is more optimized for Go's native parsing libraries which almost
// exclusively utilize slices of bytes ([]byte) as the output format.
func (client *Client) DeltaPublishBytes(topic string, data []byte, expiration ...uint) error {
	if len(topic) == 0 { return NewError(InvalidTopicError, "A topic must be specified") }
	if !client.connected { return NewError(DisconnectedError, "Client is not connected while trying to publish") }

	client.lock.Lock()
	defer client.lock.Unlock()

	// Prepare the command
	client.command.reset()
	client.command.header.command = CommandDeltaPublish
	client.command.header.topic = []byte(topic)
	client.command.data = data

	// Set expiration, if any
	if len(expiration) > 0 {
		client.command.header.expiration = &(expiration[0])
	}

	// Attempting to send the command
	err := client.send(client.command)
	if err != nil { return NewError(ConnectionError, err) }

	return nil
}


// Execute is the synchronous version of the ExecuteAsync() method which returns the MessageStream object to
// iterate over in a for loop. Iteration occurs in the main thread context.
//
// Example:
//    messages, err := client.Execute(amps.NewCommand("sow").SetTopic("orders"))
//    if err != nil { fmt.Println(err); return }
//
//    for message := messages.Next(); message != nil; message = messages.Next() {
//        fmt.Println("Message Data:", string(message.Data()))
//    }
//
// Arguments:
//
// command [*Command] - the Command object that will be executed, resulting in a MessageStream object.
func (client *Client) Execute(command *Command) (*MessageStream, error) {
	var messageStream *MessageStream

	// Detect type of the stream
	var isSow, isSubscribe, isStatsOnly bool
	switch command.header.command {
	case CommandSOW:
		isSow = true
	case CommandSOWAndSubscribe:
		fallthrough
	case CommandSOWAndDeltaSubscribe:
		fallthrough
	case CommandDeltaSubscribe:
		isSow = true
		isSubscribe = true
	case CommandSubscribe:
		isSubscribe = true
	}

	ackType, hasAckType := command.AckType()
	if !isSubscribe && hasAckType && (ackType & AckTypeStats) > 0 {
		isStatsOnly = true
	}

	if !isSow && !isSubscribe && !isStatsOnly && !hasAckType {
		messageStream = emptyMessageStream()
	} else {
		messageStream = newMessageStream(client)
		if defaultDepth := client.DefaultMaxDepth(); defaultDepth > 0 {
			messageStream.SetMaxDepth(uint64(defaultDepth))
		}
	}

	cmdID, err := client.ExecuteAsync(command, func(message *Message) error {
		// return client.msgRouter.messageHandler(message)
		return messageStream.messageHandler(message)
	})
	// client.msgRouter.AddRoute(cmdID, messageStream)
	// client.messageStreams.Store(cmdID, messageStream)

	if isStatsOnly {
		messageStream.setStatsOnly()
	} else if isSow {
		messageStream.setQueryID(cmdID)
		if !isSubscribe {  messageStream.setSowOnly() }
	} else if isSubscribe && !isSow {
		messageStream.setSubID(cmdID)
	}

	if err != nil { return nil, err }

    return messageStream, err
}


// Subscribe is the synchronous version of the SubscribeAsync() method that returns the MessageStream object
// which can be iterated over in a for loop. Executed in the main thread context.
//
// The subscribe command is the primary way to retrieve messages from the AMPS processing stream. A client can issue a
// subscribe command on a topic to receive all published messages to that topic in the future. Additionally, content
// filtering can be used to choose which messages the client is interested in receiving.
//
// Example:
//
//   messages, err := client.Subscribe("invoices", "/price > 10000")
//   if err != nil { fmt.Println(err); return }
//
//   for message := messages.Next(); message != nil; message = messages.Next() {
//       fmt.Println("Message Data:", string(message.Data()))
//   }
//
// Arguments:
//
// topic [string] - The topic argument.
//
// filter [string] (optional) - An optional content filter value.
func (client *Client) Subscribe(topic string, filter ...string) (*MessageStream, error) {
	cmd := NewCommand("subscribe").SetTopic(topic)
	if len(filter) > 0 { cmd.SetFilter(filter[0]) }
	return client.Execute(cmd)
}


// Flush ...
func (client *Client) Flush() (err error){
	result := make(chan error)

    cmd := NewCommand("flush").AddAckType(AckTypeCompleted)
	_, err = client.ExecuteAsync(cmd, func(message *Message) error {

		if status, hasStatus := client.message.Status(); hasStatus && status == "success" {
			result <-nil
			return nil
		}

		reason, hasReason := client.message.Reason()
		if hasReason {
			result <-errors.New(reason)
			return errors.New(reason)
		}
		return nil
	})

	if err != nil { return err }
	return nil
}


// DeltaSubscribe is the synchronous version of the DeltaSubscribeAsync() method that returns the MessageStream object
// which can be iterated over in a for loop. Executed in the main thread context.
//
// The delta_subscribe command is like the subscribe command except that subscriptions placed through delta_subscribe
// will receive only messages that have changed between the SOW record and the new update. If delta_subscribe is used
// on a record which does not currently exist in the SOW or if it is used on a topic which does not have a SOW-topic
// store defined, then delta_subscribe behaves like a subscribe command.
//
// Example:
//
//   messages, err := client.DeltaSubscribe("orders", "/id > 20")
//   if err != nil { fmt.Println(err); return }
//
//   for message := messages.Next(); message != nil; message = messages.Next() {
//       fmt.Println("Message Data:", string(message.Data()))
//   }
//
// Arguments:
//
// topic [string] - The topic argument in SOW.
//
// filter [string] (optional) - An optional content filter value.
func (client *Client) DeltaSubscribe(topic string, filter ...string) (*MessageStream, error) {
	cmd := NewCommand("delta_subscribe").SetTopic(topic)
	if len(filter) > 0 { cmd.SetFilter(filter[0]) }
	return client.Execute(cmd)
}


// Sow is the synchronous version of the SowAsync() method that returns the MessageStream object
// which can be iterated over in a for loop. Executed in the main thread context.
//
// The sow command is used to query the contents of a previously defined SOW Topic. A sow command can be used to query
// an entire SOW Topic, or a filter can be used to further refine the results found inside a SOW Topic. For more
// information, see the State of the World and SOW Queries chapters in the AMPS User Guide.
//
// Example:
//
//   messages, err := client.Sow("orders", "/id > 20")
//   if err != nil { fmt.Println(err); return }
//
//   for message := messages.Next(); message != nil; message = messages.Next() {
//       fmt.Println("Message Data:", string(message.Data()))
//   }
//
// Arguments:
//
// topic [string] - The topic argument in SOW.
//
// filter [string] (optional) - An optional content filter value.
func (client *Client) Sow(topic string, filter ...string) (*MessageStream, error) {
	cmd := NewCommand("sow").SetTopic(topic)
	if len(filter) > 0 { cmd.SetFilter(filter[0]) }
	return client.Execute(cmd)
}


// SowAndSubscribe is the synchronous version of the SowAndSubscribeAsync() method that returns the MessageStream object
// which can be iterated over in a for loop. Executed in the main thread context.
//
// A sow_and_subscribe command is used to combine the functionality of sow and a subscribe command in a single command.
// The sow_and_subscribe command is used:
//
// - to query the contents of a SOW topic (this is the sow command); and
//
// - to place a subscription such that any messages matching the subscribed SOW topic and query filter will be published
// to the AMPS client (this is the subscribe command).
//
// As with the subscribe command, publish messages representing updates to SOW records will contain only information
// that has changed.
//
// Example:
//
//   messages, err := client.SowAndSubscribe("orders", "/id > 20")
//   if err != nil { fmt.Println(err); return }
//
//   for message := messages.Next(); message != nil; message = messages.Next() {
//       fmt.Println("Message Data:", string(message.Data()))
//   }
//
// Arguments:
//
// topic [string] - The topic argument in SOW.
//
// filter [string] (optional) - An optional content filter value.
func (client *Client) SowAndSubscribe(topic string, filter ...string) (*MessageStream, error) {
	cmd := NewCommand("sow_and_subscribe").SetTopic(topic)
	if len(filter) > 0 { cmd.SetFilter(filter[0]) }
	return client.Execute(cmd)
}


// SowAndDeltaSubscribe is the synchronous version of the SowAndDeltaSubscribeAsync() method that returns the
// MessageStream object which can be iterated over in a for loop. Executed in the main thread context.
//
// A sow_and_delta_subscribe command is used to combine the functionality of commands sow and a delta_subscribe in a
// single command. The sow_and_delta_subscribe command is used:
//
// - to query the contents of a SOW topic (this is the sow command); and
//
// - to place a subscription such that any messages matching the subscribed SOW topic and query filter will be published
// to the AMPS client (this is the delta_subscribe command).
//
// As with the delta_subscribe command, publish messages representing updates to SOW records will contain only the
// information that has changed. If a sow_and_delta_subscribe is issued on a record that does not currently exist in the
// SOW topic, or if it is used on a topic that does not have a SOW-topic store defined, then a sow_and_delta_subscribe
// will behave like a sow_and_subscribe command.
//
// Example:
//
//   messages, err := client.SowAndDeltaSubscribe("orders", "/id > 20")
//   if err != nil { fmt.Println(err); return }
//
//   for message := messages.Next(); message != nil; message = messages.Next() {
//       fmt.Println("Message Data:", string(message.Data()))
//   }
//
// Arguments:
//
// topic [string] - The topic argument in SOW.
//
// filter [string] (optional) - An optional content filter value.
func (client *Client) SowAndDeltaSubscribe(topic string, filter ...string) (*MessageStream, error) {
	cmd := NewCommand("sow_and_delta_subscribe").SetTopic(topic)
	if len(filter) > 0 { cmd.SetFilter(filter[0]) }
	return client.Execute(cmd)
}


// ExecuteAsync is the command execution interface method that allows to send commands that don't have a convenience
// method or require additional settings that are not provided by the convenience methods. The purpose of the method is
// to execute Command objects.
//
// Example:
//
//		cmdID, err := client.ExecuteAsync(amps.NewCommand("sow").SetTopic("orders"), func(message *amps.Message) error {
//			fmt.Println("Message Data:", string(message.Data()))
//			return nil
//		})
//
// Arguments:
//
// command [*Command] - A command object to execute.
//
// messageHandler [func(*Message) error] (optional) - The message handler that will be called each time a message is
// received. Message handler is called from the background thread context. If no message handler is needed, provide nil
// as the second argument.
func (client *Client) ExecuteAsync(command *Command, messageHandler func(message *Message) error) (string, error) {
	if command == nil || command.header.command == CommandUnknown {
		return "", NewError(CommandError, "Invalid Command provided")
	}

	client.lock.Lock()
	defer client.lock.Unlock()

	// Assign the command id
	commandID := client.makeCommandID()
	command.SetCommandID(commandID)
	state := ensureClientState(client)

	if !client.connected {
		if client.shouldRetryCommand(command.header.command) {
			client.queueRetryCommand(command, messageHandler)
			return commandID, nil
		}
		return commandID, NewError(DisconnectedError, "Client is not connected while trying to send data")
	}

	// var isReplace bool
	var notSow, isSubscribe, isPublish, isReplace bool
	var routeID string
	var systemAcks int
	userAcks, hasUserAcks := command.AckType()

	switch command.header.command {
	case CommandSubscribe:
		fallthrough

	case CommandDeltaSubscribe:
		notSow = true
		fallthrough

	case CommandSOWAndSubscribe:
		fallthrough

	case CommandSOWAndDeltaSubscribe:
		if command.header.subID == nil {
			command.header.subID = command.header.commandID
		}
		isSubscribe = true;
		fallthrough

	case CommandSOW:
		if !notSow && command.header.queryID == nil {
			if command.header.subID != nil {
				command.header.queryID = command.header.subID
			} else {
				command.header.queryID = command.header.commandID
			}
		}

		systemAcks |= AckTypeProcessed;

		// for SOW only, we get a completed ack so we know when to remove the handler
		if !isSubscribe { systemAcks |= AckTypeCompleted; }

		// if a custom batchSize value is not set, it is 10 by default for SOW queries
		_, hasBatchSizeSet := command.BatchSize()
		if !notSow && !hasBatchSizeSet { command.SetBatchSize(10) }

		if isSubscribe {
			if queryID, hasQueryID := command.QueryID(); hasQueryID && !notSow {
				routeID = queryID
			} else { routeID, _ = command.SubID() }

			if options, hasOptions := command.Options(); hasOptions {
				isReplace = strings.Contains(options, "replace")
			}
		} else { routeID = commandID }

		// make command handler here (processAcksSync)
		client.syncAckProcessing = make(chan _Result)
		// client.msgRouter.AddRoute(routeID, messageHandler, userAcks, systemAcks, isSubscribe, isReplace)
		// if isReplace {
		// 	msgStream := client.msgRouter.FindRoute(routeID)
		// 	if msgStream != nil {
		// 	}
		// }
		err := client.addRoute(routeID, messageHandler, systemAcks, userAcks, isSubscribe, isReplace)
		if err != nil { return commandID, err }
		if state != nil {
			state.lock.Lock()
			subscriptionManager := state.subscriptionManager
			state.lock.Unlock()
			if subscriptionManager != nil {
				subscriptionManager.Subscribe(messageHandler, cloneCommand(command), userAcks)
			}
		}

	case CommandUnsubscribe:
		// delete all subscriptions
		systemAcks = AckTypeNone
		client.syncAckProcessing = nil
		subID, hasSubID := command.SubID()
		if !hasSubID || subID == "all" {
			// Close the message streams first
			var closeErr error;
			client.messageStreams.Range(func(key interface{}, ms interface{}) bool {
				// client.msgRouter.RemoveRoute(key.(string))
				// return client.msgRouter.FindRoute(key.(string)) == nil
				closeErr = client.deleteRoute(key.(string));
				return closeErr == nil
			})
			if closeErr != nil { return subID, errors.New("Error deleting routes") }
			client.routes = new(sync.Map)
		} else {
			// client.msgRouter.RemoveRoute(subID)
			err := client.deleteRoute(subID);
			if err != nil { return subID, errors.New("Error deleting route") }
		}
		if state != nil {
			state.lock.Lock()
			subscriptionManager := state.subscriptionManager
			state.lock.Unlock()
			if subscriptionManager != nil {
				subscriptionManager.Unsubscribe(subID)
			}
		}

	case CommandFlush:
		systemAcks = AckTypeProcessed

		client.syncAckProcessing = make(chan _Result)
		routeID = commandID
		// client.msgRouter.AddRoute(commandID, messageHandler, userAcks, systemAcks, false, false)
		err := client.addRoute(commandID, messageHandler, systemAcks, userAcks, false, false)
		if err != nil { return commandID, err }

	case CommandSOWDelete:
		fallthrough

	case CommandDeltaPublish:
		fallthrough

	case CommandPublish:
		isPublish = true
		systemAcks = AckTypeNone
		client.registerPendingPublishCommand(commandID, command)
		fallthrough

	case commandStartTimer:
		fallthrough

	case commandStopTimer:
		fallthrough

	case commandHeartbeat:
		// assign a handler for a user-requested acks, if any
		if hasUserAcks {
			routeID = commandID
			// client.msgRouter.AddRoute(commandID, messageHandler, userAcks, AckTypeNone, false, false)
			err := client.addRoute(commandID, messageHandler, AckTypeNone, userAcks, false, false)
			if err != nil { return commandID, err }
		}
	}

	// Add user-requested acks (those will be delivered in the message handler as well)
	if command.header.ackType != nil {
		*command.header.ackType |= systemAcks
	} else {
		command.header.ackType = &systemAcks
	}

	// Send the command
	if isPublish {
		if storeErr := client.storePublishCommand(command); storeErr != nil {
			return commandID, storeErr
		}
	}

	if client.connected {
		// Need to wait for processed ack case
		if client.syncAckProcessing != nil {
			client.acksLock.Lock()

			// Send the command
			err := client.send(command)
			if err != nil {
				if client.shouldRetryCommand(command.header.command) {
					client.queueRetryCommand(command, messageHandler)
				}
				// delete a subscription, if any
				// client.msgRouter.RemoveRoute(routeID)
				err = client.deleteRoute(routeID)
				if err != nil { return routeID, errors.New("Error deleting route") }

				// close channel, if any
				client.ackProcessingLock.Lock()
				if client.syncAckProcessing != nil {
					close(client.syncAckProcessing)
					client.syncAckProcessing = nil
				}
				client.ackProcessingLock.Unlock()

				// Remove the lock
				client.acksLock.Unlock()

				// now we're done
				client.handleSendFailure(command, err)
				return commandID, NewError(DisconnectedError, err)
			}

			// Wait for the ack response
			result := <-client.syncAckProcessing
			client.ackProcessingLock.Lock()
			if client.syncAckProcessing != nil {
				close(client.syncAckProcessing)
				client.syncAckProcessing = nil
			}
			client.ackProcessingLock.Unlock()

			// Success
			if result.Status { return routeID, nil }

			// command was invalid -- delete a subscription, if any
			// client.msgRouter.RemoveRoute(routeID)
			err = client.deleteRoute(routeID)
			if err != nil { return routeID, errors.New("Error deleting route") }

			return commandID, reasonToError(result.Reason)
		}

		// Send the command
		err := client.send(command)
		if err != nil {
			if client.shouldRetryCommand(command.header.command) {
				client.queueRetryCommand(command, messageHandler)
			}
			// delete a subscription, if any
			// client.msgRouter.RemoveRoute(routeID)
			err = client.deleteRoute(routeID)
			if err != nil { return routeID, errors.New("Error deleting route") }

			// now we're done
			client.handleSendFailure(command, err)
			return commandID, NewError(DisconnectedError, err)
		}

		return routeID, nil
	}

	return commandID, NewError(DisconnectedError, "Client is not connected while trying to send data")
}


// SubscribeAsync performs the subscribe command. The subscribe command is the primary way to retrieve messages from the
// AMPS processing stream. A client can issue a subscribe command on a topic to receive all published messages to that
// topic in the future. Additionally, content filtering can be used to choose which messages the client is interested in
// receiving.
//
// Example:
//
//		subID, err := client.SubscribeAsync(func(message *amps.Message) error {
//			fmt.Println("Message Data:", string(message.Data()))
//			return nil
//		}, "invoices", "/price < 10000")
//
//
// Arguments:
//
// messageHandler [func(*Message) error] - The message handler that will be called each time a message is received.
// Message handler is called from the background thread context.
//
// topic [string] - The topic argument.
//
// filter [string] (optional) - An optional content filter value.
func (client *Client) SubscribeAsync(messageHandler func(*Message) error, topic string, filter ...string) (string, error) {
	cmd := NewCommand("subscribe").SetTopic(topic)
	if len(filter) > 0 { cmd.SetFilter(filter[0]) }
	return client.ExecuteAsync(cmd, messageHandler)
}


// DeltaSubscribeAsync performs the delta_subscribe command. The delta_subscribe command is like the subscribe command
// except that subscriptions placed through delta_subscribe will receive only messages that have changed between the SOW
// record and the new update. If delta_subscribe is used on a record which does not currently exist in the SOW or if it
// is used on a topic which does not have a SOW-topic store defined, then delta_subscribe behaves like a subscribe command.
//
// Example:
//
//		subID, err := client.DeltaSubscribeAsync(func(message *amps.Message) error {
//			fmt.Println("Message Data:", string(message.Data()))
//			return nil
//		}, "orders")
//
//
// Arguments:
//
// messageHandler [func(*Message) error] - The message handler that will be called each time a message is received.
// Message handler is called from the background thread context.
//
// topic [string] - The topic argument in SOW.
//
// filter [string] (optional) - An optional content filter value.
func (client *Client) DeltaSubscribeAsync(messageHandler func(*Message) error, topic string, filter ...string) (string, error) {
	cmd := NewCommand("delta_subscribe").SetTopic(topic)
	if len(filter) > 0 { cmd.SetFilter(filter[0]) }
	return client.ExecuteAsync(cmd, messageHandler)
}


// SowAsync performs the sow command. The sow command is used to query the contents of a previously defined SOW Topic.
// A sow command can be used to query an entire SOW Topic, or a filter can be used to further refine the results found
// inside a SOW Topic. For more information, see the State of the World and SOW Queries chapters in the AMPS User Guide.
//
// Example:
//
//		subID, err := client.SowAsync(func(message *amps.Message) error {
//			fmt.Println("Message Data:", string(message.Data()))
//			return nil
//		}, "orders")
//
//
// Arguments:
//
// messageHandler [func(*Message) error] - The message handler that will be called each time a message is received.
// Message handler is called from the background thread context.
//
// topic [string] - The topic argument in SOW.
//
// filter [string] (optional) - An optional content filter value.
func (client *Client) SowAsync(messageHandler func(*Message) error, topic string, filter ...string) (string, error) {
	cmd := NewCommand("sow").SetTopic(topic)
	if len(filter) > 0 { cmd.SetFilter(filter[0]) }
	return client.ExecuteAsync(cmd, messageHandler)
}


// SowAndSubscribeAsync performs the sow_and_subscribe command. A sow_and_subscribe command is used to combine the
// functionality of sow and a subscribe command in a single command. The sow_and_subscribe command is used:
//
// - to query the contents of a SOW topic (this is the sow command); and
//
// - to place a subscription such that any messages matching the subscribed SOW topic and query filter will be published
// to the AMPS client (this is the subscribe command).
//
// As with the subscribe command, publish messages representing updates to SOW records will contain only information
// that has changed.
//
// Example:
//
//		subID, err := client.SowAndSubscribeAsync(func(message *amps.Message) error {
//			fmt.Println("Message Data:", string(message.Data()))
//			return nil
//		}, "orders")
//
//
// Arguments:
//
// messageHandler [func(*Message) error] - The message handler that will be called each time a message is received.
// Message handler is called from the background thread context.
//
// topic [string] - The topic argument in SOW.
//
// filter [string] (optional) - An optional content filter value.
func (client *Client) SowAndSubscribeAsync(messageHandler func(*Message) error, topic string, filter ...string) (string, error) {
	cmd := NewCommand("sow_and_subscribe").SetTopic(topic)
	if len(filter) > 0 { cmd.SetFilter(filter[0]) }
	return client.ExecuteAsync(cmd, messageHandler)
}


// SowAndDeltaSubscribeAsync performs the sow_and_delta_subscribe command. A sow_and_delta_subscribe command is used to
// combine the functionality of commands sow and a delta_subscribe in a single command. The sow_and_delta_subscribe
// command is used:
//
// - to query the contents of a SOW topic (this is the sow command); and
//
// - to place a subscription such that any messages matching the subscribed SOW topic and query filter will be published
// to the AMPS client (this is the delta_subscribe command).
//
// As with the delta_subscribe command, publish messages representing updates to SOW records will contain only the
// information that has changed. If a sow_and_delta_subscribe is issued on a record that does not currently exist in the
// SOW topic, or if it is used on a topic that does not have a SOW-topic store defined, then a sow_and_delta_subscribe
// will behave like a sow_and_subscribe command.
//
// Example:
//
//		subID, err := client.SowAndDeltaSubscribeAsync(func(message *amps.Message) error {
//			fmt.Println("Message Data:", string(message.Data()))
//			return nil
//		}, "orders")
//
//
// Arguments:
//
// messageHandler [func(*Message) error] - The message handler that will be called each time a message is received.
// Message handler is called from the background thread context.
//
// topic [string] - The topic argument in SOW.
//
// filter [string] (optional) - An optional content filter value.
func (client *Client) SowAndDeltaSubscribeAsync(messageHandler func(*Message) error, topic string, filter ...string) (string, error) {
	cmd := NewCommand("sow_and_delta_subscribe").SetTopic(topic)
	if len(filter) > 0 { cmd.SetFilter(filter[0]) }
	return client.ExecuteAsync(cmd, messageHandler)
}


// SowDelete executes a SOW delete with a filter.
//
// Example:
//
//     stats, err := client.SowDelete("orders", "1=1")
//     if err != nil { ... }
//
//     fmt.Println(stats.AckType())
//     fmt.Println(stats.Status())
//
// Arguments:
//
// topic [string] - The topic to execute the SOW delete against.
//
// filter [string] - The filter. To delete all records, set a filter that is always true: "1 = 1"
//
// Returns the stats message in case of success, error as the second argument otherwise.
func (client *Client) SowDelete(topic string, filter string) (*Message, error) {
	result := make(chan _Stats)

	cmd := NewCommand("sow_delete").SetTopic(topic).SetFilter(filter).AddAckType(AckTypeStats)
	_, err := client.ExecuteAsync(cmd, func(message *Message) error {
		if ackType, hasAckType := message.AckType(); hasAckType && ackType == AckTypeStats {
			status, _ := message.Status()
			if status != "success" {
				result <- _Stats{Error: reasonToError(string(message.header.reason))}
			} else {
				result <- _Stats{Stats: message.Copy()}
			}
		} else {
			result <- _Stats{Error: NewError(UnknownError, "Unexpected response from AMPS")}
		}

		return nil
	})

	// the command failed before getting the ack
	if err != nil { return nil, err }

	// Waiting for the result
	stats := <-result
	close(result)

	return stats.Stats, stats.Error
}


// SowDeleteByData deletes a message from a SOW, using data supplied to locate a SOW entry with matching keys.
//
// Example:
//
//     topic, _ := message.Topic()
//
//     stats, err := client.SowDeleteByData(topic, message.Data())
//     if err != nil { ... }
//
//     fmt.Println(stats.AckType())
//     fmt.Println(stats.Status())
//
// Arguments:
//
// topic [string] - The topic to execute the SOW delete against.
//
// data [[]byte] - The message data whose keys match the message to be deleted in the server’s SOW.
//
// Returns the stats message in case of success, error as the second argument otherwise.
func (client *Client) SowDeleteByData(topic string, data []byte) (*Message, error) {
	result := make(chan _Stats)

	cmd := NewCommand("sow_delete").SetTopic(topic).SetData(data).AddAckType(AckTypeStats)
	_, err := client.ExecuteAsync(cmd, func(message *Message) error {
		if ackType, hasAckType := message.AckType(); hasAckType && ackType == AckTypeStats {
			status, _ := message.Status()
			if status != "success" {
				result <- _Stats{Error: reasonToError(string(message.header.reason))}
			} else {
				result <- _Stats{Stats: message.Copy()}
			}
		} else {
			result <- _Stats{Error: NewError(UnknownError, "Unexpected response from AMPS")}
		}

		return nil
	})

	// the command failed before getting the ack
	if err != nil { return nil, err }

	// Waiting for the result
	stats := <-result
	close(result)

	return stats.Stats, stats.Error
}


// SowDeleteByKeys executes a SOW delete with sow keys (supplied as a comma-separated values in a string). SOW keys are
// provided in the header of a SOW message, and are the internal identifier AMPS uses for that SOW message.
//
// Example:
//
//     topic, _ := sowMessage1.Topic()
//     firstMessageKey, _ := sowMessage1.SowKey()
//     secondMessageKey, _ := sowMessage2.SowKey()
//
//     stats, err := client.SowDeleteByKeys(topic, firstMessageKey + "," + secondMessageKey)
//     if err != nil { ... }
//
//     fmt.Println(stats.AckType())
//     fmt.Println(stats.Status())
//
// Arguments:
//
// topic [string] - The topic to execute the SOW delete against.
//
// keys [string] - A comma separated list of SOW keys to be deleted. SOW keys are provided in the header of a SOW
// message, and are the internal identifier AMPS uses for that SOW message.
//
// Returns the stats message in case of success, error as the second argument otherwise.
func (client *Client) SowDeleteByKeys(topic string, keys string) (*Message, error) {
	result := make(chan _Stats)

	cmd := NewCommand("sow_delete").SetTopic(topic).SetSowKeys(keys).AddAckType(AckTypeStats)
	_, err := client.ExecuteAsync(cmd, func(message *Message) error {
		if ackType, hasAckType := message.AckType(); hasAckType && ackType == AckTypeStats {
			status, _ := message.Status()
			if status != "success" {
				result <- _Stats{Error: reasonToError(string(message.header.reason))}
			} else {
				result <- _Stats{Stats: message.Copy()}
			}
		} else {
			result <- _Stats{Error: NewError(UnknownError, "Unexpected response from AMPS")}
		}

		return nil
	})

	// the command failed before getting the ack
	if err != nil { return nil, err }

	// Waiting for the result
	stats := <-result
	close(result)

	return stats.Stats, stats.Error
}


// Unsubscribe performs the unsubscribe command. The unsubscribe command unsubscribes the client from the topic which
// messages the client is is no more interested in receiving. If the subID is not provided, the client will unsubscribe
// from all subscriptions.
//
// Arguments:
//
// subID [string] (optional) - The id of the subscription to unsubscribe from. If not provided, the client will
// unsubscribe from all subscriptions.
func (client *Client) Unsubscribe(subID ...string) error {
	var subIDInternal string
	if len(subID) > 0 { subIDInternal = subID[0] } else  { subIDInternal = "all" }

	cmd := NewCommand("unsubscribe").SetSubID(subIDInternal)
	_, err := client.ExecuteAsync(cmd, nil)

	return err
}


// Disconnect disconnects the client from an AMPS server (if the connection existed). Same as Close()
func (client *Client) Disconnect() (err error) {
	client.lock.Lock()
	defer client.lock.Unlock()
	client.markManualDisconnect(true)
	if state := ensureClientState(client); state != nil {
		state.lock.Lock()
		if state.ackTimer != nil {
			_ = state.ackTimer.Stop()
			state.ackTimer = nil
		}
		state.pendingAcks = make(map[string]*pendingAckBatch)
		state.pendingAckCount = 0
		state.lock.Unlock()
	}

	client.connected = false
	client.logging = false
	client.stopped = true
	client.notifyConnectionState(ConnectionStateShutdown)

	// wipe the routes
	client.routes = new(sync.Map)
	client.messageStreams = new(sync.Map)
	// client.msgRouter.Clear()

	if client.connection != nil {
		err = client.connection.Close()
		client.connection = nil
		if err != nil { return NewError(ConnectionError, err) }
		client.notifyConnectionState(ConnectionStateDisconnected)
		return
	}

	// destroying heartbeat
	if client.heartbeatTimeout != 0 {
		_ = client.heartbeatTimeoutID.Stop()
		client.heartbeatTimeout = 0
		client.heartbeatInterval = 0
		client.heartbeatTimestamp = 0
		client.heartbeatTimeoutID = nil
	}

	client.ackProcessingLock.Lock()
	if client.syncAckProcessing != nil {
		close(client.syncAckProcessing)
		client.syncAckProcessing = nil
	}
	client.ackProcessingLock.Unlock()

	client.notifyConnectionState(ConnectionStateDisconnected)
	return NewError(DisconnectedError, "Client is not Connected")
}


// Close disconnects the client from an AMPS server (if the connection existed).
func (client *Client) Close() error {
	return client.Disconnect()
}


// NewClient creates a new Client object and returns it.
//
// Arguments:
//
//
// clientName [string] (optional)
//
// The client name is the optional parameter. Unique name for the client is required (important for queues and sow).
// It is strongly recommended to set a client name, but if it's not set, it'll be assigned automatically.
func NewClient(clientName ...string) *Client {
	var clientNameInternal string
	if len(clientName) > 0 {
		clientNameInternal = clientName[0]
	} else {
		clientNameInternal = ClientVersion + "-" + strconv.FormatInt(time.Now().Unix(), 10) +
		"-" + strconv.FormatInt(rand.Int63n(1000000000000), 10)
	}

	client := &Client{
		clientName: clientNameInternal,
		sendBuffer: bytes.NewBuffer(nil),
		command: &Command{header: new(_Header)},
		hbCommand: &Command{header: new(_Header)},
		message: &Message{header: new(_Header)},
		routes: new(sync.Map),
		messageStreams: new(sync.Map),
	}

	state := ensureClientState(client)
	if state != nil {
		state.lock.Lock()
		if state.subscriptionManager == nil {
			state.subscriptionManager = NewDefaultSubscriptionManager()
		}
		state.lock.Unlock()
	}

	return client
}
