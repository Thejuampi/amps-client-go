package amps

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ClientVersion and related constants define protocol and client behavior values.
const (
	ClientVersion = "0.1.0"

	BookmarksEPOCH  = "0"
	BookmarksRECENT = "recent"
	BookmarksNOW    = "0|1|"

	AckTypeNone      = 0
	AckTypeReceived  = 1
	AckTypeParsed    = 2
	AckTypeProcessed = 4
	AckTypePersisted = 8
	AckTypeCompleted = 16
	AckTypeStats     = 32
)

// Client manages a single AMPS connection, command execution, and message routing.
type Client struct {
	clientName         string
	nameHash           string
	nameHashValue      uint64
	serverVersion      string
	logonCorrelationID string
	nextID             uint64
	messageType        []byte
	errorHandler       func(err error)
	disconnectHandler  func(client *Client, err error)
	heartbeatInterval  uint
	heartbeatTimeout   uint
	heartbeatTimestamp uint
	heartbeatTimeoutID *time.Timer

	lock              sync.Mutex
	acksLock          sync.Mutex
	ackProcessingLock sync.Mutex
	syncAckProcessing chan _Result
	routes            *sync.Map
	messageStreams    *sync.Map

	connected  bool
	connection net.Conn
	logging    bool
	url        *url.URL
	tlsConfig  *tls.Config

	sendBuffer *bytes.Buffer
	command    *Command
	sendHb     bool
	hbCommand  *Command

	stopped         bool
	readTimeout     uint
	receiveBuffer   []byte
	readPosition    int
	receivePosition int
	lengthBytes     []byte
	message         *Message
	msgRouter       *MessageRouter
}

type _Result struct {
	Status bool
	Reason string
}

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
	if !client.connected {
		return errors.New("Client is not connected while trying to send data")
	}
	if client.sendBuffer == nil {
		return errors.New("Socket error while sending message (NullPointer)")
	}

	client.sendBuffer.Reset()

	_, err = client.sendBuffer.WriteString("    ")
	if err != nil {
		return
	}

	err = command.header.write(client.sendBuffer)
	if err != nil {
		return
	}

	if command.data != nil {
		client.sendBuffer.Write(command.data)
		if err != nil {
			return
		}
	}

	length := uint32(client.sendBuffer.Len()) - 4
	rawBytes := client.sendBuffer.Bytes()
	rawBytes[0] = (byte)((length & 0xFF000000) >> 24)
	rawBytes[1] = (byte)((length & 0x00FF0000) >> 16)
	rawBytes[2] = (byte)((length & 0x0000FF00) >> 8)
	rawBytes[3] = (byte)(length & 0x000000FF)

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

	if err != nil {
		client.onConnectionError(NewError(ConnectionError, fmt.Sprintf("Socket error while sending message (%v)", err)))
	}

	return
}

func (client *Client) readRoutine() {
	if !client.connected {
		return
	}
	if client.stopped {
		return
	}
	client.callReceiveRoutineStartedCallback()

	client.receiveBuffer = make([]byte, 128*1024)
	client.lengthBytes = make([]byte, 4)
	client.readTimeout = 0
	client.readPosition = 0
	client.receivePosition = 0

	for {
		if client.stopped {
			return
		}

		for client.receivePosition-client.readPosition < 4 {
			count, err := client.connection.Read(client.receiveBuffer[client.receivePosition:])
			client.receivePosition += count
			if err != nil {
				if client.connected {
					client.onConnectionError(NewError(ConnectionError, fmt.Sprintf("Socket Read Error: (%v)", err)))
				}

				return
			}
		}

		for client.receivePosition-client.readPosition > 4 {
			messageLength := int(client.receiveBuffer[client.readPosition])<<24 +
				int(client.receiveBuffer[client.readPosition+1])<<16 +
				int(client.receiveBuffer[client.readPosition+2])<<8 +
				int(client.receiveBuffer[client.readPosition+3])

			endByte := client.readPosition + messageLength + 4

			for endByte > client.receivePosition {
				if endByte > len(client.receiveBuffer) {

					if messageLength > len(client.receiveBuffer) {

						newBuffer := make([]byte, 2*len(client.receiveBuffer))
						copy(newBuffer, client.receiveBuffer[client.readPosition:client.receivePosition])
						client.receiveBuffer = newBuffer
					} else {

						copy(client.receiveBuffer, client.receiveBuffer[client.readPosition:client.receivePosition])
					}

					client.receivePosition -= client.readPosition
					endByte -= client.readPosition
					client.readPosition = 0
				}

				count, err := client.connection.Read(client.receiveBuffer[client.receivePosition:])
				client.receivePosition += count
				if err != nil {
					if client.connected {
						client.onConnectionError(NewError(ConnectionError, fmt.Sprintf("Socket Read Error: (%v)", err)))
					}

					return
				}
			}

			filteredFrame := client.applyTransportFilter(TransportFilterInbound, client.receiveBuffer[client.readPosition:endByte])
			if len(filteredFrame) < 4 {
				client.onError(NewError(ProtocolError, "invalid inbound frame"))
				return
			}

			left, err := parseHeader(client.message, true, filteredFrame[4:])
			if err != nil {
				client.onError(NewError(ProtocolError))
				return
			}

			if client.message.header.command == CommandSOW {
				for len(left) > 0 {

					left, err = parseHeader(client.message, false, left)
					if err != nil {
						client.onError(NewError(ProtocolError, err))
						return
					}

					dataLength := (*client.message.header.messageLength)
					client.message.data = left[:dataLength]

					err = client.onMessage(client.message)
					if err != nil {
						client.onError(NewError(MessageHandlerError, err))
					}

					left = left[dataLength:]
				}
			} else {

				client.message.data = left

				err = client.onMessage(client.message)
				if err != nil {
					client.onError(NewError(MessageHandlerError, err))
				}
			}

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
	message.client = client
	message.valid = true
	message.rawTransmissionTime = time.Now().UTC().Format(time.RFC3339Nano)

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

	if globalErr, globalHandled := client.callGlobalCommandTypeHandler(command, message); globalHandled {
		handled = true
		if err == nil {
			err = globalErr
		}
	}

	if (command == CommandPublish || command == CommandSOW || command == CommandOOF) && client.detectAndTrackDuplicate(message) {
		handled = true
		if duplicateErr := client.callDuplicateHandler(message); err == nil {
			err = duplicateErr
		}
	}

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
		messageStream.(*MessageStream).client = nil
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
) (routeErr error) {
	success := systemAcks == AckTypeNone

	previousMessageHandler, messageHandlerExists := client.routes.Load(routeID)

	if !isReplace {
		if messageHandlerExists {
			if isSubscribe {
				return NewError(SubidInUseError, "Subscription with ID '"+routeID+"' already exists")
			}
			return NewError(CommandError, "SubID is set to a non-Subscribe command")
		}
	} else {

		if messageHandler == nil {
			messageHandler = previousMessageHandler.(func(*Message) error)
		}
	}

	client.routes.Store(routeID, func(message *Message) error {
		var err error

		if message.header.command == CommandAck {
			ack, _ := message.AckType()

			if systemAcks&ack > 0 {

				systemAcks &^= ack

				if ack == AckTypeProcessed && client.syncAckProcessing != nil {
					status, _ := message.Status()
					reason, _ := message.Reason()
					success = status == "success"
					client.syncAckProcessing <- _Result{success, reason}

					client.acksLock.Unlock()
				}
			}

			if !success {
				return nil
			}

			if requestedAcks&ack > 0 {

				requestedAcks &^= ack

				err = messageHandler(message)
				if err != nil {
					err = NewError(MessageHandlerError, err)
				}
			}

			if systemAcks == AckTypeNone && requestedAcks == AckTypeNone && !isSubscribe {
				err = client.deleteRoute(routeID)
				if err != nil {
					err = errors.New("Error deleting route")
				}
			} else if systemAcks == AckTypeNone {

				client.routes.Store(routeID, messageHandler)
			}
		} else {

			if messageHandler != nil {
				err = messageHandler(message)
				if err != nil {
					err = NewError(MessageHandlerError, err)
				}
			}
		}

		return err
	})

	return
}

func (client *Client) onError(err error) {
	if client.errorHandler != nil {
		client.errorHandler(err)
	}
	client.reportException(err)
}

func (client *Client) onConnectionError(err error) {
	if client.connection != nil {
		_ = client.connection.Close()
	}
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

	client.routes = new(sync.Map)
	client.messageStreams = new(sync.Map)

	client.onError(err)
	client.onInternalDisconnect(err)

	if client.disconnectHandler != nil {
		client.disconnectHandler(client, err)
	}
}

func (client *Client) onHeartbeatAbsence() {
	if (uint(time.Now().Unix()) - client.heartbeatTimestamp) > (client.heartbeatTimeout * 1000) {
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
		if status == "success" && command == CommandAck && ackType == AckTypeProcessed {
			client.heartbeatTimeoutID = time.AfterFunc(time.Duration(client.heartbeatTimeout*1000), client.onHeartbeatAbsence)
			client.notifyConnectionState(ConnectionStateHeartbeatInitiated)
		} else {
			err = errors.New("Heartbeat Establishment Error")
		}

		done <- err

		return
	})

	if hbError == nil {
		return <-done
	}

	return
}

func (client *Client) checkAndSendHeartbeat(force bool) {
	if client.heartbeatTimeout == 0 || (client.heartbeatTimeout != 0 && client.heartbeatTimestamp == 0) {
		return
	}

	if force || ((uint(time.Now().Unix()) - client.heartbeatTimestamp) > client.heartbeatInterval) {
		client.heartbeatTimestamp = uint(time.Now().Unix())
		_ = client.heartbeatTimeoutID.Stop()
		client.heartbeatTimeoutID = time.AfterFunc(
			time.Duration(client.heartbeatTimeout*1000),
			client.onHeartbeatAbsence,
		)

		err := client.sendHeartbeat()
		if err != nil {
			client.onConnectionError(err)
		}
	}
}

// LogonParams stores exported state used by AMPS client APIs.
type LogonParams struct {
	Timeout       uint
	Authenticator Authenticator
	CorrelationID string
}

// ClientName executes the exported clientname operation.
func (client *Client) ClientName() string { return client.clientName }

// SetClientName sets client name on the receiver.
func (client *Client) SetClientName(clientName string) *Client {
	client.clientName = clientName
	return client
}

// ErrorHandler executes the exported errorhandler operation.
func (client *Client) ErrorHandler() func(error) { return client.errorHandler }

// SetErrorHandler sets error handler on the receiver.
func (client *Client) SetErrorHandler(errorHandler func(error)) *Client {
	client.errorHandler = errorHandler
	return client
}

// DisconnectHandler executes the exported disconnecthandler operation.
func (client *Client) DisconnectHandler() func(*Client, error) { return client.disconnectHandler }

// SetDisconnectHandler sets disconnect handler on the receiver.
func (client *Client) SetDisconnectHandler(disconnectHandler func(*Client, error)) *Client {
	client.disconnectHandler = disconnectHandler
	return client
}

// SetTLSConfig sets tlsconfig on the receiver.
func (client *Client) SetTLSConfig(config *tls.Config) *Client {
	client.tlsConfig = config
	return client
}

// LogonCorrelationID executes the exported logoncorrelationid operation.
func (client *Client) LogonCorrelationID() string { return client.logonCorrelationID }

// SetLogonCorrelationID sets logon correlation id on the receiver.
func (client *Client) SetLogonCorrelationID(logonCorrelationID string) *Client {
	client.logonCorrelationID = logonCorrelationID
	return client
}

// SetHeartbeat sets heartbeat on the receiver.
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

// ServerVersion executes the exported serverversion operation.
func (client *Client) ServerVersion() string { return client.serverVersion }

// Connect opens a transport connection to the provided AMPS URI.
func (client *Client) Connect(uri string) error {
	if client.connected {
		return NewError(AlreadyConnectedError)
	}

	client.logging = false

	if client.errorHandler == nil {
		client.errorHandler = func(err error) {
			fmt.Println(time.Now().Local().String()+" ["+client.clientName+"] >>>", err)
		}
	}

	client.lock.Lock()
	defer client.lock.Unlock()

	parsedURI, err := url.Parse(uri)
	if err != nil {
		return NewError(InvalidURIError, err)
	}
	state := ensureClientState(client)
	if state != nil {
		state.lock.Lock()
		state.uri = uri
		state.manualDisconnect = false
		state.lock.Unlock()
	}

	pathParts := strings.Split(parsedURI.Path, "/")
	partsLength := len(pathParts)
	if partsLength > 1 {
		if pathParts[1] != "amps" {
			return NewError(ProtocolError, "Specification of message type requires amps protocol")
		}

		if partsLength > 2 {
			client.messageType = []byte(pathParts[2])
		}
	}

	client.url = parsedURI

	if parsedURI.Scheme == "tcps" {
		if client.tlsConfig == nil {

			client.tlsConfig = &tls.Config{InsecureSkipVerify: true}
		}

		connection, err := tls.Dial("tcp", parsedURI.Host, client.tlsConfig)
		client.connection = connection
		if err != nil {
			return NewError(ConnectionRefusedError, err)
		}
	} else {
		connection, err := net.Dial("tcp", parsedURI.Host)
		client.connection = connection
		if err != nil {
			return NewError(ConnectionRefusedError, err)
		}
	}

	client.connected = true
	client.notifyConnectionState(ConnectionStateConnected)

	client.stopped = false
	go client.readRoutine()

	return nil
}

// Logon authenticates the connected session and initializes server metadata.
func (client *Client) Logon(optionalParams ...LogonParams) (err error) {
	client.lock.Lock()

	hasParams := len(optionalParams) > 0
	hasAuthenticator := hasParams && (optionalParams[0].Authenticator) != nil

	if hasParams && len(optionalParams[0].CorrelationID) > 0 {
		client.logonCorrelationID = optionalParams[0].CorrelationID
	}

	client.command.reset()
	client.command.header.command = commandLogon
	ack := AckTypeProcessed
	client.command.header.ackType = &ack

	commandID := client.makeCommandID()
	client.command.header.clientName = []byte(client.clientName)
	client.command.header.version = []byte(ClientVersion)
	client.command.header.messageType = client.messageType

	var username, password string
	user, hasUser := client.url.User, client.url.User != nil
	if hasUser {
		username = user.Username()

		client.command.header.userID = []byte(username)

		localPassword, hasPassword := user.Password()
		password = localPassword

		if hasAuthenticator {
			password, err = optionalParams[0].Authenticator.Authenticate(username, password)
			if err != nil {
				return NewError(AuthenticationError, err)
			}
			hasPassword = true
		}

		if hasPassword {
			client.command.header.password = []byte(password)
		}
	}

	if len(client.logonCorrelationID) > 0 {
		client.command.header.correlationID = []byte(client.logonCorrelationID)
	}

	doneLoggingIn := make(chan error)

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
					if len(message.header.clientName) > 0 {
						client.nameHash = string(message.header.clientName)
					} else {
						client.nameHash = fmt.Sprintf("%x", unsafeStringHash(client.clientName))
					}
					if parsedHash, parseErr := strconv.ParseUint(client.nameHash, 10, 64); parseErr == nil {
						client.nameHashValue = parsedHash
					} else {
						client.nameHashValue = unsafeStringHash(client.nameHash)
					}
					doneLoggingIn <- nil

				case "failure":
					if hasReason {
						loggingError = reasonToError(reason)
					} else {
						loggingError = NewError(AuthenticationError)
					}
					doneLoggingIn <- loggingError

				default:
					logonRetries--
					if logonRetries == 0 {
						loggingError = NewError(RetryOperationError, "Exceeded the maximum of logon retry attempts (3)")
						doneLoggingIn <- loggingError
						return
					}

					if hasAuthenticator {
						password, logonAckErr = optionalParams[0].Authenticator.Retry(username, password)
						if logonAckErr != nil {
							return NewError(AuthenticationError, logonAckErr)
						}
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

	logonFailed := <-doneLoggingIn
	client.logging = false

	client.routes.Delete(commandID)

	if logonFailed == nil {
		client.lock.Unlock()
		client.notifyConnectionState(ConnectionStateLoggedOn)
		if bookmarkStore := client.BookmarkStore(); bookmarkStore != nil {
			bookmarkStore.SetServerVersion(client.serverVersion)
		}
		if client.heartbeatTimeout != 0 {

			client.hbCommand.reset()
			client.hbCommand.header.command = commandHeartbeat
			client.hbCommand.header.options = []byte("beat")

			err = client.establishHeartbeat()
			if err != nil {
				client.onError(err)
				return
			}
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

	err := client.send(client.hbCommand)
	if err != nil {
		return NewError(ConnectionError, err)
	}

	return nil
}

// Publish sends a textual publish command for the specified topic.
func (client *Client) Publish(topic string, data string, expiration ...uint) error {
	return client.PublishBytes(topic, []byte(data), expiration...)
}

// PublishBytes sends a binary publish command for the specified topic.
func (client *Client) PublishBytes(topic string, data []byte, expiration ...uint) error {
	_, err := client.publishBytesWithCommand(CommandPublish, topic, data, expiration...)
	return err
}

func (client *Client) publishBytesWithCommand(commandType int, topic string, data []byte, expiration ...uint) (uint64, error) {
	if len(topic) == 0 {
		return 0, NewError(InvalidTopicError, "A topic must be specified")
	}
	if !client.connected {
		return 0, NewError(DisconnectedError, "Client is not connected while trying to publish")
	}

	client.lock.Lock()
	defer client.lock.Unlock()

	client.command.reset()
	client.command.header.command = commandType
	client.command.header.topic = []byte(topic)
	client.command.data = data

	if len(expiration) > 0 {
		client.command.header.expiration = &(expiration[0])
	}
	commandID := client.makeCommandID()
	client.command.header.commandID = []byte(commandID)
	client.registerPendingPublishCommand(commandID, client.command)

	sequence := uint64(0)
	if storeErr := client.storePublishCommand(client.command); storeErr != nil {
		return 0, storeErr
	}
	if storedSequence, hasSequence := client.command.SequenceID(); hasSequence {
		sequence = storedSequence
	}

	err := client.send(client.command)
	if err != nil {
		client.handleSendFailure(client.command, err)
		return sequence, NewError(ConnectionError, err)
	}

	return sequence, nil
}

// DeltaPublish executes the exported deltapublish operation.
func (client *Client) DeltaPublish(topic string, data string, expiration ...uint) error {
	return client.DeltaPublishBytes(topic, []byte(data), expiration...)
}

// DeltaPublishBytes executes the exported deltapublishbytes operation.
func (client *Client) DeltaPublishBytes(topic string, data []byte, expiration ...uint) error {
	_, err := client.publishBytesWithCommand(CommandDeltaPublish, topic, data, expiration...)
	return err
}

// Execute sends a command and returns a MessageStream for synchronous consumption.
func (client *Client) Execute(command *Command) (*MessageStream, error) {
	var messageStream *MessageStream

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
	if !isSubscribe && hasAckType && (ackType&AckTypeStats) > 0 {
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

		return messageStream.messageHandler(message)
	})

	if isStatsOnly {
		messageStream.setStatsOnly()
	} else if isSow {
		messageStream.setQueryID(cmdID)
		if !isSubscribe {
			messageStream.setSowOnly()
		}
	} else if isSubscribe && !isSow {
		messageStream.setSubID(cmdID)
	}

	if err != nil {
		return nil, err
	}

	return messageStream, err
}

// Subscribe executes a subscription command and returns a MessageStream.
func (client *Client) Subscribe(topic string, filter ...string) (*MessageStream, error) {
	cmd := NewCommand("subscribe").SetTopic(topic)
	if len(filter) > 0 {
		cmd.SetFilter(filter[0])
	}
	return client.Execute(cmd)
}

// Flush executes the exported flush operation.
func (client *Client) Flush() (err error) {
	result := make(chan error)

	cmd := NewCommand("flush").AddAckType(AckTypeCompleted)
	_, err = client.ExecuteAsync(cmd, func(message *Message) error {

		if status, hasStatus := client.message.Status(); hasStatus && status == "success" {
			result <- nil
			return nil
		}

		reason, hasReason := client.message.Reason()
		if hasReason {
			result <- errors.New(reason)
			return errors.New(reason)
		}
		return nil
	})

	if err != nil {
		return err
	}
	return nil
}

// DeltaSubscribe executes the exported deltasubscribe operation.
func (client *Client) DeltaSubscribe(topic string, filter ...string) (*MessageStream, error) {
	cmd := NewCommand("delta_subscribe").SetTopic(topic)
	if len(filter) > 0 {
		cmd.SetFilter(filter[0])
	}
	return client.Execute(cmd)
}

// Sow executes the exported sow operation.
func (client *Client) Sow(topic string, filter ...string) (*MessageStream, error) {
	cmd := NewCommand("sow").SetTopic(topic)
	if len(filter) > 0 {
		cmd.SetFilter(filter[0])
	}
	return client.Execute(cmd)
}

// SowAndSubscribe executes the exported sowandsubscribe operation.
func (client *Client) SowAndSubscribe(topic string, filter ...string) (*MessageStream, error) {
	cmd := NewCommand("sow_and_subscribe").SetTopic(topic)
	if len(filter) > 0 {
		cmd.SetFilter(filter[0])
	}
	return client.Execute(cmd)
}

// SowAndDeltaSubscribe executes the exported sowanddeltasubscribe operation.
func (client *Client) SowAndDeltaSubscribe(topic string, filter ...string) (*MessageStream, error) {
	cmd := NewCommand("sow_and_delta_subscribe").SetTopic(topic)
	if len(filter) > 0 {
		cmd.SetFilter(filter[0])
	}
	return client.Execute(cmd)
}

// ExecuteAsync sends a command and routes resulting messages to a callback.
func (client *Client) ExecuteAsync(command *Command, messageHandler func(message *Message) error) (string, error) {
	if command == nil || command.header.command == CommandUnknown {
		return "", NewError(CommandError, "Invalid Command provided")
	}

	client.lock.Lock()
	defer client.lock.Unlock()

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
		isSubscribe = true
		fallthrough

	case CommandSOW:
		if !notSow && command.header.queryID == nil {
			if command.header.subID != nil {
				command.header.queryID = command.header.subID
			} else {
				command.header.queryID = command.header.commandID
			}
		}

		systemAcks |= AckTypeProcessed

		if !isSubscribe {
			systemAcks |= AckTypeCompleted
		}

		_, hasBatchSizeSet := command.BatchSize()
		if !notSow && !hasBatchSizeSet {
			command.SetBatchSize(10)
		}

		if isSubscribe {
			if queryID, hasQueryID := command.QueryID(); hasQueryID && !notSow {
				routeID = queryID
			} else {
				routeID, _ = command.SubID()
			}

			if options, hasOptions := command.Options(); hasOptions {
				isReplace = strings.Contains(options, "replace")
			}
		} else {
			routeID = commandID
		}

		client.syncAckProcessing = make(chan _Result)

		err := client.addRoute(routeID, messageHandler, systemAcks, userAcks, isSubscribe, isReplace)
		if err != nil {
			return commandID, err
		}
		if state != nil {
			state.lock.Lock()
			subscriptionManager := state.subscriptionManager
			state.lock.Unlock()
			if subscriptionManager != nil {
				subscriptionManager.Subscribe(messageHandler, cloneCommand(command), userAcks)
			}
		}

	case CommandUnsubscribe:

		systemAcks = AckTypeNone
		client.syncAckProcessing = nil
		subID, hasSubID := command.SubID()
		if !hasSubID || subID == "all" {

			var closeErr error
			client.messageStreams.Range(func(key interface{}, ms interface{}) bool {

				closeErr = client.deleteRoute(key.(string))
				return closeErr == nil
			})
			if closeErr != nil {
				return subID, errors.New("Error deleting routes")
			}
			client.routes = new(sync.Map)
		} else {

			err := client.deleteRoute(subID)
			if err != nil {
				return subID, errors.New("Error deleting route")
			}
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

		err := client.addRoute(commandID, messageHandler, systemAcks, userAcks, false, false)
		if err != nil {
			return commandID, err
		}

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

		if hasUserAcks {
			routeID = commandID

			err := client.addRoute(commandID, messageHandler, AckTypeNone, userAcks, false, false)
			if err != nil {
				return commandID, err
			}
		}
	}

	if command.header.ackType != nil {
		*command.header.ackType |= systemAcks
	} else {
		command.header.ackType = &systemAcks
	}

	if isPublish {
		if storeErr := client.storePublishCommand(command); storeErr != nil {
			return commandID, storeErr
		}
	}

	if client.connected {
		if client.syncAckProcessing != nil {
			client.acksLock.Lock()

			sendErr := client.send(command)
			if sendErr != nil {
				if client.shouldRetryCommand(command.header.command) {
					client.queueRetryCommand(command, messageHandler)
				}

				routeErr := client.deleteRoute(routeID)
				if routeErr != nil {
					return routeID, errors.New("Error deleting route")
				}

				client.ackProcessingLock.Lock()
				if client.syncAckProcessing != nil {
					close(client.syncAckProcessing)
					client.syncAckProcessing = nil
				}
				client.ackProcessingLock.Unlock()

				client.acksLock.Unlock()

				client.handleSendFailure(command, sendErr)
				return commandID, NewError(DisconnectedError, sendErr)
			}

			result := <-client.syncAckProcessing
			client.ackProcessingLock.Lock()
			if client.syncAckProcessing != nil {
				close(client.syncAckProcessing)
				client.syncAckProcessing = nil
			}
			client.ackProcessingLock.Unlock()

			if result.Status {
				return routeID, nil
			}

			routeErr := client.deleteRoute(routeID)
			if routeErr != nil {
				return routeID, errors.New("Error deleting route")
			}

			return commandID, reasonToError(result.Reason)
		}

		sendErr := client.send(command)
		if sendErr != nil {
			if client.shouldRetryCommand(command.header.command) {
				client.queueRetryCommand(command, messageHandler)
			}

			routeErr := client.deleteRoute(routeID)
			if routeErr != nil {
				return routeID, errors.New("Error deleting route")
			}

			client.handleSendFailure(command, sendErr)
			return commandID, NewError(DisconnectedError, sendErr)
		}

		return routeID, nil
	}

	return commandID, NewError(DisconnectedError, "Client is not connected while trying to send data")
}

// SubscribeAsync executes a subscription command and dispatches messages to a callback.
func (client *Client) SubscribeAsync(messageHandler func(*Message) error, topic string, filter ...string) (string, error) {
	cmd := NewCommand("subscribe").SetTopic(topic)
	if len(filter) > 0 {
		cmd.SetFilter(filter[0])
	}
	return client.ExecuteAsync(cmd, messageHandler)
}

// DeltaSubscribeAsync performs the asynchronous deltasubscribeasync operation.
func (client *Client) DeltaSubscribeAsync(messageHandler func(*Message) error, topic string, filter ...string) (string, error) {
	cmd := NewCommand("delta_subscribe").SetTopic(topic)
	if len(filter) > 0 {
		cmd.SetFilter(filter[0])
	}
	return client.ExecuteAsync(cmd, messageHandler)
}

// SowAsync performs the asynchronous sowasync operation.
func (client *Client) SowAsync(messageHandler func(*Message) error, topic string, filter ...string) (string, error) {
	cmd := NewCommand("sow").SetTopic(topic)
	if len(filter) > 0 {
		cmd.SetFilter(filter[0])
	}
	return client.ExecuteAsync(cmd, messageHandler)
}

// SowAndSubscribeAsync performs the asynchronous sowandsubscribeasync operation.
func (client *Client) SowAndSubscribeAsync(messageHandler func(*Message) error, topic string, filter ...string) (string, error) {
	cmd := NewCommand("sow_and_subscribe").SetTopic(topic)
	if len(filter) > 0 {
		cmd.SetFilter(filter[0])
	}
	return client.ExecuteAsync(cmd, messageHandler)
}

// SowAndDeltaSubscribeAsync performs the asynchronous sowanddeltasubscribeasync operation.
func (client *Client) SowAndDeltaSubscribeAsync(messageHandler func(*Message) error, topic string, filter ...string) (string, error) {
	cmd := NewCommand("sow_and_delta_subscribe").SetTopic(topic)
	if len(filter) > 0 {
		cmd.SetFilter(filter[0])
	}
	return client.ExecuteAsync(cmd, messageHandler)
}

// SowDelete executes the exported sowdelete operation.
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

	if err != nil {
		return nil, err
	}

	stats := <-result
	close(result)

	return stats.Stats, stats.Error
}

// SowDeleteByData executes the exported sowdeletebydata operation.
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

	if err != nil {
		return nil, err
	}

	stats := <-result
	close(result)

	return stats.Stats, stats.Error
}

// SowDeleteByKeys executes the exported sowdeletebykeys operation.
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

	if err != nil {
		return nil, err
	}

	stats := <-result
	close(result)

	return stats.Stats, stats.Error
}

// Unsubscribe executes the exported unsubscribe operation.
func (client *Client) Unsubscribe(subID ...string) error {
	var subIDInternal string
	if len(subID) > 0 {
		subIDInternal = subID[0]
	} else {
		subIDInternal = "all"
	}

	cmd := NewCommand("unsubscribe").SetSubID(subIDInternal)
	_, err := client.ExecuteAsync(cmd, nil)

	return err
}

// Disconnect stops receive processing and closes the active connection.
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

	client.routes = new(sync.Map)
	client.messageStreams = new(sync.Map)

	if client.connection != nil {
		err = client.connection.Close()
		client.connection = nil
		if err != nil {
			return NewError(ConnectionError, err)
		}
		client.notifyConnectionState(ConnectionStateDisconnected)
		return
	}

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

// Close is an alias for Disconnect.
func (client *Client) Close() error {
	return client.Disconnect()
}

// NewClient returns a new Client.
func NewClient(clientName ...string) *Client {
	var clientNameInternal string
	if len(clientName) > 0 {
		clientNameInternal = clientName[0]
	} else {
		clientNameInternal = ClientVersion + "-" + strconv.FormatInt(time.Now().Unix(), 10) +
			"-" + strconv.FormatInt(rand.Int63n(1000000000000), 10)
	}

	client := &Client{
		clientName:     clientNameInternal,
		nameHash:       fmt.Sprintf("%x", unsafeStringHash(clientNameInternal)),
		sendBuffer:     bytes.NewBuffer(nil),
		command:        &Command{header: new(_Header)},
		hbCommand:      &Command{header: new(_Header)},
		message:        &Message{header: new(_Header)},
		routes:         new(sync.Map),
		messageStreams: new(sync.Map),
	}
	client.nameHashValue = unsafeStringHash(client.nameHash)

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
