package amps

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"
)

func (client *Client) reportException(err error) {
	if err == nil {
		return
	}

	state := ensureClientState(client)
	if state == nil {
		return
	}

	state.lock.Lock()
	listener := state.exceptionListener
	state.lock.Unlock()

	if listener != nil {
		listener.ExceptionThrown(err)
	}
}

func bookmarkStoreLog(store BookmarkStore, message *Message) (uint64, error) {
	if store == nil {
		return 0, nil
	}
	if storeWithErrors, ok := store.(BookmarkStoreWithErrors); ok {
		return storeWithErrors.LogWithError(message)
	}
	return store.Log(message), nil
}

func bookmarkStorePersisted(store BookmarkStore, subID string, bookmark string) (string, error) {
	if store == nil {
		return "", nil
	}
	if storeWithErrors, ok := store.(BookmarkStoreWithErrors); ok {
		return storeWithErrors.PersistedWithError(subID, bookmark)
	}
	return store.Persisted(subID, bookmark), nil
}

func bookmarkStoreSetServerVersion(store BookmarkStore, version string) error {
	if store == nil {
		return nil
	}
	if storeWithErrors, ok := store.(BookmarkStoreWithErrors); ok {
		return storeWithErrors.SetServerVersionWithError(version)
	}
	store.SetServerVersion(version)
	return nil
}

func (client *Client) notifyConnectionState(connectionState ConnectionState) {
	state := ensureClientState(client)
	if state == nil {
		return
	}
	state.broadcastConnectionState(connectionState)
}

func (client *Client) onInternalDisconnect(err error) {
	state := ensureClientState(client)
	if state == nil {
		return
	}

	state.lock.Lock()
	manualDisconnect := state.manualDisconnect
	callback := state.internalDisconnect
	state.lock.Unlock()

	if manualDisconnect || callback == nil {
		return
	}

	callback(err)
}

func (client *Client) markManualDisconnect(value bool) {
	state := ensureClientState(client)
	if state == nil {
		return
	}

	state.lock.Lock()
	state.manualDisconnect = value
	state.lock.Unlock()
}

func (client *Client) shouldRetryCommand(commandType int) bool {
	state := ensureClientState(client)
	if state == nil {
		return false
	}

	state.lock.Lock()
	retryOnDisconnect := state.retryOnDisconnect
	manualDisconnect := state.manualDisconnect
	state.lock.Unlock()
	if !retryOnDisconnect || manualDisconnect {
		return false
	}

	switch commandType {
	case CommandSubscribe, CommandDeltaSubscribe, CommandSOW, CommandSOWAndSubscribe, CommandSOWAndDeltaSubscribe, CommandSOWDelete, CommandFlush, CommandUnsubscribe, commandStartTimer, commandStopTimer:
		return true
	default:
		return false
	}
}

func (client *Client) queueRetryCommand(command *Command, messageHandler func(*Message) error) {
	state := ensureClientState(client)
	if state == nil || command == nil {
		return
	}

	state.lock.Lock()
	state.pendingRetry = append(state.pendingRetry, retryCommand{
		command:        cloneCommand(command),
		messageHandler: messageHandler,
	})
	state.lock.Unlock()
}

// maxPendingPublishRetained bounds how many un-acknowledged publish commands are
// retained for failed-write reporting. A publish that requests no acks can never
// be acknowledged, so without a bound the retention map grows for the lifetime
// of the connection. Oldest entries are evicted first: a failure ack is most
// likely to refer to a recent publish.
const maxPendingPublishRetained = 4096

// retainPendingPublishLocked stores a cloned command for failed-write reporting,
// evicting the oldest retained entries once the bound is exceeded. Callers must
// hold state.lock.
func retainPendingPublishLocked(state *clientParityState, commandID string, command *Command) {
	if _, exists := state.pendingPublishByCmdID[commandID]; !exists {
		state.pendingPublishOrder = append(state.pendingPublishOrder, commandID)
	}
	state.pendingPublishByCmdID[commandID] = cloneCommand(command)

	for len(state.pendingPublishOrder) > maxPendingPublishRetained {
		var oldest = state.pendingPublishOrder[0]
		state.pendingPublishOrder = state.pendingPublishOrder[1:]
		delete(state.pendingPublishByCmdID, oldest)
	}
}

// releasePendingPublishLocked drops a retained command once it is resolved.
// Callers must hold state.lock.
func releasePendingPublishLocked(state *clientParityState, commandID string) {
	if _, exists := state.pendingPublishByCmdID[commandID]; !exists {
		return
	}
	delete(state.pendingPublishByCmdID, commandID)
	for index, candidate := range state.pendingPublishOrder {
		if candidate == commandID {
			state.pendingPublishOrder = append(state.pendingPublishOrder[:index], state.pendingPublishOrder[index+1:]...)
			return
		}
	}
}

func (client *Client) registerPendingPublishCommand(commandID string, command *Command) {
	state := ensureClientState(client)
	if state == nil || command == nil || commandID == "" {
		return
	}

	state.lock.Lock()
	if state.failedWriteHandler == nil {
		state.lock.Unlock()
		return
	}
	retainPendingPublishLocked(state, commandID, command)
	state.lock.Unlock()
}

func (client *Client) registerPendingPublishCommandBytes(commandID []byte, command *Command) {
	state := ensureClientState(client)
	if state == nil || command == nil || len(commandID) == 0 {
		return
	}

	state.lock.Lock()
	if state.failedWriteHandler == nil {
		state.lock.Unlock()
		return
	}
	retainPendingPublishLocked(state, string(commandID), command)
	state.lock.Unlock()
}

func (client *Client) releasePendingPublishCommand(commandID string) {
	var state = client.parityState.Load()
	if state == nil || commandID == "" {
		return
	}
	state.lock.Lock()
	releasePendingPublishLocked(state, commandID)
	state.lock.Unlock()
}

func (client *Client) storePublishCommand(command *Command) (bool, error) {
	if command == nil {
		return false, nil
	}

	state := ensureClientState(client)
	if state == nil {
		return false, nil
	}

	if !state.hasPublishStore.Load() {
		return false, nil
	}

	state.lock.Lock()
	store := state.publishStore
	state.lock.Unlock()
	if store == nil {
		return false, nil
	}

	command.AddAckType(AckTypePersisted)
	sequence, err := store.Store(command)
	var accepted *PublishStoreAcceptedError
	if err != nil && (sequence == 0 || !errors.As(err, &accepted)) {
		return false, err
	}

	if sequence > 0 {
		command.SetSequenceID(sequence)
	}
	return true, err
}

func commandToMessage(command *Command) *Message {
	if command == nil || command.header == nil {
		return &Message{header: newHeader()}
	}

	message := newOwnedMessage()
	message.header.command = command.header.command

	if command.header.ackType != nil {
		ackType := *command.header.ackType
		message.header.ackType = &ackType
	}
	if command.header.batchSize != nil {
		batchSize := *command.header.batchSize
		message.header.batchSize = &batchSize
	}
	if command.header.expiration != nil {
		expiration := *command.header.expiration
		message.header.expiration = &expiration
	}
	if command.header.groupSequenceNumber != nil {
		gseq := *command.header.groupSequenceNumber
		message.header.groupSequenceNumber = &gseq
	}
	if command.header.maximumMessages != nil {
		maximumMessages := *command.header.maximumMessages
		message.header.maximumMessages = &maximumMessages
	}
	if command.header.matches != nil {
		matches := *command.header.matches
		message.header.matches = &matches
	}
	if command.header.messageLength != nil {
		msgLen := *command.header.messageLength
		message.header.messageLength = &msgLen
	}
	if command.header.recordsDeleted != nil {
		rD := *command.header.recordsDeleted
		message.header.recordsDeleted = &rD
	}
	if command.header.recordsInserted != nil {
		rI := *command.header.recordsInserted
		message.header.recordsInserted = &rI
	}
	if command.header.recordsReturned != nil {
		rR := *command.header.recordsReturned
		message.header.recordsReturned = &rR
	}
	if command.header.recordsUpdated != nil {
		rU := *command.header.recordsUpdated
		message.header.recordsUpdated = &rU
	}
	if command.header.sequenceID != nil {
		sequence := *command.header.sequenceID
		message.header.sequenceID = &sequence
	}
	if command.header.skipN != nil {
		skipN := *command.header.skipN
		message.header.skipN = &skipN
	}
	if command.header.timeoutInterval != nil {
		timeoutInterval := *command.header.timeoutInterval
		message.header.timeoutInterval = &timeoutInterval
	}
	if command.header.topN != nil {
		topN := *command.header.topN
		message.header.topN = &topN
	}
	if command.header.topicMatches != nil {
		tM := *command.header.topicMatches
		message.header.topicMatches = &tM
	}
	if command.header.gracePeriod != nil {
		gracePeriod := *command.header.gracePeriod
		message.header.gracePeriod = &gracePeriod
	}

	var commandTextExtras = headerTextExtras(command.header)
	var messageTextExtras *_HeaderTextExtras
	if commandTextExtras != nil {
		messageTextExtras = ensureHeaderTextExtras(message.header)
	}

	var totalBytes = len(command.data) +
		len(command.header.commandID) +
		len(command.header.topic) +
		len(command.header.bookmark) +
		len(command.header.correlationID) +
		len(commandTextBytes(commandTextExtras, func(extras *_HeaderTextExtras) []byte { return extras.dataOnly })) +
		len(command.header.filter) +
		len(commandTextBytes(commandTextExtras, func(extras *_HeaderTextExtras) []byte { return extras.leasePeriod })) +
		len(command.header.messageType) +
		len(command.header.options) +
		len(command.header.orderBy) +
		len(command.header.queryID) +
		len(commandTextBytes(commandTextExtras, func(extras *_HeaderTextExtras) []byte { return extras.sendEmpty })) +
		len(commandTextBytes(commandTextExtras, func(extras *_HeaderTextExtras) []byte { return extras.sendKeys })) +
		len(commandTextBytes(commandTextExtras, func(extras *_HeaderTextExtras) []byte { return extras.sendOOF })) +
		len(command.header.sowKey) +
		len(command.header.sowKeys) +
		len(command.header.subID) +
		len(command.header.subIDs)

	var buf []byte
	if totalBytes > 0 {
		buf = make([]byte, totalBytes)
	}
	buf, message.data = copyMessageBytes(buf, command.data)
	buf, message.header.commandID = copyMessageBytes(buf, command.header.commandID)
	buf, message.header.topic = copyMessageBytes(buf, command.header.topic)
	buf, message.header.bookmark = copyMessageBytes(buf, command.header.bookmark)
	buf, message.header.correlationID = copyMessageBytes(buf, command.header.correlationID)
	if messageTextExtras != nil {
		buf, messageTextExtras.dataOnly = copyMessageBytes(buf, commandTextExtras.dataOnly)
	}
	buf, message.header.filter = copyMessageBytes(buf, command.header.filter)
	if messageTextExtras != nil {
		buf, messageTextExtras.leasePeriod = copyMessageBytes(buf, commandTextExtras.leasePeriod)
	}
	buf, message.header.messageType = copyMessageBytes(buf, command.header.messageType)
	buf, message.header.options = copyMessageBytes(buf, command.header.options)
	buf, message.header.orderBy = copyMessageBytes(buf, command.header.orderBy)
	buf, message.header.queryID = copyMessageBytes(buf, command.header.queryID)
	if messageTextExtras != nil {
		buf, messageTextExtras.sendEmpty = copyMessageBytes(buf, commandTextExtras.sendEmpty)
		buf, messageTextExtras.sendKeys = copyMessageBytes(buf, commandTextExtras.sendKeys)
		buf, messageTextExtras.sendOOF = copyMessageBytes(buf, commandTextExtras.sendOOF)
	}
	buf, message.header.sowKey = copyMessageBytes(buf, command.header.sowKey)
	buf, message.header.sowKeys = copyMessageBytes(buf, command.header.sowKeys)
	buf, message.header.subID = copyMessageBytes(buf, command.header.subID)
	_, message.header.subIDs = copyMessageBytes(buf, command.header.subIDs)

	return message
}

func (client *Client) handleSendFailure(command *Command, sendErr error) {
	state := ensureClientState(client)
	if state == nil || sendErr == nil || command == nil || command.header == nil {
		return
	}

	if command.header.command != CommandPublish && command.header.command != CommandDeltaPublish && command.header.command != CommandSOWDelete {
		return
	}

	state.lock.Lock()
	handler := state.failedWriteHandler
	state.lock.Unlock()
	if handler == nil {
		return
	}

	message := commandToMessage(command)
	handler.FailedWrite(message, sendErr.Error())
}

func (client *Client) applyAckBookkeeping(message *Message) {
	if message == nil {
		return
	}
	command, _ := message.Command()
	if command != CommandAck {
		return
	}

	state := ensureClientState(client)
	if state == nil {
		return
	}

	ackType, _ := message.AckType()
	status, _ := message.Status()
	commandID, _ := message.CommandID()

	state.lock.Lock()
	store := state.publishStore
	bookmarkStore := state.bookmarkStore
	handler := state.failedWriteHandler
	pendingCommand := state.pendingPublishByCmdID[commandID]
	// Failures and persisted successes are terminal. Earlier success stages may
	// be followed by another acknowledgement, so keep the command available for
	// later failure reporting.
	if commandID != "" && (status == "failure" || (status == "success" && (ackType&AckTypePersisted) != 0)) {
		releasePendingPublishLocked(state, commandID)
	}
	state.lock.Unlock()

	if status == "failure" && handler != nil {
		reason, _ := message.Reason()
		failedMessage := commandToMessage(pendingCommand)
		handler.FailedWrite(failedMessage, reason)
	}

	if store != nil && status == "success" && (ackType&AckTypePersisted) > 0 {
		if sequence, ok := message.SequenceID(); ok {
			if err := store.DiscardUpTo(sequence); err != nil {
				client.reportException(err)
			}
		}
	}

	if bookmarkStore != nil && status == "success" && (ackType&AckTypeCompleted) > 0 {
		if subID, hasSubID := message.SubID(); hasSubID {
			if bookmark, hasBookmark := message.Bookmark(); hasBookmark {
				if _, err := bookmarkStorePersisted(bookmarkStore, subID, bookmark); err != nil {
					client.reportException(err)
				}
			}
		}
	}
}

func (client *Client) callGlobalCommandTypeHandler(commandType int, message *Message) (error, bool) {
	state := ensureClientState(client)
	if state == nil || !state.hasGlobalHandlers.Load() {
		return nil, false
	}

	state.lock.Lock()
	handler := state.globalCommandHandlers[commandType]
	state.lock.Unlock()

	if handler == nil {
		return nil, false
	}
	return handler(message), true
}

func (client *Client) detectAndTrackDuplicate(message *Message) bool {
	if message == nil {
		return false
	}
	if _, hasBookmark := message.Bookmark(); !hasBookmark {
		return false
	}

	state := ensureClientState(client)
	if state == nil {
		return false
	}

	state.lock.Lock()
	bookmarkStore := state.bookmarkStore
	state.lock.Unlock()
	if bookmarkStore == nil {
		return false
	}
	if bookmarkStore.IsDiscarded(message) {
		return true
	}

	if _, err := bookmarkStoreLog(bookmarkStore, message); err != nil {
		client.reportException(err)
	}
	return false
}

func (client *Client) callDuplicateHandler(message *Message) error {
	state := ensureClientState(client)
	if state == nil {
		return nil
	}

	state.lock.Lock()
	handler := state.duplicateHandler
	state.lock.Unlock()
	if handler == nil {
		return nil
	}
	return handler(message)
}

func (client *Client) callUnhandledHandler(message *Message) error {
	state := ensureClientState(client)
	if state == nil {
		return nil
	}

	state.lock.Lock()
	handler := state.unhandledHandler
	state.lock.Unlock()
	if handler == nil {
		return nil
	}
	return handler(message)
}

func (client *Client) callLastChanceHandler(message *Message) error {
	state := ensureClientState(client)
	if state == nil {
		return nil
	}

	state.lock.Lock()
	handler := state.lastChanceHandler
	state.lock.Unlock()
	if handler == nil {
		return nil
	}
	return handler(message)
}

func makeAckBatchKey(topic string, subID string) string {
	return topic + "\x1f" + subID
}

func commandTextBytes(extras *_HeaderTextExtras, selector func(*_HeaderTextExtras) []byte) []byte {
	if extras == nil {
		return nil
	}
	return selector(extras)
}

func (client *Client) maybeAutoAck(message *Message) error {
	_, err := client.queueAutoAck(message, "")
	if err != nil {
		client.reportException(err)
	}
	return err
}

// queueAutoAck reports whether ownership of the ack moved to the batch queue.
func (client *Client) queueAutoAck(message *Message, options string) (bool, error) {
	if message == nil || message.GetIgnoreAutoAck() {
		return false, nil
	}
	var header = messageHeader(message)
	var extras = headerTextExtras(header)
	if header == nil || extras == nil || extras.leasePeriod == nil || len(header.topic) == 0 || len(header.bookmark) == 0 {
		return false, nil
	}
	var state = ensureClientState(client)
	if state == nil || !state.autoAckValue.Load() {
		return false, nil
	}
	if !client.connected.Load() {
		return false, NewError(DisconnectedError, "Client is not connected while trying to ack")
	}
	return true, client.queueAck(string(header.topic), string(header.bookmark), options)
}

func (client *Client) queueAck(topic, bookmark, options string) error {
	var state = ensureClientState(client)
	var key = makeAckBatchKey(topic, options)
	state.lock.Lock()
	var batch = state.pendingAcks[key]
	if batch == nil {
		batch = &pendingAckBatch{topic: topic, options: options}
		state.pendingAcks[key] = batch
	}
	batch.bookmarks = append(batch.bookmarks, bookmark)
	state.pendingAckCount++
	var flush = state.pendingAckCount >= state.ackBatchSize
	if state.ackTimeout > 0 && state.ackTimer == nil {
		state.ackTimer = time.AfterFunc(state.ackTimeout, func() {
			if err := client.FlushAcks(); err != nil {
				client.reportException(err)
			}
		})
	}
	state.lock.Unlock()
	if flush {
		return client.FlushAcks()
	}
	return nil
}

// FlushAcks sends all pending queue acknowledgement batches immediately.
func (client *Client) FlushAcks() error {
	var state = ensureClientState(client)
	if state == nil {
		return nil
	}
	state.ackFlushLock.Lock()
	defer state.ackFlushLock.Unlock()
	state.lock.Lock()
	if state.ackTimer != nil {
		state.ackTimer.Stop()
		state.ackTimer = nil
	}
	if len(state.pendingAcks) == 0 {
		state.lock.Unlock()
		return nil
	}
	var pending = state.pendingAcks
	state.pendingAcks = make(map[string]*pendingAckBatch)
	state.pendingAckCount = 0
	state.lock.Unlock()
	var keys = make([]string, 0, len(pending))
	for key := range pending {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for index, key := range keys {
		var batch = pending[key]
		if batch == nil || len(batch.bookmarks) == 0 {
			continue
		}
		var command = NewCommand("sow_delete").SetCommandID("AMPS-queue-ack").SetTopic(batch.topic).SetBookmark(strings.Join(batch.bookmarks, ","))
		if batch.options != "" {
			command.SetOptions(batch.options)
		}
		client.lock.Lock()
		var err = client.send(command)
		client.lock.Unlock()
		if err != nil {
			// A partial write may already have applied an ack. Repeating the
			// same bookmarks is safe; forgetting an unsent batch is not.
			state.lock.Lock()
			for _, unsentKey := range keys[index:] {
				var unsent = pending[unsentKey]
				if unsent == nil {
					continue
				}
				if newer := state.pendingAcks[unsentKey]; newer != nil {
					unsent.bookmarks = append(unsent.bookmarks, newer.bookmarks...)
				}
				state.pendingAcks[unsentKey] = unsent
			}
			// Recount after merging concurrently queued acknowledgements.
			state.pendingAckCount = 0
			for _, entry := range state.pendingAcks {
				if entry != nil {
					state.pendingAckCount += uint(len(entry.bookmarks))
				}
			}
			state.lock.Unlock()
			return NewError(ConnectionError, err)
		}
	}
	return nil
}

func (client *Client) replayPublishStore() (bool, error) {
	client.publishLock.Lock()
	defer client.publishLock.Unlock()
	var state = ensureClientState(client)
	state.lock.Lock()
	var store = state.publishStore
	state.lock.Unlock()
	client.lock.Lock()
	var generation = client.generation.Load()
	var wasConnected = client.connected.Load()
	client.awaitingReplay = true
	client.lock.Unlock()
	if store != nil {
		if err := store.Replay(func(command *Command) error {
			client.lock.Lock()
			defer client.lock.Unlock()
			if generation != client.generation.Load() || !client.connected.Load() {
				return NewError(DisconnectedError, "connection changed during publish replay")
			}
			if ack, _ := command.AckType(); ack&AckTypePersisted == 0 {
				var replay = *command
				var header = *command.header
				header.ackTypeValue = ack | AckTypePersisted
				header.ackType = &header.ackTypeValue
				replay.header = &header
				command = &replay
			}
			return client.send(command)
		}); err != nil {
			return true, err
		}
	}
	client.lock.Lock()
	if generation == client.generation.Load() && client.connected.Load() {
		client.awaitingReplay = false
	} else if wasConnected {
		client.lock.Unlock()
		return store != nil, NewError(DisconnectedError, "connection changed during publish replay")
	}
	client.lock.Unlock()
	return store != nil, nil
}

func (client *Client) postLogonRecovery() error {
	var state = ensureClientState(client)
	if state == nil {
		return nil
	}
	state.lock.Lock()
	if state.recoveryInProgress {
		state.recoveryRequested = true
		state.lock.Unlock()
		return nil
	}
	state.recoveryInProgress = true
	state.recoveryRequested = false
	state.lock.Unlock()
	var ownsRecovery = true
	defer func() {
		if ownsRecovery {
			state.lock.Lock()
			state.recoveryInProgress = false
			state.lock.Unlock()
		}
	}()
	for {
		state.lock.Lock()
		state.manualDisconnect = false
		var manager = state.subscriptionManager
		var retries = state.pendingRetry
		var deferred = state.deferredExecutions
		state.pendingRetry, state.deferredExecutions = nil, nil
		state.lock.Unlock()
		var failed = func(err error, pending []retryCommand) error {
			state.lock.Lock()
			state.pendingRetry = append(pending, state.pendingRetry...)
			state.deferredExecutions = append(deferred, state.deferredExecutions...)
			state.lock.Unlock()
			client.reportException(err)
			return err
		}
		var replayed, replayErr = client.replayPublishStore()
		if replayErr != nil {
			return failed(replayErr, retries)
		}
		if replayed {
			client.notifyConnectionState(ConnectionStatePublishReplayed)
		}
		if err := client.FlushAcks(); err != nil {
			return failed(err, retries)
		}
		if manager != nil {
			if err := manager.Resubscribe(client); err != nil {
				return failed(err, retries)
			}
			client.notifyConnectionState(ConnectionStateResubscribed)
		}
		for index, pending := range retries {
			if pending.command == nil {
				continue
			}
			if _, err := client.ExecuteAsync(cloneCommand(pending.command), pending.messageHandler); err != nil {
				if errors.Is(err, errReplaySuperseded) {
					continue
				}
				return failed(err, retries[index:])
			}
		}
		for _, entry := range deferred {
			if entry.callback == nil {
				continue
			}
			func(entry deferredExecutionCall) {
				defer func() {
					if recovered := recover(); recovered != nil {
						client.reportException(fmt.Errorf("deferred execution panic: %v", recovered))
					}
				}()
				entry.callback(client, entry.userData)
			}(entry)
		}
		state.lock.Lock()
		var rerun = state.recoveryRequested
		state.recoveryRequested = false
		if !rerun {
			state.recoveryInProgress = false
			ownsRecovery = false
		}
		state.lock.Unlock()
		if !rerun {
			return nil
		}
	}
}

// SetName sets name on the receiver.
func (client *Client) SetName(name string) *Client {
	return client.SetClientName(name)
}

// Name executes the exported name operation.
func (client *Client) Name() string {
	return client.ClientName()
}

// SetLogonCorrelationData sets logon correlation data on the receiver.
func (client *Client) SetLogonCorrelationData(correlationData string) *Client {
	return client.SetLogonCorrelationID(correlationData)
}

// LogonCorrelationData executes the exported logoncorrelationdata operation.
func (client *Client) LogonCorrelationData() string {
	return client.LogonCorrelationID()
}

// URI executes the exported uri operation.
func (client *Client) URI() string {
	state := ensureClientState(client)
	if state == nil {
		return ""
	}
	state.lock.Lock()
	var uri = state.uri
	state.lock.Unlock()
	if uri != "" {
		return uri
	}
	if client.url != nil {
		return client.url.String()
	}
	return ""
}

// GetConnectionInfo returns the current connection info value.
func (client *Client) GetConnectionInfo() ConnectionInfo {
	return client.buildConnectionInfo()
}

// GatherConnectionInfo executes the exported gatherconnectioninfo operation.
func (client *Client) GatherConnectionInfo() ConnectionInfo {
	return client.GetConnectionInfo()
}

// BookmarkSubscribe executes the exported bookmarksubscribe operation.
func (client *Client) BookmarkSubscribe(topic string, bookmark string, filter ...string) (*MessageStream, error) {
	command := NewCommand("subscribe").SetTopic(topic).AddAckType(AckTypeCompleted)
	command.SetBookmark(client.resolveBookmark(command, bookmark))
	if len(filter) > 0 {
		command.SetFilter(filter[0])
	}
	options, _ := command.Options()
	if !strings.Contains(options, "bookmark") {
		if options == "" {
			command.SetOptions("bookmark")
		} else {
			command.SetOptions(options + ",bookmark")
		}
	}
	return client.Execute(command)
}

// BookmarkSubscribeAsync performs the asynchronous bookmarksubscribeasync operation.
func (client *Client) BookmarkSubscribeAsync(messageHandler func(*Message) error, topic string, bookmark string, filter ...string) (string, error) {
	command := NewCommand("subscribe").SetTopic(topic).AddAckType(AckTypeCompleted)
	command.SetBookmark(client.resolveBookmark(command, bookmark))
	if len(filter) > 0 {
		command.SetFilter(filter[0])
	}
	options, _ := command.Options()
	if !strings.Contains(options, "bookmark") {
		if options == "" {
			command.SetOptions("bookmark")
		} else {
			command.SetOptions(options + ",bookmark")
		}
	}
	return client.ExecuteAsync(command, messageHandler)
}

func (client *Client) resolveBookmark(command *Command, bookmark string) string {
	if bookmark != BookmarksRECENT {
		return bookmark
	}
	if client == nil || command == nil {
		return bookmark
	}
	store := client.BookmarkStore()
	if store == nil {
		return bookmark
	}

	subID, _ := command.SubID()
	if subID == "" {
		subID, _ = command.QueryID()
	}
	if subID == "" {
		return bookmark
	}

	if stored := store.GetMostRecent(subID); stored != "" {
		return stored
	}
	return bookmark
}

// Ack acknowledges queue bookmarks, optionally with C++ queue options such as cancel.
func (client *Client) Ack(topic string, bookmark string, options ...string) error {
	if topic == "" {
		return NewError(CommandError, "topic is required for ack")
	}
	if bookmark == "" {
		return NewError(CommandError, "bookmark is required for ack")
	}
	if !client.connected.Load() {
		return NewError(DisconnectedError, "Client is not connected while trying to ack")
	}
	var option string
	if len(options) > 0 {
		option = options[0]
	}
	return client.queueAck(topic, bookmark, option)
}

// AckMessage extracts topic and bookmark fields from a message and acknowledges it.
func (client *Client) AckMessage(message *Message) error {
	if message == nil {
		return NewError(CommandError, "nil message")
	}
	topic, hasTopic := message.Topic()
	bookmark, hasBookmark := message.Bookmark()
	if !hasTopic || !hasBookmark {
		return NewError(CommandError, "message does not contain topic/bookmark for ack")
	}
	return client.Ack(topic, bookmark)
}

// SetAutoAck sets auto ack on the receiver.
func (client *Client) SetAutoAck(enabled bool) *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	state.autoAck = enabled
	state.lock.Unlock()
	state.autoAckValue.Store(enabled)
	return client
}

// AutoAck executes the exported autoack operation.
func (client *Client) AutoAck() bool {
	state := ensureClientState(client)
	if state == nil {
		return false
	}
	return state.autoAckValue.Load()
}

// SetAckBatchSize sets ack batch size on the receiver.
func (client *Client) SetAckBatchSize(batchSize uint) *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	if batchSize == 0 {
		batchSize = 1
	}
	state.lock.Lock()
	state.ackBatchSize = batchSize
	state.lock.Unlock()
	return client
}

// AckBatchSize executes the exported ackbatchsize operation.
func (client *Client) AckBatchSize() uint {
	state := ensureClientState(client)
	if state == nil {
		return 0
	}
	state.lock.Lock()
	defer state.lock.Unlock()
	return state.ackBatchSize
}

// SetAckTimeout sets ack timeout on the receiver.
func (client *Client) SetAckTimeout(timeout time.Duration) *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	state.ackTimeout = timeout
	state.lock.Unlock()
	return client
}

// AckTimeout executes the exported acktimeout operation.
func (client *Client) AckTimeout() time.Duration {
	state := ensureClientState(client)
	if state == nil {
		return 0
	}
	state.lock.Lock()
	defer state.lock.Unlock()
	return state.ackTimeout
}

// SetRetryOnDisconnect sets retry on disconnect on the receiver.
func (client *Client) SetRetryOnDisconnect(enabled bool) *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	state.retryOnDisconnect = enabled
	state.lock.Unlock()
	return client
}

// RetryOnDisconnect executes the exported retryondisconnect operation.
func (client *Client) RetryOnDisconnect() bool {
	state := ensureClientState(client)
	if state == nil {
		return false
	}
	state.lock.Lock()
	defer state.lock.Unlock()
	return state.retryOnDisconnect
}

// SetDefaultMaxDepth sets default max depth on the receiver.
func (client *Client) SetDefaultMaxDepth(depth uint) *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	state.defaultMaxDepth = depth
	state.lock.Unlock()
	return client
}

// DefaultMaxDepth executes the exported defaultmaxdepth operation.
func (client *Client) DefaultMaxDepth() uint {
	state := ensureClientState(client)
	if state == nil {
		return 0
	}
	state.lock.Lock()
	defer state.lock.Unlock()
	return state.defaultMaxDepth
}

// PublishFlush executes the exported publishflush operation.
func (client *Client) PublishFlush(timeout ...time.Duration) error {
	state := ensureClientState(client)
	if state == nil {
		return nil
	}

	state.lock.Lock()
	store := state.publishStore
	state.lock.Unlock()

	if store != nil {
		wait := time.Duration(0)
		if len(timeout) > 0 {
			wait = timeout[0]
		}
		return store.Flush(wait)
	}

	command := NewCommand("flush").AddAckType(AckTypeProcessed)
	_, err := client.ExecuteAsync(command, nil)
	return err
}

// StartTimer executes the exported starttimer operation.
func (client *Client) StartTimer(timerID string, options ...string) (string, error) {
	command := NewCommand("start_timer")
	if timerID != "" {
		command.SetTopic(timerID)
	}
	if len(options) > 0 {
		command.SetOptions(options[0])
	}
	return client.ExecuteAsync(command, nil)
}

// StopTimer executes the exported stoptimer operation.
func (client *Client) StopTimer(timerID string, options ...string) (string, error) {
	command := NewCommand("stop_timer")
	if timerID != "" {
		command.SetTopic(timerID)
	}
	if len(options) > 0 {
		command.SetOptions(options[0])
	}
	return client.ExecuteAsync(command, nil)
}

// ExecuteAsyncNoResubscribe executes the exported executeasyncnoresubscribe operation.
func (client *Client) ExecuteAsyncNoResubscribe(command *Command, messageHandler func(*Message) error) (string, error) {
	routeID, err := client.ExecuteAsync(command, messageHandler)
	if err != nil || routeID == "" {
		return routeID, err
	}

	state := ensureClientState(client)
	if state != nil {
		state.lock.Lock()
		state.noResubscribeRoutes[routeID] = struct{}{}
		subscriptionManager := state.subscriptionManager
		state.lock.Unlock()
		if subscriptionManager != nil {
			subscriptionManager.Unsubscribe(routeID)
		}
	}

	return routeID, nil
}

// SetBookmarkStore sets bookmark store on the receiver.
func (client *Client) SetBookmarkStore(store BookmarkStore) *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	state.bookmarkStore = store
	state.lock.Unlock()
	return client
}

// BookmarkStore returns the configured store instance used by the receiver.
func (client *Client) BookmarkStore() BookmarkStore {
	state := ensureClientState(client)
	if state == nil {
		return nil
	}
	state.lock.Lock()
	defer state.lock.Unlock()
	return state.bookmarkStore
}

// SetPublishStore sets the publish store after in-flight publications and replay.
// It must not be called reentrantly from this client's store Store or Replay methods.
func (client *Client) SetPublishStore(store PublishStore) *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	client.publishLock.Lock()
	client.lock.Lock()
	state.lock.Lock()
	state.publishStore = store
	state.hasPublishStore.Store(store != nil)
	state.lock.Unlock()
	client.lock.Unlock()
	client.publishLock.Unlock()
	return client
}

// PublishStore returns the configured store. Replacement waits for in-flight
// stored publishes and replay; SetPublishStore must not be called from those
// store callbacks on the same client.
func (client *Client) PublishStore() PublishStore {
	state := ensureClientState(client)
	if state == nil {
		return nil
	}
	state.lock.Lock()
	defer state.lock.Unlock()
	return state.publishStore
}

// SetSubscriptionManager sets subscription manager on the receiver.
func (client *Client) SetSubscriptionManager(manager SubscriptionManager) *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	if manager == nil {
		manager = NewDefaultSubscriptionManager()
	}
	state.lock.Lock()
	state.subscriptionManager = manager
	state.lock.Unlock()
	return client
}

// SubscriptionManager executes the exported subscriptionmanager operation.
func (client *Client) SubscriptionManager() SubscriptionManager {
	state := ensureClientState(client)
	if state == nil {
		return nil
	}
	state.lock.Lock()
	defer state.lock.Unlock()
	return state.subscriptionManager
}

// SetDuplicateMessageHandler sets duplicate message handler on the receiver.
func (client *Client) SetDuplicateMessageHandler(handler func(*Message) error) *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	state.duplicateHandler = handler
	state.lock.Unlock()
	return client
}

// DuplicateMessageHandler executes the exported duplicatemessagehandler operation.
func (client *Client) DuplicateMessageHandler() func(*Message) error {
	state := ensureClientState(client)
	if state == nil {
		return nil
	}
	state.lock.Lock()
	defer state.lock.Unlock()
	return state.duplicateHandler
}

// SetFailedWriteHandler sets failed write handler on the receiver.
func (client *Client) SetFailedWriteHandler(handler FailedWriteHandler) *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	state.failedWriteHandler = handler
	state.lock.Unlock()
	return client
}

// FailedWriteHandler executes the exported failedwritehandler operation.
func (client *Client) FailedWriteHandler() FailedWriteHandler {
	state := ensureClientState(client)
	if state == nil {
		return nil
	}
	state.lock.Lock()
	defer state.lock.Unlock()
	return state.failedWriteHandler
}

// SetExceptionListener sets exception listener on the receiver.
func (client *Client) SetExceptionListener(listener ExceptionListener) *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	state.exceptionListener = listener
	state.refreshTransportSnapshotLocked()
	state.lock.Unlock()
	return client
}

// ExceptionListener executes the exported exceptionlistener operation.
func (client *Client) ExceptionListener() ExceptionListener {
	state := ensureClientState(client)
	if state == nil {
		return nil
	}
	state.lock.Lock()
	defer state.lock.Unlock()
	return state.exceptionListener
}

// SetUnhandledMessageHandler sets unhandled message handler on the receiver.
func (client *Client) SetUnhandledMessageHandler(handler func(*Message) error) *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	state.unhandledHandler = handler
	state.lock.Unlock()
	return client
}

// SetLastChanceMessageHandler sets last chance message handler on the receiver.
func (client *Client) SetLastChanceMessageHandler(handler func(*Message) error) *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	state.lastChanceHandler = handler
	state.lock.Unlock()
	return client
}

// SetGlobalCommandTypeMessageHandler sets global command type message handler on the receiver.
func (client *Client) SetGlobalCommandTypeMessageHandler(commandType int, handler func(*Message) error) *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	if handler == nil {
		delete(state.globalCommandHandlers, commandType)
	} else {
		state.globalCommandHandlers[commandType] = handler
	}
	state.hasGlobalHandlers.Store(len(state.globalCommandHandlers) != 0)
	state.lock.Unlock()
	return client
}

// AddConnectionStateListener adds connection state listener behavior on the receiver.
func (client *Client) AddConnectionStateListener(listener ConnectionStateListener) *Client {
	if listener == nil {
		return client
	}
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	state.connectionListeners[listenerKey(listener)] = listener
	state.lock.Unlock()
	return client
}

// RemoveConnectionStateListener removes previously registered connection state listener behavior.
func (client *Client) RemoveConnectionStateListener(listener ConnectionStateListener) *Client {
	if listener == nil {
		return client
	}
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	delete(state.connectionListeners, listenerKey(listener))
	state.lock.Unlock()
	return client
}

// ClearConnectionStateListeners clears configured connection state listeners state on the receiver.
func (client *Client) ClearConnectionStateListeners() *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	state.connectionListeners = make(map[uintptr]ConnectionStateListener)
	state.lock.Unlock()
	return client
}

// AddHTTPPreflightHeader adds httppreflight header behavior on the receiver.
func (client *Client) AddHTTPPreflightHeader(header string) *Client {
	if strings.TrimSpace(header) == "" {
		return client
	}
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	state.httpPreflightHeaders = append(state.httpPreflightHeaders, header)
	state.lock.Unlock()
	return client
}

// ClearHTTPPreflightHeaders clears configured httppreflight headers state on the receiver.
func (client *Client) ClearHTTPPreflightHeaders() *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	state.httpPreflightHeaders = nil
	state.lock.Unlock()
	return client
}

// SetHTTPPreflightHeaders sets httppreflight headers on the receiver.
func (client *Client) SetHTTPPreflightHeaders(headers []string) *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	copied := make([]string, 0, len(headers))
	for _, value := range headers {
		if strings.TrimSpace(value) == "" {
			continue
		}
		copied = append(copied, value)
	}
	state.lock.Lock()
	state.httpPreflightHeaders = copied
	state.lock.Unlock()
	return client
}

// RawConnection executes the exported rawconnection operation.
func (client *Client) RawConnection() net.Conn {
	if client == nil {
		return nil
	}
	client.connectionStateLock.Lock()
	defer client.connectionStateLock.Unlock()
	if wrapped, ok := client.connection.(interface{ NetConn() net.Conn }); ok {
		return wrapped.NetConn()
	}
	return client.connection
}

// SetTransportFilter sets transport filter on the receiver.
func (client *Client) SetTransportFilter(filter TransportFilter) *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	state.transportFilter = filter
	state.refreshTransportSnapshotLocked()
	state.lock.Unlock()
	return client
}

// SetReceiveRoutineStartedCallback sets receive routine started callback on the receiver.
func (client *Client) SetReceiveRoutineStartedCallback(callback func()) *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	state.receiveRoutineCallback = callback
	state.lock.Unlock()
	return client
}

// SetReceiveRoutineStoppedCallback sets receive routine stop callback on the receiver.
func (client *Client) SetReceiveRoutineStoppedCallback(callback func()) *Client {
	state := ensureClientState(client)
	if state == nil {
		return client
	}
	state.lock.Lock()
	state.receiveRoutineStop = callback
	state.lock.Unlock()
	return client
}

func (client *Client) setInternalDisconnectHandler(handler func(error)) {
	state := ensureClientState(client)
	if state == nil {
		return
	}
	state.lock.Lock()
	state.internalDisconnect = handler
	state.lock.Unlock()
}

// String returns a diagnostic summary of current connection state.
func (client *Client) String() string {
	connectionInfo := client.GetConnectionInfo()
	if len(connectionInfo) == 0 {
		return "Client{}"
	}
	keys := make([]string, 0, len(connectionInfo))
	for key := range connectionInfo {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", key, connectionInfo[key]))
	}
	return "Client{" + strings.Join(parts, ",") + "}"
}
