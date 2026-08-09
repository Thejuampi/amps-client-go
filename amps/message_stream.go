package amps

import (
	"math"
	"sync"
	"sync/atomic"

	"time"

	"github.com/Thejuampi/amps-client-go/internal/safecast"
)

// Constants in this block define protocol and client behavior values.
const (
	messageStreamStateUnset        = 0x0
	messageStreamStateReading      = 0x10
	messageStreamStateSubscribed   = 0x11
	messageStreamStateSOWOnly      = 0x12
	messageStreamStateStatsOnly    = 0x13
	messageStreamStateDisconnected = 0x01
	messageStreamStateComplete     = 0x02
)

const maxMessageStreamTimeoutMillis = uint64(math.MaxInt64 / int64(time.Millisecond))

// MessageStream stores exported state used by AMPS client APIs.
type MessageStream struct {
	client        *Client
	clientLock    sync.RWMutex
	lifecycleLock sync.Mutex
	commandID     string
	queryID       string
	unsubscribeID string
	current       *Message
	sowKeyMap     map[string]*Message

	state    int32
	depth    atomic.Uint64
	timeout  atomic.Uint64
	timedOut atomic.Bool

	queue *_MessageQueue

	lock sync.Mutex
}

// MessageStreamIterator iterates over messages in a stream.
type MessageStreamIterator struct {
	stream *MessageStream
}

// Next returns the next message and whether iteration should continue.
func (iterator *MessageStreamIterator) Next() (*Message, bool) {
	if iterator == nil || iterator.stream == nil {
		return nil, false
	}
	var message = iterator.stream.Next()
	return message, message != nil
}

// IsValid reports whether the stream handle is usable.
func (ms *MessageStream) IsValid() bool {
	return ms != nil && atomic.LoadInt32(&ms.state) != messageStreamStateUnset
}

// Begin returns an iterator for this stream.
func (ms *MessageStream) Begin() *MessageStreamIterator {
	return &MessageStreamIterator{stream: ms}
}

// End returns a terminal iterator marker.
func (ms *MessageStream) End() *MessageStreamIterator {
	return &MessageStreamIterator{stream: nil}
}

// FromExistingHandler binds an existing handler to this stream.
func (ms *MessageStream) FromExistingHandler(handler func(*Message) error) *MessageStream {
	if ms == nil || handler == nil {
		return ms
	}
	ms.lifecycleLock.Lock()
	defer ms.lifecycleLock.Unlock()
	if ms.commandID == "" {
		return ms
	}
	var client = ms.getClient()
	if client == nil {
		return ms
	}
	client.routes.Store(ms.commandID, handler)
	return ms
}

func (ms *MessageStream) resetForConfiguration() {
	if ms == nil {
		return
	}
	ms.current = nil
	ms.timedOut.Store(false)
	if ms.queue != nil {
		ms.queue.clear()
	}
	ms.lock.Lock()
	ms.sowKeyMap = nil
	ms.lock.Unlock()
}

// SetAcksOnly configures the stream to consume ack-only responses.
func (ms *MessageStream) SetAcksOnly(commandID string) *MessageStream {
	if ms == nil {
		return nil
	}
	ms.resetForConfiguration()
	ms.commandID = commandID
	ms.queryID = ""
	ms.unsubscribeID = ""
	ms.setState(messageStreamStateReading)
	return ms
}

// SetSOWOnly configures the stream for SOW-only responses.
func (ms *MessageStream) SetSOWOnly(commandID string, queryID string) *MessageStream {
	if ms == nil {
		return nil
	}
	ms.resetForConfiguration()
	ms.commandID = commandID
	ms.queryID = queryID
	ms.unsubscribeID = ""
	ms.setSowOnly()
	return ms
}

// SetStatsOnly configures the stream for stats-only responses.
func (ms *MessageStream) SetStatsOnly(commandID string, queryID ...string) *MessageStream {
	if ms == nil {
		return nil
	}
	ms.resetForConfiguration()
	ms.commandID = commandID
	if len(queryID) > 0 {
		ms.queryID = queryID[0]
	} else {
		ms.queryID = ""
	}
	ms.unsubscribeID = ""
	ms.setStatsOnly()
	return ms
}

// SetSubscription configures the stream for subscription responses.
func (ms *MessageStream) SetSubscription(routeID string, unsubscribeID string, queryID ...string) *MessageStream {
	if ms == nil {
		return nil
	}
	ms.resetForConfiguration()
	ms.commandID = routeID
	ms.unsubscribeID = routeID
	if unsubscribeID != "" {
		ms.unsubscribeID = unsubscribeID
	}
	if len(queryID) > 0 {
		ms.queryID = queryID[0]
	} else {
		ms.queryID = ""
	}
	ms.setState(messageStreamStateSubscribed)
	return ms
}

// Timeout executes the exported timeout operation.
func (ms *MessageStream) Timeout() uint64 {
	return ms.timeout.Load()
}

// SetTimeout sets timeout on the receiver.
func (ms *MessageStream) SetTimeout(timeout uint64) *MessageStream {
	ms.timeout.Store(timeout)
	return ms
}

// Depth executes the exported depth operation.
func (ms *MessageStream) Depth() uint64 {
	return ms.queue.length()
}

// MaxDepth executes the exported maxdepth operation.
func (ms *MessageStream) MaxDepth() uint64 {
	return ms.depth.Load()
}

// SetMaxDepth sets max depth on the receiver.
func (ms *MessageStream) SetMaxDepth(depth uint64) *MessageStream {
	ms.depth.Store(depth)
	return ms
}

// HasNext reports whether the receiver has next configured.
func (ms *MessageStream) HasNext() bool {
	if ms.current != nil {
		return true
	}

	state := atomic.LoadInt32(&ms.state)
	if ms.timedOut.Load() && state != messageStreamStateComplete {
		return true
	}

	if message, ok := ms.queue.tryDequeue(); ok {
		ms.setCurrentFromQueue(message)
		return true
	}

	if state == messageStreamStateComplete {
		ms.timedOut.Store(false)
		return false
	}

	reading := (state & messageStreamStateReading) != 0
	if !reading {
		return ms.current != nil
	}

	if ms.timeout.Load() != 0 {
		return ms.waitForNextWithTimeout()
	}
	message, ok := ms.queue.waitDequeue()
	if !ok {
		return false
	}
	ms.setCurrentFromQueue(message)
	return true
}

func (ms *MessageStream) waitForNextWithTimeout() bool {
	var timeoutMillis = min(ms.timeout.Load(), maxMessageStreamTimeoutMillis)
	var timeoutDuration = time.Millisecond * time.Duration(safecast.Int64FromUint64Saturating(timeoutMillis))
	return ms.handleWaitDequeueTimeoutResult(
		ms.queue.waitDequeueTimeout(
			timeoutDuration,
		),
	)
}

func (ms *MessageStream) handleWaitDequeueTimeoutResult(message *Message, ok bool) bool {
	if ok {
		ms.setCurrentFromQueue(message)
		return true
	}
	if atomic.LoadInt32(&ms.state) == messageStreamStateComplete {
		return false
	}
	ms.timedOut.Store(true)
	return true
}

func (ms *MessageStream) consumeConflateState() {
	if ms == nil || ms.current == nil {
		return
	}

	ms.lock.Lock()
	defer ms.lock.Unlock()
	if ms.sowKeyMap == nil {
		return
	}
	sowKey, isPresent := ms.current.SowKey()
	if !isPresent || sowKey == "" {
		return
	}

	if ms.sowKeyMap[sowKey] == ms.current {
		delete(ms.sowKeyMap, sowKey)
	}
}

func (ms *MessageStream) setCurrentFromQueue(message *Message) {
	ms.current = message
	ms.consumeConflateState()
}

// Next executes the exported next operation.
func (ms *MessageStream) Next() (message *Message) {
	if ms.timedOut.Swap(false) {
		return nil
	}

	if ms.current == nil {
		if !ms.HasNext() {
			return
		}
	}

	returnVal := ms.current
	ms.current = nil
	ms.lifecycleLock.Lock()
	defer ms.lifecycleLock.Unlock()

	cleanupRoutes := func() {
		var client = ms.getClient()
		if client == nil {
			ms.commandID = ""
			ms.queryID = ""
			return
		}

		if len(ms.commandID) > 0 {
			_ = client.deleteRoute(ms.commandID)
			ms.commandID = ""
		}
		if len(ms.queryID) > 0 {
			_ = client.deleteRoute(ms.queryID)
			ms.queryID = ""
		}
	}

	var state = atomic.LoadInt32(&ms.state)
	if state == messageStreamStateSOWOnly && returnVal != nil && returnVal.header.command == CommandGroupEnd {
		ms.setState(messageStreamStateComplete)
		cleanupRoutes()
	} else if state == messageStreamStateStatsOnly && returnVal != nil {
		ackType, hasAckType := returnVal.AckType()
		if hasAckType && ackType == AckTypeStats {
			ms.setState(messageStreamStateComplete)
			cleanupRoutes()
		}
	}

	return returnVal
}

// Conflate executes the exported conflate operation.
func (ms *MessageStream) Conflate() {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	if atomic.LoadInt32(&ms.state) != messageStreamStateSubscribed {
		return
	}

	if ms.sowKeyMap == nil {
		ms.sowKeyMap = make(map[string]*Message)
	}
}

func (ms *MessageStream) isConflating() bool {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	return ms.sowKeyMap != nil && atomic.LoadInt32(&ms.state) == messageStreamStateSubscribed
}

// Close is an alias for Disconnect.
func (ms *MessageStream) Close() (err error) {
	ms.lifecycleLock.Lock()
	defer ms.lifecycleLock.Unlock()
	var client = ms.getClient()
	removeMessageStream := func(routeID string) {
		client.messageStreams.Delete(routeID)
	}
	removeRoute := func(routeID string) {
		client.routes.Delete(routeID)
	}

	if client == nil {
		ms.commandID = ""
		ms.queryID = ""
		ms.unsubscribeID = ""
	} else {
		if len(ms.commandID) > 0 {
			removeMessageStream(ms.commandID)
		}
		if len(ms.queryID) > 0 {
			removeMessageStream(ms.queryID)
		}
		if len(ms.commandID) > 0 {

			if atomic.LoadInt32(&ms.state) == messageStreamStateSubscribed {
				if ms.queryID != "" && ms.queryID != ms.unsubscribeID {
					removeRoute(ms.queryID)
				}
				ms.setState(messageStreamStateComplete)
				if ms.unsubscribeID != "" {
					err = client.Unsubscribe(ms.unsubscribeID)
				} else {
					err = client.Unsubscribe(ms.commandID)
				}
			} else {
				removeRoute(ms.commandID)
				if ms.queryID != "" && ms.queryID != ms.commandID {
					removeRoute(ms.queryID)
				}
			}

			ms.commandID = ""
			ms.queryID = ""
			ms.unsubscribeID = ""
		} else if len(ms.queryID) > 0 {

			if atomic.LoadInt32(&ms.state) >= messageStreamStateComplete {
				ms.setState(messageStreamStateComplete)
				err = client.Unsubscribe(ms.queryID)
			} else {
				removeRoute(ms.queryID)
			}

			ms.queryID = ""
		}
	}

	if atomic.LoadInt32(&ms.state) != messageStreamStateComplete {
		ms.setState(messageStreamStateComplete)
	}

	return
}

func emptyMessageStream() *MessageStream {
	return &MessageStream{state: messageStreamStateComplete, queue: newQueue(256)}
}

func (ms *MessageStream) setSubID(subID string) {
	ms.commandID = subID
	ms.unsubscribeID = subID
	ms.setState(messageStreamStateSubscribed)
}

func (ms *MessageStream) setQueryID(queryID string) {
	ms.queryID = queryID
}

func (ms *MessageStream) setSowOnly() {
	ms.setState(messageStreamStateSOWOnly)
}

func (ms *MessageStream) setStatsOnly() {
	ms.setState(messageStreamStateStatsOnly)
}

func (ms *MessageStream) setRunning() {
	ms.setState(messageStreamStateReading)
}

func (ms *MessageStream) enqueueMessage(message *Message) {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	ms.enqueueMessageLocked(message)
}

func (ms *MessageStream) enqueueMessageLocked(message *Message) {
	var droppedMessages = ms.queue.enqueueWithDepth(message, ms.depth.Load())
	ms.removeConflatedMessagesLocked(droppedMessages)
}

func (ms *MessageStream) removeConflatedMessagesLocked(messages []*Message) {
	if len(messages) == 0 {
		return
	}
	if ms.sowKeyMap == nil {
		return
	}

	for _, message := range messages {
		if message == nil {
			continue
		}
		sowKey, isPresent := message.SowKey()
		if !isPresent || sowKey == "" || ms.sowKeyMap[sowKey] != message {
			continue
		}
		delete(ms.sowKeyMap, sowKey)
	}
}

func (ms *MessageStream) messageHandler(message *Message) (err error) {
	if message == nil {
		return nil
	}

	if !ms.isConflating() {
		ms.queue.enqueueWithDepth(message.Copy(), ms.depth.Load())
		return nil
	}

	var copiedMessage = message.Copy()
	var sowKey, hasSowKey = copiedMessage.SowKey()
	if !hasSowKey || sowKey == "" {
		ms.enqueueMessage(copiedMessage)
		return nil
	}

	ms.lock.Lock()
	if existingMessage, exists := ms.sowKeyMap[sowKey]; exists {
		existingMessage.Replace(copiedMessage)
		ms.lock.Unlock()
		return nil
	}
	ms.sowKeyMap[sowKey] = copiedMessage
	ms.enqueueMessageLocked(copiedMessage)
	ms.lock.Unlock()

	return nil
}

func (ms *MessageStream) setState(state int32) {
	if state != messageStreamStateDisconnected {
		atomic.StoreInt32(&ms.state, state)
	}
	if state == messageStreamStateComplete && ms.queue != nil {
		ms.queue.close()
	}
}

func newMessageStream(client *Client) *MessageStream {
	stream := &MessageStream{}
	stream.state = messageStreamStateUnset
	stream.client = client
	stream.queue = newQueue(256)
	return stream
}

func (ms *MessageStream) getClient() *Client {
	if ms == nil {
		return nil
	}
	ms.clientLock.RLock()
	defer ms.clientLock.RUnlock()
	return ms.client
}

func (ms *MessageStream) detachClient() {
	if ms == nil {
		return
	}
	ms.clientLock.Lock()
	ms.client = nil
	ms.clientLock.Unlock()
}

type _MessageQueue struct {
	capacity uint64
	_length  uint64
	first    uint64
	last     uint64
	ring     []*Message
	closed   bool

	lock       sync.Mutex
	notEmptyCh chan struct{}
	notFull    *sync.Cond
	waiters    uint64
}

func newQueue(initialSize uint64) *_MessageQueue {
	queue := &_MessageQueue{
		capacity:   initialSize,
		ring:       make([]*Message, initialSize),
		notEmptyCh: make(chan struct{}),
	}
	queue.notFull = sync.NewCond(&queue.lock)
	return queue
}

func (queue *_MessageQueue) notifyNotEmptyLocked() {
	if queue.waiters == 0 {
		return
	}
	close(queue.notEmptyCh)
	queue.notEmptyCh = make(chan struct{})
}

func (queue *_MessageQueue) beginWaitLocked() chan struct{} {
	queue.waiters++
	return queue.notEmptyCh
}

func (queue *_MessageQueue) endWait() {
	queue.lock.Lock()
	if queue.waiters > 0 {
		queue.waiters--
	}
	queue.lock.Unlock()
}

func (queue *_MessageQueue) length() uint64 {
	queue.lock.Lock()
	defer queue.lock.Unlock()
	return queue._length
}

func (queue *_MessageQueue) enqueue(message *Message) {
	queue.enqueueWithDepth(message, 0)
}

func (queue *_MessageQueue) enqueueWithDepth(message *Message, depth uint64) []*Message {
	queue.lock.Lock()
	defer queue.lock.Unlock()
	if queue.closed {
		return nil
	}

	var droppedMessages []*Message
	for depth != 0 && queue._length >= depth {
		droppedMessages = append(droppedMessages, queue.dequeueLocked())
	}

	if queue.capacity == queue._length {
		queue.resize()
	}

	if queue._length != 0 {
		queue.last = (queue.last + 1) % queue.capacity
	}
	queue.ring[queue.last] = message
	queue._length++
	queue.notifyNotEmptyLocked()
	return droppedMessages
}

func (queue *_MessageQueue) dequeueLocked() *Message {
	message := queue.ring[queue.first]
	queue.ring[queue.first] = nil
	queue._length--

	if queue._length > 0 {
		queue.first = (queue.first + 1) % queue.capacity
	} else {
		queue.first = 0
		queue.last = 0
	}

	queue.notFull.Signal()
	return message
}

func (queue *_MessageQueue) dequeue() (message *Message, err error) {
	queue.lock.Lock()
	defer queue.lock.Unlock()

	if queue._length == 0 {
		err = NewError(UnknownError, "Queue is empty")
		return
	}

	return queue.dequeueLocked(), nil
}

func (queue *_MessageQueue) tryDequeue() (*Message, bool) {
	queue.lock.Lock()
	defer queue.lock.Unlock()

	if queue._length == 0 {
		return nil, false
	}

	return queue.dequeueLocked(), true
}

func (queue *_MessageQueue) waitDequeue() (*Message, bool) {
	for {
		queue.lock.Lock()
		if queue._length > 0 {
			message := queue.dequeueLocked()
			queue.lock.Unlock()
			return message, true
		}
		if queue.closed {
			queue.lock.Unlock()
			return nil, false
		}
		waitCh := queue.beginWaitLocked()
		queue.lock.Unlock()
		<-waitCh
		queue.endWait()
	}
}

func (queue *_MessageQueue) waitDequeueTimeout(timeout time.Duration) (*Message, bool) {
	if timeout <= 0 {
		return queue.waitDequeue()
	}

	timer := time.NewTimer(timeout)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	for {
		queue.lock.Lock()
		if queue._length > 0 {
			message := queue.dequeueLocked()
			queue.lock.Unlock()
			return message, true
		}
		if queue.closed {
			queue.lock.Unlock()
			return nil, false
		}
		waitCh := queue.beginWaitLocked()
		queue.lock.Unlock()

		select {
		case <-waitCh:
			queue.endWait()
		case <-timer.C:
			queue.endWait()
			return nil, false
		}
	}
}

func (queue *_MessageQueue) clear() {
	queue.lock.Lock()
	defer queue.lock.Unlock()

	queue.first = 0
	queue.last = 0
	queue._length = 0
	queue.closed = false

	queue.ring = make([]*Message, queue.capacity)
	queue.notFull.Broadcast()
}

func (queue *_MessageQueue) close() {
	queue.lock.Lock()
	queue.closed = true
	queue.notifyNotEmptyLocked()
	queue.notFull.Broadcast()
	queue.lock.Unlock()
}

func (queue *_MessageQueue) resize() {
	newRing := make([]*Message, queue.capacity*2)

	currentLength := queue._length
	i, j := queue.first, uint64(0)
	for ; j < currentLength; j++ {
		newRing[j] = queue.ring[i]
		i = (i + 1) % queue.capacity
	}

	queue.ring = newRing
	queue.first = 0
	if j > 0 {
		queue.last = j - 1
	} else {
		queue.last = 0
	}

	queue.capacity *= 2
}
