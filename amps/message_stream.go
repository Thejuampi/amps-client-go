package amps

import (
	"sync"

	"errors"
	"time"
)

const (
	messageStreamStateUnset        = 0x0
	messageStreamStateReading      = 0x10
	messageStreamStateSubscribed   = 0x11
	messageStreamStateSOWOnly      = 0x12
	messageStreamStateStatsOnly    = 0x13
	messageStreamStateDisconnected = 0x01
	messageStreamStateComplete     = 0x02
)

type MessageStream struct {
	client    *Client
	commandID string
	queryID   string
	current   *Message
	sowKeyMap map[string]*Message

	state    int
	depth    uint64
	timeout  uint64
	timedOut bool

	queue *_MessageQueue

	lock sync.Mutex
}

func (ms *MessageStream) Timeout() uint64 {
	return ms.timeout
}

func (ms *MessageStream) SetTimeout(timeout uint64) *MessageStream {
	ms.timeout = timeout
	return ms
}

func (ms *MessageStream) Depth() uint64 {
	return ms.queue.length()
}

func (ms *MessageStream) MaxDepth() uint64 {
	return ms.depth
}

func (ms *MessageStream) SetMaxDepth(depth uint64) *MessageStream {
	ms.depth = depth
	return ms
}

func (ms *MessageStream) HasNext() bool {
	if ms.state == messageStreamStateComplete {
		return false
	}
	timer := make(chan bool, 1)

	go func() {
		time.Sleep(1 * time.Millisecond)
		timer <- true
	}()

	ms.timedOut = false
	if ms.current != nil {
		return true
	}

	message, err := ms.queue.pollDequeue()
	ms.current = message

	if ms.current != nil && err == nil {
		return true
	}

	if ms.timeout == 0 {
		if ((ms.state & messageStreamStateReading) > 0) && ms.current == nil {
			ms.current, err = ms.queue.pollDequeue()

		}

		return ms.current != nil
	}

	currenttime := time.Now().UnixNano() / 1e6

	if ((ms.state & messageStreamStateReading) > 0) && ms.current == nil && !ms.timedOut {
		select {
		case <-timer:
			ms.timedOut = true
			return true
		default:
			message, err = ms.queue.pollDequeue()
			ms.current = message
		}

		if ms.sowKeyMap != nil {
			ms.lock.Lock()
			defer ms.lock.Unlock()

			ms.current, err = ms.queue.dequeue()

			if ms.current != nil && err == nil {
				sowKey, isPresent := ms.current.SowKey()

				if isPresent {
					delete(ms.sowKeyMap, sowKey)
				}
			}
		} else {
			ms.current, err = ms.queue.pollDequeue()
		}

		if ms.current == nil {
			ms.timedOut = uint64((time.Now().UnixNano()/1e6)-currenttime) > ms.timeout
		}
		return (((ms.state & messageStreamStateReading) != 0) && ms.timedOut) || ms.current != nil
	}

	return false
}

func (ms *MessageStream) Next() (message *Message) {
	if ms.timedOut {
		ms.timedOut = false
		return nil
	}

	if ms.current == nil {
		if !ms.HasNext() {
			return
		}
	}

	returnVal := ms.current
	ms.current = nil

	if ms.state == messageStreamStateSOWOnly && returnVal != nil && returnVal.header.command == CommandGroupEnd {
		ms.setState(messageStreamStateComplete)

		if ms.client == nil {
			ms.commandID = ""
			ms.queryID = ""
		} else {
			if len(ms.queryID) > 0 {
				err := ms.client.deleteRoute(ms.queryID)
				if err != nil {
					return
				}
				ms.queryID = ""
			}
		}
	} else if ackType, hasAckType := returnVal.AckType(); ms.state == messageStreamStateStatsOnly && returnVal == nil && (hasAckType && ackType == AckTypeStats) {
		ms.setState(messageStreamStateComplete)

		if ms.client == nil {
			ms.commandID = ""
			ms.queryID = ""
		} else {
			if len(ms.commandID) > 0 {
				err := ms.client.deleteRoute(ms.commandID)
				if err != nil {
					return
				}
				ms.commandID = ""
			} else if len(ms.queryID) > 0 {
				err := ms.client.deleteRoute(ms.queryID)
				if err != nil {
					return
				}
				ms.queryID = ""
			}
		}
	}

	return returnVal
}

func (ms *MessageStream) Conflate() {
	if ms.sowKeyMap == nil {
		ms.sowKeyMap = make(map[string]*Message)
	}
	return
}

func (ms *MessageStream) isConflating() bool {
	return ms.sowKeyMap != nil
}

func (ms *MessageStream) Close() (err error) {
	if ms.client == nil {
		ms.commandID = ""
		ms.queryID = ""
	} else {
		if len(ms.commandID) > 0 {

			if ms.state == messageStreamStateSubscribed {
				ms.setState(messageStreamStateComplete)
				err = ms.client.Unsubscribe(ms.commandID)
			} else {
				ms.client.routes.Delete(ms.commandID)
			}

			ms.commandID = ""
		} else if len(ms.queryID) > 0 {

			if ms.state >= messageStreamStateComplete {
				ms.setState(messageStreamStateComplete)
				err = ms.client.Unsubscribe(ms.queryID)
			} else {
				ms.client.routes.Delete(ms.queryID)
			}

			ms.queryID = ""
		}
	}

	if ms.state != messageStreamStateComplete {
		ms.setState(messageStreamStateComplete)
	}

	return
}

var messageStream = &MessageStream{state: messageStreamStateComplete, queue: newQueue(256)}

func emptyMessageStream() *MessageStream { return messageStream }

func (ms *MessageStream) setSubID(subID string) {
	ms.commandID = subID
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

func (ms *MessageStream) messageHandler(message *Message) (err error) {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	if ms.depth != 0 {
		for ms.queue.length() > ms.depth {
		}
	}

	ms.queue.enqueue(message.Copy())
	return
}

func (ms *MessageStream) setState(state int) {
	if state != messageStreamStateDisconnected {
		ms.state = state
	}
}

func newMessageStream(client *Client) *MessageStream {
	return &MessageStream{state: messageStreamStateUnset, client: client, queue: newQueue(256)}
}

type _MessageQueue struct {
	capacity uint64
	_length  uint64
	first    uint64
	last     uint64
	ring     []*Message

	lock sync.Mutex
}

func newQueue(initialSize uint64) *_MessageQueue {
	return &_MessageQueue{capacity: initialSize, ring: make([]*Message, initialSize)}
}

func (queue *_MessageQueue) length() uint64 {
	queue.lock.Lock()
	defer queue.lock.Unlock()

	return queue._length
}

func (queue *_MessageQueue) enqueue(message *Message) {
	queue.lock.Lock()
	defer queue.lock.Unlock()

	if queue._length == 0 {
		queue.ring[queue.last] = message
		queue._length++
		return
	}

	if queue.capacity == queue._length {
		queue.resize()
	}

	queue.last = (queue.last + 1) % queue.capacity
	queue.ring[queue.last] = message
	queue._length++
}

func (queue *_MessageQueue) dequeue() (message *Message, err error) {
	queue.lock.Lock()
	defer queue.lock.Unlock()

	if queue._length == 0 {
		err = errors.New("Queue is empty")
		return
	}

	message = queue.ring[queue.first]

	queue.ring[queue.first] = nil
	queue._length--

	if queue._length > 0 {
		queue.first = (queue.first + 1) % queue.capacity
	} else {
		queue.first = 0
		queue.last = 0
	}

	return
}

func (queue *_MessageQueue) pollDequeue() (*Message, error) {
	for {
		if queue._length > 0 {
			message, err := queue.dequeue()
			if err != nil {
				return nil, err
			}
			return message, nil
		}
	}
}

func (queue *_MessageQueue) clear() {
	queue.lock.Lock()
	defer queue.lock.Unlock()

	queue.first = uint64(0)
	queue.last = uint64(0)
	queue._length = uint64(0)

	queue.ring = make([]*Message, queue.capacity)
}

func (queue *_MessageQueue) resize() {
	newRing := make([]*Message, queue.capacity*2)

	i, j := queue.first, uint64(0)
	for ; j < queue._length; j++ {
		newRing[j] = queue.ring[i]
		i = (i + 1) % queue.capacity
	}

	queue.ring = newRing
	queue.first = 0
	queue.last = j - 1

	queue.capacity *= 2
}
