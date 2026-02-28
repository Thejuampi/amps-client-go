package amps

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const jsonBufferSize = 256

// Ring buffer mode - VERY FAST but only safe when caller processes synchronously
// (no concurrent access to same buffer slot). Use for benchmarks or single-worker loops.
const ringBufferCount = 256 // plenty of slots to minimize collision
var jsonBufferPool [ringBufferCount][jsonBufferSize]byte
var ringBufferCounter atomic.Uint64

// UseRingBuffer enables high-performance ring buffer mode.
// WARNING: Only safe when:
//   - Single-threaded (benchmarks)
//   - Each goroutine has dedicated slot (worker pools)
//   - Caller processes synchronously before next call
//
// For concurrent/production use with multiple goroutines sharing buffers,
// keep false (default) to use sync.Pool.
var UseRingBuffer = false

// sync.Pool for production - thread-safe, good performance
var jsonBufferSyncPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, jsonBufferSize)
		return buf
	},
}

func getJsonBuffer(size int) []byte {
	if size > jsonBufferSize {
		return make([]byte, size)
	}
	if UseRingBuffer {
		idx := int(ringBufferCounter.Add(1)) % ringBufferCount
		return jsonBufferPool[idx][:size]
	}
	// sync.Pool is thread-safe and uses per-P caching
	return jsonBufferSyncPool.Get().([]byte)[:size]
}

func putJsonBuffer(buf []byte) {
	if UseRingBuffer {
		return
	}
	if cap(buf) > jsonBufferSize {
		return
	}
	// Return to sync.Pool for reuse (thread-safe)
	jsonBufferSyncPool.Put(buf[:0]) // return empty slice with capacity
}

// Message stores exported state used by AMPS client APIs.
type Message struct {
	header              *_Header
	data                []byte
	client              *Client
	valid               bool
	ignoreAutoAck       bool
	bookmarkSeqNo       uint64
	subscriptionHandle  string
	rawTransmissionTime string
	disowned            bool
}

// Constants in this block define protocol and client behavior values.
const (
	inHeader = iota
	inKey
	afterKey
	inValue
	inValueString
)

var jsonWhitespaceLookup [256]bool

func init() {
	jsonWhitespaceLookup[' '] = true
	jsonWhitespaceLookup['\n'] = true
	jsonWhitespaceLookup['\r'] = true
	jsonWhitespaceLookup['\t'] = true
}

func (msg *Message) reset() {
	if msg.header != nil {
		msg.header.reset()
	}
	msg.data = nil
	msg.ignoreAutoAck = false
	msg.bookmarkSeqNo = 0
	msg.subscriptionHandle = ""
	msg.rawTransmissionTime = time.Now().UTC().Format(time.RFC3339Nano)
	msg.disowned = false
	msg.valid = true
}

func (msg *Message) resetForParse() {
	if msg.header != nil {
		msg.header.reset()
	}
	msg.data = nil
	msg.ignoreAutoAck = false
	msg.bookmarkSeqNo = 0
	msg.subscriptionHandle = ""
	msg.disowned = false
	msg.valid = true
}

func parseHeader(msg *Message, resetMessage bool, array []byte) ([]byte, error) {

	if msg == nil {
		return array, errors.New("message object error (null pointer)")
	}

	if resetMessage {
		msg.resetForParse()
	}

	header := msg.header
	state := inHeader
	var keyStart, keyEnd, valueStart, valueEnd int
	escaped := false
	for index := 0; index < len(array); index++ {
		character := array[index]

		switch state {
		case inHeader:
			switch character {
			case '"':
				state = inKey
				keyStart = index + 1
				escaped = false
			case '{':
			case '}':
				return array[index+1:], nil
			default:
				if !isJSONWhitespace(character) && character != ',' {
					return array, errors.New("malformed AMPS header")
				}
			}

		case inKey:
			if escaped {
				escaped = false
				continue
			}
			if character == '\\' {
				escaped = true
				continue
			}
			if character == '"' {
				state = afterKey
				keyEnd = index
			}

		case afterKey:
			if character == ':' {
				state = inValue
				valueStart = index + 1
			} else if !isJSONWhitespace(character) {
				return array, errors.New("malformed AMPS header")
			}

		case inValue:
			switch character {
			case '"':
				state = inValueString
				valueStart = index + 1
				escaped = false
			case ',':
				state = inHeader
				valueEnd = index
				parseFieldInline(header, array[keyStart:keyEnd], array[valueStart:valueEnd])
			case '}':
				parseFieldInline(header, array[keyStart:keyEnd], array[valueStart:index])
				return array[index+1:], nil
			}

		case inValueString:
			if escaped {
				escaped = false
				continue
			}
			if character == '\\' {
				escaped = true
				continue
			}
			if character == '"' {
				state = inHeader
				valueEnd = index
				parseFieldInline(header, array[keyStart:keyEnd], array[valueStart:valueEnd])
			}
		}
	}

	return array, errors.New("unexpected end of AMPS header")
}

func parseFieldInline(header *_Header, key []byte, value []byte) {
	l := len(key)
	if l == 0 {
		return
	}
	ck := key[0]
	if l == 1 {
		switch ck {
		case 'c':
			header.command = commandBytesToInt(value)
			return
		case 't':
			header.topic = value
			return
		case 's':
			if sequenceID, ok := parseUint64Value(value); ok {
				header.sequenceIDValue = sequenceID
				header.sequenceID = &header.sequenceIDValue
			}
			return
		case 'a':
			if ack := parseAckBytes(value); ack >= 0 {
				header.ackTypeValue = ack
				header.ackType = &header.ackTypeValue
			}
			return
		case 'o':
			header.options = value
			return
		case 'f':
			header.filter = value
			return
		case 'k':
			header.sowKey = value
			return
		case 'l':
			if messageLength, ok := parseUint32Value(value); ok {
				header.messageLengthValue = messageLength
				header.messageLength = &header.messageLengthValue
			}
			return
		case 'e':
			if expiration, ok := parseUint32Value(value); ok {
				header.expirationValue = expiration
				header.expiration = &header.expirationValue
			}
			return
		case 'x':
			header.correlationID = value
			return
		}
		return
	}
	if l == 2 {
		switch ck {
		case 'b':
			if key[1] == 'm' {
				header.bookmark = value
			} else if key[1] == 's' {
				if batchSize, ok := parseUint32Value(value); ok {
					header.batchSizeValue = batchSize
					header.batchSize = &header.batchSizeValue
				}
			}
		case 'l':
			header.leasePeriod = value
		case 't':
			header.timestamp = value
		}
		return
	}
	if l == 3 {
		if ck == 'c' {
			header.commandID = value
		}
		return
	}
	if l == 4 {
		switch ck {
		case 'g':
			if groupSequenceNumber, ok := parseUint32Value(value); ok {
				header.groupSequenceNumberValue = groupSequenceNumber
				header.groupSequenceNumber = &header.groupSequenceNumberValue
			}
		case 'o':
			header.options = value
		case 's':
			header.subIDs = value
		}
		return
	}
	if l == 5 {
		if ck == 't' &&
			key[1] == 'o' &&
			key[2] == 'p' &&
			key[3] == '_' &&
			key[4] == 'n' {
			if topN, ok := parseUint32Value(value); ok {
				header.topNValue = topN
				header.topN = &header.topNValue
			}
		}
		return
	}
	if l == 6 {
		if ck == 's' &&
			key[1] == 'u' &&
			key[2] == 'b' &&
			key[3] == '_' &&
			key[4] == 'i' &&
			key[5] == 'd' {
			header.subID = value
			return
		}
		if ck == 'r' &&
			key[1] == 'e' &&
			key[2] == 'a' &&
			key[3] == 's' &&
			key[4] == 'o' &&
			key[5] == 'n' {
			header.reason = value
			return
		}
		if ck == 's' &&
			key[1] == 't' &&
			key[2] == 'a' &&
			key[3] == 't' &&
			key[4] == 'u' &&
			key[5] == 's' {
			header.status = value
			return
		}
		if ck == 'f' &&
			key[1] == 'i' &&
			key[2] == 'l' &&
			key[3] == 't' &&
			key[4] == 'e' &&
			key[5] == 'r' {
			header.filter = value
			return
		}
		return
	}
	if l == 7 {
		switch ck {
		case 'm':
			if key[1] == 'a' &&
				key[2] == 't' &&
				key[3] == 'c' &&
				key[4] == 'h' &&
				key[5] == 'e' &&
				key[6] == 's' {
				if matches, ok := parseUint32Value(value); ok {
					header.matchesValue = matches
					header.matches = &header.matchesValue
				}
			}
		case 'o':
			if key[1] == 'r' &&
				key[2] == 'd' &&
				key[3] == 'e' &&
				key[4] == 'r' &&
				key[5] == 'B' &&
				key[6] == 'y' {
				header.orderBy = value
			}
		case 'u':
			if key[1] == 's' &&
				key[2] == 'e' &&
				key[3] == 'r' &&
				key[4] == '_' &&
				key[5] == 'i' &&
				key[6] == 'd' {
				header.userID = value
			}
		case 'v':
			if key[1] == 'e' &&
				key[2] == 'r' &&
				key[3] == 's' &&
				key[4] == 'i' &&
				key[5] == 'o' &&
				key[6] == 'n' {
				header.version = value
			}
		}
		return
	}
	if l >= 8 && l <= 16 {
		if ck == 'c' &&
			l == 10 &&
			key[1] == 'l' &&
			key[2] == 'i' &&
			key[3] == 'e' &&
			key[4] == 'n' &&
			key[5] == 't' &&
			key[6] == '_' &&
			key[7] == 'n' &&
			key[8] == 'a' &&
			key[9] == 'm' &&
			key[10] == 'e' {
			header.clientName = value
			return
		}
		if ck == 'q' &&
			l == 8 &&
			key[1] == 'u' &&
			key[2] == 'e' &&
			key[3] == 'r' &&
			key[4] == 'y' &&
			key[5] == '_' &&
			key[6] == 'i' &&
			key[7] == 'd' {
			header.queryID = value
			return
		}
		if ck == 'r' {
			if l == 14 &&
				key[1] == 'e' &&
				key[2] == 'c' &&
				key[3] == 'o' &&
				key[4] == 'r' &&
				key[5] == 'd' &&
				key[6] == 's' &&
				key[7] == '_' {
				if key[8] == 'd' &&
					key[9] == 'e' &&
					key[10] == 'l' &&
					key[11] == 'e' &&
					key[12] == 't' &&
					key[13] == 'e' &&
					key[14] == 'd' {
					if records, ok := parseUint32Value(value); ok {
						header.recordsDeletedValue = records
						header.recordsDeleted = &header.recordsDeletedValue
					}
					return
				}
				if key[8] == 'i' &&
					key[9] == 'n' &&
					key[10] == 's' &&
					key[11] == 'e' &&
					key[12] == 'r' &&
					key[13] == 't' &&
					key[14] == 'e' &&
					key[15] == 'd' {
					if records, ok := parseUint32Value(value); ok {
						header.recordsInsertedValue = records
						header.recordsInserted = &header.recordsInsertedValue
					}
					return
				}
			}
			if l == 15 &&
				key[1] == 'e' &&
				key[2] == 'c' &&
				key[3] == 'o' &&
				key[4] == 'r' &&
				key[5] == 'd' &&
				key[6] == 's' &&
				key[7] == '_' &&
				key[8] == 'r' &&
				key[9] == 'e' &&
				key[10] == 't' &&
				key[11] == 'u' &&
				key[12] == 'r' &&
				key[13] == 'n' &&
				key[14] == 'e' &&
				key[15] == 'd' {
				if records, ok := parseUint32Value(value); ok {
					header.recordsReturnedValue = records
					header.recordsReturned = &header.recordsReturnedValue
				}
				return
			}
		}
		if ck == 's' &&
			l == 7 &&
			key[1] == 'o' &&
			key[2] == 'w' &&
			key[3] == '_' &&
			key[4] == 'k' &&
			key[5] == 'e' &&
			key[6] == 'y' &&
			key[7] == 's' {
			header.sowKeys = value
			return
		}
		if ck == 't' &&
			l == 14 &&
			key[1] == 'o' &&
			key[2] == 'p' &&
			key[3] == 'i' &&
			key[4] == 'c' &&
			key[5] == '_' &&
			key[6] == 'm' &&
			key[7] == 'a' &&
			key[8] == 't' &&
			key[9] == 'c' &&
			key[10] == 'h' &&
			key[11] == 'e' &&
			key[12] == 's' {
			if tm, ok := parseUint32Value(value); ok {
				header.topicMatchesValue = tm
				header.topicMatches = &header.topicMatchesValue
			}
			return
		}
	}
}

func isJSONWhitespace(character byte) bool {
	return jsonWhitespaceLookup[character]
}

// Copy executes the exported copy operation.
func (msg *Message) Copy() *Message {
	message := &Message{header: new(_Header)}
	message.client = msg.client
	message.valid = msg.valid
	message.ignoreAutoAck = msg.ignoreAutoAck
	message.bookmarkSeqNo = msg.bookmarkSeqNo
	message.subscriptionHandle = msg.subscriptionHandle
	message.rawTransmissionTime = msg.rawTransmissionTime
	message.disowned = msg.disowned

	message.header.command = msg.header.command
	if msg.data != nil {
		message.data = make([]byte, len(msg.data))
		copy(message.data, msg.data)
	}

	if msg.header.ackType != nil {
		ackType := *msg.header.ackType
		message.header.ackType = &ackType
	}
	if msg.header.batchSize != nil {
		batchSize := *msg.header.batchSize
		message.header.batchSize = &batchSize
	}
	if msg.header.bookmark != nil {
		message.header.bookmark = make([]byte, len(msg.header.bookmark))
		copy(message.header.bookmark, msg.header.bookmark)
	}
	if msg.header.commandID != nil {
		message.header.commandID = make([]byte, len(msg.header.commandID))
		copy(message.header.commandID, msg.header.commandID)
	}
	if msg.header.correlationID != nil {
		message.header.correlationID = make([]byte, len(msg.header.correlationID))
		copy(message.header.correlationID, msg.header.correlationID)
	}
	if msg.header.expiration != nil {
		expiration := *msg.header.expiration
		message.header.expiration = &expiration
	}
	if msg.header.filter != nil {
		message.header.filter = make([]byte, len(msg.header.filter))
		copy(message.header.filter, msg.header.filter)
	}
	if msg.header.groupSequenceNumber != nil {
		gseq := *msg.header.groupSequenceNumber
		message.header.groupSequenceNumber = &gseq
	}
	if msg.header.leasePeriod != nil {
		message.header.leasePeriod = make([]byte, len(msg.header.leasePeriod))
		copy(message.header.leasePeriod, msg.header.leasePeriod)
	}
	if msg.header.matches != nil {
		matches := *msg.header.matches
		message.header.matches = &matches
	}
	if msg.header.messageLength != nil {
		msgLen := *msg.header.messageLength
		message.header.messageLength = &msgLen
	}
	if msg.header.options != nil {
		message.header.options = make([]byte, len(msg.header.options))
		copy(message.header.options, msg.header.options)
	}
	if msg.header.orderBy != nil {
		message.header.orderBy = make([]byte, len(msg.header.orderBy))
		copy(message.header.orderBy, msg.header.orderBy)
	}
	if msg.header.queryID != nil {
		message.header.queryID = make([]byte, len(msg.header.queryID))
		copy(message.header.queryID, msg.header.queryID)
	}
	if msg.header.reason != nil {
		message.header.reason = make([]byte, len(msg.header.reason))
		copy(message.header.reason, msg.header.reason)
	}
	if msg.header.recordsDeleted != nil {
		rD := *msg.header.recordsDeleted
		message.header.recordsDeleted = &rD
	}
	if msg.header.recordsInserted != nil {
		rI := *msg.header.recordsInserted
		message.header.recordsInserted = &rI
	}
	if msg.header.recordsReturned != nil {
		rR := *msg.header.recordsReturned
		message.header.recordsReturned = &rR
	}
	if msg.header.recordsUpdated != nil {
		rU := *msg.header.recordsUpdated
		message.header.recordsUpdated = &rU
	}
	if msg.header.sequenceID != nil {
		sequenceID := *msg.header.sequenceID
		message.header.sequenceID = &sequenceID
	}
	if msg.header.sowKey != nil {
		message.header.sowKey = make([]byte, len(msg.header.sowKey))
		copy(message.header.sowKey, msg.header.sowKey)
	}
	if msg.header.sowKeys != nil {
		message.header.sowKeys = make([]byte, len(msg.header.sowKeys))
		copy(message.header.sowKeys, msg.header.sowKeys)
	}
	if msg.header.status != nil {
		message.header.status = make([]byte, len(msg.header.status))
		copy(message.header.status, msg.header.status)
	}
	if msg.header.subID != nil {
		message.header.subID = make([]byte, len(msg.header.subID))
		copy(message.header.subID, msg.header.subID)
	}
	if msg.header.subIDs != nil {
		message.header.subIDs = make([]byte, len(msg.header.subIDs))
		copy(message.header.subIDs, msg.header.subIDs)
	}
	if msg.header.timestamp != nil {
		message.header.timestamp = make([]byte, len(msg.header.timestamp))
		copy(message.header.timestamp, msg.header.timestamp)
	}
	if msg.header.topN != nil {
		topN := *msg.header.topN
		message.header.topN = &topN
	}
	if msg.header.topic != nil {
		message.header.topic = make([]byte, len(msg.header.topic))
		copy(message.header.topic, msg.header.topic)
	}
	if msg.header.topicMatches != nil {
		tM := *msg.header.topicMatches
		message.header.topicMatches = &tM
	}
	if msg.header.userID != nil {
		message.header.userID = make([]byte, len(msg.header.userID))
		copy(message.header.userID, msg.header.userID)
	}

	return message
}

// AckType executes the exported acktype operation.
func (msg *Message) AckType() (int, bool) {
	if msg.header.ackType != nil {
		return *msg.header.ackType, true
	}
	return AckTypeNone, false
}

// GetAckTypeEnum returns ack type bitset.
func (msg *Message) GetAckTypeEnum() int {
	value, _ := msg.AckType()
	return value
}

// SetAckTypeEnum sets ack type bitset.
func (msg *Message) SetAckTypeEnum(ackType int) *Message {
	if msg == nil {
		return nil
	}
	msg.header.ackType = &ackType
	return msg
}

// BatchSize executes the exported batchsize operation.
func (msg *Message) BatchSize() (uint, bool) {
	if msg.header.batchSize != nil {
		return *msg.header.batchSize, true
	}
	return 0, false
}

// Bookmark executes the exported bookmark operation.
func (msg *Message) Bookmark() (string, bool) {
	return string(msg.header.bookmark), msg.header.bookmark != nil
}

// Command executes the exported command operation.
func (msg *Message) Command() (int, bool) { return msg.header.command, true }

// GetCommandEnum returns command enum.
func (msg *Message) GetCommandEnum() int {
	if msg == nil {
		return CommandUnknown
	}
	return msg.header.command
}

// SetCommandEnum sets command enum.
func (msg *Message) SetCommandEnum(command int) *Message {
	if msg == nil {
		return nil
	}
	msg.header.command = command
	return msg
}

// CommandID executes the exported commandid operation.
func (msg *Message) CommandID() (string, bool) {
	return string(msg.header.commandID), msg.header.commandID != nil
}

// CorrelationID executes the exported correlationid operation.
func (msg *Message) CorrelationID() (string, bool) {
	return string(msg.header.correlationID), msg.header.correlationID != nil
}

// Data executes the exported data operation.
func (msg *Message) Data() []byte { return msg.data }

// SetData sets payload bytes.
func (msg *Message) SetData(data []byte) *Message {
	if msg == nil {
		return nil
	}
	msg.data = append(msg.data[:0], data...)
	return msg
}

// Expiration executes the exported expiration operation.
func (msg *Message) Expiration() (uint, bool) {
	if msg.header.expiration != nil {
		return *msg.header.expiration, true
	}
	return 0, false
}

// Filter executes the exported filter operation.
func (msg *Message) Filter() (string, bool) {
	return string(msg.header.filter), msg.header.filter != nil
}

// GroupSequenceNumber executes the exported groupsequencenumber operation.
func (msg *Message) GroupSequenceNumber() (uint, bool) {
	if msg.header.groupSequenceNumber != nil {
		return *msg.header.groupSequenceNumber, true
	}
	return 0, false
}

// LeasePeriod executes the exported leaseperiod operation.
func (msg *Message) LeasePeriod() (string, bool) {
	return string(msg.header.leasePeriod), msg.header.leasePeriod != nil
}

// Matches executes the exported matches operation.
func (msg *Message) Matches() (uint, bool) {
	if msg.header.matches != nil {
		return *msg.header.matches, true
	}
	return 0, false
}

// MessageLength executes the exported messagelength operation.
func (msg *Message) MessageLength() (uint, bool) {
	if msg.header.messageLength != nil {
		return *msg.header.messageLength, true
	}
	return 0, false
}

// Options executes the exported options operation.
func (msg *Message) Options() (string, bool) {
	return string(msg.header.options), msg.header.options != nil
}

// OrderBy executes the exported orderby operation.
func (msg *Message) OrderBy() (string, bool) {
	return string(msg.header.orderBy), msg.header.orderBy != nil
}

// QueryID executes the exported queryid operation.
func (msg *Message) QueryID() (string, bool) {
	return string(msg.header.queryID), msg.header.queryID != nil
}

// Reason executes the exported reason operation.
func (msg *Message) Reason() (string, bool) {
	return string(msg.header.reason), msg.header.reason != nil
}

// RecordsDeleted executes the exported recordsdeleted operation.
func (msg *Message) RecordsDeleted() (uint, bool) {
	if msg.header.recordsDeleted != nil {
		return *msg.header.recordsDeleted, true
	}
	return 0, false
}

// RecordsInserted executes the exported recordsinserted operation.
func (msg *Message) RecordsInserted() (uint, bool) {
	if msg.header.recordsInserted != nil {
		return *msg.header.recordsInserted, true
	}
	return 0, false
}

// RecordsReturned executes the exported recordsreturned operation.
func (msg *Message) RecordsReturned() (uint, bool) {
	if msg.header.recordsReturned != nil {
		return *msg.header.recordsReturned, true
	}
	return 0, false
}

// RecordsUpdated executes the exported recordsupdated operation.
func (msg *Message) RecordsUpdated() (uint, bool) {
	if msg.header.recordsUpdated != nil {
		return *msg.header.recordsUpdated, true
	}
	return 0, false
}

// SequenceID executes the exported sequenceid operation.
func (msg *Message) SequenceID() (uint64, bool) {
	if msg.header.sequenceID != nil {
		return *msg.header.sequenceID, true
	}
	return 0, false
}

// SowKey executes the exported sowkey operation.
func (msg *Message) SowKey() (string, bool) {
	return string(msg.header.sowKey), msg.header.sowKey != nil
}

// SowKeys executes the exported sowkeys operation.
func (msg *Message) SowKeys() (string, bool) {
	return string(msg.header.sowKeys), msg.header.sowKeys != nil
}

// Status executes the exported status operation.
func (msg *Message) Status() (string, bool) {
	return string(msg.header.status), msg.header.status != nil
}

// SubID executes the exported subid operation.
func (msg *Message) SubID() (string, bool) { return string(msg.header.subID), msg.header.subID != nil }

// SubIDs executes the exported subids operation.
func (msg *Message) SubIDs() (string, bool) {
	return string(msg.header.subIDs), msg.header.subIDs != nil
}

// Timestamp executes the exported timestamp operation.
func (msg *Message) Timestamp() (string, bool) {
	return string(msg.header.timestamp), msg.header.timestamp != nil
}

// TopN executes the exported topn operation.
func (msg *Message) TopN() (uint, bool) {
	if msg.header.topN != nil {
		return *msg.header.topN, true
	}
	return 0, false
}

// Topic executes the exported topic operation.
func (msg *Message) Topic() (string, bool) { return string(msg.header.topic), msg.header.topic != nil }

// TopicMatches executes the exported topicmatches operation.
func (msg *Message) TopicMatches() (uint, bool) {
	if msg.header.topicMatches != nil {
		return *msg.header.topicMatches, true
	}
	return 0, false
}

// UserID executes the exported userid operation.
func (msg *Message) UserID() (string, bool) {
	return string(msg.header.userID), msg.header.userID != nil
}

// Ack acknowledges the message using topic and bookmark fields.
func (msg *Message) Ack(options ...string) error {
	if msg == nil || msg.client == nil {
		return NewError(CommandError, "message is not bound to a client")
	}
	topic, hasTopic := msg.Topic()
	bookmark, hasBookmark := msg.Bookmark()
	if !hasTopic || !hasBookmark {
		return NewError(CommandError, "message does not contain topic and bookmark")
	}
	if len(options) > 0 && options[0] != "" {
		return msg.client.Ack(topic, bookmark, options[0])
	}
	return msg.client.Ack(topic, bookmark)
}

// AssignData assigns a payload copy into the message.
func (msg *Message) AssignData(data []byte) {
	if msg == nil {
		return
	}
	msg.data = append(msg.data[:0], data...)
}

// DeepCopy returns a full copy of the message.
func (msg *Message) DeepCopy() *Message {
	if msg == nil {
		return nil
	}
	return msg.Copy()
}

// Disown marks the message payload as externally owned.
func (msg *Message) Disown() {
	if msg == nil {
		return
	}
	msg.disowned = true
}

// GetBookmarkSeqNo returns the tracked bookmark sequence number.
func (msg *Message) GetBookmarkSeqNo() uint64 {
	if msg == nil {
		return 0
	}
	return msg.bookmarkSeqNo
}

// SetBookmarkSeqNo sets the tracked bookmark sequence number.
func (msg *Message) SetBookmarkSeqNo(sequence uint64) *Message {
	if msg == nil {
		return nil
	}
	msg.bookmarkSeqNo = sequence
	return msg
}

// GetIgnoreAutoAck reports whether auto-ack is ignored for this message.
func (msg *Message) GetIgnoreAutoAck() bool {
	if msg == nil {
		return false
	}
	return msg.ignoreAutoAck
}

// SetIgnoreAutoAck configures whether auto-ack should ignore this message.
func (msg *Message) SetIgnoreAutoAck(ignore bool) *Message {
	if msg == nil {
		return nil
	}
	msg.ignoreAutoAck = ignore
	return msg
}

// GetMessage returns the message handle itself.
func (msg *Message) GetMessage() *Message {
	return msg
}

// GetRawData returns the payload bytes.
func (msg *Message) GetRawData() []byte {
	if msg == nil {
		return nil
	}
	return msg.data
}

// GetRawTransmissionTime returns the receive timestamp in RFC3339 format.
func (msg *Message) GetRawTransmissionTime() string {
	if msg == nil {
		return ""
	}
	return msg.rawTransmissionTime
}

// GetSubscriptionHandle returns the associated subscription handle.
func (msg *Message) GetSubscriptionHandle() string {
	if msg == nil {
		return ""
	}
	return msg.subscriptionHandle
}

// SetSubscriptionHandle sets the associated subscription handle.
func (msg *Message) SetSubscriptionHandle(handle string) *Message {
	if msg == nil {
		return nil
	}
	msg.subscriptionHandle = handle
	return msg
}

// Invalidate marks the message as invalid.
func (msg *Message) Invalidate() {
	if msg == nil {
		return
	}
	msg.valid = false
}

// IsValid reports whether the message is valid.
func (msg *Message) IsValid() bool {
	if msg == nil {
		return false
	}
	return msg.valid
}

// Replace replaces message contents with another message.
func (msg *Message) Replace(other *Message) *Message {
	if msg == nil || other == nil {
		return msg
	}
	copyMessage := other.Copy()
	msg.header = copyMessage.header
	msg.data = copyMessage.data
	msg.client = copyMessage.client
	msg.valid = copyMessage.valid
	msg.ignoreAutoAck = copyMessage.ignoreAutoAck
	msg.bookmarkSeqNo = copyMessage.bookmarkSeqNo
	msg.subscriptionHandle = copyMessage.subscriptionHandle
	msg.rawTransmissionTime = copyMessage.rawTransmissionTime
	msg.disowned = copyMessage.disowned
	return msg
}

// Reset clears message fields.
func (msg *Message) Reset() {
	if msg == nil {
		return
	}
	msg.reset()
}

// SetClientImpl binds a client context used by helper methods.
func (msg *Message) SetClientImpl(client *Client) *Message {
	if msg == nil {
		return nil
	}
	msg.client = client
	return msg
}

// ThrowFor converts an ack failure reason into an error.
func (msg *Message) ThrowFor() error {
	if msg == nil {
		return NewError(UnknownError, "nil message")
	}
	status, hasStatus := msg.Status()
	if !hasStatus || status == "success" {
		return nil
	}
	reason, hasReason := msg.Reason()
	if hasReason {
		return reasonToError(reason)
	}
	return NewError(UnknownError, "message failure without reason")
}
