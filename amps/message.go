package amps

import (
	"errors"
	"time"
)

// Lookup table for O(1) JSON whitespace detection.
var jsonWhitespaceLookup [256]bool

func init() {
	jsonWhitespaceLookup[' '] = true
	jsonWhitespaceLookup['\n'] = true
	jsonWhitespaceLookup['\r'] = true
	jsonWhitespaceLookup['\t'] = true
}

// Message stores exported state used by AMPS client APIs.
type Message struct {
	header                  *_Header
	data                    []byte
	client                  *Client
	valid                   bool
	ignoreAutoAck           bool
	bookmarkSeqNo           uint64
	subscriptionHandle      string
	rawTransmissionTime     string
	rawTransmissionUnixNano int64
	disowned                bool
}

// Constants in this block define protocol and client behavior values.
const (
	inHeader = iota
	inKey
	afterKey
	inValue
	inValueString
)

func (msg *Message) reset() {
	if msg.header != nil {
		msg.header.reset()
	}
	msg.data = nil
	msg.ignoreAutoAck = false
	msg.bookmarkSeqNo = 0
	msg.subscriptionHandle = ""
	msg.rawTransmissionTime = ""
	msg.rawTransmissionUnixNano = time.Now().UTC().UnixNano()
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
	msg.rawTransmissionTime = ""
	msg.rawTransmissionUnixNano = 0
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
	if len(array) > 6 && array[0] == '{' && array[1] == '"' && array[2] == 'c' && array[3] == '"' && array[4] == ':' && array[5] == '"' {
		if end, ok := parseHeaderTrustedCTSubID(msg.header, array, 1); ok {
			return array[end:], nil
		}
	}
	if len(array) > 5 && array[0] == '{' && array[1] == '"' && array[2] == 't' && array[3] == '"' && array[4] == ':' {
		if end, ok := parseHeaderTrustedTopicOnly(msg.header, array, 1); ok {
			return array[end:], nil
		}
	}

	if end, ok := parseHeaderTrusted(msg.header, array); ok {
		return array[end:], nil
	}

	return parseHeaderChecked(msg.header, array)
}

func parseHeaderTrusted(header *_Header, array []byte) (int, bool) {
	var n = len(array)
	if n == 0 {
		return 0, false
	}

	var index int
	for index < n && isJSONWhitespace(array[index]) {
		index++
	}
	if index < n && array[index] == '{' {
		index++
	}

	if end, ok := parseHeaderTrustedCTSubID(header, array, index); ok {
		return end, true
	}
	if end, ok := parseHeaderTrustedTopicOnly(header, array, index); ok {
		return end, true
	}

	for index < n {
		if isJSONWhitespace(array[index]) || array[index] == ',' {
			index++
			continue
		}
		if array[index] == '}' {
			return index + 1, true
		}
		if array[index] != '"' {
			return 0, false
		}

		var keyStart = index + 1
		index++
		for index < n {
			if array[index] == '"' {
				break
			}
			if array[index] == '\\' {
				return 0, false
			}
			index++
		}
		if index >= n {
			return 0, false
		}
		var keyEnd = index
		index++
		if index >= n || array[index] != ':' {
			return 0, false
		}
		index++
		if index >= n {
			return 0, false
		}

		var valueStart int
		var valueEnd int
		if array[index] == '"' {
			valueStart = index + 1
			index++
			for index < n {
				if array[index] == '"' {
					break
				}
				if array[index] == '\\' {
					return 0, false
				}
				index++
			}
			if index >= n {
				return 0, false
			}
			valueEnd = index
			index++
		} else {
			valueStart = index
			for index < n {
				if array[index] == ',' || array[index] == '}' {
					break
				}
				if isJSONWhitespace(array[index]) {
					return 0, false
				}
				index++
			}
			if index >= n {
				return 0, false
			}
			valueEnd = index
		}

		parseFieldTrusted(header, array[keyStart:keyEnd], array[valueStart:valueEnd])

		if index >= n {
			return 0, false
		}
		if array[index] == ',' {
			index++
			continue
		}
		if array[index] == '}' {
			return index + 1, true
		}
		return 0, false
	}

	return 0, false
}

func parseHeaderTrustedCTSubID(header *_Header, array []byte, index int) (int, bool) {
	var n = len(array)
	if index+11 >= n {
		return 0, false
	}
	if array[index] != '"' || array[index+1] != 'c' || array[index+2] != '"' || array[index+3] != ':' || array[index+4] != '"' {
		return 0, false
	}

	var i = index + 5
	var cmdStart int
	var cmdEnd int
	if i+1 < n && array[i+1] == '"' {
		cmdStart = i
		cmdEnd = i + 1
		i = cmdEnd
	} else {
		cmdStart = i
		for i < n && array[i] != '"' {
			i++
		}
		if i >= n {
			return 0, false
		}
		cmdEnd = i
	}
	if i+11 >= n {
		return 0, false
	}
	if array[i+1] != ',' || array[i+2] != '"' || array[i+3] != 't' || array[i+4] != '"' || array[i+5] != ':' || array[i+6] != '"' {
		return 0, false
	}

	var topicStart = i + 7
	i = topicStart
	for i < n && array[i] != '"' {
		i++
	}
	if i >= n || i+11 >= n {
		return 0, false
	}
	var topicEnd = i
	if array[i+1] != ',' || array[i+2] != '"' || array[i+3] != 's' || array[i+4] != 'u' || array[i+5] != 'b' || array[i+6] != '_' || array[i+7] != 'i' || array[i+8] != 'd' || array[i+9] != '"' || array[i+10] != ':' || array[i+11] != '"' {
		return 0, false
	}

	var subIDStart = i + 12
	i = subIDStart
	for i < n && array[i] != '"' {
		i++
	}
	if i >= n || i+1 >= n || array[i+1] != '}' {
		return 0, false
	}
	var subIDEnd = i

	if cmdEnd == cmdStart+1 && array[cmdStart] == 'p' {
		header.command = CommandPublish
	} else {
		header.command = commandBytesToInt(array[cmdStart:cmdEnd])
	}
	header.topic = array[topicStart:topicEnd]
	header.subID = array[subIDStart:subIDEnd]
	return i + 2, true
}

func parseHeaderTrustedTopicOnly(header *_Header, array []byte, index int) (int, bool) {
	var n = len(array)
	if index+4 >= n {
		return 0, false
	}
	if array[index] != '"' || array[index+1] != 't' || array[index+2] != '"' || array[index+3] != ':' {
		return 0, false
	}

	var valueStart = index + 4
	var valueEnd int
	if valueStart < n && array[valueStart] == '"' {
		valueStart++
		valueEnd = valueStart
		for valueEnd < n && array[valueEnd] != '"' {
			valueEnd++
		}
		if valueEnd >= n || valueEnd+1 >= n || array[valueEnd+1] != '}' {
			return 0, false
		}
		header.topic = array[valueStart:valueEnd]
		return valueEnd + 2, true
	}

	valueEnd = valueStart
	for valueEnd < n {
		if array[valueEnd] == '}' {
			break
		}
		valueEnd++
	}
	if valueEnd >= n {
		return 0, false
	}

	header.topic = array[valueStart:valueEnd]
	return valueEnd + 1, true
}

func parseFieldTrusted(header *_Header, key []byte, value []byte) {
	if len(key) == 1 {
		switch key[0] {
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
		case 'e':
			if expiration, ok := parseUint32Value(value); ok {
				header.expirationValue = expiration
				header.expiration = &header.expirationValue
			}
			return
		case 'f':
			header.filter = value
			return
		case 'o':
			header.options = value
			return
		case 'a':
			if ack := parseAckBytes(value); ack >= 0 {
				header.ackTypeValue = ack
				header.ackType = &header.ackTypeValue
			}
			return
		}
	}

	if len(key) == 3 {
		if key[0] == 'c' && key[1] == 'i' && key[2] == 'd' {
			header.commandID = value
			return
		}
	}

	if len(key) == 6 {
		if key[0] == 's' && key[1] == 'u' && key[2] == 'b' && key[3] == '_' && key[4] == 'i' && key[5] == 'd' {
			header.subID = value
			return
		}
		if key[0] == 'f' && key[1] == 'i' && key[2] == 'l' && key[3] == 't' && key[4] == 'e' && key[5] == 'r' {
			header.filter = value
			return
		}
	}

	header.parseField(key, value)
}

func parseHeaderChecked(header *_Header, array []byte) ([]byte, error) {
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
				header.parseField(array[keyStart:keyEnd], array[valueStart:valueEnd])
			case '}':
				header.parseField(array[keyStart:keyEnd], array[valueStart:index])
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
				header.parseField(array[keyStart:keyEnd], array[valueStart:valueEnd])
			}
		}
	}

	return array, errors.New("unexpected end of AMPS header")
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
	message.rawTransmissionUnixNano = msg.rawTransmissionUnixNano
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
	if msg.rawTransmissionTime == "" && msg.rawTransmissionUnixNano > 0 {
		msg.rawTransmissionTime = time.Unix(0, msg.rawTransmissionUnixNano).UTC().Format(time.RFC3339Nano)
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
	msg.rawTransmissionUnixNano = copyMessage.rawTransmissionUnixNano
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
