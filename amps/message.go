package amps

import (
	"errors"
	"strconv"
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

func messageHeader(msg *Message) *_Header {
	if msg == nil {
		return nil
	}
	return msg.header
}

func ensureMessageHeader(msg *Message) *_Header {
	if msg == nil {
		return nil
	}
	if msg.header == nil {
		msg.header = newHeader()
	}
	return msg.header
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
	if msg == nil {
		return
	}
	var header = msg.header
	if header == nil {
		header = newHeader()
		msg.header = header
	} else {
		header.reset()
	}
	msg.data = nil
	msg.ignoreAutoAck = false
	msg.bookmarkSeqNo = 0
	msg.subscriptionHandle = ""
	msg.rawTransmissionTime = ""
	msg.rawTransmissionUnixNano = time.Now().UnixNano()
	msg.disowned = false
	msg.valid = true
}

func (msg *Message) resetForParse() {
	if msg == nil {
		return
	}
	var header = msg.header
	if header == nil {
		header = newHeader()
		msg.header = header
	} else {
		header.reset()
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
	var header *_Header
	if resetMessage {
		msg.resetForParse()
		header = ensureMessageHeader(msg)
	} else {
		header = ensureMessageHeader(msg)
	}

	parseIntoHeader := func(target *_Header) ([]byte, error) {
		if len(array) > 5 && array[0] == '"' {
			if array[1] == 'c' && array[2] == '"' && array[3] == ':' && array[4] == '"' {
				if end, ok := parseHeaderTrustedCTSubID(target, array, 0); ok {
					return array[end:], nil
				}
			}
			if array[1] == 't' && array[2] == '"' && array[3] == ':' {
				if end, ok := parseHeaderTrustedTopicOnly(target, array, 0); ok {
					return array[end:], nil
				}
			}
		}
		if len(array) > 6 && array[0] == '{' && array[1] == '"' && array[2] == 'c' && array[3] == '"' && array[4] == ':' && array[5] == '"' {
			if end, ok := parseHeaderTrustedCTSubID(target, array, 1); ok {
				return array[end:], nil
			}
		}
		if len(array) > 5 && array[0] == '{' && array[1] == '"' && array[2] == 't' && array[3] == '"' && array[4] == ':' {
			if end, ok := parseHeaderTrustedTopicOnly(target, array, 1); ok {
				return array[end:], nil
			}
		}

		if end, ok := parseHeaderTrusted(target, array); ok {
			return array[end:], nil
		}

		return parseHeaderChecked(target, array)
	}

	if !resetMessage {
		original := *header
		left, err := parseIntoHeader(header)
		if err != nil {
			*header = original
			return left, err
		}
		return left, nil
	}

	return parseIntoHeader(header)
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
			if valueEnd == valueStart {
				return 0, false
			}
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
	} else if cmdEnd == cmdStart+7 &&
		array[cmdStart] == 'p' &&
		array[cmdStart+1] == 'u' &&
		array[cmdStart+2] == 'b' &&
		array[cmdStart+3] == 'l' &&
		array[cmdStart+4] == 'i' &&
		array[cmdStart+5] == 's' &&
		array[cmdStart+6] == 'h' {
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
		if array[valueEnd] == ',' || isJSONWhitespace(array[valueEnd]) || array[valueEnd] == '"' || array[valueEnd] == '\\' {
			return 0, false
		}
		valueEnd++
	}
	if valueEnd >= n {
		return 0, false
	}
	if valueEnd == valueStart {
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
				if valueStart == index {
					return array, errors.New("malformed AMPS header")
				}
				state = inHeader
				valueEnd = index
				header.parseField(array[keyStart:keyEnd], array[valueStart:valueEnd])
			case '}':
				if valueStart == index {
					return array, errors.New("malformed AMPS header")
				}
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

type ownedMessage struct {
	Message
	header _Header
}

func newOwnedMessage() *Message {
	holder := &ownedMessage{}
	holder.Message.header = &holder.header
	return &holder.Message
}

func copyMessageBytes(buffer []byte, source []byte) ([]byte, []byte) {
	if source == nil {
		return buffer, nil
	}

	var copied = buffer[:len(source)]
	copy(copied, source)
	return buffer[len(source):], copied
}

// Copy executes the exported copy operation.
func (msg *Message) Copy() *Message {
	if msg == nil {
		return nil
	}

	var header = messageHeader(msg)
	var message *Message
	if header == nil {
		message = &Message{}
	} else {
		message = newOwnedMessage()
	}
	message.client = msg.client
	message.valid = msg.valid
	message.ignoreAutoAck = msg.ignoreAutoAck
	message.bookmarkSeqNo = msg.bookmarkSeqNo
	message.subscriptionHandle = msg.subscriptionHandle
	message.rawTransmissionTime = msg.rawTransmissionTime
	message.rawTransmissionUnixNano = msg.rawTransmissionUnixNano
	message.disowned = msg.disowned

	if header == nil {
		if msg.data != nil {
			message.data = make([]byte, len(msg.data))
			copy(message.data, msg.data)
		}
		return message
	}

	message.header.command = header.command
	var totalCopiedBytes = len(msg.data) +
		len(header.bookmark) +
		len(header.commandID) +
		len(header.clientName) +
		len(header.correlationID) +
		len(header.dataOnly) +
		len(header.filter) +
		len(header.leasePeriod) +
		len(header.messageType) +
		len(header.options) +
		len(header.orderBy) +
		len(header.password) +
		len(header.queryID) +
		len(header.reason) +
		len(header.sendEmpty) +
		len(header.sendKeys) +
		len(header.sendOOF) +
		len(header.sowKey) +
		len(header.sowKeys) +
		len(header.status) +
		len(header.subID) +
		len(header.subIDs) +
		len(header.timestamp) +
		len(header.topic) +
		len(header.userID) +
		len(header.version)
	var needsCopiedBytes = msg.data != nil ||
		header.bookmark != nil ||
		header.commandID != nil ||
		header.clientName != nil ||
		header.correlationID != nil ||
		header.dataOnly != nil ||
		header.filter != nil ||
		header.leasePeriod != nil ||
		header.messageType != nil ||
		header.options != nil ||
		header.orderBy != nil ||
		header.password != nil ||
		header.queryID != nil ||
		header.reason != nil ||
		header.sendEmpty != nil ||
		header.sendKeys != nil ||
		header.sendOOF != nil ||
		header.sowKey != nil ||
		header.sowKeys != nil ||
		header.status != nil ||
		header.subID != nil ||
		header.subIDs != nil ||
		header.timestamp != nil ||
		header.topic != nil ||
		header.userID != nil ||
		header.version != nil
	var copiedBytes []byte
	if needsCopiedBytes {
		copiedBytes = make([]byte, totalCopiedBytes)
	}
	copiedBytes, message.data = copyMessageBytes(copiedBytes, msg.data)

	if header.ackType != nil {
		ackType := *header.ackType
		message.header.ackType = &ackType
	}
	if header.batchSize != nil {
		batchSize := *header.batchSize
		message.header.batchSize = &batchSize
	}
	copiedBytes, message.header.bookmark = copyMessageBytes(copiedBytes, header.bookmark)
	copiedBytes, message.header.commandID = copyMessageBytes(copiedBytes, header.commandID)
	copiedBytes, message.header.clientName = copyMessageBytes(copiedBytes, header.clientName)
	copiedBytes, message.header.correlationID = copyMessageBytes(copiedBytes, header.correlationID)
	if header.expiration != nil {
		expiration := *header.expiration
		message.header.expiration = &expiration
	}
	copiedBytes, message.header.dataOnly = copyMessageBytes(copiedBytes, header.dataOnly)
	copiedBytes, message.header.filter = copyMessageBytes(copiedBytes, header.filter)
	if header.groupSequenceNumber != nil {
		gseq := *header.groupSequenceNumber
		message.header.groupSequenceNumber = &gseq
	}
	copiedBytes, message.header.leasePeriod = copyMessageBytes(copiedBytes, header.leasePeriod)
	if header.matches != nil {
		matches := *header.matches
		message.header.matches = &matches
	}
	if header.messageLength != nil {
		msgLen := *header.messageLength
		message.header.messageLength = &msgLen
	}
	copiedBytes, message.header.messageType = copyMessageBytes(copiedBytes, header.messageType)
	copiedBytes, message.header.options = copyMessageBytes(copiedBytes, header.options)
	copiedBytes, message.header.orderBy = copyMessageBytes(copiedBytes, header.orderBy)
	copiedBytes, message.header.password = copyMessageBytes(copiedBytes, header.password)
	copiedBytes, message.header.queryID = copyMessageBytes(copiedBytes, header.queryID)
	copiedBytes, message.header.reason = copyMessageBytes(copiedBytes, header.reason)
	if header.recordsDeleted != nil {
		rD := *header.recordsDeleted
		message.header.recordsDeleted = &rD
	}
	if header.recordsInserted != nil {
		rI := *header.recordsInserted
		message.header.recordsInserted = &rI
	}
	if header.recordsReturned != nil {
		rR := *header.recordsReturned
		message.header.recordsReturned = &rR
	}
	if header.recordsUpdated != nil {
		rU := *header.recordsUpdated
		message.header.recordsUpdated = &rU
	}
	if header.sequenceID != nil {
		sequenceID := *header.sequenceID
		message.header.sequenceID = &sequenceID
	}
	copiedBytes, message.header.sendEmpty = copyMessageBytes(copiedBytes, header.sendEmpty)
	copiedBytes, message.header.sendKeys = copyMessageBytes(copiedBytes, header.sendKeys)
	copiedBytes, message.header.sendOOF = copyMessageBytes(copiedBytes, header.sendOOF)
	copiedBytes, message.header.sowKey = copyMessageBytes(copiedBytes, header.sowKey)
	copiedBytes, message.header.sowKeys = copyMessageBytes(copiedBytes, header.sowKeys)
	copiedBytes, message.header.status = copyMessageBytes(copiedBytes, header.status)
	copiedBytes, message.header.subID = copyMessageBytes(copiedBytes, header.subID)
	copiedBytes, message.header.subIDs = copyMessageBytes(copiedBytes, header.subIDs)
	copiedBytes, message.header.timestamp = copyMessageBytes(copiedBytes, header.timestamp)
	if header.topN != nil {
		topN := *header.topN
		message.header.topN = &topN
	}
	copiedBytes, message.header.topic = copyMessageBytes(copiedBytes, header.topic)
	if header.topicMatches != nil {
		tM := *header.topicMatches
		message.header.topicMatches = &tM
	}
	copiedBytes, message.header.userID = copyMessageBytes(copiedBytes, header.userID)
	_, message.header.version = copyMessageBytes(copiedBytes, header.version)
	if header.skipN != nil {
		skipN := *header.skipN
		message.header.skipN = &skipN
	}
	if header.maximumMessages != nil {
		maxMsgs := *header.maximumMessages
		message.header.maximumMessages = &maxMsgs
	}
	if header.timeoutInterval != nil {
		ti := *header.timeoutInterval
		message.header.timeoutInterval = &ti
	}
	if header.gracePeriod != nil {
		gp := *header.gracePeriod
		message.header.gracePeriod = &gp
	}

	return message
}

// AckType executes the exported acktype operation.
func (msg *Message) AckType() (int, bool) {
	var header = messageHeader(msg)
	if header != nil && header.ackType != nil {
		return *header.ackType, true
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
	var header = ensureMessageHeader(msg)
	if header == nil {
		return nil
	}
	header.ackType = &ackType
	return msg
}

// BatchSize executes the exported batchsize operation.
func (msg *Message) BatchSize() (uint, bool) {
	var header = messageHeader(msg)
	if header != nil && header.batchSize != nil {
		return *header.batchSize, true
	}
	return 0, false
}

// Bookmark executes the exported bookmark operation.
func (msg *Message) Bookmark() (string, bool) {
	var header = messageHeader(msg)
	if header == nil {
		return "", false
	}
	return string(header.bookmark), header.bookmark != nil
}

// Command executes the exported command operation.
func (msg *Message) Command() (int, bool) {
	var header = messageHeader(msg)
	if header == nil {
		return CommandUnknown, false
	}
	return header.command, header.command >= 0 && header.command < CommandUnknown
}

// GetCommandEnum returns command enum.
func (msg *Message) GetCommandEnum() int {
	var header = messageHeader(msg)
	if header == nil {
		return CommandUnknown
	}
	return header.command
}

// SetCommandEnum sets command enum.
func (msg *Message) SetCommandEnum(command int) *Message {
	var header = ensureMessageHeader(msg)
	if header == nil {
		return nil
	}
	header.command = command
	return msg
}

// CommandID executes the exported commandid operation.
func (msg *Message) CommandID() (string, bool) {
	var header = messageHeader(msg)
	if header == nil {
		return "", false
	}
	return string(header.commandID), header.commandID != nil
}

// CorrelationID executes the exported correlationid operation.
func (msg *Message) CorrelationID() (string, bool) {
	var header = messageHeader(msg)
	if header == nil {
		return "", false
	}
	return string(header.correlationID), header.correlationID != nil
}

// Data executes the exported data operation.
func (msg *Message) Data() []byte {
	if msg == nil {
		return nil
	}
	return msg.data
}

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
	var header = messageHeader(msg)
	if header != nil && header.expiration != nil {
		return *header.expiration, true
	}
	return 0, false
}

// Filter executes the exported filter operation.
func (msg *Message) Filter() (string, bool) {
	var header = messageHeader(msg)
	if header == nil {
		return "", false
	}
	return string(header.filter), header.filter != nil
}

// GroupSequenceNumber executes the exported groupsequencenumber operation.
func (msg *Message) GroupSequenceNumber() (uint, bool) {
	var header = messageHeader(msg)
	if header != nil && header.groupSequenceNumber != nil {
		return *header.groupSequenceNumber, true
	}
	return 0, false
}

// LeasePeriod executes the exported leaseperiod operation.
func (msg *Message) LeasePeriod() (string, bool) {
	var header = messageHeader(msg)
	if header == nil {
		return "", false
	}
	return string(header.leasePeriod), header.leasePeriod != nil
}

// Matches executes the exported matches operation.
func (msg *Message) Matches() (uint, bool) {
	var header = messageHeader(msg)
	if header != nil && header.matches != nil {
		return *header.matches, true
	}
	return 0, false
}

// MessageLength executes the exported messagelength operation.
func (msg *Message) MessageLength() (uint, bool) {
	var header = messageHeader(msg)
	if header != nil && header.messageLength != nil {
		return *header.messageLength, true
	}
	return 0, false
}

// Options executes the exported options operation.
func (msg *Message) Options() (string, bool) {
	var header = messageHeader(msg)
	if header == nil {
		return "", false
	}
	return string(header.options), header.options != nil
}

// OrderBy executes the exported orderby operation.
func (msg *Message) OrderBy() (string, bool) {
	var header = messageHeader(msg)
	if header == nil {
		return "", false
	}
	return string(header.orderBy), header.orderBy != nil
}

// QueryID executes the exported queryid operation.
func (msg *Message) QueryID() (string, bool) {
	var header = messageHeader(msg)
	if header == nil {
		return "", false
	}
	return string(header.queryID), header.queryID != nil
}

// Reason executes the exported reason operation.
func (msg *Message) Reason() (string, bool) {
	var header = messageHeader(msg)
	if header == nil {
		return "", false
	}
	return string(header.reason), header.reason != nil
}

// RecordsDeleted executes the exported recordsdeleted operation.
func (msg *Message) RecordsDeleted() (uint, bool) {
	var header = messageHeader(msg)
	if header != nil && header.recordsDeleted != nil {
		return *header.recordsDeleted, true
	}
	return 0, false
}

// RecordsInserted executes the exported recordsinserted operation.
func (msg *Message) RecordsInserted() (uint, bool) {
	var header = messageHeader(msg)
	if header != nil && header.recordsInserted != nil {
		return *header.recordsInserted, true
	}
	return 0, false
}

// RecordsReturned executes the exported recordsreturned operation.
func (msg *Message) RecordsReturned() (uint, bool) {
	var header = messageHeader(msg)
	if header != nil && header.recordsReturned != nil {
		return *header.recordsReturned, true
	}
	return 0, false
}

// RecordsUpdated executes the exported recordsupdated operation.
func (msg *Message) RecordsUpdated() (uint, bool) {
	var header = messageHeader(msg)
	if header != nil && header.recordsUpdated != nil {
		return *header.recordsUpdated, true
	}
	return 0, false
}

// SequenceID executes the exported sequenceid operation.
func (msg *Message) SequenceID() (uint64, bool) {
	var header = messageHeader(msg)
	if header != nil && header.sequenceID != nil {
		return *header.sequenceID, true
	}
	return 0, false
}

// SowKey executes the exported sowkey operation.
func (msg *Message) SowKey() (string, bool) {
	var header = messageHeader(msg)
	if header == nil {
		return "", false
	}
	return string(header.sowKey), header.sowKey != nil
}

// SowKeys executes the exported sowkeys operation.
func (msg *Message) SowKeys() (string, bool) {
	var header = messageHeader(msg)
	if header == nil {
		return "", false
	}
	return string(header.sowKeys), header.sowKeys != nil
}

// Status executes the exported status operation.
func (msg *Message) Status() (string, bool) {
	var header = messageHeader(msg)
	if header == nil {
		return "", false
	}
	return string(header.status), header.status != nil
}

// SubID executes the exported subid operation.
func (msg *Message) SubID() (string, bool) {
	var header = messageHeader(msg)
	if header == nil {
		return "", false
	}
	return string(header.subID), header.subID != nil
}

// SubIDs executes the exported subids operation.
func (msg *Message) SubIDs() (string, bool) {
	var header = messageHeader(msg)
	if header == nil {
		return "", false
	}
	return string(header.subIDs), header.subIDs != nil
}

// Timestamp executes the exported timestamp operation.
func (msg *Message) Timestamp() (string, bool) {
	var header = messageHeader(msg)
	if header == nil {
		return "", false
	}
	return string(header.timestamp), header.timestamp != nil
}

// TopN executes the exported topn operation.
func (msg *Message) TopN() (uint, bool) {
	var header = messageHeader(msg)
	if header != nil && header.topN != nil {
		return *header.topN, true
	}
	return 0, false
}

// Topic executes the exported topic operation.
func (msg *Message) Topic() (string, bool) {
	var header = messageHeader(msg)
	if header == nil {
		return "", false
	}
	return string(header.topic), header.topic != nil
}

// TopicMatches executes the exported topicmatches operation.
func (msg *Message) TopicMatches() (uint, bool) {
	var header = messageHeader(msg)
	if header != nil && header.topicMatches != nil {
		return *header.topicMatches, true
	}
	return 0, false
}

// UserID executes the exported userid operation.
func (msg *Message) UserID() (string, bool) {
	var header = messageHeader(msg)
	if header == nil {
		return "", false
	}
	return string(header.userID), header.userID != nil
}

func (msg *Message) Password() (string, bool) {
	var header = messageHeader(msg)
	if header == nil {
		return "", false
	}
	return string(header.password), header.password != nil
}

func (m *Message) DataOnly() (bool, bool) {
	header := messageHeader(m)
	if header != nil && header.dataOnly != nil {
		return string(header.dataOnly) == "true", true
	}
	return false, false
}

func (m *Message) SendEmpty() (bool, bool) {
	header := messageHeader(m)
	if header != nil && header.sendEmpty != nil {
		return string(header.sendEmpty) == "true", true
	}
	return false, false
}

func (m *Message) SendKeys() (bool, bool) {
	header := messageHeader(m)
	if header != nil && header.sendKeys != nil {
		return string(header.sendKeys) == "true", true
	}
	return false, false
}

func (m *Message) SendOOF() (bool, bool) {
	header := messageHeader(m)
	if header != nil && header.sendOOF != nil {
		return string(header.sendOOF) == "true", true
	}
	return false, false
}

func (m *Message) SkipN() (uint, bool) {
	header := messageHeader(m)
	if header != nil && header.skipN != nil {
		return *header.skipN, true
	}
	return 0, false
}

func (m *Message) MaximumMessages() (uint, bool) {
	header := messageHeader(m)
	if header != nil && header.maximumMessages != nil {
		return *header.maximumMessages, true
	}
	return 0, false
}

func (m *Message) TimeoutInterval() (uint, bool) {
	header := messageHeader(m)
	if header != nil && header.timeoutInterval != nil {
		return *header.timeoutInterval, true
	}
	return 0, false
}

func (m *Message) GracePeriod() (uint, bool) {
	header := messageHeader(m)
	if header != nil && header.gracePeriod != nil {
		return *header.gracePeriod, true
	}
	return 0, false
}

func (m *Message) LeasePeriodUint() (uint, bool) {
	header := messageHeader(m)
	if header != nil && header.leasePeriod != nil {
		val, err := strconv.ParseUint(string(header.leasePeriod), 10, 64)
		if err == nil {
			return uint(val), true
		}
	}
	return 0, false
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

type OOFReason int

const (
	OOFReasonDeleted OOFReason = iota
	OOFReasonExpired
	OOFReasonMatch
	OOFReasonEntitlement
	OOFReasonUnknown
)

func (r OOFReason) String() string {
	switch r {
	case OOFReasonDeleted:
		return "deleted"
	case OOFReasonExpired:
		return "expired"
	case OOFReasonMatch:
		return "match"
	case OOFReasonEntitlement:
		return "entitlement"
	default:
		return "unknown"
	}
}

func (m *Message) OOFReason() OOFReason {
	reason, _ := m.Reason()
	switch reason {
	case "deleted":
		return OOFReasonDeleted
	case "expired":
		return OOFReasonExpired
	case "match":
		return OOFReasonMatch
	case "entitlement":
		return OOFReasonEntitlement
	default:
		return OOFReasonUnknown
	}
}
