package amps

import (
	"errors"
)

type Message struct {
	header *_Header
	data   []byte
}

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
}

func parseHeader(msg *Message, resetMessage bool, array []byte) ([]byte, error) {

	if msg == nil {
		return array, errors.New("Message object error (Null Pointer)")
	}

	if resetMessage {
		msg.reset()
	}

	state := inHeader
	var keyStart, keyEnd, valueStart, valueEnd int
	for i, character := range array {
		switch character {
		case '"':
			switch state {
			case inHeader:
				state = inKey
				keyStart = i + 1
			case inKey:
				state = afterKey
				keyEnd = i
			case inValue:
				state = inValueString
				valueStart = i + 1
			case inValueString:
				state = inHeader
				valueEnd = i
				msg.header.parseField(array[keyStart:keyEnd], array[valueStart:valueEnd])
			}
		case ':':
			if state == afterKey {
				state = inValue
				valueStart = i + 1
			}
		case ',':
			if state == inValue {
				state = inHeader
				valueEnd = i
				msg.header.parseField(array[keyStart:keyEnd], array[valueStart:valueEnd])
			}
		case '}':

			if array[i-1] != '"' && i-1 > 0 {
				msg.header.parseField(array[keyStart:keyEnd], array[valueStart:i])
			}

			return array[i+1:], nil
		}
	}

	return array, errors.New("Unexpected end of AMPS header")
}

func (msg *Message) Copy() *Message {
	message := &Message{header: new(_Header)}

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

func (msg *Message) AckType() (int, bool) {
	if msg.header.ackType != nil {
		return *msg.header.ackType, true
	}
	return AckTypeNone, false
}

func (msg *Message) BatchSize() (uint, bool) {
	if msg.header.batchSize != nil {
		return *msg.header.batchSize, true
	}
	return 0, false
}

func (msg *Message) Bookmark() (string, bool) {
	return string(msg.header.bookmark), msg.header.bookmark != nil
}

func (msg *Message) Command() (int, bool) { return msg.header.command, true }

func (msg *Message) CommandID() (string, bool) {
	return string(msg.header.commandID), msg.header.commandID != nil
}

func (msg *Message) CorrelationID() (string, bool) {
	return string(msg.header.correlationID), msg.header.correlationID != nil
}

func (msg *Message) Data() []byte { return msg.data }

func (msg *Message) Expiration() (uint, bool) {
	if msg.header.expiration != nil {
		return *msg.header.expiration, true
	}
	return 0, false
}

func (msg *Message) Filter() (string, bool) {
	return string(msg.header.filter), msg.header.filter != nil
}

func (msg *Message) GroupSequenceNumber() (uint, bool) {
	if msg.header.groupSequenceNumber != nil {
		return *msg.header.groupSequenceNumber, true
	}
	return 0, false
}

func (msg *Message) LeasePeriod() (string, bool) {
	return string(msg.header.leasePeriod), msg.header.leasePeriod != nil
}

func (msg *Message) Matches() (uint, bool) {
	if msg.header.matches != nil {
		return *msg.header.matches, true
	}
	return 0, false
}

func (msg *Message) MessageLength() (uint, bool) {
	if msg.header.messageLength != nil {
		return *msg.header.messageLength, true
	}
	return 0, false
}

func (msg *Message) Options() (string, bool) {
	return string(msg.header.options), msg.header.options != nil
}

func (msg *Message) OrderBy() (string, bool) {
	return string(msg.header.orderBy), msg.header.orderBy != nil
}

func (msg *Message) QueryID() (string, bool) {
	return string(msg.header.queryID), msg.header.queryID != nil
}

func (msg *Message) Reason() (string, bool) {
	return string(msg.header.reason), msg.header.reason != nil
}

func (msg *Message) RecordsDeleted() (uint, bool) {
	if msg.header.recordsDeleted != nil {
		return *msg.header.recordsDeleted, true
	}
	return 0, false
}

func (msg *Message) RecordsInserted() (uint, bool) {
	if msg.header.recordsInserted != nil {
		return *msg.header.recordsInserted, true
	}
	return 0, false
}

func (msg *Message) RecordsReturned() (uint, bool) {
	if msg.header.recordsReturned != nil {
		return *msg.header.recordsReturned, true
	}
	return 0, false
}

func (msg *Message) RecordsUpdated() (uint, bool) {
	if msg.header.recordsUpdated != nil {
		return *msg.header.recordsUpdated, true
	}
	return 0, false
}

func (msg *Message) SequenceID() (uint64, bool) {
	if msg.header.sequenceID != nil {
		return *msg.header.sequenceID, true
	}
	return 0, false
}

func (msg *Message) SowKey() (string, bool) {
	return string(msg.header.sowKey), msg.header.sowKey != nil
}

func (msg *Message) SowKeys() (string, bool) {
	return string(msg.header.sowKeys), msg.header.sowKeys != nil
}

func (msg *Message) Status() (string, bool) {
	return string(msg.header.status), msg.header.status != nil
}

func (msg *Message) SubID() (string, bool) { return string(msg.header.subID), msg.header.subID != nil }

func (msg *Message) SubIDs() (string, bool) {
	return string(msg.header.subIDs), msg.header.subIDs != nil
}

func (msg *Message) Timestamp() (string, bool) {
	return string(msg.header.timestamp), msg.header.timestamp != nil
}

func (msg *Message) TopN() (uint, bool) {
	if msg.header.topN != nil {
		return *msg.header.topN, true
	}
	return 0, false
}

func (msg *Message) Topic() (string, bool) { return string(msg.header.topic), msg.header.topic != nil }

func (msg *Message) TopicMatches() (uint, bool) {
	if msg.header.topicMatches != nil {
		return *msg.header.topicMatches, true
	}
	return 0, false
}

func (msg *Message) UserID() (string, bool) {
	return string(msg.header.userID), msg.header.userID != nil
}
