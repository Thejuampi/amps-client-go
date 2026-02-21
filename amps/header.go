package amps

import (
	"bytes"
	"strconv"
	"strings"
)

type _Header struct {
	ackType             *int
	bookmark            []byte
	batchSize           *uint
	command             int
	commandID           []byte
	clientName          []byte
	expiration          *uint
	filter              []byte
	groupSequenceNumber *uint
	sowKey              []byte
	messageLength       *uint
	messageType         []byte
	leasePeriod         []byte
	matches             *uint
	options             []byte
	orderBy             []byte
	queryID             []byte
	reason              []byte
	recordsDeleted      *uint
	recordsInserted     *uint
	recordsReturned     *uint
	recordsUpdated      *uint
	sequenceID          *uint64
	subIDs              []byte
	sowKeys             []byte
	status              []byte
	subID               []byte
	topic               []byte
	topN                *uint
	topicMatches        *uint
	timestamp           []byte
	userID              []byte
	password            []byte
	version             []byte
	correlationID       []byte

	ackTypeValue             int
	batchSizeValue           uint
	expirationValue          uint
	groupSequenceNumberValue uint
	messageLengthValue       uint
	matchesValue             uint
	recordsDeletedValue      uint
	recordsInsertedValue     uint
	recordsReturnedValue     uint
	recordsUpdatedValue      uint
	sequenceIDValue          uint64
	topNValue                uint
	topicMatchesValue        uint
}

func (header *_Header) reset() {
	header.ackType = nil
	header.bookmark = nil
	header.batchSize = nil
	header.command = CommandUnknown
	header.commandID = nil
	header.clientName = nil
	header.expiration = nil
	header.filter = nil
	header.groupSequenceNumber = nil
	header.sowKey = nil
	header.messageLength = nil
	header.leasePeriod = nil
	header.matches = nil
	header.options = nil
	header.orderBy = nil
	header.queryID = nil
	header.reason = nil
	header.recordsDeleted = nil
	header.recordsInserted = nil
	header.recordsReturned = nil
	header.recordsUpdated = nil
	header.sequenceID = nil
	header.subIDs = nil
	header.sowKeys = nil
	header.status = nil
	header.subID = nil
	header.topic = nil
	header.topN = nil
	header.topicMatches = nil
	header.timestamp = nil
	header.userID = nil
	header.password = nil
	header.version = nil
	header.correlationID = nil
}

func parseUintBytes(value []byte) (uint64, bool) {
	if len(value) == 0 {
		return 0, false
	}
	var result uint64
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return 0, false
		}
		if result > (^uint64(0)-uint64(digit-'0'))/10 {
			return 0, false
		}
		result = (result * 10) + uint64(digit-'0')
	}
	return result, true
}

func parseUint32Value(value []byte) (uint, bool) {
	parsed, ok := parseUintBytes(value)
	if !ok || parsed > (1<<32)-1 {
		return 0, false
	}
	return uint(parsed), true
}

func parseUint64Value(value []byte) (uint64, bool) {
	parsed, ok := parseUintBytes(value)
	if !ok {
		return 0, false
	}
	return parsed, true
}

func parseAckBytes(ackType []byte) int {
	var ack int
	start := 0
	for start <= len(ackType) {
		end := start
		for end < len(ackType) && ackType[end] != ',' {
			end++
		}
		token := ackType[start:end]
		switch len(token) {
		case 5:
			if bytesEqualString(token, "stats") {
				ack |= AckTypeStats
			}
		case 6:
			if bytesEqualString(token, "parsed") {
				ack |= AckTypeParsed
			}
		case 8:
			if bytesEqualString(token, "received") {
				ack |= AckTypeReceived
			}
		case 9:
			if bytesEqualString(token, "persisted") {
				ack |= AckTypePersisted
			}
			if bytesEqualString(token, "completed") {
				ack |= AckTypeCompleted
			}
			if bytesEqualString(token, "processed") {
				ack |= AckTypeProcessed
			}
		}

		if end == len(ackType) {
			break
		}
		start = end + 1
	}
	return ack
}

func (header *_Header) parseField(key []byte, value []byte) {
	switch len(key) {
	case 0:
		return
	case 1:
		switch key[0] {
		case 'a':
			if ack := parseAckBytes(value); ack >= 0 {
				header.ackTypeValue = ack
				header.ackType = &header.ackTypeValue
			}
		case 'c':
			header.command = commandBytesToInt(value)
		case 'e':
			if expiration, ok := parseUint32Value(value); ok {
				header.expirationValue = expiration
				header.expiration = &header.expirationValue
			}
		case 'f':
			header.filter = value
		case 'o':
			header.options = value
		case 'k':
			header.sowKey = value
		case 'l':
			if messageLength, ok := parseUint32Value(value); ok {
				header.messageLengthValue = messageLength
				header.messageLength = &header.messageLengthValue
			}
		case 's':
			if sequenceID, ok := parseUint64Value(value); ok {
				header.sequenceIDValue = sequenceID
				header.sequenceID = &header.sequenceIDValue
			}
		case 't':
			header.topic = value
		case 'x':
			header.correlationID = value
		}
	case 2:
		switch key[0] {
		case 'b':
			switch key[1] {
			case 'm':
				header.bookmark = value
			case 's':
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
	case 3:
		switch key[0] {
		case 'c':
			header.commandID = value
		}
	case 4:
		switch key[0] {
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
	case 5:
		if topN, ok := parseUint32Value(value); ok {
			header.topNValue = topN
			header.topN = &header.topNValue
		}
	case 6:
		switch key[1] {
		case 'e':
			header.reason = value
		case 'u':
			header.subID = value
		case 't':
			header.status = value
		}
	case 7:
		switch key[0] {
		case 'm':
			if matches, ok := parseUint32Value(value); ok {
				header.matchesValue = matches
				header.matches = &header.matchesValue
			}
		case 'o':
			header.orderBy = value
		case 'u':
			header.userID = value
		case 'v':
			header.version = value
		}
	default:
		switch key[0] {
		case 'c':
			header.clientName = value
		case 'q':
			header.queryID = value
		case 'r':
			if records, ok := parseUint32Value(value); ok {
				switch key[8] {
				case 'd':
					header.recordsDeletedValue = records
					header.recordsDeleted = &header.recordsDeletedValue
				case 'i':
					header.recordsInsertedValue = records
					header.recordsInserted = &header.recordsInsertedValue
				case 'r':
					header.recordsReturnedValue = records
					header.recordsReturned = &header.recordsReturnedValue
				case 'u':
					header.recordsUpdatedValue = records
					header.recordsUpdated = &header.recordsUpdatedValue
				}
			}
		case 's':
			header.sowKeys = value
		case 't':
			if topicMatches, ok := parseUint32Value(value); ok {
				header.topicMatchesValue = topicMatches
				header.topicMatches = &header.topicMatchesValue
			}
		}
	}
}

// Constants in this block define protocol and client behavior values.
const (
	closeStringValue = "\","
	closeNumberValue = ","
)

func ackToString(ackType int) string {
	var result []string
	if ackType&AckTypeReceived > 0 {
		result = append(result, "received")
	}
	if ackType&AckTypeParsed > 0 {
		result = append(result, "parsed")
	}
	if ackType&AckTypeProcessed > 0 {
		result = append(result, "processed")
	}
	if ackType&AckTypePersisted > 0 {
		result = append(result, "persisted")
	}
	if ackType&AckTypeCompleted > 0 {
		result = append(result, "completed")
	}
	if ackType&AckTypeStats > 0 {
		result = append(result, "stats")
	}
	return strings.Join(result, ",")
}

func stringToAck(ackType string) int {
	var ack int

	for _, value := range strings.Split(ackType, ",") {
		switch value {
		case "received":
			ack |= AckTypeReceived
		case "parsed":
			ack |= AckTypeParsed
		case "processed":
			ack |= AckTypeProcessed
		case "persisted":
			ack |= AckTypePersisted
		case "completed":
			ack |= AckTypeCompleted
		case "stats":
			ack |= AckTypeStats
		}
	}

	return ack
}

func writeUintToBuffer(buffer *bytes.Buffer, value uint64) error {
	var digits [20]byte
	encoded := strconv.AppendUint(digits[:0], value, 10)
	_, err := buffer.Write(encoded)
	return err
}

func (header *_Header) write(buffer *bytes.Buffer) (err error) {

	err = buffer.WriteByte('{')
	if err != nil {
		return
	}

	if header.command >= 0 {
		_, err = buffer.WriteString("\"c\":\"")
		if err != nil {
			return
		}
		_, err = buffer.WriteString(commandIntToString(header.command))
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeStringValue)
		if err != nil {
			return
		}
	}

	if header.commandID != nil {
		_, err = buffer.WriteString("\"cid\":\"")
		if err != nil {
			return
		}
		_, err = buffer.Write(header.commandID)
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeStringValue)
		if err != nil {
			return
		}
	}

	if header.topic != nil {
		_, err = buffer.WriteString("\"t\":\"")
		if err != nil {
			return
		}
		_, err = buffer.Write(header.topic)
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeStringValue)
		if err != nil {
			return
		}
	}

	if header.batchSize != nil {
		_, err = buffer.WriteString("\"bs\":")
		if err != nil {
			return
		}
		err = writeUintToBuffer(buffer, uint64(*header.batchSize))
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeNumberValue)
		if err != nil {
			return
		}
	}

	if header.bookmark != nil {
		_, err = buffer.WriteString("\"bm\":\"")
		if err != nil {
			return
		}
		_, err = buffer.Write(header.bookmark)
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeStringValue)
		if err != nil {
			return
		}
	}

	if header.correlationID != nil {
		_, err = buffer.WriteString("\"x\":\"")
		if err != nil {
			return
		}
		_, err = buffer.Write(header.correlationID)
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeStringValue)
		if err != nil {
			return
		}
	}

	if header.expiration != nil {
		_, err = buffer.WriteString("\"e\":")
		if err != nil {
			return
		}
		err = writeUintToBuffer(buffer, uint64(*header.expiration))
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeNumberValue)
		if err != nil {
			return
		}
	}

	if header.filter != nil {
		_, err = buffer.WriteString("\"filter\":\"")
		if err != nil {
			return
		}
		_, err = buffer.Write(header.filter)
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeStringValue)
		if err != nil {
			return
		}
	}

	if header.options != nil {
		_, err = buffer.WriteString("\"opts\":\"")
		if err != nil {
			return
		}
		_, err = buffer.Write(header.options)
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeStringValue)
		if err != nil {
			return
		}
	}

	if header.ackType != nil {
		ack := ackToString(*header.ackType)
		if len(ack) > 0 {
			_, err = buffer.WriteString("\"a\":\"")
			if err != nil {
				return
			}
			_, err = buffer.WriteString(ack)
			if err != nil {
				return
			}
			_, err = buffer.WriteString(closeStringValue)
			if err != nil {
				return
			}
		}
	}

	if header.clientName != nil {
		_, err = buffer.WriteString("\"client_name\":\"")
		if err != nil {
			return
		}
		_, err = buffer.Write(header.clientName)
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeStringValue)
		if err != nil {
			return
		}
	}

	if header.userID != nil {
		_, err = buffer.WriteString("\"user_id\":\"")
		if err != nil {
			return
		}
		_, err = buffer.Write(header.userID)
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeStringValue)
		if err != nil {
			return
		}
	}

	if header.password != nil {
		_, err = buffer.WriteString("\"pw\":\"")
		if err != nil {
			return
		}
		_, err = buffer.Write(header.password)
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeStringValue)
		if err != nil {
			return
		}
	}

	if header.orderBy != nil {
		_, err = buffer.WriteString("\"orderby\":\"")
		if err != nil {
			return
		}
		_, err = buffer.Write(header.orderBy)
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeStringValue)
		if err != nil {
			return
		}
	}

	if header.queryID != nil {
		_, err = buffer.WriteString("\"query_id\":\"")
		if err != nil {
			return
		}
		_, err = buffer.Write(header.queryID)
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeStringValue)
		if err != nil {
			return
		}
	}

	if header.sequenceID != nil {
		_, err = buffer.WriteString("\"s\":")
		if err != nil {
			return
		}
		err = writeUintToBuffer(buffer, *header.sequenceID)
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeNumberValue)
		if err != nil {
			return
		}
	}

	if header.sowKey != nil {
		_, err = buffer.WriteString("\"k\":\"")
		if err != nil {
			return
		}
		_, err = buffer.Write(header.sowKey)
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeStringValue)
		if err != nil {
			return
		}
	}

	if header.sowKeys != nil {
		_, err = buffer.WriteString("\"sow_keys\":\"")
		if err != nil {
			return
		}
		_, err = buffer.Write(header.sowKeys)
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeStringValue)
		if err != nil {
			return
		}
	}

	if header.subID != nil {
		_, err = buffer.WriteString("\"sub_id\":\"")
		if err != nil {
			return
		}
		_, err = buffer.Write(header.subID)
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeStringValue)
		if err != nil {
			return
		}
	}

	if header.subIDs != nil {
		_, err = buffer.WriteString("\"sids\":\"")
		if err != nil {
			return
		}
		_, err = buffer.Write(header.subIDs)
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeStringValue)
		if err != nil {
			return
		}
	}

	if header.topN != nil {
		_, err = buffer.WriteString("\"top_n\":")
		if err != nil {
			return
		}
		err = writeUintToBuffer(buffer, uint64(*header.topN))
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeNumberValue)
		if err != nil {
			return
		}
	}

	if header.version != nil {
		_, err = buffer.WriteString("\"version\":\"")
		if err != nil {
			return
		}
		_, err = buffer.Write(header.version)
		if err != nil {
			return
		}
		_, err = buffer.WriteString(closeStringValue)
		if err != nil {
			return
		}

		if header.messageType != nil {
			_, err = buffer.WriteString("\"mt\":\"")
			if err != nil {
				return
			}
			_, err = buffer.Write(header.messageType)
			if err != nil {
				return
			}
			_, err = buffer.WriteString(closeStringValue)
			if err != nil {
				return
			}
		}
	}

	buffer.Bytes()[buffer.Len()-1] = '}'

	return
}
