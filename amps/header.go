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
	// Bulk zero: compiles to a single runtime.memclr rather than 25+ individual
	// store instructions, measurably faster on the message-receive hot path.
	// CommandUnknown is not zero (it is the last iota in its block), so restore
	// it explicitly after the zero — one store vs 25+ is still a significant win.
	*header = _Header{}
	header.command = CommandUnknown
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
		if bytesEqualString(key, "top_n") {
			if topN, ok := parseUint32Value(value); ok {
				header.topNValue = topN
				header.topN = &header.topNValue
			}
		}
	case 6:
		switch {
		case bytesEqualString(key, "reason"):
			header.reason = value
		case bytesEqualString(key, "sub_id"):
			header.subID = value
		case bytesEqualString(key, "status"):
			header.status = value
		case bytesEqualString(key, "filter"):
			header.filter = value
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
			if bytesEqualString(key, "client_name") {
				header.clientName = value
			}
		case 'q':
			if bytesEqualString(key, "query_id") {
				header.queryID = value
			}
		case 'r':
			if records, ok := parseUint32Value(value); ok {
				switch {
				case bytesEqualString(key, "records_deleted"):
					header.recordsDeletedValue = records
					header.recordsDeleted = &header.recordsDeletedValue
				case bytesEqualString(key, "records_inserted"):
					header.recordsInsertedValue = records
					header.recordsInserted = &header.recordsInsertedValue
				case bytesEqualString(key, "records_returned"):
					header.recordsReturnedValue = records
					header.recordsReturned = &header.recordsReturnedValue
				case bytesEqualString(key, "records_updated"):
					header.recordsUpdatedValue = records
					header.recordsUpdated = &header.recordsUpdatedValue
				}
			}
		case 's':
			if bytesEqualString(key, "sow_keys") {
				header.sowKeys = value
			}
		case 't':
			if bytesEqualString(key, "topic_matches") {
				if topicMatches, ok := parseUint32Value(value); ok {
					header.topicMatchesValue = topicMatches
					header.topicMatches = &header.topicMatchesValue
				}
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
	if ackType == AckTypeNone {
		return ""
	}

	var encoded [64]byte
	result := encoded[:0]
	appendToken := func(token string) {
		if len(result) > 0 {
			result = append(result, ',')
		}
		result = append(result, token...)
	}

	if ackType&AckTypeReceived > 0 {
		appendToken("received")
	}
	if ackType&AckTypeParsed > 0 {
		appendToken("parsed")
	}
	if ackType&AckTypeProcessed > 0 {
		appendToken("processed")
	}
	if ackType&AckTypePersisted > 0 {
		appendToken("persisted")
	}
	if ackType&AckTypeCompleted > 0 {
		appendToken("completed")
	}
	if ackType&AckTypeStats > 0 {
		appendToken("stats")
	}

	return string(result)
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
	_ = buffer.WriteByte('{')

	writeStringField := func(key string, value []byte) {
		// Avoid `"` + key + `":"` string concat (allocates a temp string per call).
		_ = buffer.WriteByte('"')
		_, _ = buffer.WriteString(key)
		_, _ = buffer.WriteString(`":"`)
		_, _ = buffer.Write(value)
		_, _ = buffer.WriteString(closeStringValue)
	}
	writeNumberField := func(key string, value uint64) {
		_ = buffer.WriteByte('"')
		_, _ = buffer.WriteString(key)
		_, _ = buffer.WriteString(`":`)
		_ = writeUintToBuffer(buffer, value)
		_, _ = buffer.WriteString(closeNumberValue)
	}

	if header.command >= 0 {
		_, _ = buffer.WriteString(`"c":"`)
		_, _ = buffer.WriteString(commandIntToString(header.command))
		_, _ = buffer.WriteString(closeStringValue)
	}
	if header.commandID != nil {
		writeStringField("cid", header.commandID)
	}
	if header.topic != nil {
		writeStringField("t", header.topic)
	}
	if header.batchSize != nil {
		writeNumberField("bs", uint64(*header.batchSize))
	}
	if header.bookmark != nil {
		writeStringField("bm", header.bookmark)
	}
	if header.correlationID != nil {
		writeStringField("x", header.correlationID)
	}
	if header.expiration != nil {
		writeNumberField("e", uint64(*header.expiration))
	}
	if header.filter != nil {
		writeStringField("filter", header.filter)
	}
	if header.options != nil {
		writeStringField("opts", header.options)
	}
	if header.ackType != nil {
		ack := ackToString(*header.ackType)
		if ack != "" {
			_, _ = buffer.WriteString(`"a":"`)
			_, _ = buffer.WriteString(ack)
			_, _ = buffer.WriteString(closeStringValue)
		}
	}
	if header.clientName != nil {
		writeStringField("client_name", header.clientName)
	}
	if header.userID != nil {
		writeStringField("user_id", header.userID)
	}
	if header.password != nil {
		writeStringField("pw", header.password)
	}
	if header.orderBy != nil {
		writeStringField("orderby", header.orderBy)
	}
	if header.queryID != nil {
		writeStringField("query_id", header.queryID)
	}
	if header.sequenceID != nil {
		writeNumberField("s", *header.sequenceID)
	}
	if header.sowKey != nil {
		writeStringField("k", header.sowKey)
	}
	if header.sowKeys != nil {
		writeStringField("sow_keys", header.sowKeys)
	}
	if header.subID != nil {
		writeStringField("sub_id", header.subID)
	}
	if header.subIDs != nil {
		writeStringField("sids", header.subIDs)
	}
	if header.topN != nil {
		writeNumberField("top_n", uint64(*header.topN))
	}
	if header.version != nil {
		writeStringField("version", header.version)
		if header.messageType != nil {
			writeStringField("mt", header.messageType)
		}
	}

	if buffer.Len() == 1 {
		_ = buffer.WriteByte('}')
		return nil
	}
	buffer.Bytes()[buffer.Len()-1] = '}'
	return nil
}
