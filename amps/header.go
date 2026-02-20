package amps

import (
	"bytes"
	"fmt"
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

func parseToUintPointer(value []byte) (*uint, error) {
	result, err := strconv.ParseUint(string(value), 10, 32)
	final := uint(result)
	if err != nil {
		return nil, err
	}
	return &final, nil
}

func parseToUint64Pointer(value []byte) (*uint64, error) {
	result, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (header *_Header) parseField(key []byte, value []byte) {
	switch len(key) {
	case 0:
		return
	case 1:
		switch key[0] {
		case 'a':
			if ack := stringToAck(string(value)); ack >= 0 {
				header.ackType = &ack
			}
		case 'c':
			header.command = commandStringToInt(string(value))
		case 'e':
			if expiration, err := parseToUintPointer(value); err == nil {
				header.expiration = expiration
			}
		case 'f':
			header.filter = value
		case 'o':
			header.options = value
		case 'k':
			header.sowKey = value
		case 'l':
			if messageLength, err := parseToUintPointer(value); err == nil {
				header.messageLength = messageLength
			}
		case 's':
			if sequenceID, err := parseToUint64Pointer(value); err == nil {
				header.sequenceID = sequenceID
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
				if batchSize, err := parseToUintPointer(value); err == nil {
					header.batchSize = batchSize
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
			if groupSequenceNumber, err := parseToUintPointer(value); err == nil {
				header.groupSequenceNumber = groupSequenceNumber
			}
		case 'o':
			header.options = value
		case 's':
			header.subIDs = value
		}
	case 5:
		if topN, err := parseToUintPointer(value); err == nil {
			header.topN = topN
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
			if matches, err := parseToUintPointer(value); err == nil {
				header.matches = matches
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
			if records, err := parseToUintPointer(value); err == nil {
				switch key[8] {
				case 'd':
					header.recordsDeleted = records
				case 'i':
					header.recordsInserted = records
				case 'r':
					header.recordsReturned = records
				case 'u':
					header.recordsUpdated = records
				}
			}
		case 's':
			header.sowKeys = value
		case 't':
			if topicMatches, err := parseToUintPointer(value); err == nil {
				header.topicMatches = topicMatches
			}
		}
	}
}

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
		_, err = buffer.WriteString(fmt.Sprint(*header.batchSize))
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
		_, err = buffer.WriteString(fmt.Sprint(*header.expiration))
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
		_, err = buffer.WriteString(fmt.Sprint(*header.sequenceID))
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
		_, err = buffer.WriteString(fmt.Sprint(*header.topN))
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
