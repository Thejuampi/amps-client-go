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

	dataOnly        []byte
	sendEmpty       []byte
	sendKeys        []byte
	sendOOF         []byte
	skipN           *uint
	maximumMessages *uint
	timeoutInterval *uint
	gracePeriod     *uint

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

	strictParityEscapeState uint8
}

func newHeader() *_Header {
	var header = new(_Header)
	header.command = CommandUnknown
	return header
}

func (header *_Header) reset() {
	// Bulk zero: compiles to a single runtime.memclr rather than 25+ individual
	// store instructions, measurably faster on the message-receive hot path.
	// CommandUnknown is not zero (it is the last iota in its block), so restore
	// it explicitly after the zero — one store vs 25+ is still a significant win.
	*header = _Header{}
	header.command = CommandUnknown
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
			header.parseMessageLengthField(value)
		case 's':
			header.parseSequenceIDField(value)
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
		case 'm':
			header.messageType = value
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
		case bytesEqualString(key, "skip_n"):
			if parsed, ok := parseUintValue(value); ok {
				v := parsed
				header.skipN = &v
			}
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
			switch {
			case bytesEqualString(key, "sow_keys"):
				header.sowKeys = value
			case bytesEqualString(key, "send_empty"):
				header.sendEmpty = value
			case bytesEqualString(key, "send_keys"):
				header.sendKeys = value
			case bytesEqualString(key, "send_oof"):
				header.sendOOF = value
			}
		case 't':
			switch {
			case bytesEqualString(key, "topic_matches"):
				if topicMatches, ok := parseUint32Value(value); ok {
					header.topicMatchesValue = topicMatches
					header.topicMatches = &header.topicMatchesValue
				}
			case bytesEqualString(key, "timeout_interval"):
				if parsed, ok := parseUintValue(value); ok {
					v := parsed
					header.timeoutInterval = &v
				}
			}
		case 'd':
			if bytesEqualString(key, "data_only") {
				header.dataOnly = value
			}
		case 'g':
			switch {
			case bytesEqualString(key, "group_sequence_number"), bytesEqualString(key, "gseq"):
				if parsed, ok := parseUintValue(value); ok {
					header.groupSequenceNumberValue = parsed
					header.groupSequenceNumber = &header.groupSequenceNumberValue
				}
			case bytesEqualString(key, "grace_period"):
				if parsed, ok := parseUintValue(value); ok {
					v := parsed
					header.gracePeriod = &v
				}
			}
		case 'l':
			if bytesEqualString(key, "lease_period") {
				header.leasePeriod = value
			}
		case 'm':
			switch {
			case bytesEqualString(key, "maximum_messages"), bytesEqualString(key, "max_msgs"):
				if parsed, ok := parseUintValue(value); ok {
					v := parsed
					header.maximumMessages = &v
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

var ackTypeCache [64]string

func init() {
	var index int
	for index = 0; index < len(ackTypeCache); index++ {
		ackTypeCache[index] = ackToStringSlow(index)
	}
}

func ackToString(ackType int) string {
	if ackType >= 0 && ackType < len(ackTypeCache) {
		return ackTypeCache[ackType]
	}

	return ackToStringSlow(ackType)
}

func ackToStringSlow(ackType int) string {
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

func bytesNeedJSONEscape(value []byte) bool {
	for index := 0; index < len(value); index++ {
		ch := value[index]
		if ch == '"' || ch == '\\' || ch < 0x20 {
			return true
		}
	}
	return false
}

func strictParityFieldsNeedJSONEscape(commandID []byte, topic []byte, filter []byte, options []byte, queryID []byte, subID []byte) bool {
	return bytesNeedJSONEscape(commandID) ||
		bytesNeedJSONEscape(topic) ||
		bytesNeedJSONEscape(filter) ||
		bytesNeedJSONEscape(options) ||
		bytesNeedJSONEscape(queryID) ||
		bytesNeedJSONEscape(subID)
}

var jsonControlEscapes = [32]string{
	8:  `\b`,
	9:  `\t`,
	10: `\n`,
	12: `\f`,
	13: `\r`,
}

func writeQuotedBytesToBuffer(buffer *bytes.Buffer, value []byte) {
	if !bytesNeedJSONEscape(value) {
		_ = buffer.WriteByte('"')
		_, _ = buffer.Write(value)
		_ = buffer.WriteByte('"')
		return
	}
	_ = buffer.WriteByte('"')
	for _, ch := range value {
		if ch == '"' || ch == '\\' {
			_ = buffer.WriteByte('\\')
			_ = buffer.WriteByte(ch)
			continue
		}
		if ch >= 0x20 {
			_ = buffer.WriteByte(ch)
			continue
		}
		if escape := jsonControlEscapes[ch]; escape != "" {
			_, _ = buffer.WriteString(escape)
			continue
		}
		_, _ = buffer.WriteString(`\u00`)
		_ = buffer.WriteByte("0123456789abcdef"[ch>>4])
		_ = buffer.WriteByte("0123456789abcdef"[ch&0x0f])
	}
	_ = buffer.WriteByte('"')
}

func (header *_Header) writeStrictParityEscapedFastPath(buffer *bytes.Buffer) {
	_, _ = buffer.WriteString(`{"c":"p","cid":`)
	writeQuotedBytesToBuffer(buffer, header.commandID)
	_, _ = buffer.WriteString(`,"t":`)
	writeQuotedBytesToBuffer(buffer, header.topic)
	_, _ = buffer.WriteString(`,"e":`)
	_ = writeUintToBuffer(buffer, uint64(*header.expiration))
	_, _ = buffer.WriteString(`,"filter":`)
	writeQuotedBytesToBuffer(buffer, header.filter)
	_, _ = buffer.WriteString(`,"opts":`)
	writeQuotedBytesToBuffer(buffer, header.options)
	_, _ = buffer.WriteString(`,"query_id":`)
	writeQuotedBytesToBuffer(buffer, header.queryID)
	_, _ = buffer.WriteString(`,"s":`)
	_ = writeUintToBuffer(buffer, *header.sequenceID)
	_, _ = buffer.WriteString(`,"sub_id":`)
	writeQuotedBytesToBuffer(buffer, header.subID)
	_, _ = buffer.WriteString(`}`)
}

func (header *_Header) writeStrictParityFastPath(buffer *bytes.Buffer) bool {
	if header == nil {
		return false
	}

	if header.command != CommandPublish || header.commandID == nil || header.topic == nil || header.subID == nil || header.queryID == nil || header.options == nil || header.filter == nil || header.expiration == nil || header.sequenceID == nil {
		return false
	}

	if header.batchSize != nil || header.bookmark != nil || header.correlationID != nil || header.ackType != nil || header.clientName != nil || header.userID != nil || header.password != nil || header.orderBy != nil || header.sowKey != nil || header.sowKeys != nil || header.subIDs != nil || header.topN != nil || header.version != nil || header.messageType != nil {
		return false
	}

	if header.strictParityEscapeState == 0 {
		header.strictParityEscapeState = 2
		if !strictParityFieldsNeedJSONEscape(header.commandID, header.topic, header.filter, header.options, header.queryID, header.subID) {
			header.strictParityEscapeState = 1
		}
	}
	if header.strictParityEscapeState != 1 {
		header.writeStrictParityEscapedFastPath(buffer)
		return true
	}
	_, _ = buffer.WriteString(`{"c":"p","cid":"`)
	_, _ = buffer.Write(header.commandID)
	_, _ = buffer.WriteString(`","t":"`)
	_, _ = buffer.Write(header.topic)
	_, _ = buffer.WriteString(`","e":`)
	_ = writeUintToBuffer(buffer, uint64(*header.expiration))
	_, _ = buffer.WriteString(`,"filter":"`)
	_, _ = buffer.Write(header.filter)
	_, _ = buffer.WriteString(`","opts":"`)
	_, _ = buffer.Write(header.options)
	_, _ = buffer.WriteString(`","query_id":"`)
	_, _ = buffer.Write(header.queryID)
	_, _ = buffer.WriteString(`","s":`)
	_ = writeUintToBuffer(buffer, *header.sequenceID)
	_, _ = buffer.WriteString(`,"sub_id":"`)
	_, _ = buffer.Write(header.subID)
	_, _ = buffer.WriteString(`"}`)
	return true
}

func (header *_Header) writeSimplePublishEscapedFastPath(buffer *bytes.Buffer) {
	_, _ = buffer.WriteString(`{"c":"p","cid":`)
	writeQuotedBytesToBuffer(buffer, header.commandID)
	_, _ = buffer.WriteString(`,"t":`)
	writeQuotedBytesToBuffer(buffer, header.topic)
	_, _ = buffer.WriteString(`}`)
}

func (header *_Header) writeSimplePublishFastPath(buffer *bytes.Buffer) bool {
	if header == nil {
		return false
	}
	if header.command != CommandPublish || header.commandID == nil || header.topic == nil {
		return false
	}
	if header.ackType != nil || header.bookmark != nil || header.batchSize != nil || header.correlationID != nil || header.clientName != nil || header.expiration != nil || header.filter != nil || header.groupSequenceNumber != nil || header.sowKey != nil || header.messageLength != nil || header.messageType != nil || header.leasePeriod != nil || header.matches != nil || header.options != nil || header.orderBy != nil || header.queryID != nil || header.reason != nil || header.recordsDeleted != nil || header.recordsInserted != nil || header.recordsReturned != nil || header.recordsUpdated != nil || header.sequenceID != nil || header.subIDs != nil || header.sowKeys != nil || header.status != nil || header.subID != nil || header.topN != nil || header.topicMatches != nil || header.timestamp != nil || header.userID != nil || header.password != nil || header.version != nil || header.dataOnly != nil || header.sendEmpty != nil || header.sendKeys != nil || header.sendOOF != nil || header.skipN != nil || header.maximumMessages != nil || header.timeoutInterval != nil || header.gracePeriod != nil {
		return false
	}

	if header.strictParityEscapeState == 0 {
		header.strictParityEscapeState = 2
		if !strictParityFieldsNeedJSONEscape(header.commandID, header.topic, nil, nil, nil, nil) {
			header.strictParityEscapeState = 1
		}
	}
	if header.strictParityEscapeState != 1 {
		header.writeSimplePublishEscapedFastPath(buffer)
		return true
	}
	_, _ = buffer.WriteString(`{"c":"p","cid":"`)
	_, _ = buffer.Write(header.commandID)
	_, _ = buffer.WriteString(`","t":"`)
	_, _ = buffer.Write(header.topic)
	_, _ = buffer.WriteString(`"}`)
	return true
}

func (header *_Header) write(buffer *bytes.Buffer) (err error) {
	if header.writeStrictParityFastPath(buffer) {
		return nil
	}
	if header.writeSimplePublishFastPath(buffer) {
		return nil
	}

	_ = buffer.WriteByte('{')

	writeStringField := func(key string, value []byte) {
		// Avoid `"` + key + `":"` string concat (allocates a temp string per call).
		_ = buffer.WriteByte('"')
		_, _ = buffer.WriteString(key)
		_, _ = buffer.WriteString(`":`)
		writeQuotedBytesToBuffer(buffer, value)
		_, _ = buffer.WriteString(closeNumberValue)
	}
	writeNumberField := func(key string, value uint64) {
		_ = buffer.WriteByte('"')
		_, _ = buffer.WriteString(key)
		_, _ = buffer.WriteString(`":`)
		_ = writeUintToBuffer(buffer, value)
		_, _ = buffer.WriteString(closeNumberValue)
	}

	command := commandIntToString(header.command)
	if command != "" {
		_, _ = buffer.WriteString(`"c":"`)
		_, _ = buffer.WriteString(command)
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
	}
	if header.messageType != nil {
		writeStringField("mt", header.messageType)
	}
	if header.dataOnly != nil {
		writeStringField("data_only", header.dataOnly)
	}
	if header.sendEmpty != nil {
		writeStringField("send_empty", header.sendEmpty)
	}
	if header.sendKeys != nil {
		writeStringField("send_keys", header.sendKeys)
	}
	if header.sendOOF != nil {
		writeStringField("send_oof", header.sendOOF)
	}
	if header.leasePeriod != nil {
		writeStringField("lease_period", header.leasePeriod)
	}
	if header.groupSequenceNumber != nil {
		writeNumberField("group_sequence_number", uint64(*header.groupSequenceNumber))
	}
	if header.skipN != nil {
		writeNumberField("skip_n", uint64(*header.skipN))
	}
	if header.maximumMessages != nil {
		writeNumberField("maximum_messages", uint64(*header.maximumMessages))
	}
	if header.timeoutInterval != nil {
		writeNumberField("timeout_interval", uint64(*header.timeoutInterval))
	}
	if header.gracePeriod != nil {
		writeNumberField("grace_period", uint64(*header.gracePeriod))
	}

	if buffer.Len() == 1 {
		_ = buffer.WriteByte('}')
		return nil
	}
	buffer.Bytes()[buffer.Len()-1] = '}'
	return nil
}
