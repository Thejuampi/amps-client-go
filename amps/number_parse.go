package amps

const (
	maxUint64Div10 = ^uint64(0) / 10
	maxUint64Mod10 = ^uint64(0) % 10
)

func parseUintBytes(value []byte) (uint64, bool) {
	if len(value) == 0 {
		return 0, false
	}
	var result uint64
	for index := 0; index < len(value); index++ {
		digit := value[index]
		if digit < '0' || digit > '9' {
			return 0, false
		}
		digitValue := uint64(digit - '0')
		if result > maxUint64Div10 || (result == maxUint64Div10 && digitValue > maxUint64Mod10) {
			return 0, false
		}
		result = (result * 10) + digitValue
	}
	return result, true
}

func parseUint32Value(value []byte) (uint, bool) {
	return parseUintValueWithMax(value, (1<<32)-1)
}

func parseUintValue(value []byte) (uint, bool) {
	return parseUintValueWithMax(value, uint64(^uint(0)))
}

func parseUintValueWithMax(value []byte, max uint64) (uint, bool) {
	result, ok := parseUintBytes(value)
	if !ok || result > max {
		return 0, false
	}
	return uint(result), true
}

func parseUint64Value(value []byte) (uint64, bool) {
	parsed, ok := parseUintBytes(value)
	if !ok {
		return 0, false
	}
	return parsed, true
}

func (header *_Header) setMessageLength(value uint) {
	header.messageLengthValue = value
	header.messageLength = &header.messageLengthValue
}

func (header *_Header) parseMessageLengthField(value []byte) {
	if messageLength, ok := parseUint32Value(value); ok {
		header.setMessageLength(messageLength)
	}
}

func (header *_Header) parseSequenceIDField(value []byte) {
	if sequenceID, ok := parseUint64Value(value); ok {
		header.sequenceIDValue = sequenceID
		header.sequenceID = &header.sequenceIDValue
	}
}
