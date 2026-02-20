package amps

import "errors"

type NvfixMessageBuilder struct {
	message        []byte
	fieldSeparator byte
	size           int
	capacity       int
}

func (nmb *NvfixMessageBuilder) checkCapacity(bytesNeeded int) {

	if nmb.capacity-nmb.size < bytesNeeded {
		for nmb.capacity-nmb.size < bytesNeeded {
			nmb.capacity *= 2
		}
		newBuff := make([]byte, 0, nmb.capacity)
		copy(newBuff, nmb.message)
		nmb.message = newBuff
	}
}

func (nmb *NvfixMessageBuilder) Clear() {
	nmb.message = make([]byte, 0)
	nmb.size = 0
}

func (nmb *NvfixMessageBuilder) Size() int {
	return nmb.size
}

func (nmb *NvfixMessageBuilder) Bytes() []byte {
	return nmb.message
}

func (nmb *NvfixMessageBuilder) Data() string {
	return string(nmb.message)
}

func (nmb *NvfixMessageBuilder) AppendBytes(tag []byte, value []byte, valOffset int, valLength int) error {
	if len(tag) == 0 {
		return errors.New("Illegal argument: no tag value provided to NVFIX builder")
	}

	sizeNeeded := len(tag) + 1 + valLength + 2
	nmb.checkCapacity(sizeNeeded)
	nmb.size += sizeNeeded

	nmb.message = append(nmb.message, tag...)
	nmb.message = append(nmb.message, '=')
	nmb.message = append(nmb.message, value[valOffset:valOffset+valLength]...)
	nmb.message = append(nmb.message, nmb.fieldSeparator)

	return nil
}

func (nmb *NvfixMessageBuilder) AppendStrings(tag string, value string) error {
	return nmb.AppendBytes([]byte(tag), []byte(value), 0, len([]byte(value)))
}

func NewNVFIXBuilder(fieldSep ...byte) *NvfixMessageBuilder {
	var _fieldSep byte

	if len(fieldSep) > 0 {
		_fieldSep = fieldSep[0]
	} else {
		_fieldSep = '\x01'
	}

	return &NvfixMessageBuilder{make([]byte, 0, 1024), _fieldSep, 0, 1024}
}
