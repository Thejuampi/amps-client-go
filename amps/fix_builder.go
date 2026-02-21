package amps

import "errors"
import "strconv"

// FixMessageBuilder builds protocol payload data for publish and helper APIs.
type FixMessageBuilder struct {
	message        []byte
	fieldSeparator byte
	size           int
	capacity       int
}

func (fmb *FixMessageBuilder) checkIfLog10(tag int) int {
	scalar := uint64(10)

	for i := 1; i < 20; i++ {
		if uint64(tag) < scalar {
			return i
		}
		scalar *= 10
	}

	return 0
}

func (fmb *FixMessageBuilder) checkCapacity(bytesNeeded int) {

	if fmb.capacity-fmb.size < bytesNeeded {
		for fmb.capacity-fmb.size < bytesNeeded {
			fmb.capacity *= 2
		}
		// Preserve existing payload bytes while growing backing capacity.
		newBuff := make([]byte, 0, fmb.capacity)
		newBuff = append(newBuff, fmb.message...)
		fmb.message = newBuff
	}
}

// Clear executes the exported clear operation.
func (fmb *FixMessageBuilder) Clear() {
	fmb.message = make([]byte, 0)
	fmb.size = 0
}

// Size executes the exported size operation.
func (fmb *FixMessageBuilder) Size() int {
	return fmb.size
}

// Bytes executes the exported bytes operation.
func (fmb *FixMessageBuilder) Bytes() []byte {
	return fmb.message
}

// Data executes the exported data operation.
func (fmb *FixMessageBuilder) Data() string {
	return string(fmb.message)
}

// AppendBytes executes the exported appendbytes operation.
func (fmb *FixMessageBuilder) AppendBytes(tag int, value []byte, offset int, length int) error {
	if tag < 0 {
		return errors.New("Illegal argument: negative tag value used in FIX builder")
	}
	if offset < 0 || length < 0 || offset > len(value) || offset+length > len(value) {
		return errors.New("Illegal argument: invalid FIX value range")
	}

	tagValue := []byte(strconv.Itoa(tag))
	tagSize := len(tagValue)

	sizeNeeded := tagSize + length + 2
	fmb.checkCapacity(sizeNeeded)
	fmb.size += sizeNeeded

	fmb.message = append(fmb.message, tagValue...)
	fmb.message = append(fmb.message, '=')
	fmb.message = append(fmb.message, value[offset:offset+length]...)
	fmb.message = append(fmb.message, fmb.fieldSeparator)

	return nil
}

// Append executes the exported append operation.
func (fmb *FixMessageBuilder) Append(tag int, value string) error {
	return fmb.AppendBytes(tag, []byte(value), 0, len(value))
}

// NewFIXBuilder returns a new FIXBuilder.
func NewFIXBuilder(fieldSep ...byte) *FixMessageBuilder {
	var _fieldSep byte

	if len(fieldSep) > 0 {
		_fieldSep = fieldSep[0]
	} else {
		_fieldSep = '\x01'
	}

	return &FixMessageBuilder{make([]byte, 0, 1024), _fieldSep, 0, 1024}
}
