package amps


import "errors"
import "strconv"


// FixMessageBuilder struct
type FixMessageBuilder struct {
	message []byte
	fieldSeparator byte
	size int
	capacity int
}


// Internal API
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
	// Need to resize
	if fmb.capacity - fmb.size < bytesNeeded {
		for fmb.capacity - fmb.size < bytesNeeded { fmb.capacity *= 2 }
		newBuff := make([]byte, 0, fmb.capacity)
		copy(newBuff, fmb.message)
		fmb.message = newBuff
	}
}


// Public API

// Clear clears the FIX message
func (fmb *FixMessageBuilder) Clear() {
	fmb.message = make([]byte, 0)
	fmb.size = 0
}


// Size returns the size of the FIX message
func (fmb *FixMessageBuilder) Size() int {
	return fmb.size
}


// Bytes returns the FIX message, in the form of a byte buffer
func (fmb *FixMessageBuilder) Bytes() []byte {
	return fmb.message
}


// Data returns the FIX message, in the form of a string
func (fmb *FixMessageBuilder) Data() string {
	return string(fmb.message)
}


// AppendBytes appends the given tag (integer) and value (byte array) to the FIX message.
//
// Example:
// value := "This is the value"
// builder := amps.NewFIXMessageBuilder()
// builder.AppendBytes(1, []byte(value), 0, len(value))
//
// client.Publish("test-topic", builder.Data())
//
// Arguments:
// tag [int]  -  The integer to be the key in the FIX message
// value [byte buffer]  -  The bytes to be the value in the FIX message
// offset [int]  -  Location in the buffer where the bytes to append begin
// length [int]  -  Length of the value in the buffer
//
// Returns:
// Illegal argument error  -  If the provided tag is negative
func (fmb *FixMessageBuilder) AppendBytes(tag int, value []byte, offset int, length int) error {
	if tag < 0 { return errors.New("Illegal argument: negative tag value used in FIX builder") }

	tagValue := []byte(strconv.Itoa(tag))
	tagSize := len(tagValue)

	sizeNeeded := tagSize + length + 2
	fmb.checkCapacity(sizeNeeded)
	fmb.size += sizeNeeded

	fmb.message = append(fmb.message, tagValue...)
	fmb.message = append(fmb.message, '=')
	fmb.message = append(fmb.message, value[offset:offset + length]...)
	fmb.message = append(fmb.message, fmb.fieldSeparator)

	return nil
}


// Append appends teh given tag (int) and the value (string) to the FIX message.
//
// Example:
// value := "This is a value"
// builder := amps.NewFIXBuilder()
// builder.Append(1, value)
//
// client.Publish("fix-topic", builder.Data())
//
// Arguments:
// tag [int]  -  The int to be the key in the FIX message
// value [string]  -  The string to be the value in the FIX message
//
// Returns:
// Illegal argument error  -  If the provided tag is negative
func (fmb *FixMessageBuilder) Append(tag int, value string) error {
	return fmb.AppendBytes(tag, []byte(value), 0, len(value))
}


// NewFIXBuilder use this method to create a new FIX message.
//
// Example:
// builder := amps.NewFIXBuilder()
//
// Arguments:
// fieldSep [byte] (optional)  -  The default is \x01, a different one can be provided
func NewFIXBuilder(fieldSep ...byte) *FixMessageBuilder {
	var _fieldSep byte

	if len(fieldSep) > 0 {
		_fieldSep = fieldSep[0]
	} else {
		_fieldSep = '\x01'
	}

	return &FixMessageBuilder{make([]byte, 0, 1024), _fieldSep, 0, 1024}
}

