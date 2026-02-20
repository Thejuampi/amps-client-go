package amps


import "errors"
// import "fmt"


// NvfixMessageBuilder struct
type NvfixMessageBuilder struct {
	message []byte
	fieldSeparator byte
	size int
	capacity int
}


// Internal API
func (nmb *NvfixMessageBuilder) checkCapacity(bytesNeeded int) {
	// Need to resize
	if nmb.capacity - nmb.size < bytesNeeded {
		for nmb.capacity - nmb.size < bytesNeeded { nmb.capacity *= 2 }
		newBuff := make([]byte, 0, nmb.capacity)
		copy(newBuff, nmb.message)
		nmb.message = newBuff
	}
}

// Public API

// Clear clears the NVFIX message
func (nmb *NvfixMessageBuilder) Clear() {
	nmb.message = make([]byte, 0)
	nmb.size = 0
}

// Size returns the size of the NVFIX message
func (nmb *NvfixMessageBuilder) Size() int {
	return nmb.size
}

// Bytes returns the NVFIX message, in the form of a byte buffer
func (nmb *NvfixMessageBuilder) Bytes() []byte {
	return nmb.message
}

// Data returns the NVFIX message, in the form of a string
func (nmb *NvfixMessageBuilder) Data() string {
	return string(nmb.message)
}

// AppendBytes appends the given tag (byte array) and the value (byte array) to the NVFIX message.
//
// Example:
// value := "Here is the value"
// builder := amps.NewNVFIXBuilder()
// builder.AppendBytes([]byte("key"), []byte(value), 0, len(value))
//
// client.Publish("nvfix-topic", builder.Data())
// 
// Arguments:
// tag [byte buffer]  -  The byte buffer to be the key in the NVFIX message
// value [byte buffer]  -  The bytes to be the value in the NVFIX message
// valOffset [int]  -  The location in the buffer where the bytes to append begin (for the value)
// valLength [int]  -  The length of the value in the buffer
//
// Returns:
// Illegal Argument error  -  If the provided tag is empty
func (nmb *NvfixMessageBuilder) AppendBytes(tag []byte, value []byte, valOffset int, valLength int) error {
	if len(tag) == 0 { return errors.New("Illegal argument: no tag value provided to NVFIX builder") }

	sizeNeeded := len(tag) + 1 + valLength + 2
	nmb.checkCapacity(sizeNeeded)
	nmb.size += sizeNeeded

	// tag
	nmb.message = append(nmb.message, tag...)
	nmb.message = append(nmb.message, '=')
	nmb.message = append(nmb.message, value[valOffset:valOffset + valLength]...)
	nmb.message = append(nmb.message, nmb.fieldSeparator)

	return nil
}

// AppendStrings appends the given tag (string) and the value (string) to the NVFIX message.
//
// Example:
// value := "This is a value"
// key := "This is the key"
// builder := amps.NewNVFIXBuilder()
// builder.AppendStrings(key, value)
//
// client.Publish("nvfix-topic", builder.Data())
//
// Arguments:
// tag [string]  -  The string to be the key in the NVFIX message
// value [string]  -  The string to be the key in the NVFIX message
//
// Returns:
// Illegal Argument error  -  If the provided tag is empty
func (nmb *NvfixMessageBuilder) AppendStrings(tag string, value string) error {
	return nmb.AppendBytes([]byte(tag), []byte(value), 0, len([]byte(value)))
}

// NewNVFIXBuilder use this method to create a new NVFIX message.
//
// Example:
// builder := amps.NewNVFIXBuilder()
//
// Arguments:
// fieldSep [byte] (optional)  -  The default is \x01, a different one can be provided
func NewNVFIXBuilder(fieldSep ...byte) *NvfixMessageBuilder {
	var _fieldSep byte

	if len(fieldSep) > 0 {
		_fieldSep = fieldSep[0]
	} else {
		_fieldSep = '\x01'
	}

	return &NvfixMessageBuilder{make([]byte, 0, 1024), _fieldSep, 0, 1024}
}
