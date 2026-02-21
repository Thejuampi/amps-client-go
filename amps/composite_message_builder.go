package amps

import "errors"

// CompositeMessageBuilder builds protocol payload data for publish and helper APIs.
type CompositeMessageBuilder struct {
	message []byte
}

// Clear executes the exported clear operation.
func (cmb *CompositeMessageBuilder) Clear() {
	cmb.message = make([]byte, 0)
}

func (cmb *CompositeMessageBuilder) buildHeader(length int) {
	msgLength := len(cmb.message)
	cmb.message = append(cmb.message, 0, 0, 0, 0)
	cmb.message[msgLength] = (byte)((length & 0xFF000000) >> 24)
	cmb.message[msgLength+1] = (byte)((length & 0xFF0000) >> 16)
	cmb.message[msgLength+2] = (byte)((length & 0xFF00) >> 8)
	cmb.message[msgLength+3] = (byte)(length & 0xFF)

}

// AppendBytes executes the exported appendbytes operation.
func (cmb *CompositeMessageBuilder) AppendBytes(data []byte, offset int, length int) error {
	if length < 0 {
		return errors.New("invalid composite part length")
	}
	if offset < 0 {
		return errors.New("invalid composite part offset")
	}
	if length == 0 {
		return nil
	}
	if data == nil {
		return errors.New("composite part data is required")
	}
	if offset > len(data) || length > len(data)-offset {
		return errors.New("composite part range out of bounds")
	}

	end := offset + length
	cmb.buildHeader(length)
	cmb.message = append(cmb.message, data[offset:end]...)
	return nil
}

// Append executes the exported append operation.
func (cmb *CompositeMessageBuilder) Append(data string) error {
	buffer := []byte(data)
	length := len(buffer)
	return cmb.AppendBytes(buffer, 0, length)
}

// GetData returns the current data value.
func (cmb *CompositeMessageBuilder) GetData() string {
	return string(cmb.message)
}

// GetBytes returns the current bytes value.
func (cmb *CompositeMessageBuilder) GetBytes() []byte {
	return cmb.message
}

// NewCompositeMessageBuilder returns a new CompositeMessageBuilder.
func NewCompositeMessageBuilder() *CompositeMessageBuilder {
	return &CompositeMessageBuilder{make([]byte, 0)}
}
