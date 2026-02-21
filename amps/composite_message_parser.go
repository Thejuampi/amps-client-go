package amps

import (
	"encoding/binary"
	"errors"
)

// CompositeMessageParser parses protocol payload data into structured parts.
type CompositeMessageParser struct {
	parts [][]byte
}

func (cmp *CompositeMessageParser) reset() {
	cmp.parts = make([][]byte, 0, 0)
}

// Parse executes the exported parse operation.
func (cmp *CompositeMessageParser) Parse(data []byte) (int, error) {

	cmp.reset()

	start := 0
	length := len(data)
	for start < length {
		if length-start < 4 {
			return cmp.Size(), errors.New("Truncated composite part header")
		}
		partLength := binary.BigEndian.Uint32(data[start : start+4])
		start += 4
		remaining := uint32(length - start)
		if partLength > remaining {
			return cmp.Size(), errors.New("Invalid message part length")
		}
		end := start + int(partLength)
		cmp.parts = append(cmp.parts, data[start:end])
		start = end
	}

	return cmp.Size(), nil
}

// ParseMessage executes the exported parsemessage operation.
func (cmp *CompositeMessageParser) ParseMessage(message *Message) (int, error) {
	if message == nil {
		return 0, errors.New("nil message")
	}
	return cmp.Parse(message.Data())
}

// Size executes the exported size operation.
func (cmp *CompositeMessageParser) Size() int {
	return int(len(cmp.parts))
}

// Part executes the exported part operation.
func (cmp *CompositeMessageParser) Part(index int) ([]byte, error) {
	if index >= cmp.Size() || index < 0 {
		return nil, errors.New("Invalid part index")
	}

	return cmp.parts[index], nil
}

// NewCompositeMessageParser returns a new CompositeMessageParser.
func NewCompositeMessageParser() *CompositeMessageParser {
	return &CompositeMessageParser{}
}
