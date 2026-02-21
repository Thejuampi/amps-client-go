package amps

import "errors"

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
		partLength := int(data[start])<<24 +
			int(data[start+1])<<16 +
			int(data[start+2])<<8 +
			int(data[start+3])

		if start+partLength > length {
			return cmp.Size(), errors.New("Invalid message part length")
		}

		start += 4
		end := start + partLength
		cmp.parts = append(cmp.parts, data[start:end])
		start = end
	}

	return cmp.Size(), nil
}

// ParseMessage executes the exported parsemessage operation.
func (cmp *CompositeMessageParser) ParseMessage(message *Message) (int, error) {
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
