package amps


import "errors"


// CompositeMessageParser struct
type CompositeMessageParser struct {
    parts [][]byte
}


func (cmp *CompositeMessageParser) reset() {
    cmp.parts = make([][]byte, 0, 0)
}


// Parse parses a composite AMPS.Message, returns the number of valid parts parsed. 
//
// Example:
// _, err = client.SubscribeAsync(func(message *amps.Message) (msgError error) {
//     parser := amps.NewCompositeMessageParser()                                                                           
//     parts, _ := parser.ParseMessage(message)
//     for i := 0; i < int(parts); i++ {
//         part, _ := parser.Part(i)
//         fmt.Println("part", i, string(part))
//     }
// }
//
// Arguments:
// data [byte buffer]  -  Byte buffer containing the data to be parsed
func (cmp *CompositeMessageParser) Parse(data []byte) (int, error) {
	// reset the data first
	cmp.reset()

	start := 0
	length := len(data)
	for start < length {
		partLength :=   int(data[start]) << 24 +
					int(data[start + 1]) << 16 +
					int(data[start + 2]) << 8  +
					int(data[start + 3])

		if start + partLength > length {
			return cmp.Size(), errors.New("Invalid message part length")
		}

		start += 4
		end := start + partLength
		cmp.parts = append(cmp.parts, data[start: end])
		start = end
	}

    return cmp.Size(), nil
}


// ParseMessage parses a composite AMPS.Message, returns the data from the composite message.
//
// Example:
// _, err = client.SubscribeAsync(func(message *amps.Message) (msgError error) {
//     parser := amps.NewCompositeMessageParser()                                                                           
//     parts, _ := parser.ParseMessage(message)
//     for i := 0; i < int(parts); i++ {
//         part, _ := parser.Part(i)
//         fmt.Println("part", i, string(part))
//     }
// }
//
// Arguments:
// message [*Message]  -  Message object to parse
func (cmp *CompositeMessageParser) ParseMessage(message *Message) (int, error) {
	return cmp.Parse(message.Data())
}


// Size returns the size of the part parsed from the composite message. 
func (cmp *CompositeMessageParser) Size() int {
	return int(len(cmp.parts))
}


// Part returns the index'th part of the composite message. 
// If the index provided is greater than the size of the part, or if the index is negative, return 
// an "Invalid parts index" error. 
//
// Example:
// _, err = client.SubscribeAsync(func(message *amps.Message) (msgError error) {
//     parser := amps.NewCompositeMessageParser()                                                                           
//     parts, _ := parser.ParseMessage(message)
//     for i := 0; i < int(parts); i++ {
//         part, _ := parser.Part(i)
//         fmt.Println("part", i, string(part))
//     }
// }
//
// Arguments:
// index [int]  -  The index used to locate a part of the composite message
func (cmp *CompositeMessageParser) Part(index int) ([]byte, error) {
	if index >= cmp.Size() || index < 0 {
		return nil, errors.New("Invalid part index")
	}

	return cmp.parts[index], nil
}


// NewCompositeMessageParser ...
func NewCompositeMessageParser() *CompositeMessageParser {
    return &CompositeMessageParser{}
}


