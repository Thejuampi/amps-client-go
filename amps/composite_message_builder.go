package amps

type CompositeMessageBuilder struct {
	message []byte
}

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

func (cmb *CompositeMessageBuilder) AppendBytes(data []byte, offset int, length int) error {
	if length > 0 {
		cmb.buildHeader(length)
		cmb.message = append(cmb.message, data[offset:length]...)
	}

	return nil
}

func (cmb *CompositeMessageBuilder) Append(data string) error {
	buffer := []byte(data)
	length := len(buffer)
	return cmb.AppendBytes(buffer, 0, length)
}

func (cmb *CompositeMessageBuilder) GetData() string {
	return string(cmb.message)
}

func (cmb *CompositeMessageBuilder) GetBytes() []byte {
	return cmb.message
}

func NewCompositeMessageBuilder() *CompositeMessageBuilder {
	return &CompositeMessageBuilder{make([]byte, 0)}
}
