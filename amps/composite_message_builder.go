package amps


// CompositeMessageBuilder struct
type CompositeMessageBuilder struct {
	message []byte
}


// Clear clears the message object.
func (cmb *CompositeMessageBuilder) Clear() {
	cmb.message = make([]byte, 0)
}


func (cmb *CompositeMessageBuilder) buildHeader(length int) {
	msgLength := len(cmb.message)
	cmb.message = append(cmb.message, 0, 0, 0, 0)
	cmb.message[msgLength] = (byte) ((length & 0xFF000000) >> 24)
	cmb.message[msgLength + 1] = (byte) ((length & 0xFF0000) >> 16)
	cmb.message[msgLength + 2] = (byte) ((length & 0xFF00) >> 8)
	cmb.message[msgLength + 3] = (byte) (length & 0xFF)

}


// AppendBytes appends the given byte buffer to the composite message. 
// 
// Example:
// bin := []byte{0, 1, 2}
// builder := amps.NewCompositeMessageBuilder()
// builder.AppendBytes(bin, 0, 3)
// 
// client.Publish("test", builder.GetData())
//
// Arguments:
// data [byte buffer]  -  The bytes to append to the composite message
// offset [int]  -  Location in the buffer where the bytes to append begin
// length [int]  -  Length of the buffer
func (cmb *CompositeMessageBuilder) AppendBytes(data []byte, offset int, length int) (error) {
	if length > 0 {
		cmb.buildHeader(length)
		cmb.message = append(cmb.message, data[offset: length]...)
	}

	return nil
}


// Append appends the given string to the composite message. 
//
// Example:
// json := `{"item":"car","price":20000}`
// builder := amps.NewCompositeMessageBuilder()
// builder.Append(json)
// 
// client.Publish("test", builder.GetData())
//
// Arguments:
// data [string]  -  The string to append to the composite message
func (cmb *CompositeMessageBuilder) Append(data string) (error) {
	buffer := []byte(data)
	length := len(buffer)
	return cmb.AppendBytes(buffer, 0, length)
}


// GetData returns the composte message's data, as a string.
// Use this when you want to publish a composite message. 
//
// Example:
// json := `{"item":"car","price":20000}`
// builder := amps.NewCompositeMessageBuilder()
// builder.Append(json)
// 
// client.Publish("test", builder.GetData())
func (cmb *CompositeMessageBuilder) GetData() (string) {
	return string(cmb.message)
}


// GetBytes returns the composite message's data, as a byte array.
//
// Example:
// 
// bin := []byte{0, 1, 2}
// builder := amps.NewCompositeMessageBuilder()
// builder.AppendBytes(bin, 0, 3)
// 
// client.Publish("test", builder.GetBytes())
func (cmb *CompositeMessageBuilder) GetBytes() ([]byte) {
	return cmb.message
}


// NewCompositeMessageBuilder ...
func NewCompositeMessageBuilder() *CompositeMessageBuilder {
	return &CompositeMessageBuilder{make([]byte, 0)}
}
