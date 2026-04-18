package amps

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// JSONMessageBuilder constructs JSON-formatted messages for AMPS.
type JSONMessageBuilder struct {
	fields []kv
}

type kv struct {
	key string
	val string
}

// Reset clears the builder for reuse.
func (b *JSONMessageBuilder) Reset() {
	b.fields = b.fields[:0]
}

// Append adds a key-value pair.
func (b *JSONMessageBuilder) Append(key string, value string) {
	b.fields = append(b.fields, kv{key: key, val: value})
}

// AppendInt adds an integer key-value pair.
func (b *JSONMessageBuilder) AppendInt(key string, value int) {
	b.fields = append(b.fields, kv{key: key, val: strconv.Itoa(value)})
}

// AppendFloat adds a float64 key-value pair.
func (b *JSONMessageBuilder) AppendFloat(key string, value float64) {
	b.fields = append(b.fields, kv{key: key, val: strconv.FormatFloat(value, 'f', -1, 64)})
}

// Data returns the JSON-encoded string.
func (b *JSONMessageBuilder) Data() string {
	return string(b.Bytes())
}

// Bytes returns the JSON-encoded bytes.
func (b *JSONMessageBuilder) Bytes() []byte {
	if len(b.fields) == 0 {
		return []byte("{}")
	}
	var buf strings.Builder
	buf.WriteByte('{')
	for i, f := range b.fields {
		if i > 0 {
			buf.WriteByte(',')
		}
		keyBytes, _ := json.Marshal(f.key)
		buf.Write(keyBytes)
		buf.WriteByte(':')
		valBytes, _ := json.Marshal(f.val)
		buf.Write(valBytes)
	}
	buf.WriteByte('}')
	return []byte(buf.String())
}

// NewJSONBuilder returns a new JSONMessageBuilder.
func NewJSONBuilder() *JSONMessageBuilder {
	return &JSONMessageBuilder{}
}

// BFlatMessageBuilder constructs BFlat-formatted messages for AMPS.
// BFlat uses a compact binary format with length-prefixed fields.
type BFlatMessageBuilder struct {
	fields []kv
}

// Reset clears the builder for reuse.
func (b *BFlatMessageBuilder) Reset() {
	b.fields = b.fields[:0]
}

// Append adds a key-value pair.
func (b *BFlatMessageBuilder) Append(key string, value string) {
	b.fields = append(b.fields, kv{key: key, val: value})
}

// Data returns the BFlat-encoded string.
func (b *BFlatMessageBuilder) Data() string {
	return string(b.Bytes())
}

// Bytes returns the BFlat-encoded bytes.
func (b *BFlatMessageBuilder) Bytes() []byte {
	var buf strings.Builder
	for _, f := range b.fields {
		buf.WriteString(fmt.Sprintf("%d:%s=%d:%s,", len(f.key), f.key, len(f.val), f.val))
	}
	return []byte(buf.String())
}

// NewBFlatBuilder returns a new BFlatMessageBuilder.
func NewBFlatBuilder() *BFlatMessageBuilder {
	return &BFlatMessageBuilder{}
}

// XMLMessageBuilder constructs XML-formatted messages for AMPS.
type XMLMessageBuilder struct {
	fields []kv
	root   string
}

// Reset clears the builder for reuse.
func (b *XMLMessageBuilder) Reset() {
	b.fields = b.fields[:0]
}

// Append adds a key-value pair as an XML element.
func (b *XMLMessageBuilder) Append(key string, value string) {
	b.fields = append(b.fields, kv{key: key, val: value})
}

// SetRoot sets the root element name (default "message").
func (b *XMLMessageBuilder) SetRoot(name string) {
	b.root = name
}

// Data returns the XML-encoded string.
func (b *XMLMessageBuilder) Data() string {
	return string(b.Bytes())
}

// Bytes returns the XML-encoded bytes.
func (b *XMLMessageBuilder) Bytes() []byte {
	root := b.root
	if root == "" {
		root = "message"
	}
	var buf strings.Builder
	buf.WriteString("<" + root + ">")
	for _, f := range b.fields {
		buf.WriteString("<")
		buf.WriteString(f.key)
		buf.WriteString(">")
		buf.WriteString(escapeXML(f.val))
		buf.WriteString("</")
		buf.WriteString(f.key)
		buf.WriteString(">")
	}
	buf.WriteString("</" + root + ">")
	return []byte(buf.String())
}

func escapeXML(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	s = strings.ReplaceAll(s, "'", "&apos;")
	return s
}

// NewXMLBuilder returns a new XMLMessageBuilder.
func NewXMLBuilder() *XMLMessageBuilder {
	return &XMLMessageBuilder{}
}

// MessagePackMessageBuilder constructs MessagePack-formatted messages for AMPS.
// This produces a simple map-like structure compatible with MessagePack.
type MessagePackMessageBuilder struct {
	fields []kv
}

// Reset clears the builder for reuse.
func (b *MessagePackMessageBuilder) Reset() {
	b.fields = b.fields[:0]
}

// Append adds a key-value pair.
func (b *MessagePackMessageBuilder) Append(key string, value string) {
	b.fields = append(b.fields, kv{key: key, val: value})
}

// Data returns the MessagePack-encoded string (hex representation).
func (b *MessagePackMessageBuilder) Data() string {
	return string(b.Bytes())
}

// Bytes returns the MessagePack-encoded bytes.
// Uses the MessagePack fixmap + fixstr encoding for simple string maps.
func (b *MessagePackMessageBuilder) Bytes() []byte {
	if len(b.fields) > 15 {
		return nil
	}
	var buf []byte
	buf = append(buf, 0x80|byte(len(b.fields))) // fixmap with N entries
	for _, f := range b.fields {
		buf = appendMsgPackString(buf, f.key)
		buf = appendMsgPackString(buf, f.val)
	}
	return buf
}

func appendMsgPackString(buf []byte, s string) []byte {
	if len(s) <= 31 {
		buf = append(buf, 0xa0|byte(len(s))) // fixstr
	} else if len(s) <= 255 {
		buf = append(buf, 0xd9, byte(len(s))) // str8
	} else {
		buf = append(buf, 0xda, byte(len(s)>>8), byte(len(s))) // str16
	}
	buf = append(buf, s...)
	return buf
}

// NewMessagePackBuilder returns a new MessagePackMessageBuilder.
func NewMessagePackBuilder() *MessagePackMessageBuilder {
	return &MessagePackMessageBuilder{}
}

// BSONMessageBuilder constructs BSON-formatted messages for AMPS.
type BSONMessageBuilder struct {
	fields []kv
}

// Reset clears the builder for reuse.
func (b *BSONMessageBuilder) Reset() {
	b.fields = b.fields[:0]
}

// Append adds a key-value pair.
func (b *BSONMessageBuilder) Append(key string, value string) {
	b.fields = append(b.fields, kv{key: key, val: value})
}

// Data returns the BSON-encoded string.
func (b *BSONMessageBuilder) Data() string {
	return string(b.Bytes())
}

// Bytes returns the BSON-encoded bytes.
// Uses BSON format: int32 document size, followed by elements (type, cname, value), terminated by 0x00.
func (b *BSONMessageBuilder) Bytes() []byte {
	var body []byte
	for _, f := range b.fields {
		body = append(body, 0x02) // string type
		body = appendBSONCString(body, f.key)
		body = appendBSONString(body, f.val)
	}
	body = append(body, 0x00) // document terminator

	var size = int32(len(body) + 4)
	var header = []byte{
		byte(size), byte(size >> 8), byte(size >> 16), byte(size >> 24),
	}
	return append(header, body...)
}

func appendBSONCString(buf []byte, s string) []byte {
	buf = append(buf, s...)
	buf = append(buf, 0x00)
	return buf
}

func appendBSONString(buf []byte, s string) []byte {
	var length = int32(len(s) + 1)
	buf = append(buf, byte(length), byte(length>>8), byte(length>>16), byte(length>>24))
	buf = append(buf, s...)
	buf = append(buf, 0x00)
	return buf
}

// NewBSONBuilder returns a new BSONMessageBuilder.
func NewBSONBuilder() *BSONMessageBuilder {
	return &BSONMessageBuilder{}
}
