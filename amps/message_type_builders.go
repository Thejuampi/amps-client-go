package amps

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/Thejuampi/amps-client-go/internal/safecast"
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
	var buf = make([]byte, 0, len(b.fields)*16)
	buf = append(buf, '{')
	for i, f := range b.fields {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = strconv.AppendQuote(buf, f.key)
		buf = append(buf, ':')
		buf = strconv.AppendQuote(buf, f.val)
	}
	buf = append(buf, '}')
	return buf
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
	} else if length, ok := safecast.Uint16FromIntChecked(len(s)); ok {
		buf = append(buf, 0xda, 0x00, 0x00) // str16
		binary.BigEndian.PutUint16(buf[len(buf)-2:], length)
	} else if length, ok := safecast.Uint32FromIntChecked(len(s)); ok {
		buf = append(buf, 0xdb, 0x00, 0x00, 0x00, 0x00) // str32
		binary.BigEndian.PutUint32(buf[len(buf)-4:], length)
	} else {
		return nil
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
		if body == nil {
			return nil
		}
	}
	body = append(body, 0x00) // document terminator

	var size, ok = safecast.Uint32FromIntChecked(len(body) + 4)
	if !ok {
		return nil
	}
	var header = make([]byte, 4)
	binary.LittleEndian.PutUint32(header, size)
	return append(header, body...)
}

func appendBSONCString(buf []byte, s string) []byte {
	buf = append(buf, s...)
	buf = append(buf, 0x00)
	return buf
}

func appendBSONString(buf []byte, s string) []byte {
	var length, ok = safecast.Uint32FromIntChecked(len(s) + 1)
	if !ok {
		return nil
	}
	buf = append(buf, 0x00, 0x00, 0x00, 0x00)
	binary.LittleEndian.PutUint32(buf[len(buf)-4:], length)
	buf = append(buf, s...)
	buf = append(buf, 0x00)
	return buf
}

// NewBSONBuilder returns a new BSONMessageBuilder.
func NewBSONBuilder() *BSONMessageBuilder {
	return &BSONMessageBuilder{}
}
