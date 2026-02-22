package cppcompat

import (
	"bytes"
	"hash/crc32"
	"net/url"
	"strconv"
	"strings"

	"github.com/Thejuampi/amps-client-go/amps"
)

// VersionInfo aliases the AMPS version model.
type VersionInfo = amps.VersionInfo

// ParseVersionInfo parses a dotted version string.
func ParseVersionInfo(version string) VersionInfo {
	return amps.ParseVersionInfo(version)
}

// URI represents a parsed AMPS URI.
type URI struct {
	Raw       string
	Transport string
	User      string
	Password  string // #nosec G117 -- compatibility model includes URI credential component
	Host      string
	Port      string
	Protocol  string
}

// ParseURI parses AMPS URI format into a URI model.
func ParseURI(raw string) URI {
	parsed := URI{Raw: raw}
	uri, err := url.Parse(raw)
	if err != nil {
		return parsed
	}
	parsed.Transport = uri.Scheme
	parsed.Host = uri.Hostname()
	parsed.Port = uri.Port()
	if uri.User != nil {
		parsed.User = uri.User.Username()
		password, _ := uri.User.Password()
		parsed.Password = password
	}
	path := strings.Trim(uri.Path, "/")
	parts := strings.Split(path, "/")
	if len(parts) > 1 {
		parsed.Protocol = parts[1]
	}
	return parsed
}

// Reason provides canonical AMPS reason string values.
type Reason struct{}

// Duplicate returns duplicate reason value.
func (Reason) Duplicate() string { return "duplicate" }

// BadFilter returns bad-filter reason value.
func (Reason) BadFilter() string { return "bad filter" }

// BadRegexTopic returns bad-regex reason value.
func (Reason) BadRegexTopic() string { return "bad regex topic" }

// SubscriptionAlreadyExists returns existing-subscription reason value.
func (Reason) SubscriptionAlreadyExists() string { return "subscription already exists" }

// NameInUse returns name-in-use reason value.
func (Reason) NameInUse() string { return "name in use" }

// AuthFailure returns authentication failure reason value.
func (Reason) AuthFailure() string { return "auth failure" }

// NotEntitled returns not-entitled reason value.
func (Reason) NotEntitled() string { return "not entitled" }

// AuthDisabled returns auth-disabled reason value.
func (Reason) AuthDisabled() string { return "authentication disabled" }

// SubIDInUse returns subid-in-use reason value.
func (Reason) SubIDInUse() string { return "subid in use" }

// NoTopic returns no-topic reason value.
func (Reason) NoTopic() string { return "no topic" }

// Field represents a byte field value used by message APIs.
type Field struct {
	data []byte
}

// NewField constructs a field from bytes.
func NewField(data []byte) Field {
	copied := append([]byte(nil), data...)
	return Field{data: copied}
}

// NewFieldString constructs a field from string.
func NewFieldString(data string) Field {
	return NewField([]byte(data))
}

// Bytes returns field bytes.
func (field Field) Bytes() []byte {
	return append([]byte(nil), field.data...)
}

// String returns field text.
func (field Field) String() string {
	return string(field.data)
}

// Len returns field length.
func (field Field) Len() int {
	return len(field.data)
}

// Empty reports whether field has no bytes.
func (field Field) Empty() bool {
	return len(field.data) == 0
}

// Contains reports whether field contains value.
func (field Field) Contains(value string) bool {
	return bytes.Contains(field.data, []byte(value))
}

// Uint64 parses field value as uint64.
func (field Field) Uint64() uint64 {
	value, err := strconv.ParseUint(strings.TrimSpace(field.String()), 10, 64)
	if err != nil {
		return 0
	}
	return value
}

// BookmarkRange describes a bookmark range.
type BookmarkRange struct {
	Begin string
	End   string
}

// CRC computes CRC32 values.
type CRC struct{}

// Compute computes CRC32 checksum for data.
func (CRC) Compute(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
