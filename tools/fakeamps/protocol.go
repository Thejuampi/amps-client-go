package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Thejuampi/amps-client-go/internal/safecast"
)

// ---------------------------------------------------------------------------
// headerFields — parsed AMPS command header.
//
// Maps to all the wire-format fields the Go client sends (see header.go's
// write method). Single-char keys are the short-form used on the wire.
// ---------------------------------------------------------------------------

type headerFields struct {
	c          string // command
	cid        string // command ID
	status     string // ack status
	reason     string // ack reason
	uri        string // redirect URI
	t          string // topic
	subID      string // sub_id
	a          string // ack types requested
	opts       string // options (multi-char key "opts")
	o          string // options (single-char key "o")
	repl       string // replication source id
	replSync   string // replication sync marker
	clientName string // client_name
	version    string // version
	queryID    string // query_id
	s          string // sequence ID
	bm         string // bookmark
	filter     string // filter
	k          string // sow_key
	sowKeys    string // sow_keys
	x          string // correlation ID
	bs         string // batch_size
	e          string // expiration (seconds)
	mt         string // message_type (json, fix, nvfix, xml, binary, protobuf, etc.)
	topN       string // top_n
	orderBy    string // orderby
	pw         string // password
	userID     string // user_id
	subIDs     string // sids (multiple subscription IDs)
	compress   string // compression ("zlib")
}

// ---------------------------------------------------------------------------
// parseAMPSHeader — fast, minimal JSON header parser.
//
// Extracts all fields the client may send. Operates on raw frame bytes
// without allocating intermediate structures for plain scalar fields. Field
// names are matched exactly; escaped strings use the JSON decoder.
// ---------------------------------------------------------------------------

func parseAMPSHeader(frame []byte) (headerFields, []byte) {
	var h headerFields
	var i = skipHeaderWhitespace(frame, 0)
	if i == len(frame) || frame[i] != '{' {
		return headerFields{}, nil
	}
	i = skipHeaderWhitespace(frame, i+1)
	if i < len(frame) && frame[i] == '}' {
		return h, frame[i+1:]
	}
	for i < len(frame) {
		var key, next, ok = readHeaderString(frame, i)
		if !ok {
			return headerFields{}, nil
		}
		i = skipHeaderWhitespace(frame, next)
		if i == len(frame) || frame[i] != ':' {
			return headerFields{}, nil
		}
		i = skipHeaderWhitespace(frame, i+1)
		var value []byte
		value, next, ok = readHeaderValue(frame, i)
		if !ok {
			return headerFields{}, nil
		}
		switch string(key) {
		case "c":
			h.c = string(value)
		case "cid":
			h.cid = string(value)
		case "t":
			h.t = string(value)
		case "a":
			h.a = string(value)
		case "o":
			h.o = string(value)
		case "s":
			h.s = string(value)
		case "k":
			h.k = string(value)
		case "x":
			h.x = string(value)
		case "e":
			h.e = string(value)
		case "f", "filter":
			h.filter = string(value)
		case "bm", "bookmark":
			h.bm = string(value)
		case "bs":
			h.bs = string(value)
		case "mt":
			h.mt = string(value)
		case "pw":
			h.pw = string(value)
		case "uri":
			h.uri = string(value)
		case "opts":
			h.opts = string(value)
		case "sids":
			h.subIDs = string(value)
		case "top_n":
			h.topN = string(value)
		case "_repl":
			h.repl = string(value)
		case "_repl_sync":
			h.replSync = string(value)
		case "sub_id":
			h.subID = string(value)
		case "status":
			h.status = string(value)
		case "reason":
			h.reason = string(value)
		case "version":
			h.version = string(value)
		case "orderby":
			h.orderBy = string(value)
		case "user_id":
			h.userID = string(value)
		case "query_id":
			h.queryID = string(value)
		case "sow_keys":
			h.sowKeys = string(value)
		case "client_name":
			h.clientName = string(value)
		}
		i = skipHeaderWhitespace(frame, next)
		if i == len(frame) {
			return headerFields{}, nil
		}
		switch frame[i] {
		case '}':
			return h, frame[i+1:]
		case ',':
			i = skipHeaderWhitespace(frame, i+1)
		default:
			return headerFields{}, nil
		}
	}
	return headerFields{}, nil
}

func skipHeaderWhitespace(frame []byte, index int) int {
	for index < len(frame) {
		switch frame[index] {
		case ' ', '\t', '\r', '\n':
			index++
		default:
			return index
		}
	}
	return index
}

func readHeaderString(frame []byte, index int) ([]byte, int, bool) {
	if index == len(frame) || frame[index] != '"' {
		return nil, index, false
	}
	var start = index
	var escaped bool
	for index++; index < len(frame); index++ {
		switch frame[index] {
		case '"':
			if !escaped {
				return frame[start+1 : index], index + 1, true
			}
			var decoded string
			if err := json.Unmarshal(frame[start:index+1], &decoded); err != nil {
				return nil, index, false
			}
			return []byte(decoded), index + 1, true
		case '\\':
			escaped = true
			index++
		default:
			if frame[index] < 0x20 {
				return nil, index, false
			}
		}
	}
	return nil, index, false
}

func readHeaderValue(frame []byte, index int) ([]byte, int, bool) {
	if index == len(frame) {
		return nil, index, false
	}
	if frame[index] == '"' {
		return readHeaderString(frame, index)
	}
	if frame[index] == '{' || frame[index] == '[' {
		// Future fields may contain structured values. Decode only this uncommon
		// case so the payload and nested field names cannot become header fields.
		var decoder = json.NewDecoder(bytes.NewReader(frame[index:]))
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, index, false
		}
		return nil, index + int(decoder.InputOffset()), true
	}
	var start = index
	for index < len(frame) {
		switch frame[index] {
		case ',', '}', ' ', '\t', '\r', '\n':
			var value = frame[start:index]
			return value, index, validHeaderScalar(value)
		default:
			index++
		}
	}
	return nil, index, false
}

func validHeaderScalar(value []byte) bool {
	if len(value) == 0 {
		return false
	}
	if value[0] >= '1' && value[0] <= '9' {
		for _, digit := range value[1:] {
			if digit < '0' || digit > '9' {
				return json.Valid(value)
			}
		}
		return true
	}
	return json.Valid(value)
}

// ---------------------------------------------------------------------------
// Frame builders
//
// Each "build" function serializes a complete AMPS frame (4-byte big-endian
// length prefix + JSON header + optional payload) and returns an owned
// []byte ready to enqueue into a connWriter channel.
// ---------------------------------------------------------------------------

func finalizeFrame(buf *bytes.Buffer) []byte {
	raw := buf.Bytes()
	n := len(raw)
	if n < 4 {
		buf.Reset()
		return nil
	}
	length, ok := safecast.Uint32FromIntChecked(n - 4)
	if !ok {
		buf.Reset()
		return nil
	}
	binary.BigEndian.PutUint32(raw[:4], length)
	out := make([]byte, n)
	copy(out, raw)
	buf.Reset()
	return out
}

func startFrame(buf *bytes.Buffer) {
	buf.Reset()
	buf.Write([]byte{0, 0, 0, 0})
}

// ---- Logon Ack ----

func buildLogonAck(buf *bytes.Buffer, commandID, clientName, correlationID string) []byte {
	startFrame(buf)
	buf.WriteString(`{"c":"ack","a":"processed","status":"success"`)
	writeField(buf, "cid", commandID)
	buf.WriteString(`,"version":"`)
	buf.WriteString(*flagVersion)
	buf.WriteByte('"')
	writeField(buf, "client_name", clientName)
	writeField(buf, "x", correlationID)
	buf.WriteByte('}')
	return finalizeFrame(buf)
}

func buildRedirectFrame(buf *bytes.Buffer, commandID, uri string) []byte {
	startFrame(buf)
	buf.WriteString(`{"c":"redirect","status":"redirect"`)
	writeField(buf, "cid", commandID)
	writeField(buf, "uri", uri)
	buf.WriteByte('}')
	return finalizeFrame(buf)
}

// ---- Generic Ack ----

func buildAck(buf *bytes.Buffer, ackType, commandID, status string, extras ...kv) []byte {
	startFrame(buf)
	buf.WriteString(`{"c":"ack","a":"`)
	buf.WriteString(ackType)
	buf.WriteString(`","status":"`)
	buf.WriteString(status)
	buf.WriteByte('"')
	writeField(buf, "cid", commandID)
	for _, e := range extras {
		if e.numeric {
			writeNumericField(buf, e.k, e.v)
		} else {
			writeField(buf, e.k, e.v)
		}
	}
	buf.WriteByte('}')
	return finalizeFrame(buf)
}

// ---- Persisted Ack (echoes sequence ID for publish store discard) ----

func buildPersistedAck(buf *bytes.Buffer, commandID, seqID string) []byte {
	return buildPersistedAckStatus(buf, commandID, seqID, "success", "")
}

func buildPersistedAckStatus(buf *bytes.Buffer, commandID, seqID, status, reason string) []byte {
	startFrame(buf)
	buf.WriteString(`{"c":"ack","a":"persisted","status":"`)
	buf.WriteString(status)
	buf.WriteByte('"')
	writeField(buf, "cid", commandID)
	if seqID != "" {
		buf.WriteString(`,"s":`)
		buf.WriteString(seqID)
	}
	if reason != "" {
		writeField(buf, "reason", reason)
	}
	buf.WriteByte('}')
	return finalizeFrame(buf)
}

// ---- SOW Completed Ack (with record counts) ----

func buildSOWCompletedAck(buf *bytes.Buffer, commandID, queryID string, returned, inserted, updated, deleted, topicMatches int) []byte {
	startFrame(buf)
	buf.WriteString(`{"c":"ack","a":"completed","status":"success"`)
	writeField(buf, "cid", commandID)
	writeField(buf, "query_id", queryID)
	writeNumericField(buf, "topic_matches", strconv.Itoa(topicMatches))
	writeNumericField(buf, "records_returned", strconv.Itoa(returned))
	writeNumericField(buf, "records_inserted", strconv.Itoa(inserted))
	writeNumericField(buf, "records_updated", strconv.Itoa(updated))
	writeNumericField(buf, "records_deleted", strconv.Itoa(deleted))
	buf.WriteByte('}')
	return finalizeFrame(buf)
}

// ---- SOW Delete Ack ----

func buildSOWDeleteAck(buf *bytes.Buffer, ackType, commandID string, deleted int, topicMatches int) []byte {
	return buildAck(buf, ackType, commandID, "success",
		kv{k: "records_deleted", v: strconv.Itoa(deleted), numeric: true},
		kv{k: "records_returned", v: strconv.Itoa(deleted), numeric: true},
		kv{k: "matches", v: strconv.Itoa(deleted), numeric: true},
		kv{k: "topic_matches", v: strconv.Itoa(topicMatches), numeric: true})
}

// ---- Stats Ack ----

func buildStatsAck(buf *bytes.Buffer, commandID string, stats *connStats) []byte {
	startFrame(buf)
	buf.WriteString(`{"c":"ack","a":"stats","status":"success"`)
	writeField(buf, "cid", commandID)
	writeNumericField(buf, "records_returned", strconv.FormatUint(stats.messagesIn.Load(), 10))
	writeNumericField(buf, "topic_matches", "1")
	buf.WriteByte('}')
	return finalizeFrame(buf)
}

// ---- Heartbeat Frame ----

func buildHeartbeatFrame(buf *bytes.Buffer) []byte {
	startFrame(buf)
	buf.WriteString(`{"c":"heartbeat"}`)
	return finalizeFrame(buf)
}

// ---- Publish Delivery to Subscriber ----

func buildPublishDelivery(buf *bytes.Buffer, topic, subID string, payload []byte, bookmark, timestamp, sowKey, messageType string, isQueue bool) []byte {
	startFrame(buf)
	// Short-form "p" — matches real AMPS wire format and exercises the
	// Go client's parseHeaderTrustedCTSubID fast-path parser.
	buf.WriteString(`{"c":"p","t":"`)
	writeJSONString(buf, topic)
	buf.WriteByte('"')
	writeField(buf, "sub_id", subID)
	writeField(buf, "sids", subID)
	writeField(buf, "bm", bookmark)
	writeField(buf, "ts", timestamp)
	writeField(buf, "k", sowKey)
	writeField(buf, "mt", messageType)
	if isQueue {
		buf.WriteString(`,"lp":"`)
		buf.WriteString(strconv.FormatInt(flagLease.Milliseconds(), 10))
		buf.WriteString(`ms"`)
	}
	buf.WriteByte('}')
	if len(payload) > 0 {
		buf.Write(payload)
	}
	return finalizeFrame(buf)
}

// ---- SOW Record Delivery ----

func sowFrameBodyLength(headerLength, payloadLength int) (uint32, bool) {
	var header, ok = safecast.Uint32FromIntChecked(headerLength)
	if !ok {
		return 0, false
	}
	var payload uint32
	payload, ok = safecast.Uint32FromIntChecked(payloadLength)
	if !ok || payload > math.MaxUint32-header {
		return 0, false
	}
	return header + payload, true
}

func sowFrameAllocationSize(headerLength, payloadLength int) int {
	if payloadLength > math.MaxInt-headerLength {
		panic(bytes.ErrTooLarge)
	}
	return headerLength + payloadLength
}

func buildSOWRecord(buf *bytes.Buffer, topic, queryID, sowKey, bookmark, messageType string, payload []byte) []byte {
	startFrame(buf)
	buf.WriteString(`{"c":"sow"`)
	writeField(buf, "query_id", queryID)
	writeField(buf, "t", topic)
	buf.WriteByte('}')

	buf.WriteString(`{"t":"`)
	writeJSONString(buf, topic)
	buf.WriteByte('"')
	writeField(buf, "query_id", queryID)
	writeField(buf, "k", sowKey)
	writeField(buf, "bm", bookmark)
	writeField(buf, "mt", messageType)
	buf.WriteString(`,"l":`)
	var lengthBuffer [20]byte
	buf.Write(strconv.AppendInt(lengthBuffer[:0], int64(len(payload)), 10))
	buf.WriteByte('}')

	var raw = buf.Bytes()
	var length, ok = sowFrameBodyLength(len(raw)-4, len(payload))
	if !ok {
		buf.Reset()
		return nil
	}
	var frameSize int
	if strconv.IntSize == 32 {
		frameSize = sowFrameAllocationSize(len(raw), len(payload))
	} else {
		frameSize = len(raw) + len(payload)
	}
	var frame = make([]byte, frameSize)
	binary.BigEndian.PutUint32(frame[:4], length)
	copy(frame[4:], raw[4:])
	copy(frame[len(raw):], payload)
	buf.Reset()
	return frame
}

// ---- OOF (Out-of-Focus) Delivery ----

func buildOOFDelivery(buf *bytes.Buffer, topic, subID, sowKey, bookmark string) []byte {
	return buildOOFDeliveryWithReason(buf, topic, subID, sowKey, bookmark, "")
}

func buildOOFDeliveryWithReason(buf *bytes.Buffer, topic, subID, sowKey, bookmark, reason string) []byte {
	startFrame(buf)
	buf.WriteString(`{"c":"oof","t":"`)
	writeJSONString(buf, topic)
	buf.WriteByte('"')
	writeField(buf, "sub_id", subID)
	writeField(buf, "sids", subID)
	writeField(buf, "k", sowKey)
	writeField(buf, "bm", bookmark)
	writeField(buf, "reason", reason)
	buf.WriteByte('}')
	return finalizeFrame(buf)
}

// ---- Group Begin / Group End markers for SOW batches ----

func buildGroupBegin(buf *bytes.Buffer, queryID string) []byte {
	startFrame(buf)
	buf.WriteString(`{"c":"group_begin"`)
	writeField(buf, "query_id", queryID)
	buf.WriteByte('}')
	return finalizeFrame(buf)
}

func buildGroupEnd(buf *bytes.Buffer, queryID string) []byte {
	startFrame(buf)
	buf.WriteString(`{"c":"group_end"`)
	writeField(buf, "query_id", queryID)
	buf.WriteByte('}')
	return finalizeFrame(buf)
}

// ---------------------------------------------------------------------------
// Tiny helpers for header JSON serialization.
// ---------------------------------------------------------------------------

type kv struct {
	k, v    string
	numeric bool
}

func writeField(buf *bytes.Buffer, key, value string) {
	if value == "" {
		return
	}
	buf.WriteString(`,"`)
	buf.WriteString(key)
	buf.WriteString(`":"`)
	writeJSONString(buf, value)
	buf.WriteByte('"')
}

func writeNumericField(buf *bytes.Buffer, key, value string) {
	buf.WriteString(`,"`)
	buf.WriteString(key)
	buf.WriteString(`":`)
	buf.WriteString(value)
}

func writeJSONString(buf *bytes.Buffer, s string) {
	var i int
	for i < len(s) && s[i] >= 0x20 && s[i] != '"' && s[i] != '\\' {
		i++
	}
	buf.WriteString(s[:i])
	for ; i < len(s); i++ {
		switch s[i] {
		case '"':
			buf.WriteString(`\"`)
		case '\\':
			buf.WriteString(`\\`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		default:
			if s[i] < 0x20 {
				buf.WriteString(`\u00`)
				buf.WriteByte(hexDigits[s[i]>>4])
				buf.WriteByte(hexDigits[s[i]&0xf])
			} else {
				buf.WriteByte(s[i])
			}
		}
	}
}

var hexDigits = []byte("0123456789abcdef")

// ---------------------------------------------------------------------------
// SowKey generation (when publish doesn't carry one and SOW cache is enabled).
// ---------------------------------------------------------------------------

func makeSowKey(topic string, seq uint64) string {
	base := topic
	if idx := strings.LastIndexByte(topic, '.'); idx >= 0 {
		base = topic[idx+1:]
	}
	var b [64]byte
	out := b[:0]
	out = append(out, base...)
	out = append(out, '-')
	out = strconv.AppendUint(out, seq, 10)
	return string(out)
}

// ---------------------------------------------------------------------------
// Token helpers for ack type / options checking.
// ---------------------------------------------------------------------------

func containsToken(csv, token string) bool {
	if csv == "" || token == "" {
		return false
	}
	if csv == token {
		return true
	}
	for {
		idx := strings.IndexByte(csv, ',')
		var segment string
		if idx < 0 {
			segment = csv
		} else {
			segment = csv[:idx]
		}
		if segment == token {
			return true
		}
		if idx < 0 {
			return false
		}
		csv = csv[idx+1:]
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func isClosedError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "use of closed network connection") ||
		strings.Contains(s, "wsarecv") ||
		strings.Contains(s, "wsasend") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "forcibly closed") ||
		strings.Contains(s, "connection aborted")
}

// ---------------------------------------------------------------------------
// Write buffer pool for fan-out goroutines.
// ---------------------------------------------------------------------------

var writeBufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 512))
	},
}

func getWriteBuf() *bytes.Buffer    { return writeBufPool.Get().(*bytes.Buffer) }
func putWriteBuf(buf *bytes.Buffer) { buf.Reset(); writeBufPool.Put(buf) }

// ---------------------------------------------------------------------------
// Per-connection stats
// ---------------------------------------------------------------------------

type connStats struct {
	messagesIn  atomic.Uint64
	messagesOut atomic.Uint64
	bytesIn     atomic.Uint64
	bytesOut    atomic.Uint64
	publishIn   atomic.Uint64
	publishOut  atomic.Uint64
}
