package main

import (
	"bytes"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
// without allocating intermediate structures. Dispatch is by key length
// + first byte for speed on the hot path.
// ---------------------------------------------------------------------------

func parseAMPSHeader(frame []byte) (headerFields, []byte) {
	var h headerFields
	n := len(frame)
	if n == 0 || frame[0] != '{' {
		return h, frame
	}

	i := 1
	for i < n {
		// Skip whitespace / commas.
		for i < n && (frame[i] == ' ' || frame[i] == ',' || frame[i] == '\n' || frame[i] == '\r' || frame[i] == '\t') {
			i++
		}
		if i >= n || frame[i] == '}' {
			i++
			break
		}
		if frame[i] != '"' {
			i++
			continue
		}

		// ---- Key ----
		i++
		keyStart := i
		for i < n && frame[i] != '"' {
			if frame[i] == '\\' {
				i++
			}
			i++
		}
		if i >= n {
			break
		}
		keyEnd := i
		i++

		// Skip ':'
		for i < n && (frame[i] == ' ' || frame[i] == ':') {
			i++
		}
		if i >= n {
			break
		}

		// ---- Value ----
		var valueStart, valueEnd int
		if frame[i] == '"' {
			i++
			valueStart = i
			for i < n && frame[i] != '"' {
				if frame[i] == '\\' {
					i++
				}
				i++
			}
			if i >= n {
				break
			}
			valueEnd = i
			i++
		} else {
			valueStart = i
			for i < n && frame[i] != ',' && frame[i] != '}' {
				i++
			}
			valueEnd = i
		}

		// ---- Dispatch by key length + first byte ----
		keyLen := keyEnd - keyStart
		switch keyLen {
		case 1:
			switch frame[keyStart] {
			case 'c':
				h.c = string(frame[valueStart:valueEnd])
			case 't':
				h.t = string(frame[valueStart:valueEnd])
			case 'a':
				h.a = string(frame[valueStart:valueEnd])
			case 'o':
				h.o = string(frame[valueStart:valueEnd])
			case 's':
				h.s = string(frame[valueStart:valueEnd])
			case 'k':
				h.k = string(frame[valueStart:valueEnd])
			case 'x':
				h.x = string(frame[valueStart:valueEnd])
			case 'e':
				h.e = string(frame[valueStart:valueEnd])
			case 'f':
				h.filter = string(frame[valueStart:valueEnd])
			}
		case 2:
			switch {
			case frame[keyStart] == 'b' && frame[keyStart+1] == 'm':
				h.bm = string(frame[valueStart:valueEnd])
			case frame[keyStart] == 'b' && frame[keyStart+1] == 's':
				h.bs = string(frame[valueStart:valueEnd])
			case frame[keyStart] == 'm' && frame[keyStart+1] == 't':
				h.mt = string(frame[valueStart:valueEnd])
			case frame[keyStart] == 'l' && frame[keyStart+1] == 'p':
				// lp = lease_period (inbound — ignore, we set our own)
			case frame[keyStart] == 'p' && frame[keyStart+1] == 'w':
				h.pw = string(frame[valueStart:valueEnd])
			case frame[keyStart] == 't' && frame[keyStart+1] == 's':
				// ts = timestamp (inbound — ignore)
			}
		case 3:
			if frame[keyStart] == 'c' && frame[keyStart+1] == 'i' && frame[keyStart+2] == 'd' {
				h.cid = string(frame[valueStart:valueEnd])
			} else if frame[keyStart] == 'u' && frame[keyStart+1] == 'r' && frame[keyStart+2] == 'i' {
				h.uri = string(frame[valueStart:valueEnd])
			}
		case 4:
			switch {
			case frame[keyStart] == 'o' && frame[keyStart+1] == 'p': // opts
				h.opts = string(frame[valueStart:valueEnd])
			case frame[keyStart] == 's' && frame[keyStart+1] == 'i': // sids
				h.subIDs = string(frame[valueStart:valueEnd])
			}
		case 5:
			if frame[keyStart] == 't' && frame[keyStart+1] == 'o' { // top_n
				h.topN = string(frame[valueStart:valueEnd])
			} else if frame[keyStart] == '_' && frame[keyStart+1] == 'r' && frame[keyStart+2] == 'e' && frame[keyStart+3] == 'p' && frame[keyStart+4] == 'l' {
				h.repl = string(frame[valueStart:valueEnd])
			}
		case 6:
			switch {
			case frame[keyStart] == 's' && frame[keyStart+1] == 'u': // sub_id
				h.subID = string(frame[valueStart:valueEnd])
			case frame[keyStart] == 'f' && frame[keyStart+1] == 'i': // filter
				h.filter = string(frame[valueStart:valueEnd])
			case frame[keyStart] == 's' && frame[keyStart+1] == 't': // status
				h.status = string(frame[valueStart:valueEnd])
			case frame[keyStart] == 'r' && frame[keyStart+1] == 'e': // reason
				h.reason = string(frame[valueStart:valueEnd])
			}
		case 7:
			switch frame[keyStart] {
			case 'v': // version
				h.version = string(frame[valueStart:valueEnd])
			case 'o': // orderby
				h.orderBy = string(frame[valueStart:valueEnd])
			case 'u': // user_id
				h.userID = string(frame[valueStart:valueEnd])
			case 'm': // matches (ignore inbound)
			}
		case 8:
			switch {
			case frame[keyStart] == 'q': // query_id
				h.queryID = string(frame[valueStart:valueEnd])
			case frame[keyStart] == 's': // sow_keys
				h.sowKeys = string(frame[valueStart:valueEnd])
			}
		default:
			switch {
			case keyLen == 11 && frame[keyStart] == 'c': // client_name
				h.clientName = string(frame[valueStart:valueEnd])
			case keyLen == 10 && frame[keyStart] == '_' && frame[keyStart+1] == 'r' && frame[keyStart+2] == 'e' && frame[keyStart+3] == 'p' && frame[keyStart+4] == 'l' && frame[keyStart+5] == '_' && frame[keyStart+6] == 's' && frame[keyStart+7] == 'y' && frame[keyStart+8] == 'n' && frame[keyStart+9] == 'c':
				h.replSync = string(frame[valueStart:valueEnd])
			}
		}
	}

	if i <= n {
		return h, frame[i:]
	}
	return h, nil
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
	length := uint32(n - 4)
	raw[0] = byte(length >> 24)
	raw[1] = byte(length >> 16)
	raw[2] = byte(length >> 8)
	raw[3] = byte(length)
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
	buf.WriteString(topic)
	buf.WriteByte('"')
	writeField(buf, "sub_id", subID)
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

func buildSOWRecord(buf *bytes.Buffer, topic, queryID, sowKey, bookmark, messageType string, payload []byte) []byte {
	startFrame(buf)
	buf.WriteString(`{"c":"sow","t":"`)
	buf.WriteString(topic)
	buf.WriteByte('"')
	writeField(buf, "query_id", queryID)
	writeField(buf, "k", sowKey)
	writeField(buf, "bm", bookmark)
	writeField(buf, "mt", messageType)
	// message_length "l" — tells the client parser where the SOW data ends.
	buf.WriteString(`,"l":`)
	buf.WriteString(strconv.Itoa(len(payload)))
	buf.WriteByte('}')
	buf.Write(payload)
	return finalizeFrame(buf)
}

// ---- OOF (Out-of-Focus) Delivery ----

func buildOOFDelivery(buf *bytes.Buffer, topic, subID, sowKey, bookmark string) []byte {
	return buildOOFDeliveryWithReason(buf, topic, subID, sowKey, bookmark, "")
}

func buildOOFDeliveryWithReason(buf *bytes.Buffer, topic, subID, sowKey, bookmark, reason string) []byte {
	startFrame(buf)
	buf.WriteString(`{"c":"oof","t":"`)
	buf.WriteString(topic)
	buf.WriteByte('"')
	writeField(buf, "sub_id", subID)
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
	buf.WriteString(value)
	buf.WriteByte('"')
}

func writeNumericField(buf *bytes.Buffer, key, value string) {
	buf.WriteString(`,"`)
	buf.WriteString(key)
	buf.WriteString(`":`)
	buf.WriteString(value)
}

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
