package main

import (
	"encoding/binary"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// sowRecord — a single SOW record keyed by topic + sowKey.
// ---------------------------------------------------------------------------

type sowRecord struct {
	topic      string
	sowKey     string
	payload    []byte
	bookmark   string
	timestamp  string
	seqNum     uint64
	expiresAt  time.Time // zero means no expiration
	lastAccess time.Time // for LRU eviction
}

func (r *sowRecord) isExpired() bool {
	return !r.expiresAt.IsZero() && time.Now().After(r.expiresAt)
}

// ---------------------------------------------------------------------------
// sowEvictionPolicy — configurable eviction strategy.
// ---------------------------------------------------------------------------

type sowEvictionPolicy int

const (
	evictionNone     sowEvictionPolicy = iota
	evictionLRU                        // least recently used
	evictionOldest                     // oldest by seqNum
	evictionCapacity                   // evict when exceeding capacity
)

// ---------------------------------------------------------------------------
// sowCache — in-memory State-of-the-World cache.
//
// Real AMPS maintains a SOW cache per topic: for each sow key, it stores
// the latest value of the record. This enables:
//   - SOW queries: return all current records for a topic
//   - SOW-and-subscribe: query + live updates
//   - Delta merge: delta_publish merges into the existing record
//   - SOW delete: remove a record from cache
//   - OOF (out-of-focus): notify delta subscribers when a record is removed
//   - Expiration: records with TTL are automatically expired
//   - TopN: return only top N records (sorted by seqNum)
//   - top_n=0: returns no records but populates records_returned count
//     and topic_matches (per official AMPS spec)
//   - Content filtering: SOW queries support filter expressions
//   - Eviction policies: LRU, oldest-first, capacity-bound
//
// The implementation uses two layers:
//   - topicIndex: topic → *topicSOW (lock-free sync.Map at top level)
//   - topicSOW:   sowKey → *sowRecord (per-topic RWMutex)
// ---------------------------------------------------------------------------

type topicSOW struct {
	mu       sync.RWMutex
	records  map[string]*sowRecord
	maxSize  int               // 0 = unlimited
	eviction sowEvictionPolicy // eviction policy when maxSize exceeded
}

type sowCache struct {
	topics         sync.Map // string → *topicSOW
	defaultMaxSize int
	defaultPolicy  sowEvictionPolicy

	// Disk persistence
	diskPath string
}

func newSOWCache() *sowCache {
	return &sowCache{}
}

func newSOWCacheWithEviction(maxSize int, policy sowEvictionPolicy) *sowCache {
	return &sowCache{
		defaultMaxSize: maxSize,
		defaultPolicy:  policy,
	}
}

func newDiskSOWCache(diskPath string, maxSize int, policy sowEvictionPolicy) *sowCache {
	c := &sowCache{
		defaultMaxSize: maxSize,
		defaultPolicy:  policy,
		diskPath:       diskPath,
	}
	c.loadFromDisk()
	return c
}

// ---------------------------------------------------------------------------
// Disk persistence for SOW.
// ---------------------------------------------------------------------------

func (c *sowCache) loadFromDisk() {
	if c.diskPath == "" {
		return
	}

	sowDir := filepath.Join(c.diskPath, "sow")
	if _, err := os.Stat(sowDir); os.IsNotExist(err) {
		return
	}

	entries, err := os.ReadDir(sowDir)
	if err != nil {
		log.Printf("fakeamps: sow load: read dir failed: %v", err)
		return
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		topic := e.Name()
		c.loadTopicFromDisk(topic, filepath.Join(sowDir, topic))
	}
}

func (c *sowCache) loadTopicFromDisk(topic, path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	ts := c.getOrCreateTopic(topic)

	reader := &readerAtWrapper{f: f}
	var offset int64 = 0

	for {
		// Read header: sowKeyLen(2) + sowKey + seqNum(8) + expiresAt(8) + timestampLen(2) + timestamp + bookmarkLen(2) + bookmark + payloadLen(4) + payload
		headerBuf := make([]byte, 2+8+8+2+2+4)
		n, err := reader.ReadAt(headerBuf, offset)
		if err != nil || n < len(headerBuf) {
			break
		}
		pos := 0

		sowKeyLen := binary.BigEndian.Uint16(headerBuf[pos : pos+2])
		pos += 2
		sowKeyBytes := make([]byte, sowKeyLen)
		n, err = reader.ReadAt(sowKeyBytes, offset+int64(pos))
		if err != nil || n < int(sowKeyLen) {
			break
		}
		sowKey := string(sowKeyBytes)
		pos += int(sowKeyLen)

		seqNum := binary.BigEndian.Uint64(headerBuf[pos : pos+8])
		pos += 8

		expiresAtNs := binary.BigEndian.Uint64(headerBuf[pos : pos+8])
		pos += 8
		var expiresAt time.Time
		if expiresAtNs > 0 {
			expiresAt = time.Unix(0, int64(expiresAtNs))
		}

		tsLen := binary.BigEndian.Uint16(headerBuf[pos : pos+2])
		pos += 2
		tsBytes := make([]byte, tsLen)
		n, err = reader.ReadAt(tsBytes, offset+int64(pos))
		if err != nil || n < int(tsLen) {
			break
		}
		timestamp := string(tsBytes)
		pos += int(tsLen)

		bmLen := binary.BigEndian.Uint16(headerBuf[pos : pos+2])
		pos += 2
		bmBytes := make([]byte, bmLen)
		n, err = reader.ReadAt(bmBytes, offset+int64(pos))
		if err != nil || n < int(bmLen) {
			break
		}
		bookmark := string(bmBytes)
		pos += int(bmLen)

		payloadLen := binary.BigEndian.Uint32(headerBuf[pos : pos+4])
		pos += 4
		payload := make([]byte, payloadLen)
		n, err = reader.ReadAt(payload, offset+int64(pos))
		if err != nil || n < int(payloadLen) {
			break
		}

		record := &sowRecord{
			topic:      topic,
			sowKey:     sowKey,
			payload:    payload,
			bookmark:   bookmark,
			timestamp:  timestamp,
			seqNum:     seqNum,
			expiresAt:  expiresAt,
			lastAccess: time.Now(),
		}
		ts.mu.Lock()
		ts.records[sowKey] = record
		ts.mu.Unlock()

		offset += int64(2 + int(sowKeyLen) + 8 + 8 + 2 + int(tsLen) + 2 + int(bmLen) + 4 + int(payloadLen))
	}

	log.Printf("fakeamps: sow loaded topic=%s count=%d", topic, len(ts.records))
}

// readerAtWrapper wraps an os.File to provide ReadAt.
type readerAtWrapper struct {
	f *os.File
}

func (r *readerAtWrapper) ReadAt(p []byte, offset int64) (n int, err error) {
	return r.f.ReadAt(p, offset)
}

// saveTopicToDisk saves a topic's SOW records to disk.
func (c *sowCache) saveTopicToDisk(topic string, records map[string]*sowRecord) {
	if c.diskPath == "" {
		return
	}

	sowDir := filepath.Join(c.diskPath, "sow")
	if err := os.MkdirAll(sowDir, 0755); err != nil {
		log.Printf("fakeamps: sow save: mkdir failed: %v", err)
		return
	}

	path := filepath.Join(sowDir, topic)
	f, err := os.Create(path)
	if err != nil {
		log.Printf("fakeamps: sow save: create failed: %v", err)
		return
	}
	defer f.Close()

	writer := &syncWriter{f: f}
	for _, r := range records {
		if r.isExpired() {
			continue
		}

		sowKeyLen := uint16(len(r.sowKey))
		tsLen := uint16(len(r.timestamp))
		bmLen := uint16(len(r.bookmark))
		payloadLen := uint32(len(r.payload))

		headerLen := 2 + int(sowKeyLen) + 8 + 8 + 2 + int(tsLen) + 2 + int(bmLen) + 4
		buf := make([]byte, headerLen+int(payloadLen))
		pos := 0

		binary.BigEndian.PutUint16(buf[pos:pos+2], sowKeyLen)
		pos += 2
		copy(buf[pos:pos+int(sowKeyLen)], r.sowKey)
		pos += int(sowKeyLen)

		binary.BigEndian.PutUint64(buf[pos:pos+8], r.seqNum)
		pos += 8

		var expiresAtNs uint64
		if !r.expiresAt.IsZero() {
			expiresAtNs = uint64(r.expiresAt.UnixNano())
		}
		binary.BigEndian.PutUint64(buf[pos:pos+8], expiresAtNs)
		pos += 8

		binary.BigEndian.PutUint16(buf[pos:pos+2], tsLen)
		pos += 2
		copy(buf[pos:pos+int(tsLen)], r.timestamp)
		pos += int(tsLen)

		binary.BigEndian.PutUint16(buf[pos:pos+2], bmLen)
		pos += 2
		copy(buf[pos:pos+int(bmLen)], r.bookmark)
		pos += int(bmLen)

		binary.BigEndian.PutUint32(buf[pos:pos+4], payloadLen)
		pos += 4
		copy(buf[pos:pos+int(payloadLen)], r.payload)

		writer.Write(buf)
	}
	writer.Sync()
}

// syncWriter buffers writes and syncs on demand.
type syncWriter struct {
	f    *os.File
	buf  []byte
	pos  int
	size int
}

func (w *syncWriter) Write(p []byte) {
	if w.pos+len(p) > cap(w.buf) {
		w.Sync()
	}
	if len(p) > cap(w.buf) {
		w.f.Write(p)
		return
	}
	w.buf = w.buf[:cap(w.buf)]
	copy(w.buf[w.pos:], p)
	w.pos += len(p)
	w.size += len(p)
}

func (w *syncWriter) Sync() {
	if w.pos > 0 {
		w.f.Write(w.buf[:w.pos])
		w.f.Sync()
		w.pos = 0
	}
}

func (c *sowCache) getOrCreateTopic(topic string) *topicSOW {
	actual, loaded := c.topics.LoadOrStore(topic, &topicSOW{
		records:  make(map[string]*sowRecord),
		maxSize:  c.defaultMaxSize,
		eviction: c.defaultPolicy,
	})
	if !loaded && c.defaultMaxSize > 0 {
		ts := actual.(*topicSOW)
		ts.maxSize = c.defaultMaxSize
		ts.eviction = c.defaultPolicy
	}
	return actual.(*topicSOW)
}

// evictIfNeeded removes the least desirable record when capacity is exceeded.
func (t *topicSOW) evictIfNeeded() *sowRecord {
	if t.maxSize <= 0 || len(t.records) < t.maxSize {
		return nil
	}

	switch t.eviction {
	case evictionLRU:
		var oldest *sowRecord
		for _, r := range t.records {
			if oldest == nil || r.lastAccess.Before(oldest.lastAccess) {
				oldest = r
			}
		}
		if oldest != nil {
			delete(t.records, oldest.sowKey)
			return oldest
		}
	case evictionOldest, evictionCapacity:
		var oldest *sowRecord
		for _, r := range t.records {
			if oldest == nil || r.seqNum < oldest.seqNum {
				oldest = r
			}
		}
		if oldest != nil {
			delete(t.records, oldest.sowKey)
			return oldest
		}
	}
	return nil
}

// upsert inserts or updates a SOW record. Returns (isInsert, isUpdate, previousPayload).
func (c *sowCache) upsert(topic, sowKey string, payload []byte, bookmark, timestamp string, seqNum uint64, expiration time.Duration) (inserted bool, updated bool, previousPayload []byte) {
	inserted, updated, previousPayload, _ = c.upsertWithEvicted(topic, sowKey, payload, bookmark, timestamp, seqNum, expiration)
	return
}

func (c *sowCache) upsertWithEvicted(topic, sowKey string, payload []byte, bookmark, timestamp string, seqNum uint64, expiration time.Duration) (inserted bool, updated bool, previousPayload []byte, evicted *sowRecord) {
	if sowKey == "" {
		sowKey = "auto-" + strconv.FormatUint(seqNum, 10)
	}

	t := c.getOrCreateTopic(topic)
	dataCopy := make([]byte, len(payload))
	copy(dataCopy, payload)

	var expires time.Time
	if expiration > 0 {
		expires = time.Now().Add(expiration)
	}

	t.mu.Lock()
	existing := t.records[sowKey]
	if existing == nil {
		// Evict if needed before inserting.
		evicted = t.evictIfNeeded()
		if evicted != nil {
			log.Printf("fakeamps: sow evicted key=%s topic=%s (policy=%d)", evicted.sowKey, topic, t.eviction)
		}

		t.records[sowKey] = &sowRecord{
			topic: topic, sowKey: sowKey, payload: dataCopy,
			bookmark: bookmark, timestamp: timestamp, seqNum: seqNum,
			expiresAt: expires, lastAccess: time.Now(),
		}
		inserted = true
	} else {
		previousPayload = existing.payload
		existing.payload = dataCopy
		existing.bookmark = bookmark
		existing.timestamp = timestamp
		existing.seqNum = seqNum
		existing.lastAccess = time.Now()
		if expiration > 0 {
			existing.expiresAt = expires
		}
		updated = true
	}
	t.mu.Unlock()
	return
}

// deltaUpsert merges a delta payload into an existing SOW record using
// JSON merge-patch semantics (RFC 7396). If the record doesn't exist,
// the delta becomes the full record.
func (c *sowCache) deltaUpsert(topic, sowKey string, deltaPayload []byte, bookmark, timestamp string, seqNum uint64, expiration time.Duration) (inserted bool, updated bool, mergedPayload []byte) {
	if sowKey == "" {
		sowKey = "auto-" + strconv.FormatUint(seqNum, 10)
	}

	t := c.getOrCreateTopic(topic)

	var expires time.Time
	if expiration > 0 {
		expires = time.Now().Add(expiration)
	}

	t.mu.Lock()
	existing := t.records[sowKey]
	if existing == nil {
		dataCopy := make([]byte, len(deltaPayload))
		copy(dataCopy, deltaPayload)

		evicted := t.evictIfNeeded()
		if evicted != nil {
			log.Printf("fakeamps: sow evicted key=%s topic=%s (policy=%d)", evicted.sowKey, topic, t.eviction)
		}

		t.records[sowKey] = &sowRecord{
			topic: topic, sowKey: sowKey, payload: dataCopy,
			bookmark: bookmark, timestamp: timestamp, seqNum: seqNum,
			expiresAt: expires, lastAccess: time.Now(),
		}
		mergedPayload = dataCopy
		inserted = true
	} else {
		merged := mergeJSON(existing.payload, deltaPayload)
		existing.payload = merged
		existing.bookmark = bookmark
		existing.timestamp = timestamp
		existing.seqNum = seqNum
		existing.lastAccess = time.Now()
		if expiration > 0 {
			existing.expiresAt = expires
		}
		mergedPayload = merged
		updated = true
	}
	t.mu.Unlock()
	return
}

// delete removes a SOW record. Returns true if it existed.
func (c *sowCache) delete(topic, sowKey string) (existed bool) {
	raw, ok := c.topics.Load(topic)
	if !ok {
		return false
	}
	t := raw.(*topicSOW)
	t.mu.Lock()
	if _, exists := t.records[sowKey]; exists {
		delete(t.records, sowKey)
		existed = true
	}
	t.mu.Unlock()
	return
}

// deleteByFilter removes all SOW records matching a filter expression.
// Returns the number of deleted records and their sow keys.
func (c *sowCache) deleteByFilter(topic, filter string) (int, []string) {
	raw, ok := c.topics.Load(topic)
	if !ok {
		return 0, nil
	}
	t := raw.(*topicSOW)
	t.mu.Lock()
	var deleted []string
	for key, r := range t.records {
		if r.isExpired() || evaluateFilter(filter, r.payload) {
			deleted = append(deleted, key)
			delete(t.records, key)
		}
	}
	t.mu.Unlock()
	return len(deleted), deleted
}

// deleteByKeys removes records by comma-separated sow_keys.
func (c *sowCache) deleteByKeys(topic, sowKeys string) int {
	count := 0
	for _, key := range strings.Split(sowKeys, ",") {
		key = strings.TrimSpace(key)
		if key != "" && c.delete(topic, key) {
			count++
		}
	}
	return count
}

// queryResult holds the result of a SOW query.
type queryResult struct {
	records    []sowRecord
	totalCount int // total matching records (before top_n limit)
}

// query returns SOW records for a topic, optionally filtered and limited.
//   - filter: content filter expression (empty = match all)
//   - topN: max records to return (0 = count-only mode per AMPS spec, -1 = unlimited)
//   - orderBy: field name to sort by (empty = insertion order)
func (c *sowCache) query(topic, filter string, topN int, orderBy string) queryResult {
	raw, ok := c.topics.Load(topic)
	if !ok {
		return queryResult{}
	}
	t := raw.(*topicSOW)
	t.mu.RLock()
	var matching []sowRecord
	for _, r := range t.records {
		if r.isExpired() {
			continue
		}
		if filter != "" && !evaluateFilter(filter, r.payload) {
			continue
		}
		r.lastAccess = time.Now() // update LRU timestamp on read
		matching = append(matching, *r)
	}
	t.mu.RUnlock()

	totalCount := len(matching)

	// Sort if orderBy is specified.
	if orderBy != "" {
		desc := false
		field := orderBy
		if strings.HasPrefix(orderBy, "-") {
			desc = true
			field = orderBy[1:]
		} else if strings.HasSuffix(strings.ToUpper(orderBy), " DESC") {
			desc = true
			field = strings.TrimSuffix(strings.TrimSuffix(orderBy, " DESC"), " desc")
			field = strings.TrimSpace(field)
		}
		sort.Slice(matching, func(i, j int) bool {
			vi := extractJSONStringField(matching[i].payload, field)
			vj := extractJSONStringField(matching[j].payload, field)
			if desc {
				return vi > vj
			}
			return vi < vj
		})
	} else {
		// Default: sort by seqNum (insertion order).
		sort.Slice(matching, func(i, j int) bool {
			return matching[i].seqNum < matching[j].seqNum
		})
	}

	// top_n=0: count-only mode (return no records, but report total count).
	if topN == 0 {
		return queryResult{records: nil, totalCount: totalCount}
	}

	// top_n > 0: limit results.
	if topN > 0 && topN < len(matching) {
		matching = matching[:topN]
	}

	return queryResult{records: matching, totalCount: totalCount}
}

func querySOWWithBookmark(topic, filter string, topN int, orderBy, bookmark string) queryResult {
	if sow == nil {
		return queryResult{}
	}

	if bookmark == "" {
		return sow.query(topic, filter, topN, orderBy)
	}

	if bookmark == "0" {
		return queryResult{}
	}

	if journal == nil {
		return sow.query(topic, filter, topN, orderBy)
	}

	var maxSeq = parseBookmarkSeq(bookmark)
	if maxSeq == 0 {
		return sow.query(topic, filter, topN, orderBy)
	}

	var snapshotByKey = make(map[string]sowRecord)
	var entries = journal.replayAll(0)
	var firstSeqForTopic uint64
	for _, entry := range entries {
		if entry.topic != topic {
			continue
		}
		if firstSeqForTopic == 0 || entry.seqNum < firstSeqForTopic {
			firstSeqForTopic = entry.seqNum
		}
	}

	var effectiveMaxSeq = maxSeq
	if firstSeqForTopic > 0 && maxSeq < firstSeqForTopic {
		effectiveMaxSeq = firstSeqForTopic - 1 + maxSeq
	}

	for _, entry := range entries {
		if entry.topic != topic {
			continue
		}
		if entry.seqNum > effectiveMaxSeq {
			continue
		}

		var key = entry.sowKey
		if key == "" {
			key = makeSowKey(topic, entry.seqNum)
		}

		snapshotByKey[key] = sowRecord{
			topic:     entry.topic,
			sowKey:    key,
			payload:   entry.payload,
			bookmark:  entry.bookmark,
			timestamp: entry.timestamp,
			seqNum:    entry.seqNum,
		}
	}

	var matching []sowRecord
	for _, record := range snapshotByKey {
		if filter != "" && !evaluateFilter(filter, record.payload) {
			continue
		}
		matching = append(matching, record)
	}

	var totalCount = len(matching)

	if orderBy != "" {
		var desc = false
		var field = orderBy
		if strings.HasPrefix(orderBy, "-") {
			desc = true
			field = orderBy[1:]
		} else if strings.HasSuffix(strings.ToUpper(orderBy), " DESC") {
			desc = true
			field = strings.TrimSuffix(strings.TrimSuffix(orderBy, " DESC"), " desc")
			field = strings.TrimSpace(field)
		}

		sort.Slice(matching, func(i, j int) bool {
			var vi = extractJSONStringField(matching[i].payload, field)
			var vj = extractJSONStringField(matching[j].payload, field)
			if desc {
				return vi > vj
			}
			return vi < vj
		})
	} else {
		sort.Slice(matching, func(i, j int) bool {
			return matching[i].seqNum < matching[j].seqNum
		})
	}

	if topN == 0 {
		return queryResult{records: nil, totalCount: totalCount}
	}

	if topN > 0 && topN < len(matching) {
		matching = matching[:topN]
	}

	return queryResult{records: matching, totalCount: totalCount}
}

// count returns the number of non-expired SOW records for a topic.
func (c *sowCache) count(topic string) int {
	raw, ok := c.topics.Load(topic)
	if !ok {
		return 0
	}
	t := raw.(*topicSOW)
	t.mu.RLock()
	n := 0
	for _, r := range t.records {
		if !r.isExpired() {
			n++
		}
	}
	t.mu.RUnlock()
	return n
}

// gcExpired removes expired records from all topics. Returns count removed.
func (c *sowCache) gcExpired() int {
	var removed = c.gcExpiredRecords()
	return len(removed)
}

func (c *sowCache) gcExpiredRecords() []sowRecord {
	var removed []sowRecord
	c.topics.Range(func(_ interface{}, value interface{}) bool {
		var t = value.(*topicSOW)
		t.mu.Lock()
		for key, record := range t.records {
			if record.isExpired() {
				removed = append(removed, *record)
				delete(t.records, key)
			}
		}
		t.mu.Unlock()
		return true
	})
	return removed
}

// allTopics returns a list of all topic names that have SOW records.
func (c *sowCache) allTopics() []string {
	var topics []string
	c.topics.Range(func(key, value interface{}) bool {
		topics = append(topics, key.(string))
		return true
	})
	return topics
}

// extractSowKey attempts to extract a SOW key from JSON payload.
func extractSowKey(payload []byte) string {
	for _, field := range []string{"_sow_key", "id", "key", "ID", "Id"} {
		if v := extractJSONStringField(payload, field); v != "" {
			return v
		}
	}
	return ""
}

// extractJSONStringField does a naive scan for "field":"value" or "field":number in JSON.
func extractJSONStringField(data []byte, field string) string {
	target := `"` + field + `":`
	n := len(target)
	for i := 0; i+n < len(data); i++ {
		if string(data[i:i+n]) == target {
			j := i + n
			for j < len(data) && (data[j] == ' ' || data[j] == '\t') {
				j++
			}
			if j >= len(data) {
				return ""
			}
			if data[j] == '"' {
				j++
				start := j
				for j < len(data) && data[j] != '"' {
					if data[j] == '\\' {
						j++
					}
					j++
				}
				return string(data[start:j])
			}
			start := j
			for j < len(data) && data[j] != ',' && data[j] != '}' && data[j] != ' ' {
				j++
			}
			return string(data[start:j])
		}
	}
	return ""
}
