package main

import (
	"encoding/binary"
	"io"
	"io/fs"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Thejuampi/amps-client-go/internal/safecast"
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

	root, err := os.OpenRoot(c.diskPath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Printf("fakeamps: sow load: open root failed: %v", err)
		return
	}
	defer func() {
		_ = root.Close()
	}()

	sowRoot, err := root.OpenRoot("sow")
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Printf("fakeamps: sow load: open sow root failed: %v", err)
		return
	}
	defer func() {
		_ = sowRoot.Close()
	}()

	entries, err := fs.ReadDir(sowRoot.FS(), ".")
	if err != nil {
		log.Printf("fakeamps: sow load: read dir failed: %v", err)
		return
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		c.loadTopicFromDisk(sowRoot, e.Name())
	}
}

func (c *sowCache) loadTopicFromDisk(sowRoot *os.Root, fileName string) {
	f, err := sowRoot.Open(fileName)
	if err != nil {
		return
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			log.Printf("fakeamps: sow load: close failed for %s: %v", fileName, closeErr)
		}
	}()

	var loaded int

	for {
		var topicLenBuf [2]byte
		if _, err := io.ReadFull(f, topicLenBuf[:]); err != nil {
			if err != io.EOF {
				log.Printf("fakeamps: sow load: read topic length failed for %s: %v", fileName, err)
			}
			break
		}
		var topicLen = binary.BigEndian.Uint16(topicLenBuf[:])
		var topicBytes = make([]byte, topicLen)
		if _, err := io.ReadFull(f, topicBytes); err != nil {
			log.Printf("fakeamps: sow load: read topic failed for %s: %v", fileName, err)
			break
		}
		var topic = string(topicBytes)

		var sowKeyLenBuf [2]byte
		if _, err := io.ReadFull(f, sowKeyLenBuf[:]); err != nil {
			log.Printf("fakeamps: sow load: read sow key length failed for %s: %v", fileName, err)
			break
		}
		var sowKeyLen = binary.BigEndian.Uint16(sowKeyLenBuf[:])
		var sowKeyBytes = make([]byte, sowKeyLen)
		if _, err := io.ReadFull(f, sowKeyBytes); err != nil {
			log.Printf("fakeamps: sow load: read sow key failed for %s: %v", fileName, err)
			break
		}
		var sowKey = string(sowKeyBytes)

		var seqBuf [8]byte
		if _, err := io.ReadFull(f, seqBuf[:]); err != nil {
			log.Printf("fakeamps: sow load: read seqNum failed for %s: %v", fileName, err)
			break
		}
		var seqNum = binary.BigEndian.Uint64(seqBuf[:])

		var expiresAtBuf [8]byte
		if _, err := io.ReadFull(f, expiresAtBuf[:]); err != nil {
			log.Printf("fakeamps: sow load: read expiresAt failed for %s: %v", fileName, err)
			break
		}
		var expiresAtNs = binary.BigEndian.Uint64(expiresAtBuf[:])
		var expiresAt time.Time
		if expiresAtNs > 0 {
			if expiresAtNs > math.MaxInt64 {
				log.Printf("fakeamps: sow load: skipping corrupt expiresAt=%d for topic=%s key=%s", expiresAtNs, topic, sowKey)
				var tsLenBuf [2]byte
				if _, err := io.ReadFull(f, tsLenBuf[:]); err != nil {
					break
				}
				var tsLen = binary.BigEndian.Uint16(tsLenBuf[:])
				if _, err := io.CopyN(io.Discard, f, int64(tsLen)); err != nil {
					break
				}
				var bookmarkLenBuf [2]byte
				if _, err := io.ReadFull(f, bookmarkLenBuf[:]); err != nil {
					break
				}
				var bookmarkLen = binary.BigEndian.Uint16(bookmarkLenBuf[:])
				if _, err := io.CopyN(io.Discard, f, int64(bookmarkLen)); err != nil {
					break
				}
				var payloadLenBuf [4]byte
				if _, err := io.ReadFull(f, payloadLenBuf[:]); err != nil {
					break
				}
				var payloadLen = binary.BigEndian.Uint32(payloadLenBuf[:])
				if _, err := io.CopyN(io.Discard, f, int64(payloadLen)); err != nil {
					break
				}
				continue
			}
			expiresAt = time.Unix(0, int64(expiresAtNs))
		}

		var tsLenBuf [2]byte
		if _, err := io.ReadFull(f, tsLenBuf[:]); err != nil {
			log.Printf("fakeamps: sow load: read timestamp length failed for %s: %v", fileName, err)
			break
		}
		var tsLen = binary.BigEndian.Uint16(tsLenBuf[:])
		var tsBytes = make([]byte, tsLen)
		if _, err := io.ReadFull(f, tsBytes); err != nil {
			log.Printf("fakeamps: sow load: read timestamp failed for %s: %v", fileName, err)
			break
		}
		var timestamp = string(tsBytes)

		var bookmarkLenBuf [2]byte
		if _, err := io.ReadFull(f, bookmarkLenBuf[:]); err != nil {
			log.Printf("fakeamps: sow load: read bookmark length failed for %s: %v", fileName, err)
			break
		}
		var bookmarkLen = binary.BigEndian.Uint16(bookmarkLenBuf[:])
		var bookmarkBytes = make([]byte, bookmarkLen)
		if _, err := io.ReadFull(f, bookmarkBytes); err != nil {
			log.Printf("fakeamps: sow load: read bookmark failed for %s: %v", fileName, err)
			break
		}
		var bookmark = string(bookmarkBytes)

		var payloadLenBuf [4]byte
		if _, err := io.ReadFull(f, payloadLenBuf[:]); err != nil {
			log.Printf("fakeamps: sow load: read payload length failed for %s: %v", fileName, err)
			break
		}
		var payloadLen = binary.BigEndian.Uint32(payloadLenBuf[:])
		var payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(f, payload); err != nil {
			log.Printf("fakeamps: sow load: read payload failed for %s: %v", fileName, err)
			break
		}

		var record = &sowRecord{
			topic:      topic,
			sowKey:     sowKey,
			payload:    payload,
			bookmark:   bookmark,
			timestamp:  timestamp,
			seqNum:     seqNum,
			expiresAt:  expiresAt,
			lastAccess: time.Now(),
		}
		var ts = c.getOrCreateTopic(topic)
		ts.mu.Lock()
		ts.records[sowKey] = record
		ts.mu.Unlock()
		loaded++
	}

	log.Printf("fakeamps: sow loaded file=%s count=%d", fileName, loaded)
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

	root, err := openFakeampsDiskRoot(c.diskPath)
	if err != nil {
		log.Printf("fakeamps: sow save: mkdir failed: %v", err)
		return
	}
	defer func() {
		_ = root.Close()
	}()
	if err := root.MkdirAll("sow", fakeampsDataDirMode); err != nil {
		log.Printf("fakeamps: sow save: mkdir failed: %v", err)
		return
	}
	sowRoot, err := root.OpenRoot("sow")
	if err != nil {
		log.Printf("fakeamps: sow save: open sow root failed: %v", err)
		return
	}
	defer func() {
		_ = sowRoot.Close()
	}()

	var fileName = encodeSOWTopicFilename(topic)
	if len(records) == 0 {
		if err := sowRoot.Remove(fileName); err != nil && !os.IsNotExist(err) {
			log.Printf("fakeamps: sow save: remove failed for topic=%s: %v", topic, err)
		}
		return
	}

	f, err := sowRoot.OpenFile(fileName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, fakeampsDataFileMode)
	if err != nil {
		log.Printf("fakeamps: sow save: create failed: %v", err)
		return
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			log.Printf("fakeamps: sow save: close failed for %s: %v", fileName, closeErr)
		}
	}()

	writer := &syncWriter{f: f}
	for _, r := range records {
		if r.isExpired() {
			continue
		}

		var recordTopic = r.topic
		if recordTopic == "" {
			recordTopic = topic
		}
		var topicLen, topicLenOK = safecast.Uint16FromIntChecked(len(recordTopic))
		if !topicLenOK {
			log.Printf("fakeamps: sow save: topic too large for topic=%s", topic)
			return
		}
		var sowKeyLen, sowKeyLenOK = safecast.Uint16FromIntChecked(len(r.sowKey))
		if !sowKeyLenOK {
			log.Printf("fakeamps: sow save: sow key too large for topic=%s", topic)
			return
		}
		var tsLen, tsLenOK = safecast.Uint16FromIntChecked(len(r.timestamp))
		if !tsLenOK {
			log.Printf("fakeamps: sow save: timestamp too large for topic=%s", topic)
			return
		}
		var bookmarkLen, bookmarkLenOK = safecast.Uint16FromIntChecked(len(r.bookmark))
		if !bookmarkLenOK {
			log.Printf("fakeamps: sow save: bookmark too large for topic=%s", topic)
			return
		}
		var payloadLen, payloadLenOK = safecast.Uint32FromIntChecked(len(r.payload))
		if !payloadLenOK {
			log.Printf("fakeamps: sow save: payload too large for topic=%s", topic)
			return
		}

		headerLen := 2 + int(topicLen) + 2 + int(sowKeyLen) + 8 + 8 + 2 + int(tsLen) + 2 + int(bookmarkLen) + 4
		buf := make([]byte, headerLen+int(payloadLen))
		pos := 0

		binary.BigEndian.PutUint16(buf[pos:pos+2], topicLen)
		pos += 2
		copy(buf[pos:pos+int(topicLen)], recordTopic)
		pos += int(topicLen)

		binary.BigEndian.PutUint16(buf[pos:pos+2], sowKeyLen)
		pos += 2
		copy(buf[pos:pos+int(sowKeyLen)], r.sowKey)
		pos += int(sowKeyLen)

		binary.BigEndian.PutUint64(buf[pos:pos+8], r.seqNum)
		pos += 8

		var expiresAtNs uint64
		if !r.expiresAt.IsZero() {
			if value, ok := safecast.Uint64FromInt64Checked(r.expiresAt.UnixNano()); ok {
				expiresAtNs = value
			}
		}
		binary.BigEndian.PutUint64(buf[pos:pos+8], expiresAtNs)
		pos += 8

		binary.BigEndian.PutUint16(buf[pos:pos+2], tsLen)
		pos += 2
		copy(buf[pos:pos+int(tsLen)], r.timestamp)
		pos += int(tsLen)

		binary.BigEndian.PutUint16(buf[pos:pos+2], bookmarkLen)
		pos += 2
		copy(buf[pos:pos+int(bookmarkLen)], r.bookmark)
		pos += int(bookmarkLen)

		binary.BigEndian.PutUint32(buf[pos:pos+4], payloadLen)
		pos += 4
		copy(buf[pos:pos+int(payloadLen)], r.payload)

		if err := writer.Write(buf); err != nil {
			log.Printf("fakeamps: sow save: write failed for topic=%s: %v", topic, err)
			return
		}
	}
	if err := writer.Sync(); err != nil {
		log.Printf("fakeamps: sow save: sync failed for topic=%s: %v", topic, err)
	}
}

func cloneSOWRecords(records map[string]*sowRecord) map[string]*sowRecord {
	var snapshot = make(map[string]*sowRecord, len(records))
	for key, record := range records {
		if record == nil {
			continue
		}
		var payloadCopy = append([]byte(nil), record.payload...)
		var recordCopy = *record
		recordCopy.payload = payloadCopy
		snapshot[key] = &recordCopy
	}
	return snapshot
}

// syncWriter buffers writes and syncs on demand.
type syncWriter struct {
	f    *os.File
	buf  []byte
	pos  int
	size int
}

func (w *syncWriter) Write(p []byte) error {
	if w.pos+len(p) > cap(w.buf) {
		if err := w.Sync(); err != nil {
			return err
		}
	}
	if len(p) > cap(w.buf) {
		_, err := w.f.Write(p)
		return err
	}
	w.buf = w.buf[:cap(w.buf)]
	copy(w.buf[w.pos:], p)
	w.pos += len(p)
	w.size += len(p)
	return nil
}

func (w *syncWriter) Sync() error {
	if w.pos > 0 {
		if _, err := w.f.Write(w.buf[:w.pos]); err != nil {
			return err
		}
		if err := w.f.Sync(); err != nil {
			return err
		}
		w.pos = 0
	}
	return nil
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

	var snapshot map[string]*sowRecord
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
	if c.diskPath != "" {
		snapshot = cloneSOWRecords(t.records)
	}
	t.mu.Unlock()
	if snapshot != nil {
		c.saveTopicToDisk(topic, snapshot)
	}
	return
}

// deltaUpsert merges a delta payload into an existing SOW record using
// JSON merge-patch semantics (RFC 7396). If the record doesn't exist,
// the delta becomes the full record.
func (c *sowCache) deltaUpsert(topic, sowKey string, deltaPayload []byte, bookmark, timestamp string, seqNum uint64, expiration time.Duration) (inserted bool, updated bool, mergedPayload []byte) {
	inserted, updated, _, mergedPayload, _ = c.deltaUpsertWithPrevious(topic, sowKey, deltaPayload, bookmark, timestamp, seqNum, expiration)
	return
}

func (c *sowCache) deltaUpsertWithPrevious(topic, sowKey string, deltaPayload []byte, bookmark, timestamp string, seqNum uint64, expiration time.Duration) (inserted bool, updated bool, previousPayload []byte, mergedPayload []byte, evicted *sowRecord) {
	if sowKey == "" {
		sowKey = "auto-" + strconv.FormatUint(seqNum, 10)
	}

	t := c.getOrCreateTopic(topic)

	var expires time.Time
	if expiration > 0 {
		expires = time.Now().Add(expiration)
	}

	var snapshot map[string]*sowRecord
	t.mu.Lock()
	existing := t.records[sowKey]
	if existing == nil {
		dataCopy := make([]byte, len(deltaPayload))
		copy(dataCopy, deltaPayload)

		evicted = t.evictIfNeeded()
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
		previousPayload = append([]byte(nil), existing.payload...)
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
	if c.diskPath != "" {
		snapshot = cloneSOWRecords(t.records)
	}
	t.mu.Unlock()
	if snapshot != nil {
		c.saveTopicToDisk(topic, snapshot)
	}
	return
}

// delete removes a SOW record. Returns true if it existed.
func (c *sowCache) delete(topic, sowKey string) (existed bool) {
	raw, ok := c.topics.Load(topic)
	if !ok {
		return false
	}
	t := raw.(*topicSOW)
	var snapshot map[string]*sowRecord
	t.mu.Lock()
	if _, exists := t.records[sowKey]; exists {
		delete(t.records, sowKey)
		existed = true
	}
	if existed && c.diskPath != "" {
		snapshot = cloneSOWRecords(t.records)
	}
	t.mu.Unlock()
	if snapshot != nil {
		c.saveTopicToDisk(topic, snapshot)
	}
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
	var snapshot map[string]*sowRecord
	t.mu.Lock()
	var deleted []string
	for key, r := range t.records {
		if !r.isExpired() && evaluateFilter(filter, r.payload) {
			deleted = append(deleted, key)
			delete(t.records, key)
		}
	}
	if len(deleted) > 0 && c.diskPath != "" {
		snapshot = cloneSOWRecords(t.records)
	}
	t.mu.Unlock()
	if snapshot != nil {
		c.saveTopicToDisk(topic, snapshot)
	}
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
	t.mu.Lock()
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
	t.mu.Unlock()

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
	c.topics.Range(func(key interface{}, value interface{}) bool {
		var t = value.(*topicSOW)
		var snapshot map[string]*sowRecord
		var changed bool
		t.mu.Lock()
		for recordKey, record := range t.records {
			if record.isExpired() {
				removed = append(removed, *record)
				delete(t.records, recordKey)
				changed = true
			}
		}
		if c.diskPath != "" && changed {
			snapshot = cloneSOWRecords(t.records)
		}
		t.mu.Unlock()
		if snapshot != nil {
			c.saveTopicToDisk(key.(string), snapshot)
		}
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
