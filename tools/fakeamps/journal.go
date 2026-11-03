package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Thejuampi/amps-client-go/internal/safecast"
)

// ---------------------------------------------------------------------------
// journalEntry — a single published message stored in the journal.
// ---------------------------------------------------------------------------

type journalEntry struct {
	bookmark  string
	topic     string
	sowKey    string
	payload   []byte
	timestamp string
	seqNum    uint64 // monotonic global sequence, used for bookmark ordering
}

// ---------------------------------------------------------------------------
// messageJournal — append-only, bounded message log with disk persistence.
//
// Real AMPS maintains a transaction log (journal) of all published messages.
// Bookmark subscribers can request replay from any bookmark position.
// This implementation uses a ring buffer with a global sequence counter
// to support efficient replay-from-bookmark queries.
//
// When diskPath is set, entries are also written to a binary log file
// for durability across restarts.
// ---------------------------------------------------------------------------

type messageJournal struct {
	mu         sync.RWMutex
	entries    []journalEntry
	maxSize    int
	head       int            // next write position (ring buffer)
	count      int            // current number of entries
	seqMap     map[uint64]int // seqNum → index in entries (for fast bookmark lookup)
	publishers *brokerPublishTracker

	// Disk persistence
	diskPath     string
	diskFile     *os.File
	diskWriter   *bufio.Writer
	diskSeqFile  *os.File
	diskFlushMu  sync.Mutex
	diskSyncDone chan struct{}
	diskErr      error
}

var journalSyncFile = func(file *os.File) error { return file.Sync() }

// Typed journal records reserve the length prefix's high bit. Ordinary frame
// lengths are below this bound and remain readable without a format marker.
const typedJournalRecordFlag uint32 = 1 << 31

func newMessageJournal(maxSize int) *messageJournal {
	if maxSize <= 0 {
		maxSize = 1_000_000
	}
	return &messageJournal{
		entries:    make([]journalEntry, maxSize),
		maxSize:    maxSize,
		seqMap:     make(map[uint64]int, maxSize),
		publishers: newBrokerPublishTracker(),
	}
}

// newDiskJournal creates a journal with disk persistence.
func newDiskJournal(maxSize int, diskPath string) *messageJournal {
	j := newMessageJournal(maxSize)
	j.diskPath = diskPath
	j.diskSyncDone = make(chan struct{})
	j.initDisk()
	return j
}

// initDisk initializes disk persistence files.
func (j *messageJournal) initDisk() {
	if j.diskPath == "" {
		return
	}

	// Create directory if needed.
	root, err := openFakeampsDiskRoot(j.diskPath)
	if err != nil {
		log.Printf("fakeamps: journal disk init: mkdir failed: %v", err)
		j.diskErr = err
		return
	}
	defer func() {
		_ = root.Close()
	}()

	// Open or create journal file (append mode).
	f, err := root.OpenFile("journal.dat", os.O_CREATE|os.O_RDWR|os.O_APPEND, fakeampsDataFileMode)
	if err != nil {
		log.Printf("fakeamps: journal disk init: open failed: %v", err)
		j.diskErr = err
		return
	}
	j.diskFile = f
	j.diskWriter = bufio.NewWriterSize(f, 256*1024)

	// Open or create sequence file.
	sf, err := root.OpenFile("sequence.dat", os.O_CREATE|os.O_RDWR, fakeampsDataFileMode)
	if err != nil {
		log.Printf("fakeamps: journal disk init: seq file failed: %v", err)
		_ = f.Close()
		j.diskFile = nil
		j.diskWriter = nil
		j.diskErr = err
		return
	}
	j.diskSeqFile = sf

	// Load existing entries from disk.
	j.diskErr = j.loadFromDisk()
}

// loadFromDisk loads journal entries from disk into memory.
func (j *messageJournal) loadFromDisk() error {
	if j.diskFile == nil {
		return errors.New("journal file is unavailable")
	}

	if _, err := j.diskSeqFile.Seek(0, io.SeekStart); err != nil {
		log.Printf("fakeamps: journal load: seek seq file failed: %v", err)
		return err
	}
	var seqBuf [8]byte
	if _, err := io.ReadFull(j.diskSeqFile, seqBuf[:]); err == nil {
		maxSeq := binary.BigEndian.Uint64(seqBuf[:])
		if maxSeq > 0 {
			globalBookmarkSeq.Store(maxSeq)
			log.Printf("fakeamps: journal loaded seq=%d", maxSeq)
		}
	}

	if _, err := j.diskFile.Seek(0, io.SeekStart); err != nil {
		log.Printf("fakeamps: journal load: seek journal file failed: %v", err)
		return err
	}
	reader := bufio.NewReader(j.diskFile)
	var info, err = j.diskFile.Stat()
	if err != nil {
		return err
	}
	var committed int64

	for {
		// Read record length.
		var lenBuf [4]byte
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			if err == io.EOF {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return j.repairDiskTail(committed)
			}
			return err
		}
		var recLen = binary.BigEndian.Uint32(lenBuf[:])
		var typed = recLen&typedJournalRecordFlag != 0
		recLen &^= typedJournalRecordFlag
		if int64(recLen) > info.Size()-committed-4 {
			return j.repairDiskTail(committed)
		}

		// Read record.
		recBuf := make([]byte, recLen)
		if _, err := io.ReadFull(reader, recBuf); err != nil {
			return err
		}
		if typed {
			if err := j.restoreDeleteRecord(recBuf); err != nil {
				return err
			}
			committed += 4 + int64(recLen)
			continue
		}

		// Parse: topicLen(2) + topic + sowKeyLen(2) + sowKey + timestampLen(2) + timestamp + seqNum(8) + payloadLen(4) + payload
		pos := 0
		n := len(recBuf)

		// Topic.
		if pos+2 > n {
			return errors.New("journal record truncated at topic length")
		}
		topicLen := int(binary.BigEndian.Uint16(recBuf[pos : pos+2]))
		pos += 2
		if pos+topicLen > n {
			return errors.New("journal record truncated at topic data")
		}
		topic := string(recBuf[pos : pos+topicLen])
		pos += topicLen

		// SowKey.
		if pos+2 > n {
			return errors.New("journal record truncated at sow key length")
		}
		sowKeyLen := int(binary.BigEndian.Uint16(recBuf[pos : pos+2]))
		pos += 2
		if pos+sowKeyLen > n {
			return errors.New("journal record truncated at sow key data")
		}
		sowKey := string(recBuf[pos : pos+sowKeyLen])
		pos += sowKeyLen

		// Timestamp.
		if pos+2 > n {
			return errors.New("journal record truncated at timestamp length")
		}
		tsLen := int(binary.BigEndian.Uint16(recBuf[pos : pos+2]))
		pos += 2
		if pos+tsLen > n {
			return errors.New("journal record truncated at timestamp data")
		}
		timestamp := string(recBuf[pos : pos+tsLen])
		pos += tsLen

		// SeqNum.
		if pos+8 > n {
			return errors.New("journal record truncated at sequence")
		}
		seqNum := binary.BigEndian.Uint64(recBuf[pos : pos+8])
		pos += 8

		// Payload.
		if pos+4 > n {
			return errors.New("journal record truncated at payload length")
		}
		payloadLen := int(binary.BigEndian.Uint32(recBuf[pos : pos+4]))
		pos += 4
		if pos+payloadLen > n {
			return errors.New("journal record truncated at payload data")
		}
		payload := make([]byte, payloadLen)
		copy(payload, recBuf[pos:pos+payloadLen])
		pos += payloadLen
		if pos+17 <= n && string(recBuf[pos:pos+4]) == "PUB1" {
			var nameLen = int(binary.BigEndian.Uint16(recBuf[pos+4 : pos+6]))
			var userLen = int(binary.BigEndian.Uint16(recBuf[pos+6 : pos+8]))
			var publisherSequence = binary.BigEndian.Uint64(recBuf[pos+8 : pos+16])
			var persisted = recBuf[pos+16] == 1
			pos += 17
			if pos+nameLen+userLen == n {
				var identity = publisherIdentity{name: string(recBuf[pos : pos+nameLen]), user: string(recBuf[pos+nameLen : pos+nameLen+userLen])}
				var state, sequence, duplicate, _ = j.publishers.begin(identity, strconv.FormatUint(publisherSequence, 10))
				if !duplicate {
					state.finish(sequence, persisted)
				}
			} else {
				return errors.New("journal publisher identity is malformed")
			}
		} else if pos != n {
			return errors.New("journal record has invalid trailing data")
		}
		for previous := globalBookmarkSeq.Load(); seqNum > previous; previous = globalBookmarkSeq.Load() {
			if globalBookmarkSeq.CompareAndSwap(previous, seqNum) {
				break
			}
		}

		// Add to in-memory ring buffer.
		bookmark := makeBookmark(seqNum)
		j.addToMemory(topic, sowKey, payload, timestamp, seqNum, bookmark)
		committed += 4 + int64(recLen)
	}

	log.Printf("fakeamps: journal loaded %d entries from disk", j.count)
	return nil
}

func (j *messageJournal) restoreDeleteRecord(record []byte) error {
	if len(record) < 16 || string(record[:4]) != "DEL1" {
		return errors.New("invalid typed journal record")
	}
	var nameLen = int(binary.BigEndian.Uint16(record[4:6]))
	var userLen = int(binary.BigEndian.Uint16(record[6:8]))
	var sequence = binary.BigEndian.Uint64(record[8:16])
	if 16+nameLen+userLen != len(record) {
		return errors.New("journal delete publisher identity is malformed")
	}
	var identity = publisherIdentity{name: string(record[16 : 16+nameLen]), user: string(record[16+nameLen:])}
	var state, parsed, duplicate, _ = j.publishers.begin(identity, strconv.FormatUint(sequence, 10))
	if !duplicate {
		state.finish(parsed, true)
	}
	return nil
}

func (j *messageJournal) repairDiskTail(size int64) error {
	var root, err = openFakeampsDiskRoot(j.diskPath)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	var file *os.File
	file, err = root.OpenFile("journal.dat", os.O_RDWR, fakeampsDataFileMode)
	if err != nil {
		return err
	}
	if err = file.Truncate(size); err == nil {
		err = journalSyncFile(file)
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}

// addToMemory adds an entry to the in-memory ring buffer.
func (j *messageJournal) addToMemory(topic, sowKey string, payload []byte, timestamp string, seqNum uint64, bookmark string) {
	// Evict oldest if full.
	if j.count == j.maxSize {
		evicted := j.entries[j.head]
		delete(j.seqMap, evicted.seqNum)
	} else {
		j.count++
	}

	j.entries[j.head] = journalEntry{
		bookmark:  bookmark,
		topic:     topic,
		sowKey:    sowKey,
		payload:   payload,
		timestamp: timestamp,
		seqNum:    seqNum,
	}
	j.seqMap[seqNum] = j.head
	j.head = (j.head + 1) % j.maxSize
}

// writeToDisk writes a journal entry to the disk file.
func (j *messageJournal) writeToDisk(topic, sowKey string, payload []byte, timestamp string, seqNum uint64) {
	_ = j.writeToDiskWithPublisher(topic, sowKey, payload, timestamp, seqNum, publisherIdentity{}, 0, false, false)
}

func (j *messageJournal) writeToDiskWithPublisher(topic, sowKey string, payload []byte, timestamp string, seqNum uint64, identity publisherIdentity, publisherSequence uint64, persisted, syncWrite bool) error {
	if j.diskWriter == nil {
		return j.unavailableDiskError()
	}
	// Calculate record size.
	topicLen := len(topic)
	sowKeyLen := len(sowKey)
	tsLen := len(timestamp)
	payloadLen := len(payload)
	var topicLenField, topicLenOK = safecast.Uint16FromIntChecked(topicLen)
	if !topicLenOK {
		log.Printf("fakeamps: journal write: topic too large")
		return errors.New("journal topic too large")
	}
	var sowKeyLenField, sowKeyLenOK = safecast.Uint16FromIntChecked(sowKeyLen)
	if !sowKeyLenOK {
		log.Printf("fakeamps: journal write: sow key too large")
		return errors.New("journal sow key too large")
	}
	var tsLenField, tsLenOK = safecast.Uint16FromIntChecked(tsLen)
	if !tsLenOK {
		log.Printf("fakeamps: journal write: timestamp too large")
		return errors.New("journal timestamp too large")
	}
	var payloadLenField, payloadLenOK = safecast.Uint32FromIntChecked(payloadLen)
	if !payloadLenOK {
		log.Printf("fakeamps: journal write: payload too large")
		return errors.New("journal payload too large")
	}
	recordSize := 2 + topicLen + 2 + sowKeyLen + 2 + tsLen + 8 + 4 + payloadLen
	var includePublisher = publisherSequence != 0 && identity.anonymous == ""
	if includePublisher {
		if len(identity.name) > 65535 || len(identity.user) > 65535 {
			return errors.New("journal publisher identity too large")
		}
		recordSize += 17 + len(identity.name) + len(identity.user)
	}
	if recordSize < 0 || int64(recordSize) >= int64(typedJournalRecordFlag) {
		log.Printf("fakeamps: journal write: record too large")
		return errors.New("journal record too large")
	}

	// Reserve buffer.
	recBuf := make([]byte, recordSize)
	pos := 0

	// Topic.
	binary.BigEndian.PutUint16(recBuf[pos:pos+2], topicLenField)
	pos += 2
	copy(recBuf[pos:pos+topicLen], topic)
	pos += topicLen

	// SowKey.
	binary.BigEndian.PutUint16(recBuf[pos:pos+2], sowKeyLenField)
	pos += 2
	copy(recBuf[pos:pos+sowKeyLen], sowKey)
	pos += sowKeyLen

	// Timestamp.
	binary.BigEndian.PutUint16(recBuf[pos:pos+2], tsLenField)
	pos += 2
	copy(recBuf[pos:pos+tsLen], timestamp)
	pos += tsLen

	// SeqNum.
	binary.BigEndian.PutUint64(recBuf[pos:pos+8], seqNum)
	pos += 8

	// Payload.
	binary.BigEndian.PutUint32(recBuf[pos:pos+4], payloadLenField)
	pos += 4
	copy(recBuf[pos:pos+payloadLen], payload)
	pos += payloadLen
	if includePublisher {
		copy(recBuf[pos:pos+4], "PUB1")
		binary.BigEndian.PutUint16(recBuf[pos+4:pos+6], uint16(len(identity.name))) // #nosec G115 -- length checked above
		binary.BigEndian.PutUint16(recBuf[pos+6:pos+8], uint16(len(identity.user))) // #nosec G115 -- length checked above
		binary.BigEndian.PutUint64(recBuf[pos+8:pos+16], publisherSequence)
		if persisted {
			recBuf[pos+16] = 1
		}
		pos += 17
		copy(recBuf[pos:], identity.name)
		pos += len(identity.name)
		copy(recBuf[pos:], identity.user)
	}

	return j.appendDiskRecord(recBuf, false, seqNum, syncWrite)
}

func (j *messageJournal) appendDelete(identity publisherIdentity, publisherSequence uint64) error {
	if j.diskWriter == nil {
		return j.unavailableDiskError()
	}
	if identity.anonymous != "" {
		publisherSequence = 0
	}
	if len(identity.name) > 65535 || len(identity.user) > 65535 {
		return errors.New("journal delete publisher identity too large")
	}
	var record = make([]byte, 16+len(identity.name)+len(identity.user))
	copy(record[:4], "DEL1")
	binary.BigEndian.PutUint16(record[4:6], uint16(len(identity.name))) // #nosec G115 -- length checked above
	binary.BigEndian.PutUint16(record[6:8], uint16(len(identity.user))) // #nosec G115 -- length checked above
	binary.BigEndian.PutUint64(record[8:16], publisherSequence)
	copy(record[16:], identity.name)
	copy(record[16+len(identity.name):], identity.user)
	return j.appendDiskRecord(record, true, 0, true)
}

func (j *messageJournal) unavailableDiskError() error {
	if j.diskPath == "" {
		return nil
	}
	if j.diskErr != nil {
		return j.diskErr
	}
	return errors.New("disk journal is unavailable")
}

func (j *messageJournal) appendDiskRecord(record []byte, typed bool, seqNum uint64, syncWrite bool) error {
	if j.diskWriter == nil {
		return j.unavailableDiskError()
	}
	var size = uint32(len(record)) // #nosec G115 -- record is bounded by the caller
	if typed {
		size |= typedJournalRecordFlag
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], size)

	j.diskFlushMu.Lock()
	defer j.diskFlushMu.Unlock()
	if j.diskErr != nil {
		return j.diskErr
	}
	var originalSize int64
	if syncWrite {
		if err := j.diskWriter.Flush(); err != nil {
			j.diskErr = err
			return err
		}
		var info, err = j.diskFile.Stat()
		if err != nil {
			return err
		}
		originalSize = info.Size()
	}
	var rollback = func(cause error) error {
		if !syncWrite {
			j.diskErr = cause
			return cause
		}
		j.diskWriter.Reset(j.diskFile)
		if rollbackErr := j.repairDiskTail(originalSize); rollbackErr != nil {
			j.diskErr = errors.Join(cause, rollbackErr)
			return j.diskErr
		}
		return cause
	}
	if _, err := j.diskWriter.Write(lenBuf[:]); err != nil {
		return rollback(err)
	}
	if _, err := j.diskWriter.Write(record); err != nil {
		return rollback(err)
	}

	// Write current max sequence.
	if !typed {
		var seqBuf [8]byte
		binary.BigEndian.PutUint64(seqBuf[:], seqNum)
		if _, err := j.diskSeqFile.WriteAt(seqBuf[:], 0); err != nil {
			log.Printf("fakeamps: journal write: seq write failed: %v", err)
		}
	}
	if syncWrite {
		if err := j.diskWriter.Flush(); err != nil {
			return rollback(err)
		}
		if err := journalSyncFile(j.diskFile); err != nil {
			return rollback(err)
		}
	}
	return nil
}

// FlushDisk ensures all pending disk writes are flushed.
func (j *messageJournal) FlushDisk() {
	if j.diskWriter != nil {
		j.diskFlushMu.Lock()
		if err := j.diskWriter.Flush(); err != nil {
			log.Printf("fakeamps: journal flush failed: %v", err)
		}
		if err := j.diskFile.Sync(); err != nil {
			log.Printf("fakeamps: journal sync failed: %v", err)
		}
		j.diskFlushMu.Unlock()
	}
}

// Close shuts down disk persistence.
func (j *messageJournal) Close() {
	if j.diskSyncDone != nil {
		close(j.diskSyncDone)
	}
	if j.diskWriter != nil {
		if err := j.diskWriter.Flush(); err != nil {
			log.Printf("fakeamps: journal close flush failed: %v", err)
		}
		if err := j.diskFile.Sync(); err != nil {
			log.Printf("fakeamps: journal close sync failed: %v", err)
		}
		if err := j.diskFile.Close(); err != nil {
			log.Printf("fakeamps: journal close disk file failed: %v", err)
		}
	}
	if j.diskSeqFile != nil {
		if err := j.diskSeqFile.Close(); err != nil {
			log.Printf("fakeamps: journal close sequence file failed: %v", err)
		}
	}
}

// append adds a message to the journal and returns the assigned bookmark.
func (j *messageJournal) append(topic, sowKey string, payload []byte) (bookmark string, seqNum uint64) {
	seqNum = globalBookmarkSeq.Add(1)
	bookmark = makeBookmark(seqNum)
	ts := makeTimestamp()

	// Deep copy payload so the journal entry is independent of the
	// caller's (reused) read buffer.
	dataCopy := make([]byte, len(payload))
	copy(dataCopy, payload)

	j.mu.Lock()
	// Evict oldest entry if ring buffer is full.
	if j.count == j.maxSize {
		evicted := j.entries[j.head]
		delete(j.seqMap, evicted.seqNum)
	} else {
		j.count++
	}
	j.entries[j.head] = journalEntry{
		bookmark:  bookmark,
		topic:     topic,
		sowKey:    sowKey,
		payload:   dataCopy,
		timestamp: ts,
		seqNum:    seqNum,
	}
	j.seqMap[seqNum] = j.head
	j.head = (j.head + 1) % j.maxSize
	j.mu.Unlock()

	// Write to disk for durability.
	j.writeToDisk(topic, sowKey, dataCopy, ts, seqNum)

	return bookmark, seqNum
}

func (j *messageJournal) appendPublish(topic, sowKey string, payload []byte, identity publisherIdentity, publisherSequence uint64, persisted bool) (string, uint64, error) {
	var seqNum = globalBookmarkSeq.Add(1)
	var bookmark = makeBookmark(seqNum)
	var timestamp = makeTimestamp()
	var dataCopy = append([]byte(nil), payload...)
	if err := j.writeToDiskWithPublisher(topic, sowKey, dataCopy, timestamp, seqNum, identity, publisherSequence, persisted, true); err != nil {
		return "", 0, err
	}
	j.mu.Lock()
	j.addToMemory(topic, sowKey, dataCopy, timestamp, seqNum, bookmark)
	j.mu.Unlock()
	return bookmark, seqNum, nil
}

// replayFrom returns all journal entries for the given topic whose seqNum
// is strictly greater than afterSeq. If afterSeq is 0 (EPOCH), all entries
// for the topic are returned.
//
// The returned slice is a copy and safe to use without holding any lock.
func (j *messageJournal) replayFrom(topic string, afterSeq uint64) []journalEntry {
	j.mu.RLock()
	defer j.mu.RUnlock()

	result := make([]journalEntry, 0, 64)
	if j.count == 0 {
		return result
	}

	// Walk the ring buffer in insertion order.
	start := (j.head - j.count + j.maxSize) % j.maxSize
	for i := 0; i < j.count; i++ {
		idx := (start + i) % j.maxSize
		e := j.entries[idx]
		if e.topic == topic && e.seqNum > afterSeq {
			result = append(result, e)
		}
	}
	return result
}

func (j *messageJournal) replayAll(afterSeq uint64) []journalEntry {
	j.mu.RLock()
	defer j.mu.RUnlock()

	var result = make([]journalEntry, 0, 64)
	if j.count == 0 {
		return result
	}

	var start = (j.head - j.count + j.maxSize) % j.maxSize
	var index int
	for index = 0; index < j.count; index++ {
		var current = (start + index) % j.maxSize
		var entry = j.entries[current]
		if entry.seqNum > afterSeq {
			result = append(result, entry)
		}
	}

	return result
}

// parseBookmarkSeq extracts the sequence number from a bookmark string.
// Bookmark format: "epoch_us|publisher|seq|"
// Returns 0 if the bookmark is "0" (EPOCH) or unparseable.
func parseBookmarkSeq(bookmark string) uint64 {
	if bookmark == "" || bookmark == "0" {
		return 0
	}
	// Find the last "|" separated field that contains the sequence.
	// Format: "epoch|pub|seq|" — we want the 3rd field.
	fields := 0
	start := 0
	for i := 0; i < len(bookmark); i++ {
		if bookmark[i] == '|' {
			fields++
			if fields == 2 {
				start = i + 1
			}
			if fields == 3 {
				seq, err := strconv.ParseUint(bookmark[start:i], 10, 64)
				if err == nil {
					return seq
				}
				return 0
			}
		}
	}
	return 0
}

// ---------------------------------------------------------------------------
// Bookmark generation — "epoch_us|publisher|seq|"
//
// Real AMPS bookmarks use this pipe-delimited format:
//   - epoch: microseconds since Unix epoch
//   - publisher: publisher ID (we use "1")
//   - seq: global sequence number
//   - trailing pipe for completeness
// ---------------------------------------------------------------------------

func makeBookmark(seq uint64) string {
	var epochMicros = time.Now().UnixMicro()
	var epoch uint64
	if value, ok := safecast.Uint64FromInt64Checked(epochMicros); ok {
		epoch = value
	}
	var buf [64]byte
	b := buf[:0]
	b = strconv.AppendUint(b, epoch, 10)
	b = append(b, '|')
	b = append(b, '1')
	b = append(b, '|')
	b = strconv.AppendUint(b, seq, 10)
	b = append(b, '|')
	return string(b)
}

// ---------------------------------------------------------------------------
// Timestamp — AMPS format: "YYYYMMDDTHHMMSSnnnnnnnnnn"
// ---------------------------------------------------------------------------

func makeTimestamp() string {
	now := time.Now().UTC()
	var buf [25]byte
	b := buf[:0]
	year, month, day := now.Date()
	b = appendPadded(b, year, 4)
	b = appendPadded(b, int(month), 2)
	b = appendPadded(b, day, 2)
	b = append(b, 'T')
	b = appendPadded(b, now.Hour(), 2)
	b = appendPadded(b, now.Minute(), 2)
	b = appendPadded(b, now.Second(), 2)
	b = appendPadded(b, now.Nanosecond(), 9)
	return string(b)
}

func appendPadded(b []byte, val int, width int) []byte {
	var digits [10]byte
	for i := width - 1; i >= 0; i-- {
		digits[i] = byte('0' + val%10)
		val /= 10
	}
	return append(b, digits[:width]...)
}
