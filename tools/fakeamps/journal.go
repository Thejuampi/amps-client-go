package main

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
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
	mu      sync.RWMutex
	entries []journalEntry
	maxSize int
	head    int            // next write position (ring buffer)
	count   int            // current number of entries
	seqMap  map[uint64]int // seqNum → index in entries (for fast bookmark lookup)

	// Disk persistence
	diskPath     string
	diskFile     *os.File
	diskWriter   *bufio.Writer
	diskSeqFile  *os.File
	diskFlushMu  sync.Mutex
	diskSyncDone chan struct{}
}

func newMessageJournal(maxSize int) *messageJournal {
	if maxSize <= 0 {
		maxSize = 1_000_000
	}
	return &messageJournal{
		entries: make([]journalEntry, maxSize),
		maxSize: maxSize,
		seqMap:  make(map[uint64]int, maxSize),
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
	if err := os.MkdirAll(j.diskPath, 0755); err != nil {
		log.Printf("fakeamps: journal disk init: mkdir failed: %v", err)
		return
	}

	// Open or create journal file (append mode).
	journalFile := filepath.Join(j.diskPath, "journal.dat")
	f, err := os.OpenFile(journalFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("fakeamps: journal disk init: open failed: %v", err)
		return
	}
	j.diskFile = f
	j.diskWriter = bufio.NewWriterSize(f, 256*1024)

	// Open or create sequence file.
	seqFile := filepath.Join(j.diskPath, "sequence.dat")
	sf, err := os.OpenFile(seqFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Printf("fakeamps: journal disk init: seq file failed: %v", err)
		return
	}
	j.diskSeqFile = sf

	// Load existing entries from disk.
	j.loadFromDisk()
}

// loadFromDisk loads journal entries from disk into memory.
func (j *messageJournal) loadFromDisk() {
	if j.diskFile == nil {
		return
	}

	j.diskSeqFile.Seek(0, io.SeekStart)
	var seqBuf [8]byte
	if _, err := io.ReadFull(j.diskSeqFile, seqBuf[:]); err == nil {
		maxSeq := binary.BigEndian.Uint64(seqBuf[:])
		if maxSeq > 0 {
			globalBookmarkSeq.Store(maxSeq)
			log.Printf("fakeamps: journal loaded seq=%d", maxSeq)
		}
	}

	j.diskFile.Seek(0, io.SeekStart)
	reader := bufio.NewReader(j.diskFile)

	for {
		// Read record length.
		var lenBuf [4]byte
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			if err == io.EOF {
				break
			}
			log.Printf("fakeamps: journal load: read len: %v", err)
			break
		}
		recLen := binary.BigEndian.Uint32(lenBuf[:])

		// Read record.
		recBuf := make([]byte, recLen)
		if _, err := io.ReadFull(reader, recBuf); err != nil {
			log.Printf("fakeamps: journal load: read rec: %v", err)
			break
		}

		// Parse: topicLen(2) + topic + sowKeyLen(2) + sowKey + timestampLen(2) + timestamp + seqNum(8) + payloadLen(4) + payload
		pos := 0

		// Topic.
		topicLen := binary.BigEndian.Uint16(recBuf[pos : pos+2])
		pos += 2
		topic := string(recBuf[pos : pos+int(topicLen)])
		pos += int(topicLen)

		// SowKey.
		sowKeyLen := binary.BigEndian.Uint16(recBuf[pos : pos+2])
		pos += 2
		sowKey := string(recBuf[pos : pos+int(sowKeyLen)])
		pos += int(sowKeyLen)

		// Timestamp.
		tsLen := binary.BigEndian.Uint16(recBuf[pos : pos+2])
		pos += 2
		timestamp := string(recBuf[pos : pos+int(tsLen)])
		pos += int(tsLen)

		// SeqNum.
		seqNum := binary.BigEndian.Uint64(recBuf[pos : pos+8])
		pos += 8

		// Payload.
		payloadLen := binary.BigEndian.Uint32(recBuf[pos : pos+4])
		pos += 4
		payload := make([]byte, payloadLen)
		copy(payload, recBuf[pos:pos+int(payloadLen)])

		// Add to in-memory ring buffer.
		bookmark := makeBookmark(seqNum)
		j.addToMemory(topic, sowKey, payload, timestamp, seqNum, bookmark)
	}

	log.Printf("fakeamps: journal loaded %d entries from disk", j.count)
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
	if j.diskWriter == nil {
		return
	}

	// Calculate record size.
	topicLen := len(topic)
	sowKeyLen := len(sowKey)
	tsLen := len(timestamp)
	payloadLen := len(payload)
	recordSize := 2 + topicLen + 2 + sowKeyLen + 2 + tsLen + 8 + 4 + payloadLen

	// Reserve buffer.
	recBuf := make([]byte, recordSize)
	pos := 0

	// Topic.
	binary.BigEndian.PutUint16(recBuf[pos:pos+2], uint16(topicLen))
	pos += 2
	copy(recBuf[pos:pos+topicLen], topic)
	pos += topicLen

	// SowKey.
	binary.BigEndian.PutUint16(recBuf[pos:pos+2], uint16(sowKeyLen))
	pos += 2
	copy(recBuf[pos:pos+sowKeyLen], sowKey)
	pos += sowKeyLen

	// Timestamp.
	binary.BigEndian.PutUint16(recBuf[pos:pos+2], uint16(tsLen))
	pos += 2
	copy(recBuf[pos:pos+tsLen], timestamp)
	pos += tsLen

	// SeqNum.
	binary.BigEndian.PutUint64(recBuf[pos:pos+8], seqNum)
	pos += 8

	// Payload.
	binary.BigEndian.PutUint32(recBuf[pos:pos+4], uint32(payloadLen))
	pos += 4
	copy(recBuf[pos:pos+payloadLen], payload)

	// Write length prefix + record.
	lenBuf := [4]byte{}
	binary.BigEndian.PutUint32(lenBuf[:], uint32(recordSize))

	j.diskFlushMu.Lock()
	if _, err := j.diskFile.Write(lenBuf[:]); err != nil {
		j.diskFlushMu.Unlock()
		return
	}
	if _, err := j.diskFile.Write(recBuf); err != nil {
		j.diskFlushMu.Unlock()
		return
	}

	// Write current max sequence.
	seqBuf := [8]byte{}
	binary.BigEndian.PutUint64(seqBuf[:], seqNum)
	j.diskSeqFile.WriteAt(seqBuf[:], 0)
	j.diskFlushMu.Unlock()
}

// FlushDisk ensures all pending disk writes are flushed.
func (j *messageJournal) FlushDisk() {
	if j.diskWriter != nil {
		j.diskFlushMu.Lock()
		j.diskWriter.Flush()
		j.diskFile.Sync()
		j.diskFlushMu.Unlock()
	}
}

// Close shuts down disk persistence.
func (j *messageJournal) Close() {
	if j.diskSyncDone != nil {
		close(j.diskSyncDone)
	}
	if j.diskWriter != nil {
		j.diskWriter.Flush()
		j.diskFile.Sync()
		j.diskFile.Close()
	}
	if j.diskSeqFile != nil {
		j.diskSeqFile.Close()
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
	epoch := uint64(time.Now().UnixMicro())
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
