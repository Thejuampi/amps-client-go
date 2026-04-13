package amps

import (
	"encoding/binary"
	"sync"
	"testing"
	"time"
)

// delayConn wraps testConn and inserts a configurable delay before each Read,
// allowing tests to create measurably different timestamps between Reads.
type delayConn struct {
	testConn
	delayLock   sync.Mutex
	readDelays  []time.Duration // per-Read delay, consumed in order
	defaultWait time.Duration   // fallback if readDelays is empty
}

func newDelayConn(defaultWait time.Duration) *delayConn {
	return &delayConn{defaultWait: defaultWait}
}

func (dc *delayConn) Read(buffer []byte) (int, error) {
	dc.delayLock.Lock()
	var delay = dc.defaultWait
	if len(dc.readDelays) > 0 {
		delay = dc.readDelays[0]
		dc.readDelays = dc.readDelays[1:]
	}
	dc.delayLock.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}
	return dc.testConn.Read(buffer)
}

func (dc *delayConn) setReadDelays(delays ...time.Duration) {
	dc.delayLock.Lock()
	dc.readDelays = append(dc.readDelays[:0], delays...)
	dc.delayLock.Unlock()
}

// TestReadRoutineBatchReceiveTimeStaleBug verifies that every message delivered
// through readRoutine receives a timestamp that reflects when its data was
// actually read from the socket, not a stale timestamp from a prior Read.
//
// Reproduction scenario:
//  1. Frame 1 header (4 bytes) arrives in Read #1 at line 576 (batchReceiveTime
//     is NOT set in the buggy code).
//  2. Frame 1 body + first 2 bytes of Frame 2 header arrive in Read #2 at
//     line 623 → batchReceiveTime = T_early.
//  3. Frame 1 is processed.
//  4. Outer loop restarts. Only 2 bytes of Frame 2 header buffered.
//  5. After a measurable delay, Read #3 at line 576 brings the remaining 2
//     header bytes + entire Frame 2 body. In the buggy code, batchReceiveTime
//     is NOT updated, so Frame 2 gets T_early (stale).
//  6. With the fix, batchReceiveTime IS updated at Read #3, so Frame 2 gets a
//     fresh timestamp ≥ T_early + delay.
func TestReadRoutineBatchReceiveTimeStaleBug(t *testing.T) {
	const readDelay = 50 * time.Millisecond

	var client = NewClient("batch-receive-stale")
	var conn = newDelayConn(0)
	client.connection = conn
	client.connected.Store(true)
	client.stopped.Store(false)
	client.SetErrorHandler(func(error) {})

	type deliveredMsg struct {
		data      string
		timestamp int64
	}
	var delivered []deliveredMsg
	client.routes.Store("sub-ts", func(message *Message) error {
		delivered = append(delivered, deliveredMsg{
			data:      string(message.Data()),
			timestamp: message.rawTransmissionUnixNano,
		})
		return nil
	})

	var frame1 = buildFrameFromCommand(t, NewCommand("publish").SetSubID("sub-ts").SetTopic("orders").SetData([]byte("first")))
	var frame2 = buildFrameFromCommand(t, NewCommand("publish").SetSubID("sub-ts").SetTopic("orders").SetData([]byte("second")))

	// Read #1: 4-byte header prefix of frame1 (no delay).
	conn.enqueueRead(frame1[:4])

	// Read #2: rest of frame1 body + 2 bytes of frame2 header (no delay).
	// This Read fires at line 623 and sets batchReceiveTime.
	var bodyPlusPartial = make([]byte, 0, len(frame1[4:])+2)
	bodyPlusPartial = append(bodyPlusPartial, frame1[4:]...)
	bodyPlusPartial = append(bodyPlusPartial, frame2[:2]...)
	conn.enqueueRead(bodyPlusPartial)

	// Read #3: remaining 2 bytes of frame2 header + frame2 body (WITH delay).
	// In buggy code, this Read at line 576 does NOT update batchReceiveTime.
	conn.enqueueRead(frame2[2:])

	// Configure delays: Read #1 = 0, Read #2 = 0, Read #3 = readDelay.
	conn.setReadDelays(0, 0, readDelay)

	client.readRoutine()

	if len(delivered) != 2 {
		t.Fatalf("expected 2 delivered messages, got %d", len(delivered))
	}
	if delivered[0].data != "first" || delivered[1].data != "second" {
		t.Fatalf("unexpected message data: %v", delivered)
	}

	// Frame 2 must have a timestamp that is at least readDelay after Frame 1's
	// timestamp, because its data arrived in Read #3 which happened after the
	// delay. If batchReceiveTime is stale (not updated at line 576), Frame 2
	// inherits Frame 1's timestamp from Read #2.
	var delta = time.Duration(delivered[1].timestamp - delivered[0].timestamp)
	if delta < readDelay/2 {
		t.Fatalf("Frame 2 timestamp is stale: delta between messages = %s, expected >= %s. "+
			"frame1_ts=%d frame2_ts=%d",
			delta, readDelay/2, delivered[0].timestamp, delivered[1].timestamp)
	}
}

// TestReadRoutineFirstReadSetsTimestamp verifies that when an entire frame
// arrives in a single Read at the header-length read site (line 576), the
// batchReceiveTime is set by that Read (not relying on the onMessage fallback).
// In the buggy code, batchReceiveTime stays 0 and resetForParse sets
// rawTransmissionUnixNano = 0, then line 646 copies 0, then onMessage's
// fallback masks the bug. With the fix, batchReceiveTime is set at line 576
// and the onMessage check becomes redundant (timestamp is already set).
func TestReadRoutineFirstReadSetsTimestamp(t *testing.T) {
	var client = NewClient("first-read-ts")
	var conn = newTestConn()
	client.connection = conn
	client.connected.Store(true)
	client.stopped.Store(false)
	client.SetErrorHandler(func(error) {})

	var capturedTimestamp int64
	client.routes.Store("sub-ts", func(message *Message) error {
		capturedTimestamp = message.rawTransmissionUnixNano
		return nil
	})

	// Enqueue a complete frame in one shot.
	var frame = buildFrameFromCommand(t, NewCommand("publish").SetSubID("sub-ts").SetTopic("orders").SetData([]byte("hello")))
	conn.enqueueRead(frame)

	var beforeRun = time.Now().UnixNano()
	client.readRoutine()

	if capturedTimestamp == 0 {
		t.Fatalf("expected non-zero rawTransmissionUnixNano from first-read-only path")
	}
	if capturedTimestamp < beforeRun {
		t.Fatalf("timestamp is stale: got %d, want >= %d", capturedTimestamp, beforeRun)
	}
}

// TestReadRoutineMultipleBufferedFramesShareBatchTimestamp verifies that when
// multiple complete frames arrive in a single Read, all messages from that
// batch share the same timestamp (from that Read call).
func TestReadRoutineMultipleBufferedFramesShareBatchTimestamp(t *testing.T) {
	var client = NewClient("multi-buffered-ts")
	var conn = newTestConn()
	client.connection = conn
	client.connected.Store(true)
	client.stopped.Store(false)
	client.SetErrorHandler(func(error) {})

	var timestamps []int64
	client.routes.Store("sub-ts", func(message *Message) error {
		timestamps = append(timestamps, message.rawTransmissionUnixNano)
		return nil
	})

	// Build 3 frames and concatenate them into one TCP read.
	var allFrames []byte
	for i := 0; i < 3; i++ {
		frame := buildFrameFromCommand(t, NewCommand("publish").SetSubID("sub-ts").SetTopic("orders").SetData([]byte("data")))
		allFrames = append(allFrames, frame...)
	}
	conn.enqueueRead(allFrames)

	var beforeRun = time.Now().UnixNano()
	client.readRoutine()

	if len(timestamps) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(timestamps))
	}

	for i, ts := range timestamps {
		if ts == 0 {
			t.Fatalf("message %d has zero rawTransmissionUnixNano", i)
		}
		if ts < beforeRun {
			t.Fatalf("message %d has stale rawTransmissionUnixNano: got %d, want >= %d",
				i, ts, beforeRun)
		}
	}

	// All messages in the same batch should share the same timestamp.
	for i := 1; i < len(timestamps); i++ {
		if timestamps[i] != timestamps[0] {
			t.Fatalf("expected all messages from same Read to share timestamp, msg[0]=%d msg[%d]=%d",
				timestamps[0], i, timestamps[i])
		}
	}
}

// buildRawFrame constructs a framed AMPS message from raw header+body bytes
// with the 4-byte big-endian length prefix.
func buildRawFrame(header string, body []byte) []byte {
	var payload = append([]byte(header), body...)
	var frame = make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	return frame
}
