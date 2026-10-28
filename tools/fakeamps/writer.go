package main

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// connWriter — dedicated write goroutine per connection.
//
// Real AMPS spawns a dedicated writer thread per transport that drains an
// internal outbound queue. This goroutine models that behavior:
//   - Coalesces data frames and flushes acknowledgements in queue order
//   - Uses bufio.Writer to batch small frames into fewer syscalls
//   - Never blocks the reader goroutine
//   - Optional zlib compression for continuous streaming
// ---------------------------------------------------------------------------

type connWriter struct {
	ch        chan []byte
	done      chan struct{}
	stats     *connStats
	closeOnce sync.Once
	closed    atomic.Bool

	// Compression
	compress    bool
	compressBuf *bytes.Buffer
	zlibWriter  *zlib.Writer

	overflow sync.Once
	onSlow   func()
}

func newConnWriter(conn interface{ Write([]byte) (int, error) }, stats *connStats) *connWriter {
	cw := &connWriter{
		ch:    make(chan []byte, *flagOutDepth),
		done:  make(chan struct{}),
		stats: stats,
	}
	if closer, ok := conn.(interface{ Close() error }); ok {
		cw.onSlow = func() {
			_ = closer.Close()
		}
	}
	go cw.run(conn)
	return cw
}

func (cw *connWriter) run(conn interface{ Write([]byte) (int, error) }) {
	defer close(cw.done)
	bw := bufio.NewWriterSize(conn, *flagWriteBuf)

	for frame := range cw.ch {
		channelClosed := false
		var flushAck = isAcknowledgementFrame(frame)
		// Compress frame if compression is enabled.
		if cw.compress && len(frame) > 64 {
			frame = cw.compressFrame(frame)
		}

		if _, err := bw.Write(frame); err != nil {
			return
		}
		cw.stats.messagesOut.Add(1)
		cw.stats.bytesOut.Add(uint64(len(frame)))

		// A processed ack must reach the client before queued snapshot data
		// can delay it. Keep FIFO order and coalesce data between acks.
		drained := !flushAck
		for drained {
			select {
			case f, ok := <-cw.ch:
				if !ok {
					channelClosed = true
					drained = false
					continue
				}
				flushAck = isAcknowledgementFrame(f)
				if cw.compress && len(f) > 64 {
					f = cw.compressFrame(f)
				}
				if _, err := bw.Write(f); err != nil {
					return
				}
				cw.stats.messagesOut.Add(1)
				cw.stats.bytesOut.Add(uint64(len(f)))
				if flushAck {
					drained = false
				}
			default:
				drained = false
			}
		}

		if err := bw.Flush(); err != nil {
			return
		}
		if channelClosed {
			return
		}
	}
	_ = bw.Flush()
}

func isAcknowledgementFrame(frame []byte) bool {
	return len(frame) >= 4 && bytes.HasPrefix(frame[4:], []byte(`{"c":"ack"`))
}

func (cw *connWriter) compressFrame(frame []byte) []byte {
	if cw.compressBuf == nil {
		cw.compressBuf = new(bytes.Buffer)
	}
	cw.compressBuf.Reset()

	if cw.zlibWriter == nil {
		cw.zlibWriter = zlib.NewWriter(cw.compressBuf)
	} else {
		cw.zlibWriter.Reset(cw.compressBuf)
	}

	if _, err := cw.zlibWriter.Write(frame); err != nil {
		return frame
	}
	if err := cw.zlibWriter.Close(); err != nil {
		return frame
	}

	// Prepend compression indicator.
	compressed := cw.compressBuf.Bytes()
	result := make([]byte, 1+len(compressed))
	result[0] = 'z' // compression flag
	copy(result[1:], compressed)
	return result
}

func (cw *connWriter) EnableCompression() {
	cw.compress = true
}

// send copies data and enqueues it for async writing.
func (cw *connWriter) send(data []byte) {
	if data == nil {
		return
	}
	var frame = append([]byte(nil), data...)
	cw.enqueue(frame)
}

// sendDirect enqueues without copy — caller guarantees slice won't be reused.
func (cw *connWriter) sendDirect(data []byte) {
	if data == nil {
		return
	}
	cw.enqueue(data)
}

func (cw *connWriter) enqueue(data []byte) {
	if cw.closed.Load() {
		return
	}

	select {
	case cw.ch <- data:
	default:
		cw.failSlowConsumer()
	}
}

func (cw *connWriter) failSlowConsumer() {
	cw.overflow.Do(func() {
		if cw.onSlow != nil {
			cw.onSlow()
		}
	})
}

func (cw *connWriter) close() {
	cw.closed.Store(true)
	cw.closeOnce.Do(func() {
		close(cw.ch)
	})
	<-cw.done
}

// ---------------------------------------------------------------------------
// Stats logger goroutine
// ---------------------------------------------------------------------------

func statsLogger(addr string, stats *connStats, done chan struct{}) {
	var interval = *flagStatsIvl
	statsLoggerWithInterval(addr, stats, done, interval)
}

func statsLoggerWithInterval(addr string, stats *connStats, done <-chan struct{}, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}

	var ticker = time.NewTicker(interval)
	defer ticker.Stop()

	var prevIn, prevOut, prevPubIn, prevPubOut uint64
	var prev = time.Now()
	for {
		select {
		case <-done:
			return
		case now := <-ticker.C:
			var elapsed = now.Sub(prev).Seconds()
			if elapsed <= 0 {
				elapsed = 1
			}
			prev = now

			var mIn = stats.messagesIn.Load()
			var mOut = stats.messagesOut.Load()
			var pIn = stats.publishIn.Load()
			var pOut = stats.publishOut.Load()

			var dIn = mIn - prevIn
			var dOut = mOut - prevOut
			var dPIn = pIn - prevPubIn
			var dPOut = pOut - prevPubOut
			prevIn, prevOut, prevPubIn, prevPubOut = mIn, mOut, pIn, pOut

			if dIn > 0 || dOut > 0 {
				log.Printf("fakeamps: stats %-21s  msg/s in=%-8d out=%-8d  pub/s in=%-8d out=%-8d  total in=%-10d out=%-10d",
					addr,
					uint64(float64(dIn)/elapsed),
					uint64(float64(dOut)/elapsed),
					uint64(float64(dPIn)/elapsed),
					uint64(float64(dPOut)/elapsed),
					mIn, mOut)
			}
		}
	}
}
