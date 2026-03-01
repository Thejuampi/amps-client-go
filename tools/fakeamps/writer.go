package main

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"log"
	"time"
)

// ---------------------------------------------------------------------------
// connWriter — dedicated write goroutine per connection.
//
// Real AMPS spawns a dedicated writer thread per transport that drains an
// internal outbound queue. This goroutine models that behavior:
//   - Drains all available frames before flushing (write coalescing)
//   - Uses bufio.Writer to batch small frames into fewer syscalls
//   - Never blocks the reader goroutine
//   - Optional zlib compression for continuous streaming
// ---------------------------------------------------------------------------

type connWriter struct {
	ch    chan []byte
	done  chan struct{}
	stats *connStats

	// Compression
	compress    bool
	compressBuf *bytes.Buffer
	zlibWriter  *zlib.Writer
}

func newConnWriter(conn interface{ Write([]byte) (int, error) }, stats *connStats) *connWriter {
	cw := &connWriter{
		ch:    make(chan []byte, *flagOutDepth),
		done:  make(chan struct{}),
		stats: stats,
	}
	go cw.run(conn)
	return cw
}

func (cw *connWriter) run(conn interface{ Write([]byte) (int, error) }) {
	defer close(cw.done)
	bw := bufio.NewWriterSize(conn, *flagWriteBuf)

	for frame := range cw.ch {
		channelClosed := false
		// Compress frame if compression is enabled.
		if cw.compress && len(frame) > 64 {
			frame = cw.compressFrame(frame)
		}

		_, _ = bw.Write(frame)
		cw.stats.messagesOut.Add(1)
		cw.stats.bytesOut.Add(uint64(len(frame)))

		// Drain all available frames before flushing — write coalescing.
		// This minimizes syscalls under high throughput while keeping
		// latency low when the queue is empty (we flush immediately
		// when there's nothing else pending).
		drained := true
		for drained {
			select {
			case f, ok := <-cw.ch:
				if !ok {
					channelClosed = true
					drained = false
					continue
				}
				if cw.compress && len(f) > 64 {
					f = cw.compressFrame(f)
				}
				_, _ = bw.Write(f)
				cw.stats.messagesOut.Add(1)
				cw.stats.bytesOut.Add(uint64(len(f)))
			default:
				drained = false
			}
		}

		_ = bw.Flush()
		if channelClosed {
			return
		}
	}
	_ = bw.Flush()
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
	select {
	case cw.ch <- data:
	default:
		// Channel full — back-pressure / slow consumer.
	}
}

// sendDirect enqueues without copy — caller guarantees slice won't be reused.
func (cw *connWriter) sendDirect(data []byte) {
	select {
	case cw.ch <- data:
	default:
	}
}

func (cw *connWriter) close() {
	close(cw.ch)
	<-cw.done
}

// ---------------------------------------------------------------------------
// Stats logger goroutine
// ---------------------------------------------------------------------------

func statsLogger(addr string, stats *connStats, done chan struct{}) {
	ticker := time.NewTicker(*flagStatsIvl)
	defer ticker.Stop()

	var prevIn, prevOut, prevPubIn, prevPubOut uint64
	prev := time.Now()
	for {
		select {
		case <-done:
			return
		case now := <-ticker.C:
			elapsed := now.Sub(prev).Seconds()
			if elapsed <= 0 {
				elapsed = 1
			}
			prev = now

			mIn := stats.messagesIn.Load()
			mOut := stats.messagesOut.Load()
			pIn := stats.publishIn.Load()
			pOut := stats.publishOut.Load()

			dIn := mIn - prevIn
			dOut := mOut - prevOut
			dPIn := pIn - prevPubIn
			dPOut := pOut - prevPubOut
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
