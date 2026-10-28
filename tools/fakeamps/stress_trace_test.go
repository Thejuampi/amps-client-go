package main

import (
	"bytes"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

type reconnectStressTrace struct {
	lock   sync.Mutex
	events []string
}

func (trace *reconnectStressTrace) record(direction string, frame []byte) {
	if len(frame) < 4 || (!bytes.HasPrefix(frame[4:], []byte(`{"c":"ack"`)) && !bytes.HasPrefix(frame[4:], []byte(`{"c":"sow_and_subscribe"`))) {
		return
	}
	var header, _ = parseAMPSHeader(frame[4:])
	var event = fmt.Sprintf("%s %s/%s cid=%s at=%d", direction, header.c, header.a, header.cid, time.Now().UnixNano())
	trace.lock.Lock()
	if len(trace.events) == 32 {
		copy(trace.events, trace.events[1:])
		trace.events = trace.events[:31]
	}
	trace.events = append(trace.events, event)
	trace.lock.Unlock()
}

func (trace *reconnectStressTrace) snapshot() string {
	trace.lock.Lock()
	defer trace.lock.Unlock()
	return strings.Join(trace.events, "; ")
}

type reconnectStressTraceConn struct {
	net.Conn
	trace reconnectStressTrace
}

func (connection *reconnectStressTraceConn) Read(data []byte) (int, error) {
	var count, err = connection.Conn.Read(data)
	connection.trace.record("server-read", data[:count])
	return count, err
}

func (connection *reconnectStressTraceConn) Write(data []byte) (int, error) {
	connection.trace.record("server-write-start", data)
	var count, err = connection.Conn.Write(data)
	connection.trace.record("server-write-done", data[:count])
	return count, err
}

func (server *reconnectStressServer) diagnostic(err error, trace *reconnectStressTrace) error {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	var serverEvents string
	server.connLock.Lock()
	if connection, ok := server.active.(*reconnectStressTraceConn); ok {
		serverEvents = connection.trace.snapshot()
	}
	server.connLock.Unlock()
	return fmt.Errorf("%w; client=[%s]; server=[%s]; heap=%d gc=%d last-gc=%d pause-ns=%d", err, trace.snapshot(), serverEvents, memory.HeapAlloc, memory.NumGC, memory.LastGC, memory.PauseNs[(memory.NumGC+255)%256])
}

func traceReconnectStressClient(client *amps.Client) *reconnectStressTrace {
	var trace = new(reconnectStressTrace)
	client.SetTransportFilter(func(direction amps.TransportFilterDirection, frame []byte) []byte {
		if direction == amps.TransportFilterOutbound {
			trace.record("client-send", frame)
		} else {
			trace.record("client-receive", frame)
		}
		return frame
	})
	return trace
}
