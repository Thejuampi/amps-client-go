package main

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

func sendFrame(t *testing.T, conn net.Conn, frame []byte) {
	t.Helper()
	_, err := conn.Write(frame)
	if err != nil {
		t.Fatalf("failed to send frame: %v", err)
	}
}

func buildCommandFrame(header string, payload []byte) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, len(header)+len(payload)+8))
	startFrame(buf)
	buf.WriteString(header)
	if len(payload) > 0 {
		buf.Write(payload)
	}
	return finalizeFrame(buf)
}

func TestHandleConnectionCommandFlow(t *testing.T) {
	oldSow := sow
	oldJournal := journal
	sow = newSOWCache()
	journal = newMessageJournal(1000)
	defer func() {
		sow = oldSow
		journal = oldJournal
	}()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	done := make(chan struct{})
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			close(done)
			return
		}
		handleConnection(conn)
		close(done)
	}()

	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial test server: %v", err)
	}

	// Drain server responses so write buffers do not fill.
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		_, _ = io.Copy(io.Discard, client)
	}()

	// logon
	sendFrame(t, client, buildCommandFrame(`{"c":"logon","cid":"1","a":"processed","client_name":"test"}`, nil))

	// subscribe + delta_subscribe
	sendFrame(t, client, buildCommandFrame(`{"c":"subscribe","cid":"2","sub_id":"s1","t":"orders","a":"processed"}`, nil))
	sendFrame(t, client, buildCommandFrame(`{"c":"delta_subscribe","cid":"3","sub_id":"d1","t":"orders","a":"processed"}`, nil))

	// publish message with payload
	sendFrame(t, client, buildCommandFrame(`{"c":"publish","cid":"4","t":"orders","a":"processed,persisted","k":"order-1","mt":"json"}`, []byte(`{"id":1,"status":"active"}`)))

	// sow and sow_and_subscribe
	sendFrame(t, client, buildCommandFrame(`{"c":"sow","cid":"5","t":"orders","query_id":"q1","a":"processed,completed"}`, nil))
	sendFrame(t, client, buildCommandFrame(`{"c":"sow_and_subscribe","cid":"6","sub_id":"s2","query_id":"q2","t":"orders","a":"processed,completed"}`, nil))

	// heartbeat with beat option
	sendFrame(t, client, buildCommandFrame(`{"c":"heartbeat","cid":"7","a":"processed","o":"beat"}`, nil))

	// pause/resume
	sendFrame(t, client, buildCommandFrame(`{"c":"pause","cid":"8","sub_id":"s1","a":"processed"}`, nil))
	sendFrame(t, client, buildCommandFrame(`{"c":"resume","cid":"9","sub_id":"s1","a":"processed"}`, nil))

	// sow_delete by key
	sendFrame(t, client, buildCommandFrame(`{"c":"sow_delete","cid":"10","t":"orders","k":"order-1","a":"processed"}`, nil))

	// ack / group boundaries / flush / unknown
	sendFrame(t, client, buildCommandFrame(`{"c":"ack","cid":"11","k":"order-1"}`, nil))
	sendFrame(t, client, buildCommandFrame(`{"c":"group_begin","cid":"12"}`, nil))
	sendFrame(t, client, buildCommandFrame(`{"c":"group_end","cid":"13"}`, nil))
	sendFrame(t, client, buildCommandFrame(`{"c":"flush","cid":"14","a":"processed,completed"}`, nil))
	sendFrame(t, client, buildCommandFrame(`{"c":"unknown","cid":"15","a":"stats"}`, nil))

	// unsubscribe and close
	sendFrame(t, client, buildCommandFrame(`{"c":"unsubscribe","cid":"16","sub_id":"all","a":"processed"}`, nil))

	_ = client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("handleConnection did not exit in time")
	}

	<-readDone
}
