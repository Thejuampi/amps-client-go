package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func readFrameBody(t *testing.T, conn net.Conn, timeout time.Duration) string {
	t.Helper()

	var err = conn.SetReadDeadline(time.Now().Add(timeout))
	if err != nil {
		t.Fatalf("failed to set read deadline: %v", err)
	}

	var lenBuf [4]byte
	_, err = io.ReadFull(conn, lenBuf[:])
	if err != nil {
		t.Fatalf("failed to read frame length: %v", err)
	}

	var frameLen = binary.BigEndian.Uint32(lenBuf[:])
	var frame = make([]byte, frameLen)
	_, err = io.ReadFull(conn, frame)
	if err != nil {
		t.Fatalf("failed to read frame body: %v", err)
	}

	err = conn.SetReadDeadline(time.Time{})
	if err != nil {
		t.Fatalf("failed to clear read deadline: %v", err)
	}

	return string(frame)
}

func TestPublishCommandIDDedupeSkipsSecondApply(t *testing.T) {
	var oldSow = sow
	var oldJournal = journal
	sow = newSOWCache()
	journal = newMessageJournal(1000)
	defer func() {
		sow = oldSow
		journal = oldJournal
	}()

	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	var done = make(chan struct{})
	go func() {
		var conn, acceptErr = listener.Accept()
		if acceptErr != nil {
			close(done)
			return
		}
		handleConnection(conn)
		close(done)
	}()

	var client, dialErr = net.Dial("tcp", listener.Addr().String())
	if dialErr != nil {
		t.Fatalf("failed to dial test server: %v", dialErr)
	}

	var readDone = make(chan struct{})
	go func() {
		defer close(readDone)
		_, _ = io.Copy(io.Discard, client)
	}()

	sendFrame(t, client, buildCommandFrame(`{"c":"logon","cid":"log-1","a":"processed","client_name":"dedupe-client"}`, nil))
	sendFrame(t, client, buildCommandFrame(`{"c":"publish","cid":"dup-1","t":"orders","a":"processed,persisted","s":"10","k":"order-1","mt":"json"}`, []byte(`{"id":1}`)))
	sendFrame(t, client, buildCommandFrame(`{"c":"publish","cid":"dup-1","t":"orders","a":"processed,persisted","s":"11","k":"order-2","mt":"json"}`, []byte(`{"id":2}`)))

	_ = client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("handleConnection did not exit in time")
	}

	<-readDone

	var result = sow.query("orders", "", -1, "")
	if result.totalCount != 1 {
		t.Fatalf("expected exactly one applied publish for duplicate command id, got %d", result.totalCount)
	}
}

func TestSOWDeletePersistedAckEchoesSequence(t *testing.T) {
	var oldSow = sow
	var oldJournal = journal
	sow = newSOWCache()
	journal = newMessageJournal(1000)
	defer func() {
		sow = oldSow
		journal = oldJournal
	}()

	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	var done = make(chan struct{})
	go func() {
		var conn, acceptErr = listener.Accept()
		if acceptErr != nil {
			close(done)
			return
		}
		handleConnection(conn)
		close(done)
	}()

	var client, dialErr = net.Dial("tcp", listener.Addr().String())
	if dialErr != nil {
		t.Fatalf("failed to dial test server: %v", dialErr)
	}

	sendFrame(t, client, buildCommandFrame(`{"c":"logon","cid":"log-1","a":"processed","client_name":"sowdel-client"}`, nil))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"publish","cid":"pub-1","t":"orders","a":"processed","k":"order-1","mt":"json"}`, []byte(`{"id":1}`)))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"sow_delete","cid":"del-1","t":"orders","a":"processed,persisted","s":"55","k":"order-1"}`, nil))

	var foundPersistedSeq = false
	var attempts = 0
	for attempts = 0; attempts < 3; attempts++ {
		var body = readFrameBody(t, client, 500*time.Millisecond)
		if strings.Contains(body, `"cid":"del-1"`) && strings.Contains(body, `"a":"persisted"`) && strings.Contains(body, `"s":55`) {
			foundPersistedSeq = true
			break
		}
	}

	_ = client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("handleConnection did not exit in time")
	}

	if !foundPersistedSeq {
		t.Fatalf("expected sow_delete persisted ack with echoed sequence id")
	}
}

func TestPublishSyncAckReturnsFailureWhenReplicationWriteFails(t *testing.T) {
	var oldSow = sow
	var oldJournal = journal
	var oldPeers = replicaPeers
	sow = newSOWCache()
	journal = newMessageJournal(1000)

	var localPeer, remotePeer = net.Pipe()
	_ = remotePeer.Close()
	replicaPeers = []*peerConn{{
		addr:  "pipe",
		conn:  localPeer,
		alive: true,
	}}

	defer func() {
		_ = localPeer.Close()
		replicaPeers = oldPeers
		sow = oldSow
		journal = oldJournal
	}()

	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	var done = make(chan struct{})
	go func() {
		var conn, acceptErr = listener.Accept()
		if acceptErr != nil {
			close(done)
			return
		}
		handleConnection(conn)
		close(done)
	}()

	var client, dialErr = net.Dial("tcp", listener.Addr().String())
	if dialErr != nil {
		t.Fatalf("failed to dial test server: %v", dialErr)
	}

	sendFrame(t, client, buildCommandFrame(`{"c":"logon","cid":"log-1","a":"processed","client_name":"sync-client"}`, nil))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"publish","cid":"sync-1","t":"orders","a":"processed,persisted,sync","s":"77","k":"order-1","mt":"json"}`, []byte(`{"id":1}`)))

	var persistedFailure = false
	var readCount = 0
	for readCount = 0; readCount < 3; readCount++ {
		var body = readFrameBody(t, client, 500*time.Millisecond)
		if strings.Contains(body, `"cid":"sync-1"`) && strings.Contains(body, `"a":"persisted"`) && strings.Contains(body, `"status":"failure"`) {
			persistedFailure = true
			break
		}
	}

	_ = client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("handleConnection did not exit in time")
	}

	if !persistedFailure {
		t.Fatalf("expected persisted failure ack when sync replication write fails")
	}
}

func TestPublishSyncAckReturnsFailureWhenReplicaRejectsApply(t *testing.T) {
	var oldSow = sow
	var oldJournal = journal
	var oldPeers = replicaPeers
	sow = newSOWCache()
	journal = newMessageJournal(1000)

	var localPeer, remotePeer = net.Pipe()
	replicaPeers = []*peerConn{{
		addr:       "pipe",
		conn:       localPeer,
		alive:      true,
		pendingAck: make(map[string]chan headerFields),
	}}
	var peerReadDone = make(chan struct{})
	go func() {
		replicaPeers[0].readFromPeer(localPeer)
		close(peerReadDone)
	}()

	var remoteDone = make(chan struct{})
	var remoteErr = make(chan error, 1)
	go func() {
		defer close(remoteDone)
		var err = remotePeer.SetReadDeadline(time.Now().Add(2 * time.Second))
		if err != nil {
			remoteErr <- err
			return
		}

		var lenBuf [4]byte
		_, err = io.ReadFull(remotePeer, lenBuf[:])
		if err != nil {
			remoteErr <- err
			return
		}

		var frameLen = binary.BigEndian.Uint32(lenBuf[:])
		var frame = make([]byte, frameLen)
		_, err = io.ReadFull(remotePeer, frame)
		if err != nil {
			remoteErr <- err
			return
		}

		var header, _ = parseAMPSHeader(frame)
		var ackBuf = bytes.NewBuffer(nil)
		var ackFrame = buildAck(ackBuf, "processed", header.cid, "failure", kv{k: "reason", v: "replica rejected"})
		var _, writeErr = remotePeer.Write(ackFrame)
		remoteErr <- writeErr
	}()

	defer func() {
		_ = localPeer.Close()
		_ = remotePeer.Close()
		<-peerReadDone
		replicaPeers = oldPeers
		sow = oldSow
		journal = oldJournal
	}()

	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	var done = make(chan struct{})
	go func() {
		var conn, acceptErr = listener.Accept()
		if acceptErr != nil {
			close(done)
			return
		}
		handleConnection(conn)
		close(done)
	}()

	var client, dialErr = net.Dial("tcp", listener.Addr().String())
	if dialErr != nil {
		t.Fatalf("failed to dial test server: %v", dialErr)
	}

	sendFrame(t, client, buildCommandFrame(`{"c":"logon","cid":"log-1","a":"processed","client_name":"sync-client"}`, nil))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"publish","cid":"sync-2","t":"orders","a":"processed,persisted,sync","s":"78","k":"order-1","mt":"json"}`, []byte(`{"id":1}`)))

	var persistedFailure = false
	var readCount = 0
	for readCount = 0; readCount < 6; readCount++ {
		var body = readFrameBody(t, client, 500*time.Millisecond)
		if strings.Contains(body, `"cid":"sync-2"`) && strings.Contains(body, `"a":"persisted"`) && strings.Contains(body, `"status":"failure"`) {
			persistedFailure = true
			break
		}
	}

	_ = client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("handleConnection did not exit in time")
	}

	<-remoteDone
	if err := <-remoteErr; err != nil {
		t.Fatalf("replica helper failed: %v", err)
	}

	if !persistedFailure {
		t.Fatalf("expected persisted failure ack when replica sends failure ack")
	}
}

func TestBuildPersistedAckFailureIncludesSequence(t *testing.T) {
	var buf = bytes.NewBuffer(nil)
	var frame = buildPersistedAckStatus(buf, "cid-1", "42", "failure", "replication failed")
	var body = string(frame[4:])

	if !strings.Contains(body, `"a":"persisted"`) || !strings.Contains(body, `"status":"failure"`) || !strings.Contains(body, `"s":42`) {
		t.Fatalf("unexpected persisted failure ack body: %s", body)
	}
}
