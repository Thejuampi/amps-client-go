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

func tryReadFrameBody(conn net.Conn, timeout time.Duration) (string, bool) {
	var err = conn.SetReadDeadline(time.Now().Add(timeout))
	if err != nil {
		return "", false
	}

	var lenBuf [4]byte
	_, err = io.ReadFull(conn, lenBuf[:])
	if err != nil {
		_ = conn.SetReadDeadline(time.Time{})
		return "", false
	}

	var frameLen = binary.BigEndian.Uint32(lenBuf[:])
	var frame = make([]byte, frameLen)
	_, err = io.ReadFull(conn, frame)
	if err != nil {
		_ = conn.SetReadDeadline(time.Time{})
		return "", false
	}

	_ = conn.SetReadDeadline(time.Time{})
	return string(frame), true
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

func TestPublishSequenceReplaySkipsSecondApply(t *testing.T) {
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

	sendFrame(t, client, buildCommandFrame(`{"c":"logon","cid":"log-1","a":"processed","client_name":"seq-client"}`, nil))
	sendFrame(t, client, buildCommandFrame(`{"c":"publish","cid":"seq-1","t":"orders","a":"processed,persisted","s":"10","k":"order-1","mt":"json"}`, []byte(`{"id":1}`)))
	sendFrame(t, client, buildCommandFrame(`{"c":"publish","cid":"seq-2","t":"orders","a":"processed,persisted","s":"9","k":"order-2","mt":"json"}`, []byte(`{"id":2}`)))

	_ = client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("handleConnection did not exit in time")
	}

	<-readDone

	var result = sow.query("orders", "", -1, "")
	if result.totalCount != 1 {
		t.Fatalf("expected exactly one applied publish for replayed sequence, got %d", result.totalCount)
	}
}

func TestLogonRedirectReturnsRedirectFrame(t *testing.T) {
	var oldRedirect = *flagRedirectURI
	*flagRedirectURI = "tcp://127.0.0.1:19001/amps/json"
	defer func() {
		*flagRedirectURI = oldRedirect
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

	sendFrame(t, client, buildCommandFrame(`{"c":"logon","cid":"redir-1","a":"processed","client_name":"redir-client"}`, nil))
	var body = readFrameBody(t, client, 500*time.Millisecond)
	if !strings.Contains(body, `"c":"redirect"`) || !strings.Contains(body, `"uri":"tcp://127.0.0.1:19001/amps/json"`) {
		t.Fatalf("expected redirect frame, got %s", body)
	}

	_ = client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("handleConnection did not exit in time")
	}
}

func TestSOWHistoricalQueryReturnsBookmarkSnapshot(t *testing.T) {
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

	sendFrame(t, client, buildCommandFrame(`{"c":"logon","cid":"log-1","a":"processed","client_name":"hist-client"}`, nil))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"publish","cid":"pub-1","t":"orders","a":"processed","k":"order-1","mt":"json"}`, []byte(`{"id":1,"v":1}`)))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"publish","cid":"pub-2","t":"orders","a":"processed","k":"order-1","mt":"json"}`, []byte(`{"id":1,"v":2}`)))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"sow","cid":"sow-1","query_id":"q1","t":"orders","bm":"1|1|1|","a":"processed,completed"}`, nil))

	var foundHistoricalRecord = false
	var attempts int
	for attempts = 0; attempts < 6; attempts++ {
		var body = readFrameBody(t, client, 500*time.Millisecond)
		if strings.Contains(body, `"c":"sow"`) && strings.Contains(body, `{"id":1,"v":1}`) {
			foundHistoricalRecord = true
			break
		}
	}

	_ = client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("handleConnection did not exit in time")
	}

	if !foundHistoricalRecord {
		t.Fatalf("expected historical SOW record at bookmark sequence")
	}
}

func TestPublishEvictionSendsOOFWithEvictedReason(t *testing.T) {
	var oldSow = sow
	var oldJournal = journal
	sow = newSOWCacheWithEviction(1, evictionCapacity)
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

	sendFrame(t, client, buildCommandFrame(`{"c":"logon","cid":"log-1","a":"processed","client_name":"evict-client"}`, nil))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"delta_subscribe","cid":"sub-1","sub_id":"d1","t":"orders","a":"processed"}`, nil))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"publish","cid":"pub-1","t":"orders","a":"processed","k":"order-1","mt":"json"}`, []byte(`{"id":1}`)))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"publish","cid":"pub-2","t":"orders","a":"processed","k":"order-2","mt":"json"}`, []byte(`{"id":2}`)))

	var foundEvictionOOF = false
	var attempts int
	for attempts = 0; attempts < 6; attempts++ {
		var body = readFrameBody(t, client, 500*time.Millisecond)
		if strings.Contains(body, `"c":"oof"`) && strings.Contains(body, `"reason":"evicted"`) {
			foundEvictionOOF = true
			break
		}
	}

	_ = client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("handleConnection did not exit in time")
	}

	if !foundEvictionOOF {
		t.Fatalf("expected OOF frame with eviction reason")
	}
}

func TestSOWDeleteStatsAckIncludesTopicMatches(t *testing.T) {
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

	sendFrame(t, client, buildCommandFrame(`{"c":"logon","cid":"log-1","a":"processed","client_name":"sowdel-stats"}`, nil))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"publish","cid":"pub-1","t":"orders","a":"processed","k":"order-1","mt":"json"}`, []byte(`{"id":1}`)))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"publish","cid":"pub-2","t":"orders","a":"processed","k":"order-2","mt":"json"}`, []byte(`{"id":2}`)))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"sow_delete","cid":"del-1","t":"orders","filter":"/id = 1","a":"stats"}`, nil))

	var body = readFrameBody(t, client, 500*time.Millisecond)
	if !strings.Contains(body, `"a":"stats"`) || !strings.Contains(body, `"records_deleted":1`) || !strings.Contains(body, `"topic_matches":2`) {
		t.Fatalf("expected stats ack with topic_matches and records_deleted, got %s", body)
	}

	_ = client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("handleConnection did not exit in time")
	}
}

func TestDeltaSubscribeMismatchSendsOOFWithMatchReason(t *testing.T) {
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

	sendFrame(t, client, buildCommandFrame(`{"c":"logon","cid":"log-1","a":"processed","client_name":"mismatch-client"}`, nil))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"delta_subscribe","cid":"sub-1","sub_id":"d1","t":"orders","filter":"/status = 'active'","a":"processed"}`, nil))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"publish","cid":"pub-1","t":"orders","a":"processed","k":"order-1","mt":"json"}`, []byte(`{"id":1,"status":"active"}`)))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"publish","cid":"pub-2","t":"orders","a":"processed","k":"order-1","mt":"json"}`, []byte(`{"id":1,"status":"inactive"}`)))

	var foundMatchReasonOOF = false
	var attempts int
	for attempts = 0; attempts < 6; attempts++ {
		var body = readFrameBody(t, client, 500*time.Millisecond)
		if strings.Contains(body, `"c":"oof"`) && strings.Contains(body, `"reason":"match"`) {
			foundMatchReasonOOF = true
			break
		}
	}

	_ = client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("handleConnection did not exit in time")
	}

	if !foundMatchReasonOOF {
		t.Fatalf("expected OOF frame with match reason on filter mismatch")
	}
}

func TestSOWDeleteSendsOOFWithDeleteReason(t *testing.T) {
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

	sendFrame(t, client, buildCommandFrame(`{"c":"logon","cid":"log-1","a":"processed","client_name":"delete-oof-client"}`, nil))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"delta_subscribe","cid":"sub-1","sub_id":"d1","t":"orders","a":"processed"}`, nil))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"publish","cid":"pub-1","t":"orders","a":"processed","k":"order-1","mt":"json"}`, []byte(`{"id":1}`)))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"sow_delete","cid":"del-1","t":"orders","k":"order-1","a":"processed"}`, nil))

	var foundDeleteReasonOOF = false
	var attempts int
	for attempts = 0; attempts < 6; attempts++ {
		var body = readFrameBody(t, client, 500*time.Millisecond)
		if strings.Contains(body, `"c":"oof"`) && strings.Contains(body, `"reason":"delete"`) {
			foundDeleteReasonOOF = true
			break
		}
	}

	_ = client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("handleConnection did not exit in time")
	}

	if !foundDeleteReasonOOF {
		t.Fatalf("expected OOF frame with delete reason after sow_delete")
	}
}

func TestExpiredRecordSendsOOFWithExpireReason(t *testing.T) {
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

	sendFrame(t, client, buildCommandFrame(`{"c":"logon","cid":"log-1","a":"processed","client_name":"expire-oof-client"}`, nil))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"delta_subscribe","cid":"sub-1","sub_id":"d1","t":"orders","a":"processed"}`, nil))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"publish","cid":"pub-1","t":"orders","a":"processed","e":"1","k":"order-1","mt":"json"}`, []byte(`{"id":1}`)))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	time.Sleep(1100 * time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"heartbeat","cid":"hb-1","a":"processed"}`, nil))

	var foundExpireReasonOOF = false
	var attempts int
	for attempts = 0; attempts < 6; attempts++ {
		var body = readFrameBody(t, client, 500*time.Millisecond)
		if strings.Contains(body, `"c":"oof"`) && strings.Contains(body, `"reason":"expire"`) {
			foundExpireReasonOOF = true
			break
		}
	}

	_ = client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("handleConnection did not exit in time")
	}

	if !foundExpireReasonOOF {
		t.Fatalf("expected OOF frame with expire reason when ttl record expires")
	}
}

func TestStartTimerEmitsHeartbeatUntilStop(t *testing.T) {
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

	sendFrame(t, client, buildCommandFrame(`{"c":"logon","cid":"log-1","a":"processed","client_name":"timer-client"}`, nil))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	sendFrame(t, client, buildCommandFrame(`{"c":"start_timer","cid":"tm-1","t":"tm-1","o":"interval=20ms","a":"processed"}`, nil))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	var body, ok = tryReadFrameBody(client, 400*time.Millisecond)
	if !ok || !strings.Contains(body, `"c":"heartbeat"`) {
		t.Fatalf("expected timer heartbeat after start_timer")
	}

	sendFrame(t, client, buildCommandFrame(`{"c":"stop_timer","cid":"tm-2","t":"tm-1","a":"processed"}`, nil))
	_ = readFrameBody(t, client, 500*time.Millisecond)

	_ = client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("handleConnection did not exit in time")
	}
}

func TestQueueMaxBacklogBlocksDeliveryUntilAck(t *testing.T) {
	var oldSow = sow
	var oldJournal = journal
	var oldQueue = *flagQueue
	sow = newSOWCache()
	journal = newMessageJournal(1000)
	*flagQueue = true

	defer func() {
		sow = oldSow
		journal = oldJournal
		*flagQueue = oldQueue
	}()

	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	var serverDone = make(chan struct{})
	go func() {
		defer close(serverDone)
		for {
			var conn, acceptErr = listener.Accept()
			if acceptErr != nil {
				return
			}
			go handleConnection(conn)
		}
	}()

	var consumer, consumerErr = net.Dial("tcp", listener.Addr().String())
	if consumerErr != nil {
		t.Fatalf("failed to dial consumer: %v", consumerErr)
	}
	defer consumer.Close()

	var publisher, publisherErr = net.Dial("tcp", listener.Addr().String())
	if publisherErr != nil {
		t.Fatalf("failed to dial publisher: %v", publisherErr)
	}
	defer publisher.Close()

	sendFrame(t, consumer, buildCommandFrame(`{"c":"logon","cid":"log-c","a":"processed","client_name":"queue-consumer"}`, nil))
	_ = readFrameBody(t, consumer, 500*time.Millisecond)

	sendFrame(t, consumer, buildCommandFrame(`{"c":"subscribe","cid":"sub-c","sub_id":"queue-sub","t":"queue://orders.backlog","a":"processed","o":"max_backlog=1"}`, nil))
	_ = readFrameBody(t, consumer, 500*time.Millisecond)

	sendFrame(t, publisher, buildCommandFrame(`{"c":"logon","cid":"log-p","a":"processed","client_name":"queue-publisher"}`, nil))
	_ = readFrameBody(t, publisher, 500*time.Millisecond)

	sendFrame(t, publisher, buildCommandFrame(`{"c":"publish","cid":"pub-1","t":"queue://orders.backlog","a":"processed","k":"k-1","mt":"json"}`, []byte(`{"id":1}`)))
	_ = readFrameBody(t, publisher, 500*time.Millisecond)

	var firstDelivery = readFrameBody(t, consumer, 500*time.Millisecond)
	var firstHeader, _ = parseAMPSHeader([]byte(firstDelivery))
	if firstHeader.k != "k-1" {
		t.Fatalf("expected first queue delivery key k-1, got %s", firstDelivery)
	}

	sendFrame(t, publisher, buildCommandFrame(`{"c":"publish","cid":"pub-2","t":"queue://orders.backlog","a":"processed","k":"k-2","mt":"json"}`, []byte(`{"id":2}`)))
	_ = readFrameBody(t, publisher, 500*time.Millisecond)

	var blockedBody, blocked = tryReadFrameBody(consumer, 150*time.Millisecond)
	if blocked {
		t.Fatalf("expected second queue publish to be blocked by max_backlog, got %s", blockedBody)
	}

	sendFrame(t, consumer, buildCommandFrame(`{"c":"ack","cid":"ack-1","k":"k-1"}`, nil))

	var secondDelivery = readFrameBody(t, consumer, 500*time.Millisecond)
	var secondHeader, _ = parseAMPSHeader([]byte(secondDelivery))
	if secondHeader.k != "k-2" {
		t.Fatalf("expected queued delivery key k-2 after ack, got %s", secondDelivery)
	}

	_ = listener.Close()
	<-serverDone
}

func TestQueuePullOnlyDeliversOnPullRequest(t *testing.T) {
	var oldSow = sow
	var oldJournal = journal
	var oldQueue = *flagQueue
	sow = newSOWCache()
	journal = newMessageJournal(1000)
	*flagQueue = true

	defer func() {
		sow = oldSow
		journal = oldJournal
		*flagQueue = oldQueue
	}()

	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	var serverDone = make(chan struct{})
	go func() {
		defer close(serverDone)
		for {
			var conn, acceptErr = listener.Accept()
			if acceptErr != nil {
				return
			}
			go handleConnection(conn)
		}
	}()

	var consumer, consumerErr = net.Dial("tcp", listener.Addr().String())
	if consumerErr != nil {
		t.Fatalf("failed to dial consumer: %v", consumerErr)
	}
	defer consumer.Close()

	var publisher, publisherErr = net.Dial("tcp", listener.Addr().String())
	if publisherErr != nil {
		t.Fatalf("failed to dial publisher: %v", publisherErr)
	}
	defer publisher.Close()

	sendFrame(t, consumer, buildCommandFrame(`{"c":"logon","cid":"log-c","a":"processed","client_name":"pull-consumer"}`, nil))
	_ = readFrameBody(t, consumer, 500*time.Millisecond)

	sendFrame(t, consumer, buildCommandFrame(`{"c":"subscribe","cid":"sub-c","sub_id":"pull-sub","t":"queue://orders.pull","a":"processed","o":"pull,max_backlog=2"}`, nil))
	_ = readFrameBody(t, consumer, 500*time.Millisecond)

	sendFrame(t, publisher, buildCommandFrame(`{"c":"logon","cid":"log-p","a":"processed","client_name":"pull-publisher"}`, nil))
	_ = readFrameBody(t, publisher, 500*time.Millisecond)

	sendFrame(t, publisher, buildCommandFrame(`{"c":"publish","cid":"pub-1","t":"queue://orders.pull","a":"processed","k":"pull-1","mt":"json"}`, []byte(`{"id":1}`)))
	_ = readFrameBody(t, publisher, 500*time.Millisecond)

	sendFrame(t, publisher, buildCommandFrame(`{"c":"publish","cid":"pub-2","t":"queue://orders.pull","a":"processed","k":"pull-2","mt":"json"}`, []byte(`{"id":2}`)))
	_ = readFrameBody(t, publisher, 500*time.Millisecond)

	var prematureBody, premature = tryReadFrameBody(consumer, 150*time.Millisecond)
	if premature {
		t.Fatalf("expected pull subscription to suppress push delivery, got %s", prematureBody)
	}

	sendFrame(t, consumer, buildCommandFrame(`{"c":"sow_and_subscribe","cid":"pull-now","sub_id":"pull-sub","query_id":"q-pull","t":"queue://orders.pull","a":"processed,completed","top_n":"2","o":"pull,replace"}`, nil))

	var pulled = 0
	var attempts int
	for attempts = 0; attempts < 8; attempts++ {
		var body = readFrameBody(t, consumer, 500*time.Millisecond)
		if strings.Contains(body, `"c":"p"`) {
			pulled++
		}
		if pulled == 2 {
			break
		}
	}

	if pulled != 2 {
		t.Fatalf("expected two pulled queue deliveries, got %d", pulled)
	}

	_ = listener.Close()
	<-serverDone
}

func TestLogonChallengeFlowRequiresChallengeResponse(t *testing.T) {
	resetAuthForTest()
	configureAuth("alice:pwd")

	auth.mu.Lock()
	auth.entitlements["alice"] = &topicEntitlement{subscribeAllow: []string{"orders"}, publishAllow: []string{"orders"}, sowAllow: []string{"orders"}}
	auth.mu.Unlock()

	var oldAuthFlag = *flagAuth
	var oldChallengeFlag = *flagAuthChallenge
	*flagAuth = "alice:pwd"
	*flagAuthChallenge = true
	defer func() {
		*flagAuth = oldAuthFlag
		*flagAuthChallenge = oldChallengeFlag
		resetAuthForTest()
	}()

	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

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

	sendFrame(t, client, buildCommandFrame(`{"c":"logon","cid":"log-1","a":"processed","user_id":"alice","pw":"pwd","client_name":"challenge-client"}`, nil))
	var challengeBody = readFrameBody(t, client, 500*time.Millisecond)
	var challengeHeader, _ = parseAMPSHeader([]byte(challengeBody))
	if challengeHeader.status != "retry" || !strings.HasPrefix(challengeHeader.reason, "challenge:") {
		t.Fatalf("expected challenge retry ack, got %s", challengeBody)
	}

	var nonce = strings.TrimPrefix(challengeHeader.reason, "challenge:")
	sendFrame(t, client, buildCommandFrame(`{"c":"logon","cid":"log-2","a":"processed","user_id":"alice","pw":"`+nonce+`:pwd","client_name":"challenge-client"}`, nil))

	var successBody = readFrameBody(t, client, 500*time.Millisecond)
	var successHeader, _ = parseAMPSHeader([]byte(successBody))
	if successHeader.status != "success" {
		t.Fatalf("expected successful post-challenge logon, got %s", successBody)
	}

	_ = client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("handleConnection did not exit in time")
	}
}

func TestEntitlementFilterAppliedToSubscribeDelivery(t *testing.T) {
	resetAuthForTest()
	configureAuth("alice:pwd:/owner = 'alice',writer:pwd")

	auth.mu.Lock()
	auth.entitlements["alice"] = &topicEntitlement{subscribeAllow: []string{"orders"}, sowAllow: []string{"orders"}}
	auth.entitlements["writer"] = &topicEntitlement{publishAllow: []string{"orders"}}
	auth.mu.Unlock()

	var oldSow = sow
	var oldJournal = journal
	var oldAuthFlag = *flagAuth
	sow = newSOWCache()
	journal = newMessageJournal(1000)
	*flagAuth = "alice:pwd:/owner = 'alice',writer:pwd"

	defer func() {
		sow = oldSow
		journal = oldJournal
		*flagAuth = oldAuthFlag
		resetAuthForTest()
	}()

	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	var serverDone = make(chan struct{})
	go func() {
		defer close(serverDone)
		for {
			var conn, acceptErr = listener.Accept()
			if acceptErr != nil {
				return
			}
			go handleConnection(conn)
		}
	}()

	var subscriber, subscriberErr = net.Dial("tcp", listener.Addr().String())
	if subscriberErr != nil {
		t.Fatalf("failed to dial subscriber: %v", subscriberErr)
	}
	defer subscriber.Close()

	var publisher, publisherErr = net.Dial("tcp", listener.Addr().String())
	if publisherErr != nil {
		t.Fatalf("failed to dial publisher: %v", publisherErr)
	}
	defer publisher.Close()

	sendFrame(t, subscriber, buildCommandFrame(`{"c":"logon","cid":"s-log","a":"processed","user_id":"alice","pw":"pwd","client_name":"alice-sub"}`, nil))
	_ = readFrameBody(t, subscriber, 500*time.Millisecond)

	sendFrame(t, subscriber, buildCommandFrame(`{"c":"subscribe","cid":"s-sub","sub_id":"sub-1","t":"orders","a":"processed","filter":"/status = 'active'"}`, nil))
	_ = readFrameBody(t, subscriber, 500*time.Millisecond)

	sendFrame(t, publisher, buildCommandFrame(`{"c":"logon","cid":"p-log","a":"processed","user_id":"writer","pw":"pwd","client_name":"writer-pub"}`, nil))
	_ = readFrameBody(t, publisher, 500*time.Millisecond)

	sendFrame(t, publisher, buildCommandFrame(`{"c":"publish","cid":"p-1","t":"orders","a":"processed","k":"row-1","mt":"json"}`, []byte(`{"id":1,"status":"active","owner":"alice"}`)))
	_ = readFrameBody(t, publisher, 500*time.Millisecond)

	sendFrame(t, publisher, buildCommandFrame(`{"c":"publish","cid":"p-2","t":"orders","a":"processed","k":"row-2","mt":"json"}`, []byte(`{"id":2,"status":"active","owner":"bob"}`)))
	_ = readFrameBody(t, publisher, 500*time.Millisecond)

	var deliveryBody = readFrameBody(t, subscriber, 500*time.Millisecond)
	var _, deliveryPayload = parseAMPSHeader([]byte(deliveryBody))
	if !strings.Contains(string(deliveryPayload), `"owner":"alice"`) {
		t.Fatalf("expected entitlement-filtered payload for owner alice, got %s", deliveryBody)
	}

	var leakedBody, leaked = tryReadFrameBody(subscriber, 200*time.Millisecond)
	if leaked {
		t.Fatalf("expected no bob payload delivery under entitlement filter, got %s", leakedBody)
	}

	_ = listener.Close()
	<-serverDone
}
