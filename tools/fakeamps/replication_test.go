package main

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

func readPeerHeader(t *testing.T, conn net.Conn, timeout time.Duration) headerFields {
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

	var returnHeader, _ = parseAMPSHeader(frame)
	return returnHeader
}

func TestReplicationNoPeersHelpers(t *testing.T) {
	initReplication("", "instance-test")
	var err = replicatePublish("orders", []byte(`{"id":1}`), "json", "k1")
	stopReplication()
	if err != nil {
		t.Fatalf("expected no replication error with no peers: %v", err)
	}

	if isReplicatedMessage(headerFields{}) {
		t.Fatalf("expected isReplicatedMessage to be false")
	}
}

func TestApplyReplicatedPublish(t *testing.T) {
	var oldSow = sow
	var oldJournal = journal
	sow = newSOWCache()
	journal = newMessageJournal(100)
	defer func() {
		sow = oldSow
		journal = oldJournal
	}()

	var h = headerFields{c: "publish", t: "orders", mt: "json", k: "k1"}
	applyReplicatedPublish(h, []byte(`{"id":1}`))

	var result = sow.query("orders", "", -1, "")
	if result.totalCount != 1 {
		t.Fatalf("expected replicated publish to upsert sow record, got count=%d", result.totalCount)
	}

	var entries = journal.replayFrom("orders", 0)
	if len(entries) != 1 {
		t.Fatalf("expected replicated publish to append journal entry, got %d", len(entries))
	}
}

func TestPeerSyncCatchUpWritesExistingJournalEntry(t *testing.T) {
	var oldJournal = journal
	var oldReplID = replID
	journal = newMessageJournal(100)
	replID = "node-test"
	_, _ = journal.append("orders", "k1", []byte(`{"id":1}`))
	defer func() {
		journal = oldJournal
		replID = oldReplID
	}()

	var localPeer, remotePeer = net.Pipe()
	defer func() {
		_ = localPeer.Close()
		_ = remotePeer.Close()
	}()

	var peer = &peerConn{
		addr:       "pipe",
		conn:       localPeer,
		alive:      true,
		pendingAck: make(map[string]chan headerFields),
	}

	var headerCh = make(chan headerFields, 1)
	go func() {
		var header = readPeerHeader(t, remotePeer, 2*time.Second)
		headerCh <- header
	}()

	var err = peer.syncCatchUp()
	if err != nil {
		t.Fatalf("expected catch-up to succeed: %v", err)
	}

	var header = <-headerCh
	if header.c != "publish" || header.t != "orders" || header.repl != "node-test" {
		t.Fatalf("unexpected catch-up frame header: %+v", header)
	}
}
