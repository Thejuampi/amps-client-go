package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Replication — multi-instance HA with message forwarding.
//
// Real AMPS supports replication between instances for high availability.
// When a message is published to one instance, it is replicated to all
// peer instances. If one instance fails, clients can reconnect to another
// and continue from their last bookmark position.
//
// Configuration:
//   -peers "host1:port1,host2:port2"
//   -repl-id "instance-1"   (unique ID for this instance to prevent loops)
//
// Architecture:
//   - Each peer connection has a dedicated writer goroutine
//   - Publishes are forwarded to all connected peers
//   - Received replicated messages are applied locally (no re-forwarding)
//   - Heartbeat keepalive between peers
//   - Auto-reconnect on peer disconnect
// ---------------------------------------------------------------------------

type peerConn struct {
	addr       string
	conn       net.Conn
	mu         sync.Mutex
	alive      bool
	stopCh     chan struct{}
	pendingMu  sync.Mutex
	pendingAck map[string]chan headerFields
	nextCID    atomic.Uint64
}

var (
	replicaPeers []*peerConn
	replID       string // this instance's replication ID
)

// initReplication sets up outbound peer connections.
func initReplication(peerAddrs string, instanceID string) {
	if peerAddrs == "" {
		return
	}
	replID = instanceID
	for _, addr := range strings.Split(peerAddrs, ",") {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		p := &peerConn{addr: addr, stopCh: make(chan struct{}), pendingAck: make(map[string]chan headerFields)}
		replicaPeers = append(replicaPeers, p)
		go p.connectLoop()
	}
}

func (p *peerConn) connectLoop() {
	for {
		select {
		case <-p.stopCh:
			return
		default:
		}

		conn, err := net.DialTimeout("tcp", p.addr, 5*time.Second)
		if err != nil {
			log.Printf("fakeamps: repl peer %s connect failed: %v", p.addr, err)
			time.Sleep(2 * time.Second)
			continue
		}

		log.Printf("fakeamps: repl peer %s connected", p.addr)
		p.mu.Lock()
		p.conn = conn
		p.alive = true
		p.mu.Unlock()

		// Send logon to peer.
		p.sendLogon()
		if err := p.syncCatchUp(); err != nil {
			log.Printf("fakeamps: repl peer %s catch-up failed: %v", p.addr, err)
		}

		// Read loop — receive replicated messages from peer.
		p.readFromPeer(conn)

		p.mu.Lock()
		p.alive = false
		p.conn = nil
		p.mu.Unlock()
		_ = conn.Close()
		log.Printf("fakeamps: repl peer %s disconnected, reconnecting...", p.addr)
		time.Sleep(2 * time.Second)
	}
}

func (p *peerConn) sendLogon() {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	startFrame(buf)
	buf.WriteString(`{"c":"logon","client_name":"repl-`)
	buf.WriteString(replID)
	buf.WriteString(`","a":"processed"}`)
	frame := finalizeFrame(buf)
	p.mu.Lock()
	if p.conn != nil {
		_, _ = p.conn.Write(frame)
	}
	p.mu.Unlock()
}

func (p *peerConn) readFromPeer(conn net.Conn) {
	var frameLenBuf [4]byte
	readBuf := make([]byte, 0, 64*1024)

	for {
		if _, err := io.ReadFull(conn, frameLenBuf[:]); err != nil {
			return
		}
		frameLen := binary.BigEndian.Uint32(frameLenBuf[:])
		if frameLen == 0 {
			continue // heartbeat
		}
		if int(frameLen) > cap(readBuf) {
			readBuf = make([]byte, frameLen)
		}
		frame := readBuf[:frameLen]
		if _, err := io.ReadFull(conn, frame); err != nil {
			return
		}

		// Parse and apply locally (without re-replicating).
		header, payload := parseAMPSHeader(frame)
		if header.c == "ack" {
			p.handlePeerAck(header)
			continue
		}
		if header.c == "publish" || header.c == "delta_publish" || header.c == "p" {
			applyReplicatedPublish(header, payload)
		}
	}
}

func applyReplicatedPublish(header headerFields, payload []byte) {
	topic := header.t
	if topic == "" {
		return
	}

	mt := getOrSetTopicMessageType(topic, header.mt)
	sowKey := header.k
	if sowKey == "" && len(payload) > 0 {
		sowKey = extractSowKey(payload)
	}

	var bm string
	var seq uint64
	if journal != nil {
		bm, seq = journal.append(topic, sowKey, payload)
	} else {
		seq = globalBookmarkSeq.Add(1)
		bm = makeBookmark(seq)
	}

	if sow != nil && topic != "" {
		if sowKey == "" {
			sowKey = makeSowKey(topic, seq)
		}
		ts := makeTimestamp()
		isDelta := header.c == "delta_publish"
		if isDelta {
			sow.deltaUpsert(topic, sowKey, payload, bm, ts, seq, 0)
		} else {
			sow.upsert(topic, sowKey, payload, bm, ts, seq, 0)
		}
	}

	// Fan-out to local subscribers.
	ts := makeTimestamp()
	isQueue := strings.HasPrefix(topic, "queue://")
	buf := getWriteBuf()
	defer putWriteBuf(buf)

	forEachMatchingSubscriber(topic, payload, func(sub *subscription) {
		queueMode := isQueue || sub.isQueue
		frame := buildPublishDelivery(buf, topic, sub.subID,
			payload, bm, ts, sowKey, mt, queueMode)
		sub.writer.send(frame)
	})
}

const replicationSyncAckTimeout = 2 * time.Second

func (p *peerConn) nextCommandID() string {
	var id = p.nextCID.Add(1)
	return "repl-" + replID + "-" + strconv.FormatUint(id, 10)
}

func (p *peerConn) registerPendingAck(commandID string) chan headerFields {
	var ackCh = make(chan headerFields, 1)
	p.pendingMu.Lock()
	if p.pendingAck == nil {
		p.pendingAck = make(map[string]chan headerFields)
	}
	p.pendingAck[commandID] = ackCh
	p.pendingMu.Unlock()
	return ackCh
}

func (p *peerConn) clearPendingAck(commandID string) {
	p.pendingMu.Lock()
	delete(p.pendingAck, commandID)
	p.pendingMu.Unlock()
}

func (p *peerConn) handlePeerAck(header headerFields) {
	if p == nil || header.cid == "" {
		return
	}

	p.pendingMu.Lock()
	var ackCh = p.pendingAck[header.cid]
	if ackCh != nil {
		delete(p.pendingAck, header.cid)
	}
	p.pendingMu.Unlock()

	if ackCh != nil {
		ackCh <- header
	}
}

func (p *peerConn) writeReplicatedPublish(topic string, payload []byte, messageType, sowKey string, waitSyncAck bool) error {
	if p == nil {
		return errors.New("nil replication peer")
	}

	var commandID string
	var ackCh chan headerFields
	if waitSyncAck {
		commandID = p.nextCommandID()
		ackCh = p.registerPendingAck(commandID)
	}

	var buf = getWriteBuf()
	startFrame(buf)
	buf.WriteString(`{"c":"publish","t":"`)
	buf.WriteString(topic)
	buf.WriteByte('"')
	writeField(buf, "mt", messageType)
	writeField(buf, "k", sowKey)
	writeField(buf, "_repl", replID)
	if waitSyncAck {
		writeField(buf, "cid", commandID)
		writeField(buf, "a", "processed")
		writeField(buf, "_repl_sync", "1")
	}
	buf.WriteByte('}')
	if len(payload) > 0 {
		buf.Write(payload)
	}
	var frame = finalizeFrame(buf)
	putWriteBuf(buf)

	p.mu.Lock()
	if !p.alive || p.conn == nil {
		p.mu.Unlock()
		if waitSyncAck {
			p.clearPendingAck(commandID)
		}
		return errors.New("replication peer unavailable")
	}
	var _, err = p.conn.Write(frame)
	p.mu.Unlock()
	if err != nil {
		if waitSyncAck {
			p.clearPendingAck(commandID)
		}
		return err
	}

	if !waitSyncAck {
		return nil
	}

	select {
	case ack := <-ackCh:
		if ack.status != "success" {
			return errors.New(firstNonEmpty(ack.reason, "replication peer acknowledged failure"))
		}
		return nil
	case <-time.After(replicationSyncAckTimeout):
		p.clearPendingAck(commandID)
		return errors.New("replication sync ack timed out")
	}
}

func (p *peerConn) syncCatchUp() error {
	if p == nil || journal == nil {
		return nil
	}

	var entries = journal.replayAll(0)
	var index int
	for index = 0; index < len(entries); index++ {
		var entry = entries[index]
		if entry.topic == "" {
			continue
		}
		var err = p.writeReplicatedPublish(entry.topic, entry.payload, getTopicMessageType(entry.topic), entry.sowKey, false)
		if err != nil {
			return err
		}
	}

	return nil
}

// replicatePublish forwards a publish to all connected peers.
func replicatePublish(topic string, payload []byte, messageType, sowKey string) error {
	return replicatePublishWithMode(topic, payload, messageType, sowKey, false)
}

func replicatePublishSync(topic string, payload []byte, messageType, sowKey string) error {
	return replicatePublishWithMode(topic, payload, messageType, sowKey, true)
}

func replicatePublishWithMode(topic string, payload []byte, messageType, sowKey string, waitSyncAck bool) error {
	if len(replicaPeers) == 0 {
		return nil
	}

	var firstErr error
	for _, p := range replicaPeers {
		var err = p.writeReplicatedPublish(topic, payload, messageType, sowKey, waitSyncAck)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func stopReplication() {
	for _, p := range replicaPeers {
		close(p.stopCh)
		p.mu.Lock()
		if p.conn != nil {
			_ = p.conn.Close()
		}
		p.mu.Unlock()
	}
}

// parseReplicationID extracts the _repl field from a publish header
// to detect replication loops.
func isReplicatedMessage(header headerFields) bool {
	return header.repl != ""
}
