package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// decompressFrame decompresses a zlib-compressed frame.
// ---------------------------------------------------------------------------

func decompressFrame(data []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// ---------------------------------------------------------------------------
// topicConfig — per-topic configuration, including message type.
//
// In real AMPS, each topic is configured with a message type (json, protobuf,
// nvfix, fix, xml, binary, etc.). A connection publishing to a topic uses
// that topic's message type. You cannot mix message types on the same topic.
// ---------------------------------------------------------------------------

type topicConfig struct {
	messageType string
}

var (
	topicConfigsMu sync.RWMutex
	topicConfigs   = make(map[string]*topicConfig)
)

func getOrSetTopicMessageType(topic, mt string) string {
	topicConfigsMu.RLock()
	if cfg, ok := topicConfigs[topic]; ok {
		topicConfigsMu.RUnlock()
		return cfg.messageType
	}
	topicConfigsMu.RUnlock()

	if mt == "" {
		mt = "json"
	}

	topicConfigsMu.Lock()
	if cfg, ok := topicConfigs[topic]; ok {
		topicConfigsMu.Unlock()
		return cfg.messageType
	}
	topicConfigs[topic] = &topicConfig{messageType: mt}
	topicConfigsMu.Unlock()
	return mt
}

func getTopicMessageType(topic string) string {
	topicConfigsMu.RLock()
	defer topicConfigsMu.RUnlock()
	if cfg, ok := topicConfigs[topic]; ok {
		return cfg.messageType
	}
	return "json"
}

// ---------------------------------------------------------------------------
// handleConnection — per-connection reader goroutine.
//
// Architecture mirrors real AMPS's "army of threads":
//   - READER goroutine (this function): reads frames, parses commands,
//     dispatches responses, fan-out, auth, actions, views.
//   - WRITER goroutine (connWriter.run): drains the outbound channel,
//     coalesces writes, and flushes to the network.
//   - CONFLATION goroutine (optional): per-subscription timer-based
//     merge of pending messages for slow consumers.
// ---------------------------------------------------------------------------

func handleConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr().String()
	if *flagLogConn {
		log.Printf("fakeamps: connected  %s  (total=%d active=%d)",
			remoteAddr, globalConnectionsAccepted.Load(), globalConnectionsCurrent.Load())
	}

	// TCP tuning.
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(*flagNoDelay)
		_ = tc.SetWriteBuffer(*flagWriteBuf)
		_ = tc.SetReadBuffer(*flagReadBuf)
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(30 * time.Second)
	}

	var stats connStats
	writer := newConnWriter(conn, &stats)

	var statsDone chan struct{}
	if *flagLogStats {
		statsDone = make(chan struct{})
		go statsLogger(remoteAddr, &stats, statsDone)
	}

	// Per-connection state.
	localSubs := make(map[string]*localSub)
	conflationBuffers := make(map[string]*conflationBuffer) // subID → conflation buffer
	var connUserID string                                   // set on logon
	var connClientName string                               // set on logon
	var heartbeatWatchdog *time.Timer                       // liveness check
	var heartbeatTimeout time.Duration

	defer func() {
		// Stop all conflation timers.
		for _, cb := range conflationBuffers {
			cb.stop()
		}
		unregisterAll(conn)
		writer.close()
		_ = conn.Close()
		globalConnectionsCurrent.Add(-1)
		if statsDone != nil {
			close(statsDone)
		}
		if heartbeatWatchdog != nil {
			heartbeatWatchdog.Stop()
		}
		if *flagLogConn {
			log.Printf("fakeamps: disconnect %s user=%q (msgs_in=%d msgs_out=%d pub_in=%d pub_out=%d bytes_in=%d bytes_out=%d active=%d)",
				remoteAddr, connUserID,
				stats.messagesIn.Load(), stats.messagesOut.Load(),
				stats.publishIn.Load(), stats.publishOut.Load(),
				stats.bytesIn.Load(), stats.bytesOut.Load(),
				globalConnectionsCurrent.Load())
		}
	}()

	buf := bytes.NewBuffer(make([]byte, 0, 512))
	readBuf := make([]byte, 0, *flagReadBuf)
	var frameLenBuf [4]byte

	for {
		// ---- Read frame length ----
		if _, err := io.ReadFull(conn, frameLenBuf[:]); err != nil {
			if err != io.EOF && !isClosedError(err) {
				log.Printf("fakeamps: %s read len: %v", remoteAddr, err)
			}
			return
		}

		frameLen := binary.BigEndian.Uint32(frameLenBuf[:])

		// Zero-length: heartbeat beat echo.
		if frameLen == 0 {
			writer.sendDirect([]byte{0, 0, 0, 0})
			stats.messagesIn.Add(1)
			stats.bytesIn.Add(4)
			continue
		}

		// ---- Read frame body ----
		if int(frameLen) > cap(readBuf) {
			readBuf = make([]byte, frameLen)
		}
		frame := readBuf[:frameLen]
		if _, err := io.ReadFull(conn, frame); err != nil {
			if !isClosedError(err) {
				log.Printf("fakeamps: %s read frame: %v", remoteAddr, err)
			}
			return
		}
		stats.messagesIn.Add(1)
		stats.bytesIn.Add(uint64(frameLen) + 4)

		// Decompress if frame starts with compression flag.
		if frameLen > 1 && frame[0] == 'z' {
			if decompressed, err := decompressFrame(frame[1:]); err == nil {
				frame = decompressed
			}
		}

		if *flagLatency > 0 {
			time.Sleep(*flagLatency)
		}

		// ---- Parse ----
		header, payload := parseAMPSHeader(frame)

		command := header.c
		commandID := header.cid
		topic := header.t
		subID := header.subID
		ackTypes := header.a
		options := firstNonEmpty(header.opts, header.o)
		seqID := header.s
		bookmark := header.bm
		sowKey := header.k
		filter := header.filter
		messageType := header.mt

		wantProcessed := containsToken(ackTypes, "processed")
		wantPersisted := containsToken(ackTypes, "persisted")
		wantSync := containsToken(ackTypes, "sync")
		wantCompleted := containsToken(ackTypes, "completed")
		wantStats := containsToken(ackTypes, "stats")
		wantReceived := containsToken(ackTypes, "received")

		isQueueTopic := *flagQueue && strings.HasPrefix(topic, "queue://")
		isBookmarkSub := containsToken(options, "bookmark")
		isDelta := strings.Contains(command, "delta")

		// Parse expiration.
		var expiration time.Duration
		if header.e != "" {
			if secs, err := strconv.ParseUint(header.e, 10, 32); err == nil && secs > 0 {
				expiration = time.Duration(secs) * time.Second
			}
		}

		// Parse top_n.
		topN := -1
		if header.topN != "" {
			if n, err := strconv.Atoi(header.topN); err == nil {
				topN = n
			}
		}

		// Parse conflation interval and key from options.
		conflationInterval := parseConflationInterval(options)
		conflationKey := parseConflationKey(options)

		// Parse aggregation/projection from options.
		aggQ := parseAggQuery(options)

		// ---- Received ack ----
		if wantReceived {
			writer.send(buildAck(buf, "received", commandID, "success"))
		}

		switch command {
		// ---------------------------------------------------------------
		// LOGON
		// ---------------------------------------------------------------
		case "logon":
			connUserID = header.userID
			connClientName = firstNonEmpty(header.clientName, connUserID)

			// Authentication check.
			if *flagAuth != "" {
				result := authenticateLogon(header.userID, header.pw)
				if !result.success {
					writer.send(buildAck(buf, "processed", commandID, "failure",
						kv{k: "reason", v: result.reason}))
					return // disconnect on auth failure
				}
			}

			// Enable compression if client requests it.
			if strings.Contains(options, "c") || strings.Contains(options, "compress") {
				writer.EnableCompression()
			}

			writer.send(buildLogonAck(buf, commandID, header.clientName, header.x))

		// ---------------------------------------------------------------
		// SUBSCRIBE / DELTA_SUBSCRIBE
		// ---------------------------------------------------------------
		case "subscribe", "delta_subscribe":
			// Authorization check.
			if *flagAuth != "" && !authorizeCommand(connUserID, command, topic) {
				writer.send(buildAck(buf, "processed", commandID, "failure",
					kv{k: "reason", v: "not entitled"}))
				continue
			}

			effectiveSubID := firstNonEmpty(subID, commandID)

			if *flagFanout && topic != "" {
				// Handle "replace" option: remove existing sub with same ID.
				if containsToken(options, "replace") {
					if ls, ok := localSubs[effectiveSubID]; ok {
						unregisterSubscription(ls.topic, ls.sub)
						delete(localSubs, effectiveSubID)
						if cb, ok := conflationBuffers[effectiveSubID]; ok {
							cb.stop()
							delete(conflationBuffers, effectiveSubID)
						}
					}
				}

				sub := &subscription{
					conn:       conn,
					subID:      effectiveSubID,
					topic:      topic,
					filter:     filter,
					writer:     writer,
					isQueue:    isQueueTopic,
					isBookmark: isBookmarkSub,
					isDelta:    isDelta,
				}
				registerSubscription(topic, sub)
				localSubs[effectiveSubID] = &localSub{topic: topic, sub: sub}

				// Set up conflation buffer if configured.
				if conflationInterval > 0 {
					var cb *conflationBuffer
					if conflationKey != "" {
						cb = newConflationBufferWithKey(sub, conflationInterval, conflationKey)
					} else {
						cb = newConflationBuffer(sub, conflationInterval)
					}
					conflationBuffers[effectiveSubID] = cb
				}
			}

			if wantProcessed {
				writer.send(buildAck(buf, "processed", commandID, "success"))
			}

			// Bookmark replay.
			if isBookmarkSub && journal != nil {
				afterSeq := parseBookmarkSeq(bookmark)
				entries := journal.replayFrom(topic, afterSeq)
				mt := getTopicMessageType(topic)
				for _, e := range entries {
					if filter != "" && !evaluateFilter(filter, e.payload) {
						continue
					}
					deliveryPayload := fireOnDeliver(topic, e.payload, effectiveSubID)
					frame := buildPublishDelivery(buf, topic, effectiveSubID,
						deliveryPayload, e.bookmark, e.timestamp, e.sowKey, mt, isQueueTopic)
					writer.send(frame)
				}
			}

		// ---------------------------------------------------------------
		// SOW
		// ---------------------------------------------------------------
		case "sow":
			if *flagAuth != "" && !authorizeCommand(connUserID, command, topic) {
				writer.send(buildAck(buf, "processed", commandID, "failure",
					kv{k: "reason", v: "not entitled"}))
				continue
			}

			queryID := firstNonEmpty(header.queryID, subID, commandID)

			if wantProcessed {
				writer.send(buildAck(buf, "processed", commandID, "success"))
			}

			recordCount := 0
			totalCount := 0
			if sow != nil {
				result := sow.query(topic, filter, topN, header.orderBy)
				totalCount = result.totalCount
				mt := getTopicMessageType(topic)

				// Apply aggregation if requested.
				if aggQ != nil && aggQ.hasAgg {
					aggResults := executeAggQuery(aggQ, result.records)
					recordCount = len(aggResults)
					if recordCount > 0 {
						writer.send(buildGroupBegin(buf, queryID))
						for i, payload := range aggResults {
							key := "agg-" + strconv.Itoa(i)
							seq := globalBookmarkSeq.Add(1)
							bm := makeBookmark(seq)
							writer.send(buildSOWRecord(buf, topic, queryID, key, bm, mt, payload))
						}
						writer.send(buildGroupEnd(buf, queryID))
					}
				} else {
					recordCount = len(result.records)
					if recordCount > 0 {
						writer.send(buildGroupBegin(buf, queryID))
						for _, r := range result.records {
							recPayload := r.payload
							if aggQ != nil {
								recPayload = projectFieldsFromQuery(r.payload, aggQ)
							}
							writer.send(buildSOWRecord(buf, topic, queryID, r.sowKey, r.bookmark, mt, recPayload))
						}
						writer.send(buildGroupEnd(buf, queryID))
					}
				}
			}

			if wantCompleted {
				writer.send(buildSOWCompletedAck(buf, commandID, queryID, totalCount, 0, 0, 0, 1))
			}

		// ---------------------------------------------------------------
		// SOW_AND_SUBSCRIBE / SOW_AND_DELTA_SUBSCRIBE
		// ---------------------------------------------------------------
		case "sow_and_subscribe", "sow_and_delta_subscribe":
			if *flagAuth != "" && !authorizeCommand(connUserID, command, topic) {
				writer.send(buildAck(buf, "processed", commandID, "failure",
					kv{k: "reason", v: "not entitled"}))
				continue
			}

			effectiveSubID := firstNonEmpty(subID, commandID)
			queryID := firstNonEmpty(header.queryID, effectiveSubID, commandID)

			// Register subscription FIRST.
			if *flagFanout && topic != "" {
				// Handle "replace" option: remove existing sub with same ID.
				if containsToken(options, "replace") {
					if ls, ok := localSubs[effectiveSubID]; ok {
						unregisterSubscription(ls.topic, ls.sub)
						delete(localSubs, effectiveSubID)
						if cb, ok := conflationBuffers[effectiveSubID]; ok {
							cb.stop()
							delete(conflationBuffers, effectiveSubID)
						}
					}
				}

				sub := &subscription{
					conn:       conn,
					subID:      effectiveSubID,
					topic:      topic,
					filter:     filter,
					writer:     writer,
					isQueue:    isQueueTopic,
					isBookmark: isBookmarkSub,
					isDelta:    isDelta,
				}
				registerSubscription(topic, sub)
				localSubs[effectiveSubID] = &localSub{topic: topic, sub: sub}

				if conflationInterval > 0 {
					var cb *conflationBuffer
					if conflationKey != "" {
						cb = newConflationBufferWithKey(sub, conflationInterval, conflationKey)
					} else {
						cb = newConflationBuffer(sub, conflationInterval)
					}
					conflationBuffers[effectiveSubID] = cb
				}
			}

			if wantProcessed {
				writer.send(buildAck(buf, "processed", commandID, "success"))
			}

			// Send SOW snapshot.
			recordCount := 0
			totalCount := 0
			if sow != nil {
				result := sow.query(topic, filter, topN, header.orderBy)
				totalCount = result.totalCount
				mt := getTopicMessageType(topic)

				if aggQ != nil && aggQ.hasAgg {
					aggResults := executeAggQuery(aggQ, result.records)
					recordCount = len(aggResults)
					if recordCount > 0 {
						writer.send(buildGroupBegin(buf, queryID))
						for i, payload := range aggResults {
							key := "agg-" + strconv.Itoa(i)
							seq := globalBookmarkSeq.Add(1)
							bm := makeBookmark(seq)
							writer.send(buildSOWRecord(buf, topic, queryID, key, bm, mt, payload))
						}
						writer.send(buildGroupEnd(buf, queryID))
					}
				} else {
					recordCount = len(result.records)
					if recordCount > 0 {
						writer.send(buildGroupBegin(buf, queryID))
						for _, r := range result.records {
							recPayload := r.payload
							if aggQ != nil {
								recPayload = projectFieldsFromQuery(r.payload, aggQ)
							}
							writer.send(buildSOWRecord(buf, topic, queryID, r.sowKey, r.bookmark, mt, recPayload))
						}
						writer.send(buildGroupEnd(buf, queryID))
					}
				}
			}

			if wantCompleted {
				writer.send(buildSOWCompletedAck(buf, commandID, queryID, totalCount, 0, 0, 0, 1))
			}

		// ---------------------------------------------------------------
		// UNSUBSCRIBE
		// ---------------------------------------------------------------
		case "unsubscribe":
			targetSubID := firstNonEmpty(subID, commandID)

			// Support unsubscribing multiple subs via subIDs (sids) field.
			if header.subIDs != "" {
				for _, sid := range strings.Split(header.subIDs, ",") {
					sid = strings.TrimSpace(sid)
					if ls, ok := localSubs[sid]; ok {
						unregisterSubscription(ls.topic, ls.sub)
						delete(localSubs, sid)
						if cb, ok := conflationBuffers[sid]; ok {
							cb.stop()
							delete(conflationBuffers, sid)
						}
					}
				}
			} else if targetSubID == "all" {
				for id, ls := range localSubs {
					unregisterSubscription(ls.topic, ls.sub)
					delete(localSubs, id)
					if cb, ok := conflationBuffers[id]; ok {
						cb.stop()
						delete(conflationBuffers, id)
					}
				}
			} else if ls, ok := localSubs[targetSubID]; ok {
				unregisterSubscription(ls.topic, ls.sub)
				delete(localSubs, targetSubID)
				if cb, ok := conflationBuffers[targetSubID]; ok {
					cb.stop()
					delete(conflationBuffers, targetSubID)
				}
			}
			if wantProcessed {
				writer.send(buildAck(buf, "processed", commandID, "success"))
			}

		// ---------------------------------------------------------------
		// PUBLISH / DELTA_PUBLISH / "p" (short form)
		// ---------------------------------------------------------------
		case "publish", "delta_publish", "p":
			if *flagAuth != "" && !authorizeCommand(connUserID, command, topic) {
				writer.send(buildAck(buf, "processed", commandID, "failure",
					kv{k: "reason", v: "not entitled"}))
				continue
			}

			stats.publishIn.Add(1)
			var isReplicatedCommand = header.repl != ""
			var needsPostApplyProcessedAck = isReplicatedCommand && (header.replSync == "1" || strings.EqualFold(header.replSync, "true"))
			var dedupeClientID = firstNonEmpty(connClientName, connUserID, remoteAddr)
			var isDuplicateCommand = commandDedupe.seenBefore(dedupeClientID, commandID)

			if wantProcessed && !needsPostApplyProcessedAck {
				writer.send(buildAck(buf, "processed", commandID, "success"))
			}

			if isDuplicateCommand {
				if wantProcessed && needsPostApplyProcessedAck {
					writer.send(buildAck(buf, "processed", commandID, "success"))
				}
				if wantPersisted {
					writer.send(buildPersistedAck(buf, commandID, seqID))
				}
				continue
			}

			mt := getOrSetTopicMessageType(topic, messageType)

			effectiveSowKey := sowKey
			if effectiveSowKey == "" && len(payload) > 0 {
				effectiveSowKey = extractSowKey(payload)
			}

			// Journal.
			var bm string
			var seq uint64
			if journal != nil {
				bm, seq = journal.append(topic, effectiveSowKey, payload)
			} else {
				seq = globalBookmarkSeq.Add(1)
				bm = makeBookmark(seq)
			}

			// SOW cache.
			if sow != nil && topic != "" {
				if effectiveSowKey == "" {
					effectiveSowKey = makeSowKey(topic, seq)
				}
				ts := makeTimestamp()
				if isDelta {
					sow.deltaUpsert(topic, effectiveSowKey, payload, bm, ts, seq, expiration)
				} else {
					sow.upsert(topic, effectiveSowKey, payload, bm, ts, seq, expiration)
				}
			}

			var syncReplicationErr error
			if wantSync && len(replicaPeers) > 0 && !isReplicatedCommand {
				syncReplicationErr = replicatePublishSync(topic, payload, mt, effectiveSowKey)
			}

			if wantProcessed && needsPostApplyProcessedAck {
				writer.send(buildAck(buf, "processed", commandID, "success"))
			}

			if wantPersisted {
				if wantSync && syncReplicationErr != nil {
					writer.send(buildPersistedAckStatus(buf, commandID, seqID, "failure", "sync replication failed"))
				} else {
					writer.send(buildPersistedAck(buf, commandID, seqID))
				}
			}

			// Fire on-publish actions.
			fireOnPublish(topic, payload, bm)

			// Replication.
			if !wantSync && len(replicaPeers) > 0 && !isReplicatedCommand {
				_ = replicatePublish(topic, payload, mt, effectiveSowKey)
			}

			// Fan-out to subscribers.
			if *flagFanout && topic != "" {
				ts := makeTimestamp()
				fanoutPublishWithConflation(conn, topic, payload, bm, ts, effectiveSowKey, mt, isQueueTopic, &stats, conflationBuffers)

				// OOF-on-filter-mismatch: for delta subscribers whose topic
				// matches but content filter no longer matches, send OOF.
				if effectiveSowKey != "" {
					forEachOOFCandidate(topic, payload, func(sub *subscription) {
						if !*flagEcho && sub.conn == conn {
							return
						}
						oofBuf := getWriteBuf()
						frame := buildOOFDelivery(oofBuf, topic, sub.subID, effectiveSowKey, bm)
						sub.writer.send(frame)
						putWriteBuf(oofBuf)
					})
				}
			}

			// Recompute views that depend on this source topic.
			viewDeps := getViewsForSource(topic)
			for _, v := range viewDeps {
				recomputeView(v)
			}

		// ---------------------------------------------------------------
		// SOW_DELETE
		// ---------------------------------------------------------------
		case "sow_delete":
			if *flagAuth != "" && !authorizeCommand(connUserID, command, topic) {
				writer.send(buildAck(buf, "processed", commandID, "failure",
					kv{k: "reason", v: "not entitled"}))
				continue
			}

			var dedupeClientID = firstNonEmpty(connClientName, connUserID, remoteAddr)
			var isDuplicateCommand = commandDedupe.seenBefore(dedupeClientID, commandID)

			deleted := 0
			var deletedKeys []string

			if isDuplicateCommand {
				deleted = 0
			} else if header.sowKeys != "" {
				if sow != nil {
					deleted = sow.deleteByKeys(topic, header.sowKeys)
				}
			} else if sowKey != "" {
				if sow != nil && sow.delete(topic, sowKey) {
					deleted = 1
					deletedKeys = []string{sowKey}
				}
			} else if filter != "" {
				if sow != nil {
					deleted, deletedKeys = sow.deleteByFilter(topic, filter)
				}
			} else if len(payload) > 0 {
				// SowDeleteByData: infer key from payload body.
				inferredKey := extractSowKey(payload)
				if inferredKey != "" && sow != nil && sow.delete(topic, inferredKey) {
					deleted = 1
					deletedKeys = []string{inferredKey}
				}
			}

			if wantProcessed {
				writer.send(buildSOWDeleteAck(buf, "processed", commandID, deleted))
			}
			if wantStats {
				writer.send(buildSOWDeleteAck(buf, "stats", commandID, deleted))
			}
			if wantPersisted {
				writer.send(buildPersistedAck(buf, commandID, seqID))
			}

			// OOF to delta subscribers.
			if deleted > 0 && *flagFanout && topic != "" {
				for _, key := range deletedKeys {
					seq := globalBookmarkSeq.Add(1)
					bm := makeBookmark(seq)
					fanoutOOF(conn, topic, key, bm, &stats)
				}
			}

		// ---------------------------------------------------------------
		// FLUSH
		// ---------------------------------------------------------------
		case "flush":
			if wantProcessed {
				writer.send(buildAck(buf, "processed", commandID, "success"))
			}
			if wantCompleted {
				writer.send(buildAck(buf, "completed", commandID, "success"))
			}

		// ---------------------------------------------------------------
		// HEARTBEAT
		// ---------------------------------------------------------------
		case "heartbeat":
			if wantProcessed {
				writer.send(buildAck(buf, "processed", commandID, "success"))
			}
			if strings.Contains(options, "beat") {
				writer.send(buildHeartbeatFrame(buf))

				// Start or reset heartbeat liveness watchdog.
				if heartbeatTimeout == 0 {
					// First heartbeat — default to 60s timeout.
					heartbeatTimeout = 60 * time.Second
				}
				if heartbeatWatchdog != nil {
					heartbeatWatchdog.Reset(heartbeatTimeout)
				} else {
					heartbeatWatchdog = time.AfterFunc(heartbeatTimeout, func() {
						log.Printf("fakeamps: heartbeat timeout for %s, closing connection", remoteAddr)
						_ = conn.Close()
					})
				}
			}

		// ---------------------------------------------------------------
		// ACK (client → server)
		// ---------------------------------------------------------------
		case "ack":
			// Queue acknowledgment from client.
			if sowKey != "" {
				handleQueueAck(sowKey)
			}

		// ---------------------------------------------------------------
		// GROUP_BEGIN / GROUP_END (command batching boundaries)
		// ---------------------------------------------------------------
		case "group_begin", "group_end":
			// Accepted without response — real AMPS uses these for
			// atomic command batching. fakeamps processes commands
			// sequentially so these are effective no-ops.

		// ---------------------------------------------------------------
		// START_TIMER / STOP_TIMER
		// ---------------------------------------------------------------
		case "start_timer", "stop_timer":
			if wantProcessed {
				writer.send(buildAck(buf, "processed", commandID, "success"))
			}

		// ---------------------------------------------------------------
		// PAUSE (pause a subscription)
		// ---------------------------------------------------------------
		case "pause":
			targetSubID := firstNonEmpty(subID, commandID)
			pauseSubscription(localSubs, targetSubID)
			if wantProcessed {
				writer.send(buildAck(buf, "processed", commandID, "success"))
			}

		// ---------------------------------------------------------------
		// RESUME (resume a paused subscription)
		// ---------------------------------------------------------------
		case "resume":
			targetSubID := firstNonEmpty(subID, commandID)
			resumeSubscription(localSubs, targetSubID)
			if wantProcessed {
				writer.send(buildAck(buf, "processed", commandID, "success"))
			}

		// ---------------------------------------------------------------
		// UNKNOWN
		// ---------------------------------------------------------------
		default:
			if wantStats {
				writer.send(buildStatsAck(buf, commandID, &stats))
			} else {
				writer.send(buildAck(buf, "processed", commandID, "failure",
					kv{k: "reason", v: "unknown command"}))
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Fan-out with conflation support.
//
// If a subscriber has a conflation buffer, the message is added to the
// buffer instead of being sent directly. The buffer's timer goroutine
// will flush the latest value per SOW key at the configured interval.
// ---------------------------------------------------------------------------

func fanoutPublishWithConflation(publisher net.Conn, topic string, payload []byte, bookmark, timestamp, sowKey, messageType string, isQueue bool, stats *connStats, conflationBufs map[string]*conflationBuffer) {
	buf := getWriteBuf()
	defer putWriteBuf(buf)

	forEachMatchingSubscriber(topic, payload, func(sub *subscription) {
		if !*flagEcho && sub.conn == publisher {
			return
		}

		// Apply on-deliver actions.
		deliveryPayload := fireOnDeliver(topic, payload, sub.subID)

		queueMode := isQueue || sub.isQueue

		// Track lease for queue subscriptions.
		if queueMode {
			leasePeriod := *flagLease
			if sub.leaseDuration > 0 {
				leasePeriod = sub.leaseDuration
			}
			addQueueLease(topic, sub.subID, sowKey, bookmark, deliveryPayload, timestamp, messageType, leasePeriod)
		}

		// Check for conflation.
		if cb, ok := conflationBufs[sub.subID]; ok {
			cb.add(topic, sub.subID, deliveryPayload, bookmark, timestamp, sowKey, messageType, queueMode)
			stats.publishOut.Add(1)
			return
		}

		frame := buildPublishDelivery(buf, topic, sub.subID,
			deliveryPayload, bookmark, timestamp, sowKey, messageType, queueMode)
		sub.writer.send(frame)
		stats.publishOut.Add(1)
	})
}

// ---------------------------------------------------------------------------
// Fan-out: OOF delivery to delta subscribers.
// ---------------------------------------------------------------------------

func fanoutOOF(publisher net.Conn, topic, sowKey, bookmark string, stats *connStats) {
	buf := getWriteBuf()
	defer putWriteBuf(buf)

	topicSubscribers.Range(func(key, value interface{}) bool {
		ss := value.(*subscriberSet)
		ss.mu.RLock()
		for sub := range ss.subs {
			if !sub.isDelta {
				continue
			}
			if !topicMatches(topic, sub.topic) {
				continue
			}
			frame := buildOOFDelivery(buf, topic, sub.subID, sowKey, bookmark)
			sub.writer.send(frame)
			stats.messagesOut.Add(1)
		}
		ss.mu.RUnlock()
		return true
	})
}
