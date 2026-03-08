package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

const (
	reconnectStressDefaultIterations  = 1000
	reconnectStressDefaultMaxDuration = 25 * time.Second
	reconnectStressMinimumCycles      = 25
	reconnectStressStableFieldCount   = 48
	reconnectStressVariableFieldCount = 12
	reconnectStressTopicCount         = 10
	reconnectStressCycleTimeout       = 2 * time.Second
	reconnectStressReconnectTimeout   = time.Second
)

type reconnectStressTopicSpec struct {
	Topic         string
	MessageType   string
	StaticPrefix  string
	DynamicPrefix string
}

type reconnectStressRow struct {
	RecordID string
	Raw      string
}

type reconnectStressTopicData struct {
	Topic       string
	MessageType string
	Rows        []reconnectStressRow
	Expected    map[string]string
}

type reconnectStressEnvelope struct {
	RecordID    string `json:"record_id"`
	Topic       string `json:"topic"`
	MessageType string `json:"message_type"`
}

type reconnectStressTopicCycleState struct {
	seen      map[string]struct{}
	completed bool
}

type reconnectStressTracker struct {
	lock            sync.Mutex
	datasets        map[string]reconnectStressTopicData
	currentCycle    int
	completedTopics int
	topicStates     map[string]*reconnectStressTopicCycleState
	cycleDone       chan struct{}
	cycleClosed     bool
	firstErr        error
}

type reconnectStressLiveTracker struct {
	lock            sync.Mutex
	iteration       int
	expectedTopic   string
	expectedPayload string
	done            chan struct{}
	closed          bool
	seen            bool
	firstErr        error
}

type reconnectStressServer struct {
	listener net.Listener
	done     chan struct{}

	connLock    sync.Mutex
	active      net.Conn
	connections map[net.Conn]struct{}
}

func TestIntegrationClientSowAndSubscribeReconnectGeneratedDataStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}

	var iterations = reconnectStressIterations(t)
	var maxDuration = reconnectStressMaxDuration(t)
	var startedAt = time.Now()
	var datasets = buildReconnectStressDatasets(t)

	var oldSow = sow
	var oldJournal = journal
	sow = newSOWCache()
	journal = newMessageJournal(reconnectStressDatasetSize(datasets) * (iterations + 2))
	resetTopicSubscribersForTest()
	resetTopicConfigsForTest()
	resetViewsForTest()
	resetActionsForTest()
	monitoringClients.Reset()
	defer func() {
		resetTopicSubscribersForTest()
		resetTopicConfigsForTest()
		resetViewsForTest()
		resetActionsForTest()
		monitoringClients.Reset()
		sow = oldSow
		journal = oldJournal
	}()

	var server = startReconnectStressServer(t)
	defer server.close(t)

	var uri = "tcp://" + server.listener.Addr().String() + "/amps/json"
	seedReconnectStressDatasets(t, uri, datasets)
	if err := server.waitForNoActiveConnection(2 * time.Second); err != nil {
		t.Fatalf("seeder connection did not drain: %v", err)
	}

	var unexpectedErrCh = make(chan error, 32)
	var disconnectCh = make(chan struct{}, 32)
	var cleanupEnabled = true
	var client = amps.NewClient("fakeamps-reconnect-stress-client")
	client.SetRetryOnDisconnect(true)
	client.SetErrorHandler(func(err error) {
		if reconnectStressExpectedDisconnectError(err) {
			return
		}
		select {
		case unexpectedErrCh <- err:
		default:
		}
	})
	client.SetDisconnectHandler(func(*amps.Client, error) {
		select {
		case disconnectCh <- struct{}{}:
		default:
		}
	})

	if err := client.Connect(uri); err != nil {
		t.Fatalf("initial connect failed: %v", err)
	}
	defer func() {
		if cleanupEnabled {
			_ = client.Close()
		}
	}()
	if err := client.Logon(); err != nil {
		t.Fatalf("initial logon failed: %v", err)
	}

	var tracker = newReconnectStressTracker(datasets)
	var subIDs = subscribeReconnectStressTopics(t, client, tracker, datasets)
	defer func() {
		if !cleanupEnabled {
			return
		}
		for _, subID := range subIDs {
			_ = client.Unsubscribe(subID)
		}
	}()

	waitForReconnectStressCycle(t, tracker, unexpectedErrCh, 1)

	var cycle int
	for cycle = 2; cycle <= iterations; cycle++ {
		if maxDuration > 0 && time.Since(startedAt) >= maxDuration {
			break
		}
		tracker.beginCycle(cycle)
		if err := server.dropActiveConnection(); err != nil {
			t.Fatalf("cycle %d drop failed: %v", cycle, err)
		}
		waitForReconnectStressDisconnect(t, disconnectCh, cycle)
		if err := reconnectStressReconnectClient(client, uri); err != nil {
			cleanupEnabled = false
			t.Fatalf("cycle %d reconnect failed: %v", cycle, err)
		}
		waitForReconnectStressCycle(t, tracker, unexpectedErrCh, cycle)
		if cycle%100 == 0 || cycle == iterations {
			t.Logf("completed reconnect stress cycle %d/%d", cycle, iterations)
		}
	}

	select {
	case err := <-unexpectedErrCh:
		t.Fatalf("unexpected client error after stress completion: %v", err)
	default:
	}

	if err := tracker.err(); err != nil {
		t.Fatalf("reconnect stress validation failed: %v", err)
	}

	var completedCycles = cycle - 1
	if completedCycles < reconnectStressMinimumCycles {
		t.Fatalf("completed %d reconnect cycles within %v, want at least %d", completedCycles, maxDuration, reconnectStressMinimumCycles)
	}
	if maxDuration > 0 && time.Since(startedAt) > 30*time.Second {
		t.Fatalf("reconnect stress runtime exceeded 30s budget: %v", time.Since(startedAt))
	}
}

func TestIntegrationHASowAndSubscribeReconnectGeneratedDataStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}

	var iterations = reconnectStressIterations(t)
	var maxDuration = reconnectStressMaxDuration(t)
	var startedAt = time.Now()
	var datasets = buildReconnectStressDatasets(t)

	var oldSow = sow
	var oldJournal = journal
	sow = newSOWCache()
	journal = newMessageJournal(reconnectStressDatasetSize(datasets) * (iterations + 2))
	resetTopicSubscribersForTest()
	resetTopicConfigsForTest()
	resetViewsForTest()
	resetActionsForTest()
	monitoringClients.Reset()
	defer func() {
		resetTopicSubscribersForTest()
		resetTopicConfigsForTest()
		resetViewsForTest()
		resetActionsForTest()
		monitoringClients.Reset()
		sow = oldSow
		journal = oldJournal
	}()

	var server = startReconnectStressServer(t)
	defer server.close(t)

	var uri = "tcp://" + server.listener.Addr().String() + "/amps/json"
	seedReconnectStressDatasets(t, uri, datasets)
	if err := server.waitForNoActiveConnection(2 * time.Second); err != nil {
		t.Fatalf("seeder connection did not drain: %v", err)
	}

	var unexpectedErrCh = make(chan error, 32)
	var disconnectCh = make(chan struct{}, 32)
	var ha = amps.NewHAClient("fakeamps-ha-reconnect-stress-client")
	ha.SetServerChooser(amps.NewDefaultServerChooser(uri))
	ha.SetReconnectDelayStrategy(amps.NewFixedDelayStrategy(0))
	ha.Client().SetErrorHandler(func(err error) {
		if reconnectStressExpectedDisconnectError(err) {
			return
		}
		select {
		case unexpectedErrCh <- err:
		default:
		}
	})
	ha.Client().SetDisconnectHandler(func(*amps.Client, error) {
		select {
		case disconnectCh <- struct{}{}:
		default:
		}
	})
	defer func() {
		_ = ha.Disconnect()
	}()

	if err := ha.ConnectAndLogon(); err != nil {
		t.Fatalf("initial HA connect/logon failed: %v", err)
	}

	var tracker = newReconnectStressTracker(datasets)
	var subIDs = subscribeReconnectStressTopics(t, ha.Client(), tracker, datasets)
	defer func() {
		for _, subID := range subIDs {
			_ = ha.Client().Unsubscribe(subID)
		}
	}()

	waitForReconnectStressCycle(t, tracker, unexpectedErrCh, 1)

	var cycle int
	for cycle = 2; cycle <= iterations; cycle++ {
		if maxDuration > 0 && time.Since(startedAt) >= maxDuration {
			break
		}
		tracker.beginCycle(cycle)
		if err := server.dropActiveConnection(); err != nil {
			t.Fatalf("cycle %d drop failed: %v", cycle, err)
		}
		waitForReconnectStressDisconnect(t, disconnectCh, cycle)
		waitForReconnectStressCycle(t, tracker, unexpectedErrCh, cycle)
		if cycle%100 == 0 || cycle == iterations {
			t.Logf("completed HA reconnect stress cycle %d/%d", cycle, iterations)
		}
	}

	select {
	case err := <-unexpectedErrCh:
		t.Fatalf("unexpected HA client error after stress completion: %v", err)
	default:
	}

	if err := tracker.err(); err != nil {
		t.Fatalf("HA reconnect stress validation failed: %v", err)
	}

	var completedCycles = cycle - 1
	if completedCycles < reconnectStressMinimumCycles {
		t.Fatalf("completed %d HA reconnect cycles within %v, want at least %d", completedCycles, maxDuration, reconnectStressMinimumCycles)
	}
	if maxDuration > 0 && time.Since(startedAt) > 30*time.Second {
		t.Fatalf("HA reconnect stress runtime exceeded 30s budget: %v", time.Since(startedAt))
	}
}

func TestIntegrationClientSowAndSubscribeFreshClientGeneratedDataStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}

	var iterations = reconnectStressIterations(t)
	var datasets = buildReconnectStressDatasets(t)

	var oldSow = sow
	var oldJournal = journal
	sow = newSOWCache()
	journal = newMessageJournal(reconnectStressDatasetSize(datasets) * (iterations + 2))
	resetTopicSubscribersForTest()
	resetTopicConfigsForTest()
	resetViewsForTest()
	resetActionsForTest()
	monitoringClients.Reset()
	defer func() {
		resetTopicSubscribersForTest()
		resetTopicConfigsForTest()
		resetViewsForTest()
		resetActionsForTest()
		monitoringClients.Reset()
		sow = oldSow
		journal = oldJournal
	}()

	var server = startReconnectStressServer(t)
	defer server.close(t)

	var uri = "tcp://" + server.listener.Addr().String() + "/amps/json"
	seedReconnectStressDatasets(t, uri, datasets)
	if err := server.waitForNoActiveConnection(2 * time.Second); err != nil {
		t.Fatalf("seeder connection did not drain: %v", err)
	}

	var iteration int
	for iteration = 1; iteration <= iterations; iteration++ {
		runReconnectStressFreshClientIteration(t, server, uri, datasets, iteration)
		if iteration%25 == 0 || iteration == iterations {
			fmt.Printf("fresh-client sow_and_subscribe iteration %d/%d\n", iteration, iterations)
		}
	}
}

func TestIntegrationClientSowAndSubscribeFreshClientGeneratedDataLiveChurnStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}

	var iterations = reconnectStressIterations(t)
	var datasets = buildReconnectStressDatasets(t)
	var churnTopic = datasets[0].Topic
	var churnMessageType = datasets[0].MessageType

	var oldSow = sow
	var oldJournal = journal
	sow = newSOWCache()
	journal = newMessageJournal(reconnectStressDatasetSize(datasets) * (iterations + 4))
	resetTopicSubscribersForTest()
	resetTopicConfigsForTest()
	resetViewsForTest()
	resetActionsForTest()
	monitoringClients.Reset()
	defer func() {
		resetTopicSubscribersForTest()
		resetTopicConfigsForTest()
		resetViewsForTest()
		resetActionsForTest()
		monitoringClients.Reset()
		sow = oldSow
		journal = oldJournal
	}()

	var server = startReconnectStressServer(t)
	defer server.close(t)

	var uri = "tcp://" + server.listener.Addr().String() + "/amps/json"
	seedReconnectStressDatasets(t, uri, datasets)
	if err := server.waitForNoActiveConnection(2 * time.Second); err != nil {
		t.Fatalf("seeder connection did not drain: %v", err)
	}

	var publisher = amps.NewClient("fakeamps-fresh-live-churn-publisher")
	if err := publisher.Connect(uri); err != nil {
		t.Fatalf("publisher connect failed: %v", err)
	}
	defer func() { _ = publisher.Close() }()
	if err := publisher.Logon(); err != nil {
		t.Fatalf("publisher logon failed: %v", err)
	}

	var iteration int
	for iteration = 1; iteration <= iterations; iteration++ {
		runReconnectStressFreshClientLiveChurnIteration(t, server, uri, datasets, iteration, publisher, churnTopic, churnMessageType)
		if iteration%25 == 0 || iteration == iterations {
			fmt.Printf("fresh-client live-churn sow_and_subscribe iteration %d/%d\n", iteration, iterations)
		}
	}
}

func reconnectStressIterations(t *testing.T) int {
	t.Helper()

	var raw = strings.TrimSpace(os.Getenv("FAKEAMPS_RECONNECT_STRESS_ITERATIONS"))
	if raw == "" {
		return reconnectStressDefaultIterations
	}

	var value, err = strconv.Atoi(raw)
	if err != nil || value <= 0 {
		t.Fatalf("invalid FAKEAMPS_RECONNECT_STRESS_ITERATIONS value %q", raw)
	}
	return value
}

func reconnectStressMaxDuration(t *testing.T) time.Duration {
	t.Helper()

	var raw = strings.TrimSpace(os.Getenv("FAKEAMPS_RECONNECT_STRESS_MAX_DURATION"))
	if raw == "" {
		return reconnectStressDefaultMaxDuration
	}

	if raw == "0" || strings.EqualFold(raw, "off") {
		return 0
	}

	var duration, err = time.ParseDuration(raw)
	if err == nil {
		if duration < 0 {
			t.Fatalf("invalid FAKEAMPS_RECONNECT_STRESS_MAX_DURATION value %q", raw)
		}
		return duration
	}

	var seconds, convErr = strconv.Atoi(raw)
	if convErr == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}

	t.Fatalf("invalid FAKEAMPS_RECONNECT_STRESS_MAX_DURATION value %q", raw)
	return 0
}

func reconnectStressTopicSpecs() []reconnectStressTopicSpec {
	return []reconnectStressTopicSpec{
		{Topic: "people", MessageType: "person_profile", StaticPrefix: "person", DynamicPrefix: "person_delta"},
		{Topic: "companies", MessageType: "company_profile", StaticPrefix: "company", DynamicPrefix: "company_delta"},
		{Topic: "orders", MessageType: "order_event", StaticPrefix: "order", DynamicPrefix: "order_delta"},
		{Topic: "devices", MessageType: "device_snapshot", StaticPrefix: "device", DynamicPrefix: "device_delta"},
		{Topic: "invoices", MessageType: "invoice_record", StaticPrefix: "invoice", DynamicPrefix: "invoice_delta"},
		{Topic: "shipments", MessageType: "shipment_manifest", StaticPrefix: "shipment", DynamicPrefix: "shipment_delta"},
		{Topic: "trades", MessageType: "trade_capture", StaticPrefix: "trade", DynamicPrefix: "trade_delta"},
		{Topic: "flights", MessageType: "flight_status", StaticPrefix: "flight", DynamicPrefix: "flight_delta"},
		{Topic: "claims", MessageType: "claim_case", StaticPrefix: "claim", DynamicPrefix: "claim_delta"},
		{Topic: "incidents", MessageType: "incident_report", StaticPrefix: "incident", DynamicPrefix: "incident_delta"},
	}
}

func buildReconnectStressDatasets(t *testing.T) []reconnectStressTopicData {
	t.Helper()

	var random = rand.New(rand.NewSource(20260308))
	var specs = reconnectStressTopicSpecs()
	if len(specs) != reconnectStressTopicCount {
		t.Fatalf("topic spec count = %d, want %d", len(specs), reconnectStressTopicCount)
	}

	var datasets = make([]reconnectStressTopicData, 0, len(specs))
	var topicIndex int
	for topicIndex = 0; topicIndex < len(specs); topicIndex++ {
		var spec = specs[topicIndex]
		var rowCount = 50 + random.Intn(151)
		var stableValues = reconnectStressStableValues(random, spec, topicIndex)
		var rows = make([]reconnectStressRow, 0, rowCount)
		var expected = make(map[string]string, rowCount)

		var rowIndex int
		for rowIndex = 0; rowIndex < rowCount; rowIndex++ {
			var payload = make(map[string]any, reconnectStressStableFieldCount+reconnectStressVariableFieldCount)
			for key, value := range stableValues {
				payload[key] = value
			}

			var recordID = fmt.Sprintf("%s-%03d", spec.Topic, rowIndex)
			reconnectStressApplyDynamicValues(payload, spec, recordID, rowIndex, rowCount)

			var raw, err = json.Marshal(payload)
			if err != nil {
				t.Fatalf("marshal generated payload for %s row %d failed: %v", spec.Topic, rowIndex, err)
			}

			var row = reconnectStressRow{
				RecordID: recordID,
				Raw:      string(raw),
			}
			rows = append(rows, row)
			expected[recordID] = row.Raw
		}

		datasets = append(datasets, reconnectStressTopicData{
			Topic:       spec.Topic,
			MessageType: spec.MessageType,
			Rows:        rows,
			Expected:    expected,
		})
	}

	return datasets
}

func reconnectStressStableValues(random *rand.Rand, spec reconnectStressTopicSpec, topicIndex int) map[string]any {
	var values = make(map[string]any, reconnectStressStableFieldCount)
	values["topic"] = spec.Topic
	values["message_type"] = spec.MessageType
	values["schema_version"] = 1
	values["topic_index"] = topicIndex
	values["generator"] = "fakeamps-reconnect-stress"
	values["corpus_seed"] = 20260308
	values["shared_signature"] = fmt.Sprintf("%s-shared", spec.Topic)
	values["static_fingerprint"] = fmt.Sprintf("%s-%08x", spec.Topic, random.Uint32())

	var fieldIndex int
	for fieldIndex = 0; fieldIndex < reconnectStressStableFieldCount-8; fieldIndex++ {
		var fieldName = fmt.Sprintf("%s_static_%02d", spec.StaticPrefix, fieldIndex)
		values[fieldName] = fmt.Sprintf("%s-%02d-%06d", spec.MessageType, fieldIndex, random.Intn(900000)+100000)
	}

	return values
}

func reconnectStressApplyDynamicValues(payload map[string]any, spec reconnectStressTopicSpec, recordID string, rowIndex int, rowCount int) {
	payload["record_id"] = recordID
	payload["row_index"] = rowIndex
	payload["ordinal"] = rowIndex + 1
	payload["remaining"] = rowCount - rowIndex - 1
	payload[fmt.Sprintf("%s_metric_00", spec.DynamicPrefix)] = rowIndex % 7
	payload[fmt.Sprintf("%s_metric_01", spec.DynamicPrefix)] = (rowIndex + 3) % 11
	payload[fmt.Sprintf("%s_metric_02", spec.DynamicPrefix)] = rowIndex * (rowIndex + 1)
	payload[fmt.Sprintf("%s_metric_03", spec.DynamicPrefix)] = fmt.Sprintf("%s-bucket-%02d", spec.Topic, rowIndex%13)
	payload[fmt.Sprintf("%s_metric_04", spec.DynamicPrefix)] = fmt.Sprintf("%s-priority-%d", spec.MessageType, rowIndex%5)
	payload[fmt.Sprintf("%s_metric_05", spec.DynamicPrefix)] = fmt.Sprintf("2026-03-08T%02d:%02d:%02dZ", rowIndex%24, (rowIndex*3)%60, (rowIndex*7)%60)
	payload[fmt.Sprintf("%s_metric_06", spec.DynamicPrefix)] = (rowIndex + 1) * 100
	payload[fmt.Sprintf("%s_metric_07", spec.DynamicPrefix)] = fmt.Sprintf("checksum-%s-%04d", spec.Topic, rowIndex*17)
}

func reconnectStressDatasetSize(datasets []reconnectStressTopicData) int {
	var total = 0
	for _, dataset := range datasets {
		total += len(dataset.Rows)
	}
	return total
}

func seedReconnectStressDatasets(t *testing.T, uri string, datasets []reconnectStressTopicData) {
	t.Helper()

	var seeder = amps.NewClient("fakeamps-reconnect-stress-seeder")
	if err := seeder.Connect(uri); err != nil {
		t.Fatalf("seed connect failed: %v", err)
	}
	if err := seeder.Logon(); err != nil {
		_ = seeder.Close()
		t.Fatalf("seed logon failed: %v", err)
	}

	for _, dataset := range datasets {
		for _, row := range dataset.Rows {
			if err := seeder.Publish(dataset.Topic, row.Raw); err != nil {
				_ = seeder.Close()
				t.Fatalf("seed publish failed for %s/%s: %v", dataset.Topic, row.RecordID, err)
			}
		}
	}

	if err := seeder.Flush(); err != nil {
		_ = seeder.Close()
		t.Fatalf("seed flush failed: %v", err)
	}
	if err := seeder.Close(); err != nil {
		t.Fatalf("seed close failed: %v", err)
	}
}

func newReconnectStressTracker(datasets []reconnectStressTopicData) *reconnectStressTracker {
	var byTopic = make(map[string]reconnectStressTopicData, len(datasets))
	for _, dataset := range datasets {
		byTopic[dataset.Topic] = dataset
	}

	var tracker = &reconnectStressTracker{
		datasets: byTopic,
	}
	tracker.beginCycle(1)
	return tracker
}

func newReconnectStressLiveTracker() *reconnectStressLiveTracker {
	var tracker = &reconnectStressLiveTracker{}
	tracker.begin(0, "", "")
	return tracker
}

func (tracker *reconnectStressTracker) beginCycle(cycle int) {
	tracker.lock.Lock()
	defer tracker.lock.Unlock()

	tracker.currentCycle = cycle
	tracker.completedTopics = 0
	tracker.topicStates = make(map[string]*reconnectStressTopicCycleState, len(tracker.datasets))
	for topic := range tracker.datasets {
		tracker.topicStates[topic] = &reconnectStressTopicCycleState{seen: make(map[string]struct{})}
	}
	tracker.cycleDone = make(chan struct{})
	tracker.cycleClosed = false
	if tracker.firstErr != nil {
		tracker.closeCycleLocked()
	}
}

func (tracker *reconnectStressTracker) handleMessage(topic string, message *amps.Message) error {
	if message == nil {
		return nil
	}

	var command, _ = message.Command()
	switch command {
	case amps.CommandSOW:
		return tracker.handleSOW(topic, message)
	case amps.CommandAck:
		return tracker.handleAck(topic, message)
	default:
		return nil
	}
}

func (tracker *reconnectStressTracker) handleSOW(topic string, message *amps.Message) error {
	var envelope reconnectStressEnvelope
	if err := json.Unmarshal(message.Data(), &envelope); err != nil {
		tracker.fail(fmt.Errorf("cycle %d topic %s unmarshal failed: %w", tracker.cycle(), topic, err))
		return nil
	}
	if envelope.Topic != topic {
		tracker.fail(fmt.Errorf("cycle %d topic mismatch: route=%s payload=%s", tracker.cycle(), topic, envelope.Topic))
		return nil
	}

	tracker.lock.Lock()
	defer tracker.lock.Unlock()

	var dataset, ok = tracker.datasets[topic]
	if !ok {
		tracker.failLocked(fmt.Errorf("cycle %d received message for unknown topic %s", tracker.currentCycle, topic))
		return nil
	}
	if envelope.MessageType != dataset.MessageType {
		tracker.failLocked(fmt.Errorf("cycle %d topic %s message type = %s, want %s", tracker.currentCycle, topic, envelope.MessageType, dataset.MessageType))
		return nil
	}
	var expected, exists = dataset.Expected[envelope.RecordID]
	if !exists {
		tracker.failLocked(fmt.Errorf("cycle %d topic %s unexpected record %s", tracker.currentCycle, topic, envelope.RecordID))
		return nil
	}
	if string(message.Data()) != expected {
		tracker.failLocked(fmt.Errorf("cycle %d topic %s record %s payload mismatch", tracker.currentCycle, topic, envelope.RecordID))
		return nil
	}

	var state = tracker.topicStates[topic]
	if state == nil {
		tracker.failLocked(fmt.Errorf("ycle %d topic %s missing topic state", tracker.currentCycle, topic))
		return nil
	}
	if state.completed {
		tracker.failLocked(fmt.Errorf("cycle %d topic %s received data after completion", tracker.currentCycle, topic))
		return nil
	}
	if _, duplicate := state.seen[envelope.RecordID]; duplicate {
		tracker.failLocked(fmt.Errorf("cycle %d topic %s duplicate record %s", tracker.currentCycle, topic, envelope.RecordID))
		return nil
	}
	state.seen[envelope.RecordID] = struct{}{}
	return nil
}

func (tracker *reconnectStressTracker) handleAck(topic string, message *amps.Message) error {
	var ackType, hasAckType = message.AckType()
	if !hasAckType || ackType != amps.AckTypeCompleted {
		return nil
	}

	var status, _ = message.Status()
	if status != "success" {
		tracker.fail(fmt.Errorf("cycle %d topic %s completed ack status = %s", tracker.cycle(), topic, status))
		return nil
	}

	tracker.lock.Lock()
	defer tracker.lock.Unlock()

	var dataset = tracker.datasets[topic]
	var state = tracker.topicStates[topic]
	if state == nil {
		tracker.failLocked(fmt.Errorf("cycle %d topic %s missing ack state", tracker.currentCycle, topic))
		return nil
	}
	if state.completed {
		tracker.failLocked(fmt.Errorf("cycle %d topic %s duplicate completed ack", tracker.currentCycle, topic))
		return nil
	}
	if len(state.seen) != len(dataset.Rows) {
		tracker.failLocked(fmt.Errorf("cycle %d topic %s received %d rows, want %d", tracker.currentCycle, topic, len(state.seen), len(dataset.Rows)))
		return nil
	}

	state.completed = true
	tracker.completedTopics++
	if tracker.completedTopics == len(tracker.datasets) {
		tracker.closeCycleLocked()
	}
	return nil
}

func (tracker *reconnectStressTracker) fail(err error) {
	tracker.lock.Lock()
	defer tracker.lock.Unlock()
	tracker.failLocked(err)
}

func (tracker *reconnectStressTracker) failLocked(err error) {
	if err == nil {
		return
	}
	if tracker.firstErr == nil {
		tracker.firstErr = err
	}
	tracker.closeCycleLocked()
}

func (tracker *reconnectStressTracker) closeCycleLocked() {
	if tracker.cycleClosed {
		return
	}
	close(tracker.cycleDone)
	tracker.cycleClosed = true
}

func (tracker *reconnectStressTracker) cycle() int {
	tracker.lock.Lock()
	defer tracker.lock.Unlock()
	return tracker.currentCycle
}

func (tracker *reconnectStressTracker) done() <-chan struct{} {
	tracker.lock.Lock()
	defer tracker.lock.Unlock()
	return tracker.cycleDone
}

func (tracker *reconnectStressTracker) err() error {
	tracker.lock.Lock()
	defer tracker.lock.Unlock()
	return tracker.firstErr
}

func (tracker *reconnectStressLiveTracker) begin(iteration int, topic string, payload string) {
	tracker.lock.Lock()
	defer tracker.lock.Unlock()

	tracker.iteration = iteration
	tracker.expectedTopic = topic
	tracker.expectedPayload = payload
	tracker.done = make(chan struct{})
	tracker.closed = false
	tracker.seen = false
	tracker.firstErr = nil
	if topic == "" {
		tracker.closeLocked()
	}
}

func (tracker *reconnectStressLiveTracker) handleMessage(topic string, message *amps.Message) error {
	if message == nil {
		return nil
	}

	var command, _ = message.Command()
	if command != amps.CommandPublish {
		return nil
	}

	tracker.lock.Lock()
	defer tracker.lock.Unlock()

	if tracker.expectedTopic == "" || topic != tracker.expectedTopic {
		return nil
	}
	if tracker.seen {
		tracker.failLocked(fmt.Errorf("iteration %d duplicate live publish for topic %s", tracker.iteration, topic))
		return nil
	}
	if string(message.Data()) != tracker.expectedPayload {
		tracker.failLocked(fmt.Errorf("iteration %d live publish payload mismatch for topic %s", tracker.iteration, topic))
		return nil
	}

	tracker.seen = true
	tracker.closeLocked()
	return nil
}

func (tracker *reconnectStressLiveTracker) failLocked(err error) {
	if err == nil {
		return
	}
	if tracker.firstErr == nil {
		tracker.firstErr = err
	}
	tracker.closeLocked()
	tracker.seen = false
}

func (tracker *reconnectStressLiveTracker) closeLocked() {
	if tracker.closed {
		return
	}
	close(tracker.done)
	tracker.closed = true
}

func (tracker *reconnectStressLiveTracker) doneCh() <-chan struct{} {
	tracker.lock.Lock()
	defer tracker.lock.Unlock()
	return tracker.done
}

func (tracker *reconnectStressLiveTracker) err() error {
	tracker.lock.Lock()
	defer tracker.lock.Unlock()
	return tracker.firstErr
}

func (tracker *reconnectStressTracker) progress() string {
	tracker.lock.Lock()
	defer tracker.lock.Unlock()

	var parts = make([]string, 0, len(tracker.datasets))
	for topic, dataset := range tracker.datasets {
		var state = tracker.topicStates[topic]
		var seen = 0
		var completed = false
		if state != nil {
			seen = len(state.seen)
			completed = state.completed
		}
		parts = append(parts, fmt.Sprintf("%s=%d/%d completed=%v", topic, seen, len(dataset.Rows), completed))
	}
	return strings.Join(parts, "; ")
}

func subscribeReconnectStressTopics(t *testing.T, client *amps.Client, tracker *reconnectStressTracker, datasets []reconnectStressTopicData) []string {
	return subscribeReconnectStressTopicsWithHook(t, client, tracker, datasets, nil)
}

func subscribeReconnectStressTopicsWithHook(t *testing.T, client *amps.Client, tracker *reconnectStressTracker, datasets []reconnectStressTopicData, hook func(string, *amps.Message) error) []string {
	t.Helper()

	var subIDs = make([]string, 0, len(datasets))
	for _, dataset := range datasets {
		var topic = dataset.Topic
		var command = amps.NewCommand("sow_and_subscribe").SetTopic(topic).SetFilter("1=1").AddAckType(amps.AckTypeCompleted)
		var subID, err = client.ExecuteAsync(command, func(message *amps.Message) error {
			var handleErr = tracker.handleMessage(topic, message)
			if handleErr != nil {
				return handleErr
			}
			if hook != nil {
				return hook(topic, message)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("sow_and_subscribe failed for %s: %v", topic, err)
		}
		subIDs = append(subIDs, subID)
	}
	return subIDs
}

func runReconnectStressFreshClientIteration(t *testing.T, server *reconnectStressServer, uri string, datasets []reconnectStressTopicData, iteration int) {
	t.Helper()

	var unexpectedErrCh = make(chan error, 8)
	var client = amps.NewClient(fmt.Sprintf("fakeamps-fresh-sow-sub-client-%d", iteration))
	client.SetErrorHandler(func(err error) {
		select {
		case unexpectedErrCh <- err:
		default:
		}
	})

	if err := client.Connect(uri); err != nil {
		t.Fatalf("iteration %d connect failed: %v", iteration, err)
	}
	if err := client.Logon(); err != nil {
		_ = client.Close()
		t.Fatalf("iteration %d logon failed: %v", iteration, err)
	}

	var tracker = newReconnectStressTracker(datasets)
	var subIDs = subscribeReconnectStressTopics(t, client, tracker, datasets)
	waitForReconnectStressCycle(t, tracker, unexpectedErrCh, iteration)

	select {
	case err := <-unexpectedErrCh:
		_ = client.Close()
		t.Fatalf("iteration %d unexpected client error after cycle completion: %v", iteration, err)
	default:
	}

	for _, subID := range subIDs {
		_ = client.Unsubscribe(subID)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("iteration %d close failed: %v", iteration, err)
	}
	if err := server.waitForNoActiveConnection(2 * time.Second); err != nil {
		t.Fatalf("iteration %d connection did not drain: %v", iteration, err)
	}
}

func runReconnectStressFreshClientLiveChurnIteration(t *testing.T, server *reconnectStressServer, uri string, datasets []reconnectStressTopicData, iteration int, publisher *amps.Client, churnTopic string, churnMessageType string) {
	t.Helper()

	var unexpectedErrCh = make(chan error, 8)
	var client = amps.NewClient(fmt.Sprintf("fakeamps-fresh-live-client-%d", iteration))
	client.SetErrorHandler(func(err error) {
		select {
		case unexpectedErrCh <- err:
		default:
		}
	})

	if err := client.Connect(uri); err != nil {
		t.Fatalf("iteration %d connect failed: %v", iteration, err)
	}
	if err := client.Logon(); err != nil {
		_ = client.Close()
		t.Fatalf("iteration %d logon failed: %v", iteration, err)
	}

	var tracker = newReconnectStressTracker(datasets)
	var liveTracker = newReconnectStressLiveTracker()
	var subIDs = subscribeReconnectStressTopicsWithHook(t, client, tracker, datasets, liveTracker.handleMessage)
	waitForReconnectStressCycle(t, tracker, unexpectedErrCh, iteration)

	var churnKey = fmt.Sprintf("fresh-live-%04d", iteration)
	var churnPayload = buildReconnectStressLivePayload(churnTopic, churnMessageType, iteration)
	liveTracker.begin(iteration, churnTopic, churnPayload)
	if err := publishReconnectStressLiveChurn(publisher, churnTopic, churnKey, churnPayload); err != nil {
		_ = client.Close()
		t.Fatalf("iteration %d live publish failed: %v", iteration, err)
	}
	waitForReconnectStressLivePublish(t, liveTracker, unexpectedErrCh, iteration)
	if _, err := publisher.SowDeleteByKeys(churnTopic, churnKey); err != nil {
		_ = client.Close()
		t.Fatalf("iteration %d live cleanup failed: %v", iteration, err)
	}

	select {
	case err := <-unexpectedErrCh:
		_ = client.Close()
		t.Fatalf("iteration %d unexpected client error after live publish: %v", iteration, err)
	default:
	}

	for _, subID := range subIDs {
		_ = client.Unsubscribe(subID)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("iteration %d close failed: %v", iteration, err)
	}
	if err := server.waitForActiveConnectionCount(2*time.Second, 1); err != nil {
		t.Fatalf("iteration %d subscriber connection did not drain: %v", iteration, err)
	}
}

func buildReconnectStressLivePayload(topic string, messageType string, iteration int) string {
	var payload = map[string]any{
		"record_id":    fmt.Sprintf("live-%s-%04d", topic, iteration),
		"topic":        topic,
		"message_type": messageType,
		"phase":        "live_churn",
		"iteration":    iteration,
	}
	var raw, err = json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return string(raw)
}

func publishReconnectStressLiveChurn(publisher *amps.Client, topic string, sowKey string, payload string) error {
	var command = amps.NewCommand("publish").SetTopic(topic).SetSowKey(sowKey).SetData([]byte(payload))
	if _, err := publisher.ExecuteAsync(command, nil); err != nil {
		return err
	}
	return publisher.Flush()
}

func waitForReconnectStressCycle(t *testing.T, tracker *reconnectStressTracker, unexpectedErrCh <-chan error, cycle int) {
	t.Helper()

	var timer = time.NewTimer(reconnectStressCycleTimeout)
	defer timer.Stop()

	select {
	case <-tracker.done():
	case err := <-unexpectedErrCh:
		t.Fatalf("unexpected client error in cycle %d: %v", cycle, err)
	case <-timer.C:
		if err := tracker.err(); err != nil {
			t.Fatalf("cycle %d validation failed: %v", cycle, err)
		}
		t.Fatalf("cycle %d timed out waiting for completed acks: %s", cycle, tracker.progress())
	}

	if err := tracker.err(); err != nil {
		t.Fatalf("cycle %d validation failed: %v", cycle, err)
	}

	select {
	case err := <-unexpectedErrCh:
		t.Fatalf("unexpected client error after cycle %d completion: %v", cycle, err)
	default:
	}
}

func waitForReconnectStressLivePublish(t *testing.T, tracker *reconnectStressLiveTracker, unexpectedErrCh <-chan error, iteration int) {
	t.Helper()

	var timer = time.NewTimer(reconnectStressCycleTimeout)
	defer timer.Stop()

	select {
	case <-tracker.doneCh():
	case err := <-unexpectedErrCh:
		t.Fatalf("unexpected client error in iteration %d during live churn: %v", iteration, err)
	case <-timer.C:
		if err := tracker.err(); err != nil {
			t.Fatalf("iteration %d live churn validation failed: %v", iteration, err)
		}
		t.Fatalf("iteration %d timed out waiting for live publish delivery", iteration)
	}

	if err := tracker.err(); err != nil {
		t.Fatalf("iteration %d live churn validation failed: %v", iteration, err)
	}
}

func waitForReconnectStressDisconnect(t *testing.T, disconnectCh <-chan struct{}, cycle int) {
	t.Helper()

	select {
	case <-disconnectCh:
	case <-time.After(reconnectStressReconnectTimeout):
		t.Fatalf("cycle %d timed out waiting for disconnect callback", cycle)
	}
}

func reconnectStressReconnectClient(client *amps.Client, uri string) error {
	var deadline = time.Now().Add(reconnectStressReconnectTimeout)
	for {
		var connectErr = client.Connect(uri)
		if connectErr == nil {
			var logonErr = reconnectStressLogonWithTimeout(client, reconnectStressReconnectTimeout)
			if logonErr == nil {
				return nil
			}
			connectErr = logonErr
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("reconnect failed before timeout: %w", connectErr)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func reconnectStressLogonWithTimeout(client *amps.Client, timeout time.Duration) error {
	var result = make(chan error, 1)
	go func() {
		result <- client.Logon()
	}()

	select {
	case err := <-result:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("logon timed out after %v during reconnect recovery", timeout)
	}
}

func reconnectStressExpectedDisconnectError(err error) bool {
	if err == nil {
		return false
	}

	var text = strings.ToLower(err.Error())
	return strings.Contains(text, "use of closed network connection") ||
		strings.Contains(text, "connection reset by peer") ||
		strings.Contains(text, "forcibly closed by the remote host") ||
		strings.Contains(text, "broken pipe") ||
		strings.Contains(text, "eof")
}

func startReconnectStressServer(t *testing.T) *reconnectStressServer {
	t.Helper()

	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	var server = &reconnectStressServer{
		listener:    listener,
		done:        make(chan struct{}),
		connections: make(map[net.Conn]struct{}),
	}

	go func() {
		defer close(server.done)
		for {
			var conn, acceptErr = listener.Accept()
			if acceptErr != nil {
				return
			}
			server.setActive(conn)
			go func(current net.Conn) {
				handleConnection(current)
				server.clearActive(current)
			}(conn)
		}
	}()

	return server
}

func (server *reconnectStressServer) setActive(conn net.Conn) {
	server.connLock.Lock()
	server.active = conn
	server.connections[conn] = struct{}{}
	server.connLock.Unlock()
}

func (server *reconnectStressServer) clearActive(conn net.Conn) {
	server.connLock.Lock()
	delete(server.connections, conn)
	if server.active == conn {
		server.active = nil
	}
	server.connLock.Unlock()
}

func (server *reconnectStressServer) dropActiveConnection() error {
	server.connLock.Lock()
	var conn = server.active
	server.connLock.Unlock()
	if conn == nil {
		return fmt.Errorf("no active connection to drop")
	}
	return conn.Close()
}

func (server *reconnectStressServer) waitForNoActiveConnection(timeout time.Duration) error {
	return server.waitForActiveConnectionCount(timeout, 0)
}

func (server *reconnectStressServer) waitForActiveConnectionCount(timeout time.Duration, expected int) error {
	var deadline = time.Now().Add(timeout)
	for {
		server.connLock.Lock()
		var count = len(server.connections)
		server.connLock.Unlock()
		if count == expected {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("active connection count = %d after %v, want %d", count, timeout, expected)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (server *reconnectStressServer) close(t *testing.T) {
	t.Helper()

	_ = server.listener.Close()
	select {
	case <-server.done:
	case <-time.After(2 * time.Second):
		t.Fatalf("fakeamps reconnect stress server did not exit in time")
	}
}
