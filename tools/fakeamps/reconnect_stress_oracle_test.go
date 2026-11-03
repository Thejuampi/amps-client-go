package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

func reconnectOracleDataset(tb testing.TB) reconnectStressTopicData {
	tb.Helper()
	var dataset = reconnectStressTopicData{
		Topic:              "orders",
		MessageType:        "order_event",
		Expected:           make(map[string]string),
		expectedByPayload:  make(map[string]string),
		indexedTopic:       "orders",
		indexedMessageType: "order_event",
	}
	for index := 0; index < 2; index++ {
		var recordID = fmt.Sprintf("order-%d", index)
		var fields = map[string]any{
			"record_id": recordID, "topic": dataset.Topic, "message_type": dataset.MessageType,
			"aaa_first": "first", "middle_field": 42, "zzz_last": "last",
		}
		for field := 0; field < 54; field++ {
			fields[fmt.Sprintf("field_%02d", field)] = strings.Repeat("representative-value", 2)
		}
		var raw, err = json.Marshal(fields)
		if err != nil {
			tb.Fatal(err)
		}
		var payload = string(raw)
		dataset.Rows = append(dataset.Rows, reconnectStressRow{RecordID: recordID, Raw: payload})
		dataset.Expected[recordID] = payload
		dataset.expectedByPayload[payload] = recordID
	}
	return dataset
}

// This is the original snapshot oracle, retained to compare acceptance and
// diagnostics against the optimized checker.
func referenceReconnectStressSOW(tracker *reconnectStressTracker, topic string, message *amps.Message) {
	var envelope reconnectStressEnvelope
	if err := json.Unmarshal(message.Data(), &envelope); err != nil {
		tracker.fail(fmt.Errorf("cycle %d topic %s unmarshal failed: %w", tracker.cycle(), topic, err))
		return
	}
	if envelope.Topic != topic {
		tracker.fail(fmt.Errorf("cycle %d topic mismatch: route=%s payload=%s", tracker.cycle(), topic, envelope.Topic))
		return
	}
	tracker.lock.Lock()
	defer tracker.lock.Unlock()
	var dataset, ok = tracker.datasets[topic]
	if !ok {
		tracker.failLocked(fmt.Errorf("cycle %d received message for unknown topic %s", tracker.currentCycle, topic))
		return
	}
	if envelope.MessageType != dataset.MessageType {
		tracker.failLocked(fmt.Errorf("cycle %d topic %s message type = %s, want %s", tracker.currentCycle, topic, envelope.MessageType, dataset.MessageType))
		return
	}
	var expected, exists = dataset.Expected[envelope.RecordID]
	if !exists {
		tracker.failLocked(fmt.Errorf("cycle %d topic %s unexpected record %s", tracker.currentCycle, topic, envelope.RecordID))
		return
	}
	if string(message.Data()) != expected {
		tracker.failLocked(fmt.Errorf("cycle %d topic %s record %s payload mismatch", tracker.currentCycle, topic, envelope.RecordID))
		return
	}
	var state = tracker.topicStates[topic]
	if state == nil {
		tracker.failLocked(fmt.Errorf("ycle %d topic %s missing topic state", tracker.currentCycle, topic))
		return
	}
	if state.completed {
		tracker.failLocked(fmt.Errorf("cycle %d topic %s received data after completion", tracker.currentCycle, topic))
		return
	}
	if _, duplicate := state.seen[envelope.RecordID]; duplicate {
		tracker.failLocked(fmt.Errorf("cycle %d topic %s duplicate record %s", tracker.currentCycle, topic, envelope.RecordID))
		return
	}
	state.seen[envelope.RecordID] = struct{}{}
}

func compareReconnectStressSOW(t *testing.T, dataset reconnectStressTopicData, topic string, payload []byte, accept bool, repeated bool) {
	t.Helper()
	var actual = newReconnectStressTracker([]reconnectStressTopicData{dataset})
	var reference = newReconnectStressTracker([]reconnectStressTopicData{dataset})
	var message = new(amps.Message).SetData(payload)
	_ = actual.handleSOW(topic, message)
	referenceReconnectStressSOW(reference, topic, message)
	if repeated {
		_ = actual.handleSOW(topic, message)
		referenceReconnectStressSOW(reference, topic, message)
	}
	var actualErr, referenceErr = actual.err(), reference.err()
	if (actualErr == nil) != accept || (referenceErr == nil) != accept {
		t.Fatalf("topic=%s payload=%q: actual=%v reference=%v, want accept=%v", topic, payload, actualErr, referenceErr, accept)
	}
	if actualErr != nil && actualErr.Error() != referenceErr.Error() {
		t.Fatalf("diagnostics differ: actual=%v reference=%v", actualErr, referenceErr)
	}
	if !reflect.DeepEqual(actual.topicStates, reference.topicStates) {
		t.Fatalf("seen/completed state differs: actual=%v reference=%v", actual.topicStates, reference.topicStates)
	}
}

func TestReconnectStressSnapshotOracle(t *testing.T) {
	var dataset = reconnectOracleDataset(t)
	var original = []byte(dataset.Rows[0].Raw)
	compareReconnectStressSOW(t, dataset, dataset.Topic, original, true, false)
	compareReconnectStressSOW(t, dataset, dataset.Topic, []byte(dataset.Rows[1].Raw), true, false)
	compareReconnectStressSOW(t, dataset, dataset.Topic, original, false, true)
	compareReconnectStressSOW(t, dataset, "other-topic", original, false, false)
	compareReconnectStressSOW(t, dataset, dataset.Topic, []byte(`{"record_id":`), false, false)
	var changedExpected = reconnectOracleDataset(t)
	changedExpected.Expected[changedExpected.Rows[0].RecordID] = "different"
	compareReconnectStressSOW(t, changedExpected, changedExpected.Topic, original, false, false)
	var changedType = reconnectOracleDataset(t)
	changedType.MessageType = "different"
	compareReconnectStressSOW(t, changedType, changedType.Topic, original, false, false)
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, original, "", "  "); err != nil {
		t.Fatal(err)
	}
	compareReconnectStressSOW(t, dataset, dataset.Topic, pretty.Bytes(), false, false)
	var escaped = strings.Replace(string(original), `"topic":"orders"`, `"topic":"\u006frders"`, 1)
	if escaped == string(original) {
		t.Fatal("escape mutation did not apply")
	}
	compareReconnectStressSOW(t, dataset, dataset.Topic, []byte(escaped), false, false)
	var idField = `"record_id":"order-0",`
	var reordered = strings.Replace(string(original), idField, "", 1)
	if reordered == string(original) {
		t.Fatal("field-order mutation did not apply")
	}
	reordered = strings.TrimSuffix(reordered, "}") + `,"record_id":"order-0"}`
	compareReconnectStressSOW(t, dataset, dataset.Topic, []byte(reordered), false, false)

	var fields map[string]any
	if err := json.Unmarshal(original, &fields); err != nil {
		t.Fatal(err)
	}
	for name, value := range fields {
		for _, mutation := range []string{"missing", "value", "type"} {
			var changed = make(map[string]any, len(fields))
			for key, field := range fields {
				changed[key] = field
			}
			switch mutation {
			case "missing":
				delete(changed, name)
			case "value":
				switch current := value.(type) {
				case string:
					changed[name] = current + "-changed"
				case float64:
					changed[name] = current + 1
				default:
					t.Fatalf("field %s has unsupported type %T", name, value)
				}
			case "type":
				if _, ok := value.(string); ok {
					changed[name] = 37
				} else {
					changed[name] = "wrong-type"
				}
			}
			var raw, err = json.Marshal(changed)
			if err != nil {
				t.Fatal(err)
			}
			compareReconnectStressSOW(t, dataset, dataset.Topic, raw, false, false)
		}
	}
}

func TestReconnectStressRejectsDataAfterCompletedTopic(t *testing.T) {
	var dataset = reconnectOracleDataset(t)
	var actual = newReconnectStressTracker([]reconnectStressTopicData{dataset})
	var reference = newReconnectStressTracker([]reconnectStressTopicData{dataset})
	actual.topicStates[dataset.Topic].completed = true
	reference.topicStates[dataset.Topic].completed = true
	var message = new(amps.Message).SetData([]byte(dataset.Rows[0].Raw))
	_ = actual.handleSOW(dataset.Topic, message)
	referenceReconnectStressSOW(reference, dataset.Topic, message)
	if actual.err() == nil || reference.err() == nil || actual.err().Error() != reference.err().Error() {
		t.Fatalf("data after completed ack: actual=%v reference=%v", actual.err(), reference.err())
	}
}

func TestReconnectStressCompletedAckRejectsMissingRows(t *testing.T) {
	var dataset = reconnectOracleDataset(t)
	var oldSOW, oldJournal = sow, journal
	sow = newSOWCache()
	journal = newMessageJournal(16)
	resetTopicSubscribersForTest()
	t.Cleanup(func() {
		resetTopicSubscribersForTest()
		sow, journal = oldSOW, oldJournal
	})
	var server = startReconnectStressServer(t)
	t.Cleanup(func() { server.close(t) })
	var client = amps.NewClient("incomplete-snapshot-oracle")
	t.Cleanup(func() { _ = client.Close() })
	var uri = "tcp://" + server.listener.Addr().String() + "/amps/json"
	if err := client.Connect(uri); err != nil {
		t.Fatal(err)
	}
	if err := client.Logon(); err != nil {
		t.Fatal(err)
	}
	var tracker = newReconnectStressTracker([]reconnectStressTopicData{dataset})
	_ = subscribeReconnectStressTopics(t, client, tracker, []reconnectStressTopicData{dataset})
	select {
	case <-tracker.done():
	case <-time.After(reconnectStressCycleTimeout):
		t.Fatal("completed acknowledgement did not reach the tracker")
	}
	if err := tracker.err(); err == nil || !strings.Contains(err.Error(), "received 0 rows, want 2") {
		t.Fatalf("incomplete snapshot error = %v", err)
	}
}

func TestReconnectStressIndexValidatesCorpus(t *testing.T) {
	if err := indexReconnectStressDataset(nil); err == nil {
		t.Fatal("nil corpus was accepted")
	}
	t.Run("valid", func(t *testing.T) {
		var dataset = reconnectOracleDataset(t)
		dataset.expectedByPayload = nil
		if err := indexReconnectStressDataset(&dataset); err != nil {
			t.Fatalf("valid corpus: %v", err)
		}
		for _, row := range dataset.Rows {
			if got := dataset.expectedByPayload[row.Raw]; got != row.RecordID {
				t.Fatalf("index[%q] = %q, want %q", row.Raw, got, row.RecordID)
			}
		}
	})
	for _, test := range []struct {
		name   string
		mutate func(*reconnectStressTopicData)
	}{
		{name: "malformed JSON", mutate: func(dataset *reconnectStressTopicData) {
			dataset.Rows[0].Raw = "{"
			dataset.Expected[dataset.Rows[0].RecordID] = "{"
		}},
		{name: "wrong topic", mutate: func(dataset *reconnectStressTopicData) { dataset.Topic = "different" }},
		{name: "wrong message type", mutate: func(dataset *reconnectStressTopicData) { dataset.MessageType = "different" }},
		{name: "wrong record ID", mutate: func(dataset *reconnectStressTopicData) {
			var row = &dataset.Rows[0]
			delete(dataset.Expected, row.RecordID)
			row.RecordID = "different"
			dataset.Expected[row.RecordID] = row.Raw
		}},
		{name: "missing expected record", mutate: func(dataset *reconnectStressTopicData) {
			delete(dataset.Expected, dataset.Rows[0].RecordID)
		}},
		{name: "mismatched expected payload", mutate: func(dataset *reconnectStressTopicData) {
			dataset.Expected[dataset.Rows[0].RecordID] = "different"
		}},
		{name: "unexpected expected record", mutate: func(dataset *reconnectStressTopicData) {
			dataset.Expected["extra"] = `{"record_id":"extra"}`
		}},
		{name: "duplicate row", mutate: func(dataset *reconnectStressTopicData) {
			dataset.Rows[1] = dataset.Rows[0]
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var dataset = reconnectOracleDataset(t)
			test.mutate(&dataset)
			if err := indexReconnectStressDataset(&dataset); err == nil {
				t.Fatal("invalid corpus was accepted")
			}
			if dataset.expectedByPayload != nil {
				t.Fatal("invalid corpus kept a stale index")
			}
		})
	}
}

func TestReconnectStressGeneratedCorpusIsIndexed(t *testing.T) {
	var datasets = buildReconnectStressDatasets(t)
	var total int
	for _, dataset := range datasets {
		if dataset.indexedTopic != dataset.Topic || dataset.indexedMessageType != dataset.MessageType || len(dataset.expectedByPayload) != len(dataset.Rows) {
			t.Fatalf("topic %s has an incomplete payload index", dataset.Topic)
		}
		for _, row := range dataset.Rows {
			if dataset.expectedByPayload[row.Raw] != row.RecordID {
				t.Fatalf("topic %s record %s missing from payload index", dataset.Topic, row.RecordID)
			}
			total++
		}
	}
	if total == 0 {
		t.Fatal("generated corpus is empty")
	}
}

func TestReconnectStressSnapshotFastPathAllocationBudget(t *testing.T) {
	var dataset = reconnectOracleDataset(t)
	var tracker = newReconnectStressTracker([]reconnectStressTopicData{dataset})
	var row = dataset.Rows[0]
	var message = new(amps.Message).SetData([]byte(row.Raw))
	var seen = tracker.topicStates[dataset.Topic].seen
	var allocations = testing.AllocsPerRun(100, func() {
		delete(seen, row.RecordID)
		_ = tracker.handleSOW(dataset.Topic, message)
	})
	if err := tracker.err(); err != nil {
		t.Fatal(err)
	}
	if allocations > 1 {
		t.Fatalf("snapshot validation allocated %.1f times per record, want at most 1", allocations)
	}
}

func BenchmarkReconnectStressSnapshotValidation(b *testing.B) {
	var dataset = reconnectOracleDataset(b)
	var tracker = newReconnectStressTracker([]reconnectStressTopicData{dataset})
	var row = dataset.Rows[0]
	var message = new(amps.Message).SetData([]byte(row.Raw))
	var seen = tracker.topicStates[dataset.Topic].seen
	b.ReportAllocs()
	b.SetBytes(int64(len(row.Raw)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		delete(seen, row.RecordID)
		_ = tracker.handleSOW(dataset.Topic, message)
	}
	b.StopTimer()
	if err := tracker.err(); err != nil {
		b.Fatal(err)
	}
}
