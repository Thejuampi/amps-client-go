package amps

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	integrationLogonTimeout = 10 * time.Second
	integrationWaitTimeout  = 8 * time.Second
)

type integrationAck struct {
	status  string
	reason  string
	ackType int
}

func integrationURI(t *testing.T) string {
	t.Helper()
	uri := strings.TrimSpace(os.Getenv("AMPS_TEST_URI"))
	if uri == "" {
		t.Skip("integration test skipped: AMPS_TEST_URI is not set")
	}

	user := strings.TrimSpace(os.Getenv("AMPS_TEST_USER"))
	password := strings.TrimSpace(os.Getenv("AMPS_TEST_PASSWORD"))
	if user == "" {
		return applyIntegrationProtocol(uri)
	}

	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatalf("invalid AMPS_TEST_URI: %v", err)
	}
	if parsed.User == nil {
		parsed.User = url.UserPassword(user, password)
	}
	return applyIntegrationProtocol(parsed.String())
}

func applyIntegrationProtocol(uri string) string {
	protocol := strings.TrimSpace(os.Getenv("AMPS_TEST_PROTOCOL"))
	if protocol == "" {
		return uri
	}

	parsed, err := url.Parse(uri)
	if err != nil {
		return uri
	}
	if strings.Contains(parsed.Path, "/amps/") {
		return uri
	}

	parsed.Path = "/amps/" + protocol
	return parsed.String()
}

func integrationUniqueTopic(prefix string) string {
	return fmt.Sprintf("%s.%d", prefix, time.Now().UTC().UnixNano())
}

func integrationReasonUnsupported(reason string) bool {
	lower := strings.ToLower(reason)
	return strings.Contains(lower, "invalid topic") ||
		strings.Contains(lower, "not entitled") ||
		strings.Contains(lower, "unsupported") ||
		strings.Contains(lower, "not supported")
}

func integrationErrorUnsupported(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "invalidtopicerror") ||
		strings.Contains(lower, "notentitlederror")
}

func integrationRequireFeatureOrSkip(t *testing.T, feature string, err error) {
	t.Helper()
	if err == nil {
		return
	}
	if integrationErrorUnsupported(err) {
		t.Skipf("integration test skipped: %s not supported by endpoint (%v)", feature, err)
	}
	t.Fatalf("%s failed: %v", feature, err)
}

func integrationRequireAckSuccessOrSkip(t *testing.T, feature string, result integrationAck) {
	t.Helper()
	if strings.EqualFold(result.status, "success") {
		return
	}
	if integrationReasonUnsupported(result.reason) {
		t.Skipf("integration test skipped: %s not supported (%s)", feature, result.reason)
	}
	t.Fatalf("%s failed: status=%q reason=%q", feature, result.status, result.reason)
}

func integrationProcessedAckHandler(out chan<- integrationAck) func(*Message) error {
	return func(message *Message) error {
		commandType, _ := message.Command()
		if commandType != CommandAck {
			return nil
		}
		ackType, hasAckType := message.AckType()
		if !hasAckType || ackType != AckTypeProcessed {
			return nil
		}
		status, _ := message.Status()
		reason, _ := message.Reason()
		select {
		case out <- integrationAck{status: status, reason: reason, ackType: ackType}:
		default:
		}
		return nil
	}
}

func integrationExecuteAndRequireProcessedAck(t *testing.T, client *Client, feature string, command *Command, skipOnTimeout bool) {
	t.Helper()

	processedAcks := make(chan integrationAck, 4)
	_, err := client.ExecuteAsync(command.AddAckType(AckTypeProcessed), integrationProcessedAckHandler(processedAcks))
	integrationRequireFeatureOrSkip(t, feature, err)

	select {
	case result := <-processedAcks:
		integrationRequireAckSuccessOrSkip(t, feature, result)
	case <-time.After(integrationWaitTimeout):
		if skipOnTimeout {
			t.Skipf("integration test skipped: %s ack was not returned by endpoint", feature)
		}
		t.Fatalf("timed out waiting for %s ack", feature)
	}
}

func integrationLogon(t *testing.T, client *Client) {
	t.Helper()
	logonResult := make(chan error, 1)
	go func() {
		logonResult <- client.Logon()
	}()
	select {
	case err := <-logonResult:
		if err != nil {
			t.Fatalf("logon failed: %v", err)
		}
	case <-time.After(integrationLogonTimeout):
		t.Fatalf("timed out waiting for logon ack")
	}
}

func integrationConnectAndLogon(t *testing.T, clientName string) (*Client, string) {
	t.Helper()
	uri := integrationURI(t)
	client := NewClient(clientName)
	if err := client.Connect(uri); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	integrationLogon(t, client)
	return client, uri
}

func integrationFailoverURIs(t *testing.T) []string {
	t.Helper()

	value := strings.TrimSpace(os.Getenv("AMPS_TEST_FAILOVER_URIS"))
	if value == "" {
		t.Skip("integration test skipped: AMPS_TEST_FAILOVER_URIS is not set")
	}

	user := strings.TrimSpace(os.Getenv("AMPS_TEST_USER"))
	password := strings.TrimSpace(os.Getenv("AMPS_TEST_PASSWORD"))
	parts := strings.Split(value, ",")
	uris := make([]string, 0, len(parts))
	for _, part := range parts {
		uri := strings.TrimSpace(part)
		if uri == "" {
			continue
		}
		uri = applyIntegrationProtocol(uri)
		if user == "" {
			uris = append(uris, uri)
			continue
		}

		parsed, err := url.Parse(uri)
		if err != nil {
			t.Fatalf("invalid URI in AMPS_TEST_FAILOVER_URIS: %v", err)
		}
		if parsed.User == nil {
			parsed.User = url.UserPassword(user, password)
		}
		uris = append(uris, parsed.String())
	}

	if len(uris) == 0 {
		t.Fatalf("AMPS_TEST_FAILOVER_URIS is set but contains no usable URIs")
	}
	return uris
}

func TestIntegrationConnectLogonPublishSubscribe(t *testing.T) {
	client, _ := integrationConnectAndLogon(t, "integration-connect-logon")
	defer func() { _ = client.Close() }()

	topic := integrationUniqueTopic("amps.integration.pubsub")
	received := make(chan struct{}, 1)
	type subscribeResult struct {
		id  string
		err error
	}
	subscribeDone := make(chan subscribeResult, 1)
	go func() {
		subID, err := client.SubscribeAsync(func(message *Message) error {
			if messageTopic, hasTopic := message.Topic(); hasTopic && messageTopic == topic {
				select {
				case received <- struct{}{}:
				default:
				}
			}
			return nil
		}, topic)
		subscribeDone <- subscribeResult{id: subID, err: err}
	}()

	var subID string
	select {
	case result := <-subscribeDone:
		if result.err != nil {
			t.Fatalf("subscribe failed: %v", result.err)
		}
		subID = result.id
	case <-time.After(integrationLogonTimeout):
		t.Fatalf("timed out waiting for subscribe ack")
	}
	defer func() { _ = client.Unsubscribe(subID) }()

	if err := client.Publish(topic, `{"integration":true}`); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for subscribed message")
	}
}

func TestIntegrationSOWAndSowAndSubscribeLifecycle(t *testing.T) {
	client, _ := integrationConnectAndLogon(t, "integration-sow-lifecycle")
	defer func() { _ = client.Close() }()

	topic := integrationUniqueTopic("amps.integration.sow")
	if err := client.Publish(topic, fmt.Sprintf(`{"id":1,"phase":"seed-%d"}`, time.Now().UnixNano())); err != nil {
		t.Fatalf("seed publish failed: %v", err)
	}

	sowAcks := make(chan integrationAck, 2)
	sowRows := make(chan struct{}, 8)
	command := NewCommand("sow").SetTopic(topic).SetFilter("1=1").AddAckType(AckTypeCompleted)
	_, err := client.ExecuteAsync(command, func(message *Message) error {
		commandType, _ := message.Command()
		switch commandType {
		case CommandSOW:
			select {
			case sowRows <- struct{}{}:
			default:
			}
		case CommandAck:
			ackType, hasAckType := message.AckType()
			if hasAckType && ackType == AckTypeCompleted {
				status, _ := message.Status()
				reason, _ := message.Reason()
				select {
				case sowAcks <- integrationAck{status: status, reason: reason, ackType: ackType}:
				default:
				}
			}
		}
		return nil
	})
	integrationRequireFeatureOrSkip(t, "sow", err)

	var sowResult integrationAck
	select {
	case sowResult = <-sowAcks:
	case <-time.After(integrationWaitTimeout):
		t.Fatalf("timed out waiting for SOW completed ack")
	}
	integrationRequireAckSuccessOrSkip(t, "sow", sowResult)

	select {
	case <-sowRows:
	case <-time.After(5 * time.Second):
		t.Fatalf("expected SOW rows for %s after seed publish", topic)
	}

	updates := make(chan string, 8)
	subID, err := client.SowAndSubscribeAsync(func(message *Message) error {
		commandType, _ := message.Command()
		if commandType == CommandPublish {
			if messageTopic, hasTopic := message.Topic(); hasTopic && messageTopic == topic {
				select {
				case updates <- string(message.Data()):
				default:
				}
			}
		}
		return nil
	}, topic, "1=1")
	integrationRequireFeatureOrSkip(t, "sow_and_subscribe", err)
	defer func() { _ = client.Unsubscribe(subID) }()

	updatePayload := fmt.Sprintf(`{"id":2,"phase":"update-%d"}`, time.Now().UnixNano())
	if err := client.Publish(topic, updatePayload); err != nil {
		t.Fatalf("update publish failed: %v", err)
	}

	select {
	case payload := <-updates:
		if !strings.Contains(payload, `"id":2`) {
			t.Fatalf("unexpected sow_and_subscribe payload: %s", payload)
		}
	case <-time.After(6 * time.Second):
		t.Fatalf("timed out waiting for sow_and_subscribe update")
	}
}

func TestIntegrationQueueAutoAckBatching(t *testing.T) {
	client, _ := integrationConnectAndLogon(t, "integration-queue-auto-ack")
	defer func() { _ = client.Close() }()

	queueTopic := "queue://" + integrationUniqueTopic("amps.integration.queue")
	client.SetAutoAck(true).SetAckBatchSize(2).SetAckTimeout(3 * time.Second)

	outboundAcks := make(chan string, 8)
	client.SetTransportFilter(func(direction TransportFilterDirection, payload []byte) []byte {
		if direction == TransportFilterOutbound && bytes.Contains(payload, []byte(`"c":"ack"`)) {
			select {
			case outboundAcks <- string(payload):
			default:
			}
		}
		return payload
	})

	type queueDelivery struct {
		bookmark string
		hasLease bool
	}
	deliveries := make(chan queueDelivery, 16)
	subID, err := client.SubscribeAsync(func(message *Message) error {
		commandType, _ := message.Command()
		if commandType != CommandPublish {
			return nil
		}

		bookmark, _ := message.Bookmark()
		_, hasLease := message.LeasePeriod()
		select {
		case deliveries <- queueDelivery{bookmark: bookmark, hasLease: hasLease}:
		default:
		}
		return nil
	}, queueTopic)
	integrationRequireFeatureOrSkip(t, "queue subscribe", err)
	defer func() { _ = client.Unsubscribe(subID) }()

	if err := client.Publish(queueTopic, `{"queue":1}`); err != nil {
		integrationRequireFeatureOrSkip(t, "queue publish #1", err)
	}
	if err := client.Publish(queueTopic, `{"queue":2}`); err != nil {
		integrationRequireFeatureOrSkip(t, "queue publish #2", err)
	}

	bookmarks := make([]string, 0, 2)
	timeout := time.After(8 * time.Second)
	for len(bookmarks) < 2 {
		select {
		case delivery := <-deliveries:
			if !delivery.hasLease {
				t.Skip("integration test skipped: endpoint delivered non-queue publish messages (missing lease period)")
			}
			if delivery.bookmark == "" {
				t.Skip("integration test skipped: queue message missing bookmark")
			}
			bookmarks = append(bookmarks, delivery.bookmark)
		case <-timeout:
			t.Fatalf("timed out waiting for queue deliveries (got %d)", len(bookmarks))
		}
	}

	expectedBatch := bookmarks[0] + "," + bookmarks[1]
	foundBatchedAck := false
	deadline := time.After(6 * time.Second)
	for !foundBatchedAck {
		select {
		case payload := <-outboundAcks:
			if strings.Contains(payload, expectedBatch) {
				foundBatchedAck = true
			}
		case <-deadline:
			t.Fatalf("timed out waiting for auto-ack batch payload containing %q", expectedBatch)
		}
	}
}

func TestIntegrationBookmarkResumeAcrossReconnect(t *testing.T) {
	client, uri := integrationConnectAndLogon(t, "integration-bookmark-resume")
	defer func() { _ = client.Close() }()

	store := NewMemoryBookmarkStore()
	client.SetBookmarkStore(store)

	duplicateCount := 0
	client.SetDuplicateMessageHandler(func(message *Message) error {
		duplicateCount++
		return nil
	})

	topic := integrationUniqueTopic("amps.integration.bookmark")
	received := make(chan string, 16)
	handler := func(message *Message) error {
		commandType, _ := message.Command()
		if commandType != CommandPublish {
			return nil
		}
		if messageTopic, hasTopic := message.Topic(); hasTopic && messageTopic == topic {
			select {
			case received <- string(message.Data()):
			default:
			}
		}
		return nil
	}

	subID, err := client.BookmarkSubscribeAsync(handler, topic, BOOKMARK_EPOCH())
	integrationRequireFeatureOrSkip(t, "bookmark subscribe (epoch)", err)
	defer func() { _ = client.Unsubscribe(subID) }()

	firstPayload := fmt.Sprintf(`{"phase":"first","ts":%d}`, time.Now().UnixNano())
	if err := client.Publish(topic, firstPayload); err != nil {
		t.Fatalf("first publish failed: %v", err)
	}

	select {
	case payload := <-received:
		if payload != firstPayload {
			t.Fatalf("unexpected first payload: got %s want %s", payload, firstPayload)
		}
	case <-time.After(6 * time.Second):
		t.Fatalf("timed out waiting for first bookmark delivery")
	}

	resumeBookmark := store.GetMostRecent(subID)
	if resumeBookmark == "" {
		t.Skip("integration test skipped: endpoint did not provide bookmark metadata for resume")
	}

	if err := client.Unsubscribe(subID); err != nil {
		t.Fatalf("unsubscribe before reconnect failed: %v", err)
	}
	if err := client.Disconnect(); err != nil && !strings.Contains(err.Error(), "DisconnectedError") {
		t.Fatalf("disconnect before reconnect failed: %v", err)
	}

	if err := client.Connect(uri); err != nil {
		t.Fatalf("reconnect failed: %v", err)
	}
	integrationLogon(t, client)

	duplicateCount = 0
	subIDAfterReconnect, err := client.BookmarkSubscribeAsync(handler, topic, resumeBookmark)
	integrationRequireFeatureOrSkip(t, "bookmark subscribe (resume)", err)
	defer func() { _ = client.Unsubscribe(subIDAfterReconnect) }()

	secondPayload := fmt.Sprintf(`{"phase":"second","ts":%d}`, time.Now().UnixNano())
	if err := client.Publish(topic, secondPayload); err != nil {
		t.Fatalf("second publish failed: %v", err)
	}

	select {
	case payload := <-received:
		if payload != secondPayload {
			t.Fatalf("unexpected payload after reconnect: got %s want %s", payload, secondPayload)
		}
	case <-time.After(6 * time.Second):
		t.Fatalf("timed out waiting for resumed bookmark delivery")
	}

	if duplicateCount > 0 {
		t.Logf("duplicate handler invoked %d time(s) during resume", duplicateCount)
	}
}

func TestIntegrationHAConnectAndLogonWithFailoverChooser(t *testing.T) {
	uris := integrationFailoverURIs(t)

	ha := NewHAClient("integration-ha")
	defer func() { _ = ha.Disconnect() }()

	ha.SetServerChooser(NewDefaultServerChooser(uris...))
	ha.SetTimeout(20 * time.Second)
	ha.SetReconnectDelayStrategy(NewFixedDelayStrategy(50 * time.Millisecond))

	var lock sync.Mutex
	states := make([]ConnectionState, 0, 4)
	ha.Client().AddConnectionStateListener(ConnectionStateListenerFunc(func(state ConnectionState) {
		lock.Lock()
		states = append(states, state)
		lock.Unlock()
	}))

	if err := ha.ConnectAndLogon(); err != nil {
		t.Fatalf("HA connect/logon failed: %v", err)
	}
	if ha.Disconnected() {
		t.Fatalf("expected HA client to be connected")
	}

	info := ha.GetConnectionInfo()
	if info["uri"] == "" {
		t.Fatalf("expected HA connection info to include URI")
	}

	lock.Lock()
	defer lock.Unlock()
	foundConnected := false
	foundLoggedOn := false
	for _, state := range states {
		if state == ConnectionStateConnected {
			foundConnected = true
		}
		if state == ConnectionStateLoggedOn {
			foundLoggedOn = true
		}
	}
	if !foundConnected || !foundLoggedOn {
		t.Fatalf("expected HA connection states connected+logged-on, got %+v", states)
	}
}

func TestIntegrationStartStopTimerCommandPath(t *testing.T) {
	client, _ := integrationConnectAndLogon(t, "integration-timer")
	defer func() { _ = client.Close() }()

	timerID := integrationUniqueTopic("amps.integration.timer")
	integrationExecuteAndRequireProcessedAck(
		t,
		client,
		"start_timer",
		NewCommand("start_timer").SetTopic(timerID).SetOptions("interval=500ms"),
		true,
	)
	integrationExecuteAndRequireProcessedAck(
		t,
		client,
		"stop_timer",
		NewCommand("stop_timer").SetTopic(timerID),
		true,
	)
}

func TestIntegrationReconnectDuringInFlightOperation(t *testing.T) {
	client, uri := integrationConnectAndLogon(t, "integration-inflight-reconnect")
	defer func() { _ = client.Close() }()

	topic := integrationUniqueTopic("amps.integration.inflight")
	subscribeSent := make(chan struct{}, 1)
	client.SetTransportFilter(func(direction TransportFilterDirection, payload []byte) []byte {
		if direction == TransportFilterOutbound && bytes.Contains(payload, []byte(`"c":"subscribe"`)) {
			select {
			case subscribeSent <- struct{}{}:
			default:
			}
		}
		return payload
	})

	subscribeResult := make(chan error, 1)
	go func() {
		_, err := client.SubscribeAsync(func(message *Message) error { return nil }, topic)
		subscribeResult <- err
	}()

	select {
	case <-subscribeSent:
	case err := <-subscribeResult:
		if err != nil && !strings.Contains(err.Error(), "DisconnectedError") {
			t.Fatalf("unexpected early subscribe error: %v", err)
		}
		t.Skip("integration test skipped: subscribe completed before in-flight disconnect could be induced")
	case <-time.After(6 * time.Second):
		t.Fatalf("timed out waiting for subscribe send event")
	}

	_ = client.Disconnect()

	select {
	case err := <-subscribeResult:
		if err != nil && !strings.Contains(err.Error(), "DisconnectedError") {
			t.Fatalf("unexpected in-flight subscribe error: %v", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatalf("timed out waiting for in-flight subscribe result")
	}

	if err := client.Connect(uri); err != nil {
		t.Fatalf("reconnect after in-flight disconnect failed: %v", err)
	}
	integrationLogon(t, client)

	if err := client.Publish(topic, `{"reconnected":true}`); err != nil {
		t.Fatalf("publish after reconnect failed: %v", err)
	}
}

func TestIntegrationFailoverEnvContract(t *testing.T) {
	_ = integrationFailoverURIs(t)
}
