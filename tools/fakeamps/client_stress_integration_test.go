package main

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

type stressPayload struct {
	Topic string `json:"topic"`
	Phase string `json:"phase"`
	Seq   int    `json:"seq"`
}

func connectAndLogonStressClient(t *testing.T, uri string, clientName string, errCh chan<- error) *amps.Client {
	t.Helper()

	var client = amps.NewClient(clientName)
	client.SetErrorHandler(func(err error) {
		select {
		case errCh <- err:
		default:
		}
	})

	if err := client.Connect(uri); err != nil {
		t.Fatalf("connect failed for %s: %v", clientName, err)
	}
	if err := client.Logon(); err != nil {
		_ = client.Close()
		t.Fatalf("logon failed for %s: %v", clientName, err)
	}

	return client
}

func TestIntegrationClientSowAndSubscribeAsyncSingleConnectionStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}

	const (
		topicCount   = 6
		seedPerTopic = 200
		livePerTopic = 800
	)

	var oldSow = sow
	var oldJournal = journal
	sow = newSOWCache()
	journal = newMessageJournal(topicCount * (seedPerTopic + livePerTopic) * 4)
	resetTopicSubscribersForTest()
	resetTopicConfigsForTest()
	resetViewsForTest()
	resetActionsForTest()
	defer func() {
		resetTopicSubscribersForTest()
		resetTopicConfigsForTest()
		resetViewsForTest()
		resetActionsForTest()
		sow = oldSow
		journal = oldJournal
	}()

	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

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

	var uri = "tcp://" + listener.Addr().String() + "/amps/json"
	var errCh = make(chan error, 128)

	var subscriber = connectAndLogonStressClient(t, uri, "fakeamps-stress-subscriber", errCh)
	defer func() { _ = subscriber.Close() }()

	var seeder = connectAndLogonStressClient(t, uri, "fakeamps-stress-seeder", errCh)
	var topicPrefix = fmt.Sprintf("orders.stress.%d", time.Now().UTC().UnixNano())
	var topics = make([]string, topicCount)
	var topicIndex int
	for topicIndex = 0; topicIndex < topicCount; topicIndex++ {
		topics[topicIndex] = fmt.Sprintf("%s.%d", topicPrefix, topicIndex)
	}

	for _, topic := range topics {
		var seq int
		for seq = 0; seq < seedPerTopic; seq++ {
			var payload = fmt.Sprintf(`{"topic":"%s","phase":"seed","seq":%d}`, topic, seq)
			if err := seeder.Publish(topic, payload); err != nil {
				_ = seeder.Close()
				t.Fatalf("seed publish failed for %s seq=%d: %v", topic, seq, err)
			}
		}
	}
	if err := seeder.Flush(); err != nil {
		_ = seeder.Close()
		t.Fatalf("seed flush failed: %v", err)
	}
	if err := seeder.Close(); err != nil {
		t.Fatalf("seed client close failed: %v", err)
	}

	var lock sync.Mutex
	var sowSeen = make(map[string]map[int]struct{}, topicCount)
	var liveSeen = make(map[string]map[int]struct{}, topicCount)
	var sowCallbacks int
	var liveCallbacks int
	var uniqueMessages int
	var expectedUnique = topicCount * (seedPerTopic + livePerTopic)
	var expectedSOWCallbacks = topicCount * seedPerTopic
	var expectedLiveCallbacks = topicCount * livePerTopic
	var done = make(chan struct{})
	var doneOnce sync.Once
	var firstCallbackErr error

	signalCallbackError := func(callbackErr error) {
		lock.Lock()
		if firstCallbackErr == nil {
			firstCallbackErr = callbackErr
		}
		lock.Unlock()
		doneOnce.Do(func() {
			close(done)
		})
	}

	var handler = func(message *amps.Message) error {
		if message == nil {
			return nil
		}

		var command, _ = message.Command()
		if command != amps.CommandSOW && command != amps.CommandPublish {
			return nil
		}

		var topic, hasTopic = message.Topic()
		if !hasTopic {
			signalCallbackError(fmt.Errorf("missing topic on command %d", command))
			return nil
		}

		var payload stressPayload
		if err := json.Unmarshal(message.Data(), &payload); err != nil {
			signalCallbackError(fmt.Errorf("unmarshal payload for %s failed: %w", topic, err))
			return nil
		}
		if payload.Topic != topic {
			signalCallbackError(fmt.Errorf("topic mismatch: message=%q payload=%q", topic, payload.Topic))
			return nil
		}
		if command == amps.CommandSOW && payload.Phase != "seed" {
			signalCallbackError(fmt.Errorf("unexpected SOW phase for %s seq=%d: %q", topic, payload.Seq, payload.Phase))
			return nil
		}
		if command == amps.CommandPublish && payload.Phase != "live" {
			signalCallbackError(fmt.Errorf("unexpected publish phase for %s seq=%d: %q", topic, payload.Seq, payload.Phase))
			return nil
		}

		lock.Lock()
		defer lock.Unlock()

		var target map[string]map[int]struct{}
		if command == amps.CommandSOW {
			target = sowSeen
			sowCallbacks++
		} else {
			target = liveSeen
			liveCallbacks++
		}

		var perTopic = target[topic]
		if perTopic == nil {
			perTopic = make(map[int]struct{})
			target[topic] = perTopic
		}
		if _, exists := perTopic[payload.Seq]; !exists {
			perTopic[payload.Seq] = struct{}{}
			uniqueMessages++
			if uniqueMessages == expectedUnique {
				doneOnce.Do(func() {
					close(done)
				})
			}
		}

		return nil
	}

	var subIDs = make([]string, 0, topicCount)
	for _, topic := range topics {
		var subID, subErr = subscriber.SowAndSubscribeAsync(handler, topic, "1=1")
		if subErr != nil {
			t.Fatalf("sow_and_subscribe failed for %s: %v", topic, subErr)
		}
		subIDs = append(subIDs, subID)
	}
	defer func() {
		for _, subID := range subIDs {
			_ = subscriber.Unsubscribe(subID)
		}
	}()

	var publishers = make([]*amps.Client, 0, topicCount)
	for topicIndex = 0; topicIndex < topicCount; topicIndex++ {
		publishers = append(publishers, connectAndLogonStressClient(t, uri, fmt.Sprintf("fakeamps-stress-publisher-%d", topicIndex), errCh))
	}
	defer func() {
		for _, publisher := range publishers {
			_ = publisher.Close()
		}
	}()

	var publishErrCh = make(chan error, topicCount)
	var publisherWG sync.WaitGroup
	for topicIndex = 0; topicIndex < topicCount; topicIndex++ {
		var publisher = publishers[topicIndex]
		var topic = topics[topicIndex]
		publisherWG.Add(1)
		go func(client *amps.Client, publishTopic string) {
			defer publisherWG.Done()

			var seq int
			for seq = 0; seq < livePerTopic; seq++ {
				var payload = fmt.Sprintf(`{"topic":"%s","phase":"live","seq":%d}`, publishTopic, seq)
				if err := client.Publish(publishTopic, payload); err != nil {
					select {
					case publishErrCh <- fmt.Errorf("live publish failed for %s seq=%d: %w", publishTopic, seq, err):
					default:
					}
					return
				}
			}

			if err := client.Flush(); err != nil {
				select {
				case publishErrCh <- fmt.Errorf("live flush failed for %s: %w", publishTopic, err):
				default:
				}
			}
		}(publisher, topic)
	}

	var publishersDone = make(chan struct{})
	go func() {
		publisherWG.Wait()
		close(publishersDone)
	}()

	var timeout = time.After(30 * time.Second)
	var receiveComplete bool
	var publishComplete bool
	for !receiveComplete || !publishComplete {
		select {
		case err := <-publishErrCh:
			t.Fatalf("publisher error: %v", err)
		case err := <-errCh:
			t.Fatalf("client error: %v", err)
		case <-done:
			receiveComplete = true
		case <-publishersDone:
			publishComplete = true
		case <-timeout:
			lock.Lock()
			var timeoutErr = firstCallbackErr
			var currentSOW = sowCallbacks
			var currentLive = liveCallbacks
			var currentUnique = uniqueMessages
			lock.Unlock()
			if timeoutErr != nil {
				t.Fatalf("stress callback error: %v", timeoutErr)
			}
			t.Fatalf("stress test timed out: unique=%d/%d sow_callbacks=%d/%d live_callbacks=%d/%d", currentUnique, expectedUnique, currentSOW, expectedSOWCallbacks, currentLive, expectedLiveCallbacks)
		}
	}

	select {
	case err := <-publishErrCh:
		t.Fatalf("publisher error: %v", err)
	case err := <-errCh:
		t.Fatalf("client error: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	lock.Lock()
	defer lock.Unlock()
	if firstCallbackErr != nil {
		t.Fatalf("stress callback error: %v", firstCallbackErr)
	}
	if sowCallbacks != expectedSOWCallbacks {
		t.Fatalf("unexpected SOW callback count: got %d want %d", sowCallbacks, expectedSOWCallbacks)
	}
	if liveCallbacks != expectedLiveCallbacks {
		t.Fatalf("unexpected publish callback count: got %d want %d", liveCallbacks, expectedLiveCallbacks)
	}

	for _, topic := range topics {
		if len(sowSeen[topic]) != seedPerTopic {
			t.Fatalf("unexpected SOW unique count for %s: got %d want %d", topic, len(sowSeen[topic]), seedPerTopic)
		}
		if len(liveSeen[topic]) != livePerTopic {
			t.Fatalf("unexpected publish unique count for %s: got %d want %d", topic, len(liveSeen[topic]), livePerTopic)
		}
	}

	_ = subscriber.Close()
	for _, publisher := range publishers {
		_ = publisher.Close()
	}
	listener.Close()

	select {
	case <-serverDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("fakeamps server did not exit in time")
	}
}
