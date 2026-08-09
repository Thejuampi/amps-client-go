package main

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

// deliveryHarness starts a real fakeamps listener and hands back real, logged-on
// amps clients. Everything the tests do goes through exported client API; this
// file lives outside package amps, so the compiler forbids reaching into
// unexported state.
type deliveryHarness struct {
	listener net.Listener
	clients  []*amps.Client
}

func startDeliveryHarness(t *testing.T) *deliveryHarness {
	t.Helper()

	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	go func() {
		for {
			var conn, acceptErr = listener.Accept()
			if acceptErr != nil {
				return
			}
			go handleConnection(conn)
		}
	}()

	var harness = &deliveryHarness{listener: listener}
	t.Cleanup(func() {
		for _, client := range harness.clients {
			_ = client.Close()
		}
		_ = listener.Close()
	})
	return harness
}

func (harness *deliveryHarness) newClient(t *testing.T, name string) *amps.Client {
	t.Helper()

	var client = amps.NewClient(name)
	var uri = "tcp://" + harness.listener.Addr().String() + "/amps/json"
	if err := client.Connect(uri); err != nil {
		t.Fatalf("connect(%s) failed: %v", name, err)
	}
	if err := client.Logon(); err != nil {
		t.Fatalf("logon(%s) failed: %v", name, err)
	}
	harness.clients = append(harness.clients, client)
	return client
}

// The harness is worthless unless it provably delivers. A harness that silently
// delivers nothing makes every test built on it vacuously green - that mistake
// was made once already during this work and hid a real defect - so this test
// exists to fail loudly if delivery ever stops working.
func TestDeliveryHarnessActuallyDeliversMessages(t *testing.T) {
	var harness = startDeliveryHarness(t)
	var subscriber = harness.newClient(t, "harness-subscriber")
	var publisher = harness.newClient(t, "harness-publisher")

	var stream, err = subscriber.Subscribe("orders")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	stream.SetTimeout(250)

	for index := 0; index < 20; index++ {
		if publishErr := publisher.Publish("orders", fmt.Sprintf(`{"id":%d}`, index)); publishErr != nil {
			t.Fatalf("publish failed: %v", publishErr)
		}
	}

	var received int
	var deadline = time.Now().Add(10 * time.Second)
	for received < 20 && time.Now().Before(deadline) {
		if !stream.HasNext() {
			continue
		}
		if message := stream.Next(); message != nil {
			received++
		}
	}

	if received == 0 {
		t.Fatalf("harness delivered 0 messages: any test built on this harness would be vacuous")
	}
	t.Logf("harness delivered %d/20 messages", received)
}
