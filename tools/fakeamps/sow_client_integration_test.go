package main

import (
	"net"
	"testing"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

func TestSOWFramesAreClientParseable(t *testing.T) {
	oldSow := sow
	oldJournal := journal
	sow = newSOWCache()
	journal = newMessageJournal(1000)
	defer func() {
		sow = oldSow
		journal = oldJournal
	}()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		handleConnection(conn)
	}()

	client := amps.NewClient("fakeamps-sow-client")
	errCh := make(chan error, 1)
	client.SetErrorHandler(func(err error) {
		select {
		case errCh <- err:
		default:
		}
	})

	uri := "tcp://" + listener.Addr().String() + "/amps/json"
	if err := client.Connect(uri); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer func() { _ = client.Close() }()
	if err := client.Logon(); err != nil {
		t.Fatalf("logon failed: %v", err)
	}

	if err := client.Publish("orders", `{"id":1}`); err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	if err := client.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	rowCh := make(chan string, 1)
	ackCh := make(chan struct{}, 1)
	_, err = client.ExecuteAsync(amps.NewCommand("sow").SetTopic("orders").AddAckType(amps.AckTypeCompleted), func(message *amps.Message) error {
		command, _ := message.Command()
		switch command {
		case amps.CommandSOW:
			select {
			case rowCh <- string(message.Data()):
			default:
			}
		case amps.CommandAck:
			if ackType, ok := message.AckType(); ok && ackType == amps.AckTypeCompleted {
				select {
				case ackCh <- struct{}{}:
				default:
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("execute sow failed: %v", err)
	}

	select {
	case row := <-rowCh:
		if row != `{"id":1}` {
			t.Fatalf("unexpected SOW row: %s", row)
		}
	case err := <-errCh:
		t.Fatalf("unexpected client error before SOW row: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for SOW row")
	}

	select {
	case <-ackCh:
	case err := <-errCh:
		t.Fatalf("unexpected client error before completed ack: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for SOW completed ack")
	}
}
