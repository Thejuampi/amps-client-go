package main

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

type reconnectCompletionChooser struct {
	*amps.DefaultServerChooser
	calls   atomic.Int32
	entered chan struct{}
	release <-chan struct{}
	resumed chan struct{}
}

func (chooser *reconnectCompletionChooser) ReportSuccess(info amps.ConnectionInfo) {
	chooser.DefaultServerChooser.ReportSuccess(info)
	switch chooser.calls.Add(1) {
	case 2:
		close(chooser.entered)
		<-chooser.release
	case 3:
		close(chooser.resumed)
	}
}

func TestIntegrationHAReconnectAfterDisconnectAtSuccess(t *testing.T) {
	var server = startReconnectStressServer(t)
	defer server.close(t)
	var uri = "tcp://" + server.listener.Addr().String() + "/amps/json"
	var release = make(chan struct{})
	var released sync.Once
	var unblock = func() { released.Do(func() { close(release) }) }
	var chooser = &reconnectCompletionChooser{
		DefaultServerChooser: amps.NewDefaultServerChooser(uri),
		entered:              make(chan struct{}),
		release:              release,
		resumed:              make(chan struct{}),
	}
	var ha = amps.NewHAClient("ha-reconnect-completion")
	ha.SetServerChooser(chooser)
	ha.SetReconnectDelayStrategy(amps.NewFixedDelayStrategy(0))
	ha.SetTimeout(reconnectStressReconnectTimeout)
	var disconnected = make(chan struct{}, 4)
	ha.Client().SetDisconnectHandler(func(*amps.Client, error) { disconnected <- struct{}{} })
	defer func() {
		unblock()
		_ = ha.Disconnect()
	}()
	if err := ha.ConnectAndLogon(); err != nil {
		t.Fatal(err)
	}
	if err := server.dropActiveConnection(); err != nil {
		t.Fatal(err)
	}
	waitForReconnectStressDisconnect(t, disconnected, 1)
	select {
	case <-chooser.entered:
	case <-time.After(reconnectStressReconnectTimeout):
		t.Fatal("first recovery did not reach its success callback")
	}
	if err := server.dropActiveConnection(); err != nil {
		t.Fatal(err)
	}
	waitForReconnectStressDisconnect(t, disconnected, 2)
	unblock()
	select {
	case <-chooser.resumed:
	case <-time.After(reconnectStressReconnectTimeout):
		t.Fatal("disconnect at reconnect completion was lost; no second recovery")
	}
}
