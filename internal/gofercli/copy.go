package gofercli

import (
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

type copyPublisher struct {
	client *amps.Client
	delta  bool
}

func newCopyPublisher(options connectionOptions, env []string, now func() time.Time, delta bool) (*copyPublisher, error) {
	var client, _, err = connectClient(options, env, now)
	if err != nil {
		return nil, err
	}
	return &copyPublisher{client: client, delta: delta}, nil
}

func (publisher *copyPublisher) Publish(topic string, payload []byte) error {
	if publisher == nil || publisher.client == nil {
		return nil
	}
	return publishPayload(publisher.client, topic, payload, publisher.delta)
}

func (publisher *copyPublisher) Close() {
	if publisher == nil || publisher.client == nil {
		return
	}
	_ = publisher.client.Close()
}
