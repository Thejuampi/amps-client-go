package amps

import (
	"errors"
	"time"
)

func (client *Client) retryFullMessageStream(handler func(*Message) error, message *Message) error {
	for {
		if !client.connected.Load() {
			return NewError(DisconnectedError, "disconnected while waiting for stream capacity")
		}
		client.checkAndSendHeartbeat(false)
		time.Sleep(time.Millisecond)
		var err = handler(message)
		if err == nil || !errors.Is(err, errMessageStreamFull) {
			return err
		}
	}
}
