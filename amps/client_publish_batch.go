package amps

import (
	"fmt"
	"io"
	"net"
	"time"
)

// All batch buffer and timer operations run under client.lock. Session identity
// prevents a delayed timer from writing an old batch after reconnect.
func (client *Client) resetPublishBatch() {
	if client.batchTimer != nil {
		client.batchTimer.Stop()
		client.batchTimer = nil
	}
	client.batchBuffer = client.batchBuffer[:0]
	client.batchConnection = nil
	client.batchEpoch++
}

func (client *Client) writePublishBatch(connection net.Conn, data []byte) error {
	var count, err = connection.Write(data)
	if err == nil && count != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		client.onConnectionErrorWithCallbackModeForConnection(NewError(ConnectionError, fmt.Sprintf("Socket error while sending batch (%v)", err)), true, connection, true)
	}
	return err
}

func (client *Client) flushPublishBatch() error {
	if len(client.batchBuffer) == 0 {
		return nil
	}
	var connection = client.batchConnection
	client.connectionStateLock.Lock()
	var current = client.connection
	client.connectionStateLock.Unlock()
	var data = client.batchBuffer
	var generation = client.batchGeneration
	client.resetPublishBatch()
	if connection != current || generation != client.generation.Load() || !client.connected.Load() {
		return nil
	}
	return client.writePublishBatch(connection, data)
}

func (client *Client) batchPublishFrame(connection net.Conn, frame []byte, limit uint64, timeout time.Duration) error {
	if len(client.batchBuffer) > 0 && (client.batchConnection != connection || client.batchGeneration != client.generation.Load()) {
		client.resetPublishBatch()
	}
	if len(client.batchBuffer) > 0 && uint64(len(client.batchBuffer))+uint64(len(frame)) > limit {
		if err := client.flushPublishBatch(); err != nil {
			return err
		}
	}
	if limit <= 32 || uint64(len(frame)) >= limit-32 {
		if err := client.flushPublishBatch(); err != nil {
			return err
		}
		return client.writePublishBatch(connection, frame)
	}
	client.batchBuffer = append(client.batchBuffer, frame...)
	client.batchConnection = connection
	client.batchGeneration = client.generation.Load()
	if uint64(len(client.batchBuffer)) >= limit-32 {
		return client.flushPublishBatch()
	}
	if client.batchTimer == nil {
		var epoch = client.batchEpoch
		client.batchTimer = time.AfterFunc(timeout, func() {
			client.lock.Lock()
			var err error
			if epoch == client.batchEpoch {
				err = client.flushPublishBatch()
			}
			client.lock.Unlock()
			if err != nil {
				client.reportException(err)
			}
		})
	}
	return nil
}
