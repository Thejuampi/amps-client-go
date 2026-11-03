package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const ampsWebSocketMaxParts = 1 << 20

type ampsWebsocketConn struct {
	*websocketConn
	mode       string
	readData   []byte
	readOffset int
	writeData  []byte
	writeErr   error
}

func newAMPSWebsocketConn(conn *websocketConn) *ampsWebsocketConn {
	return &ampsWebsocketConn{websocketConn: conn}
}

func (c *ampsWebsocketConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if c.readOffset == len(c.readData) {
		var err error
		c.readData, err = c.readCommand()
		c.readOffset = 0
		if err != nil {
			return 0, err
		}
	}
	var n = copy(p, c.readData[c.readOffset:])
	c.readOffset += n
	if c.readOffset == len(c.readData) {
		c.readData = nil
		c.readOffset = 0
	}
	return n, nil
}

func (c *ampsWebsocketConn) readCommand() ([]byte, error) {
	if c.mode == "" {
		var preface, opcode, err = c.readMessage()
		if err != nil {
			return nil, err
		}
		if opcode != 1 || (string(preface) != "text" && string(preface) != "binary") {
			return nil, errors.New("invalid AMPS websocket preface")
		}
		c.mode = string(preface)
	}

	var countData, opcode, err = c.readMessage()
	if err != nil {
		return nil, err
	}
	if opcode != 2 || len(countData) != 4 {
		return nil, errors.New("invalid AMPS websocket part count")
	}
	var count = binary.LittleEndian.Uint32(countData)
	if count == 0 || count > ampsWebSocketMaxParts {
		return nil, errors.New("AMPS websocket part count out of range")
	}

	var header []byte
	header, opcode, err = c.readMessage()
	if err != nil {
		return nil, err
	}
	var trimmed = bytes.TrimSpace(header)
	if opcode != 1 || len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(trimmed) {
		return nil, errors.New("invalid AMPS websocket command header")
	}
	header = trimmed
	if len(header) > websocketMaxPayloadSize {
		return nil, errors.New("AMPS websocket command too large")
	}
	var frame = make([]byte, 4, 4+len(header))
	frame = append(frame, header...)
	for part := uint32(1); part < count; part++ {
		var payload []byte
		payload, opcode, err = c.readMessage()
		if err != nil {
			return nil, err
		}
		if opcode != 1 && opcode != 2 {
			return nil, errors.New("invalid AMPS websocket payload")
		}
		if len(payload) > websocketMaxPayloadSize-(len(frame)-4) {
			return nil, errors.New("AMPS websocket command too large")
		}
		frame = append(frame, payload...)
	}
	binary.BigEndian.PutUint32(frame[:4], uint32(len(frame)-4))
	return frame, nil
}

func (c *ampsWebsocketConn) readMessage() ([]byte, byte, error) {
	var message []byte
	var messageOpcode byte
	for {
		var header, err = c.reader.Peek(2)
		if err != nil {
			return nil, 0, err
		}
		var final = header[0]&0x80 != 0
		if header[0]&0x70 != 0 || header[1]&0x80 == 0 {
			return nil, 0, errors.New("invalid websocket frame flags")
		}
		var opcode byte
		var data []byte
		data, opcode, err = c.websocketConn.readFrame()
		if err != nil {
			return nil, 0, err
		}
		if opcode >= 8 {
			if !final || len(data) > 125 {
				return nil, 0, errors.New("invalid websocket control frame")
			}
			switch opcode {
			case 8:
				return nil, 0, io.EOF
			case 9:
				if err := c.writeControlFrame(10, data); err != nil {
					return nil, 0, err
				}
			case 10:
			default:
				return nil, 0, errors.New("unsupported websocket control frame")
			}
			continue
		}
		if messageOpcode == 0 {
			if opcode != 1 && opcode != 2 {
				return nil, 0, errors.New("unexpected websocket continuation")
			}
			if final {
				return data, opcode, nil
			}
			messageOpcode = opcode
		} else if opcode != 0 {
			return nil, 0, errors.New("interleaved websocket message")
		}
		if len(data) > websocketMaxPayloadSize-len(message) {
			return nil, 0, errors.New("websocket message too large")
		}
		message = append(message, data...)
		if final {
			return message, messageOpcode, nil
		}
	}
}

func (c *ampsWebsocketConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.writeErr != nil {
		return 0, c.writeErr
	}

	var consumed int
	var completed int
	for consumed < len(p) {
		if len(c.writeData) < 4 {
			var need = 4 - len(c.writeData)
			if remaining := len(p) - consumed; need > remaining {
				need = remaining
			}
			c.writeData = append(c.writeData, p[consumed:consumed+need]...)
			consumed += need
			if len(c.writeData) < 4 {
				continue
			}
		}
		var bodyLength = binary.BigEndian.Uint32(c.writeData[:4])
		if bodyLength > websocketMaxPayloadSize {
			return completed, c.failWrite(errors.New("AMPS websocket frame too large"))
		}
		var totalLength = int(bodyLength) + 4
		var need = totalLength - len(c.writeData)
		if remaining := len(p) - consumed; need > remaining {
			need = remaining
		}
		c.writeData = append(c.writeData, p[consumed:consumed+need]...)
		consumed += need
		if len(c.writeData) != totalLength {
			continue
		}
		if err := c.writeAMPSFrame(c.writeData[4:]); err != nil {
			return completed, c.failWrite(err)
		}
		c.writeData = c.writeData[:0]
		completed = consumed
	}
	return consumed, nil
}

func (c *ampsWebsocketConn) failWrite(err error) error {
	c.writeErr = err
	c.writeData = nil
	_ = c.websocketConn.Close()
	return err
}

type ampsWebSocketMessage struct {
	opcode  byte
	payload []byte
}

func (c *ampsWebsocketConn) writeAMPSFrame(frame []byte) error {
	if c.mode != "text" && c.mode != "binary" {
		return errors.New("AMPS websocket mode not negotiated")
	}
	if len(frame) == 0 {
		return c.writeMessageLocked(1, []byte(`{"c":"heartbeat"}`))
	}
	var header, body, fields, err = ampsWebSocketObject(frame)
	if err != nil {
		return err
	}
	var command string
	if err = json.Unmarshal(fields["c"], &command); err != nil || command == "" {
		return errors.New("AMPS websocket response missing command")
	}
	var messages []ampsWebSocketMessage
	var payloadOpcode byte = 1
	if c.mode == "binary" {
		payloadOpcode = 2
	}
	switch command {
	case "p", "publish", "delta_publish", "oof":
		header, err = ampsWebSocketNumberField(header, fields, "l", len(body))
		if err != nil {
			return err
		}
		messages = append(messages, ampsWebSocketMessage{1, header})
		if len(body) > 0 {
			messages = append(messages, ampsWebSocketMessage{payloadOpcode, body})
		}
	case "sow":
		var records []ampsWebSocketMessage
		for len(body) > 0 {
			var recordHeader, rest, recordFields, recordErr = ampsWebSocketObject(body)
			if recordErr != nil {
				return recordErr
			}
			var size, present, sizeErr = ampsWebSocketInteger(recordFields, "l")
			if sizeErr != nil || !present || size > uint64(len(rest)) {
				return errors.New("invalid AMPS websocket SOW record length")
			}
			records = append(records, ampsWebSocketMessage{1, recordHeader}, ampsWebSocketMessage{payloadOpcode, rest[:int(size)]})
			body = rest[int(size):]
		}
		header, err = ampsWebSocketNumberField(header, fields, "bs", len(records)/2)
		if err != nil {
			return err
		}
		messages = append(messages, ampsWebSocketMessage{1, header})
		messages = append(messages, records...)
	default:
		if len(body) != 0 {
			return errors.New("unexpected AMPS websocket response payload")
		}
		messages = append(messages, ampsWebSocketMessage{1, header})
	}
	for _, message := range messages {
		if err := c.writeMessageLocked(message.opcode, message.payload); err != nil {
			return err
		}
	}
	return nil
}

func (c *ampsWebsocketConn) writeMessageLocked(opcode byte, payload []byte) error {
	if len(payload) != 0 {
		var n, err = c.writeDataFrameLocked(opcode, payload)
		if err != nil {
			return err
		}
		if n != len(payload) {
			return io.ErrShortWrite
		}
		return nil
	}
	if err := c.setWriteDeadline(); err != nil {
		return err
	}
	defer func() { _ = c.clearWriteDeadline() }()
	var n, err = c.Conn.Write([]byte{0x80 | opcode, 0})
	if err != nil {
		return err
	}
	if n != 2 {
		return io.ErrShortWrite
	}
	return nil
}

func ampsWebSocketObject(data []byte) ([]byte, []byte, map[string]json.RawMessage, error) {
	var decoder = json.NewDecoder(bytes.NewReader(data))
	var fields map[string]json.RawMessage
	if err := decoder.Decode(&fields); err != nil || fields == nil {
		return nil, nil, nil, errors.New("invalid AMPS websocket response header")
	}
	var end = int(decoder.InputOffset())
	return data[:end], data[end:], fields, nil
}

func ampsWebSocketNumberField(header []byte, fields map[string]json.RawMessage, name string, value int) ([]byte, error) {
	var got, present, err = ampsWebSocketInteger(fields, name)
	if err != nil {
		return nil, err
	}
	if present {
		if got != uint64(value) {
			return nil, fmt.Errorf("inconsistent AMPS websocket %s", name)
		}
		return header, nil
	}
	var result = make([]byte, 0, len(header)+len(name)+16)
	result = append(result, header[:len(header)-1]...)
	if len(fields) > 0 {
		result = append(result, ',')
	}
	result = append(result, '"')
	result = append(result, name...)
	result = append(result, '"', ':')
	result = strconv.AppendInt(result, int64(value), 10)
	return append(result, '}'), nil
}

func ampsWebSocketInteger(fields map[string]json.RawMessage, name string) (uint64, bool, error) {
	var raw, present = fields[name]
	if !present {
		return 0, false, nil
	}
	var number json.Number
	if err := json.Unmarshal(raw, &number); err != nil {
		return 0, true, fmt.Errorf("invalid AMPS websocket %s", name)
	}
	var value, err = strconv.ParseUint(string(number), 10, 32)
	if err != nil {
		return 0, true, fmt.Errorf("invalid AMPS websocket %s", name)
	}
	return value, true, nil
}
