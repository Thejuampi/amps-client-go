package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
)

func main() {
	var addr = flag.String("addr", "127.0.0.1:19000", "listen address")
	flag.Parse()

	var listener, err = net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("fakeamps listen failed: %v", err)
	}
	defer func() {
		_ = listener.Close()
	}()

	log.Printf("fakeamps listening on %s", *addr)
	for {
		var connection, acceptErr = listener.Accept()
		if acceptErr != nil {
			log.Printf("fakeamps accept failed: %v", acceptErr)
			continue
		}
		go handleConnection(connection)
	}
}

func handleConnection(connection net.Conn) {
	defer func() {
		_ = connection.Close()
	}()

	var frameLength [4]byte
	for {
		if _, err := io.ReadFull(connection, frameLength[:]); err != nil {
			if err != io.EOF {
				log.Printf("fakeamps read length failed: %v", err)
			}
			return
		}

		var length = binary.BigEndian.Uint32(frameLength[:])
		if length == 0 {
			continue
		}
		var frame = make([]byte, length)
		if _, err := io.ReadFull(connection, frame); err != nil {
			log.Printf("fakeamps read frame failed: %v", err)
			return
		}

		var header, payload = splitHeaderPayload(frame)
		_ = payload
		var command = getJSONValue(header, "c")
		var commandID = getJSONValue(header, "cid")

		switch command {
		case "logon", "subscribe", "delta_subscribe", "sow", "sow_and_subscribe", "sow_and_delta_subscribe", "publish", "delta_publish", "flush", "sow_delete", "ack", "p":
			if err := writeAck(connection, commandID, "success"); err != nil {
				log.Printf("fakeamps write ack failed: %v", err)
				return
			}
		default:
			if err := writeAck(connection, commandID, "failure"); err != nil {
				log.Printf("fakeamps write failure ack failed: %v", err)
				return
			}
		}
	}
}

func splitHeaderPayload(frame []byte) (string, []byte) {
	var index = strings.IndexByte(string(frame), '}')
	if index < 0 {
		return string(frame), nil
	}
	var header = string(frame[:index+1])
	var payload = frame[index+1:]
	return header, payload
}

func getJSONValue(header string, key string) string {
	var token = fmt.Sprintf(`"%s":"`, key)
	var start = strings.Index(header, token)
	if start < 0 {
		return ""
	}
	start += len(token)
	var end = strings.IndexByte(header[start:], '"')
	if end < 0 {
		return ""
	}
	return header[start : start+end]
}

func writeAck(connection net.Conn, commandID string, status string) error {
	var header string
	if commandID == "" {
		header = fmt.Sprintf(`{"c":"ack","a":"processed","status":"%s"}`, status)
	} else {
		header = fmt.Sprintf(`{"c":"ack","a":"processed","status":"%s","cid":"%s"}`, status, commandID)
	}

	var headerBytes = []byte(header)
	var frame = make([]byte, 4+len(headerBytes))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(headerBytes)))
	copy(frame[4:], headerBytes)
	_, err := connection.Write(frame)
	return err
}
