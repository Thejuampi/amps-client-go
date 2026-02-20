package amps

import (
	"errors"
)


// Message is the AMPS message representation class. Any message received from AMPS is a Message object. Messages have 
// two main components: header and data. Most header properties can be accessed via getters, such as command type, 
// message length, sow key, etc. Message payload can be accessed using the Data() getter method.
//
// Note: Not all messages populate all headers. The Command Reference provides detailed description of the headers 
// returned on specific messages and what they contain
type Message struct {
	header *_Header
	data []byte
}



////////////////////////////////////////////////////////////////////////
//                            Internal API                            //
////////////////////////////////////////////////////////////////////////


// Message Parser State Machine
const (
	inHeader = iota
	inKey
	afterKey
	inValue
	inValueString
)


func (msg *Message) reset() {
	if msg.header != nil { msg.header.reset() }
	msg.data = nil
}


func parseHeader(msg *Message, resetMessage bool, array []byte) ([]byte, error) {
	// Null pointer
	if msg == nil { return array, errors.New("Message object error (Null Pointer)") }

	// Prepare the message
	if resetMessage { msg.reset() }

	// Decode the message
	state := inHeader
	var keyStart, keyEnd, valueStart, valueEnd int
	for i, character := range array {
		switch character {
		case '"':
			switch state {
			case inHeader:
				state = inKey
				keyStart = i + 1
			case inKey:
				state = afterKey
				keyEnd = i
			case inValue:
				state = inValueString
				valueStart = i + 1
			case inValueString:
				state = inHeader
				valueEnd = i
				msg.header.parseField(array[keyStart:keyEnd], array[valueStart:valueEnd])
			}
		case ':':
			if state == afterKey {
				state = inValue
				valueStart = i + 1
			}
		case ',':
			if state == inValue {
				state = inHeader
				valueEnd = i
				msg.header.parseField(array[keyStart:keyEnd], array[valueStart:valueEnd])
			}
		case '}':
			// the non-string value right before end of the header: `...23}`
			if array[i - 1] != '"' && i - 1 > 0 {
				msg.header.parseField(array[keyStart:keyEnd], array[valueStart:i])
			}

			return array[i + 1:], nil
		}
	}

	return array, errors.New("Unexpected end of AMPS header")
}



////////////////////////////////////////////////////////////////////////
//                        Public API                                  //
////////////////////////////////////////////////////////////////////////


// Copy creates a deep copy of the Message object. Most common use case is to keep a copy of the message delivered to 
// a message handler by one of the async client methods, such as ExecuteAsync(), since these methods reuse the same 
// Message object in order to be GC-friendly.
func (msg *Message) Copy() *Message {
	message := &Message{header: new(_Header)}

	message.header.command = msg.header.command
	if msg.data != nil { message.data = make([]byte, len(msg.data)); copy(message.data, msg.data) }

	if msg.header.ackType != nil { ackType := *msg.header.ackType; message.header.ackType = &ackType }
	if msg.header.batchSize != nil { batchSize := *msg.header.batchSize; message.header.batchSize = &batchSize }
	if msg.header.bookmark != nil {
		message.header.bookmark = make([]byte, len(msg.header.bookmark))
		copy(message.header.bookmark, msg.header.bookmark)
	}
	if msg.header.commandID != nil {
		message.header.commandID = make([]byte, len(msg.header.commandID))
		copy(message.header.commandID, msg.header.commandID)
	}
	if msg.header.correlationID != nil {
		message.header.correlationID = make([]byte, len(msg.header.correlationID))
		copy(message.header.correlationID, msg.header.correlationID)
	}
	if msg.header.expiration != nil { expiration := *msg.header.expiration; message.header.expiration = &expiration }
	if msg.header.filter != nil {
		message.header.filter = make([]byte, len(msg.header.filter))
		copy(message.header.filter, msg.header.filter)
	}
	if msg.header.groupSequenceNumber != nil {
		gseq := *msg.header.groupSequenceNumber
		message.header.groupSequenceNumber = &gseq
	}
	if msg.header.leasePeriod != nil {
		message.header.leasePeriod = make([]byte, len(msg.header.leasePeriod))
		copy(message.header.leasePeriod, msg.header.leasePeriod)
	}
	if msg.header.matches != nil { matches := *msg.header.matches; message.header.matches = &matches }
	if msg.header.messageLength != nil { msgLen := *msg.header.messageLength; message.header.messageLength = &msgLen }
	if msg.header.options != nil {
		message.header.options = make([]byte, len(msg.header.options))
		copy(message.header.options, msg.header.options)
	}
	if msg.header.orderBy != nil {
		message.header.orderBy = make([]byte, len(msg.header.orderBy))
		copy(message.header.orderBy, msg.header.orderBy)
	}
	if msg.header.queryID != nil {
		message.header.queryID = make([]byte, len(msg.header.queryID))
		copy(message.header.queryID, msg.header.queryID)
	}
	if msg.header.reason != nil {
		message.header.reason = make([]byte, len(msg.header.reason))
		copy(message.header.reason, msg.header.reason)
	}
	if msg.header.recordsDeleted != nil { rD := *msg.header.recordsDeleted; message.header.recordsDeleted = &rD }
	if msg.header.recordsInserted != nil { rI := *msg.header.recordsInserted; message.header.recordsInserted = &rI }
	if msg.header.recordsReturned != nil { rR := *msg.header.recordsReturned; message.header.recordsReturned = &rR }
	if msg.header.recordsUpdated != nil { rU := *msg.header.recordsUpdated; message.header.recordsUpdated = &rU }
	if msg.header.sequenceID != nil { sequenceID := *msg.header.sequenceID; message.header.sequenceID = &sequenceID }
	if msg.header.sowKey != nil {
		message.header.sowKey = make([]byte, len(msg.header.sowKey))
		copy(message.header.sowKey, msg.header.sowKey)
	}
	if msg.header.sowKeys != nil {
		message.header.sowKeys = make([]byte, len(msg.header.sowKeys))
		copy(message.header.sowKeys, msg.header.sowKeys)
	}
	if msg.header.status != nil {
		message.header.status = make([]byte, len(msg.header.status))
		copy(message.header.status, msg.header.status)
	}
	if msg.header.subID != nil {
		message.header.subID = make([]byte, len(msg.header.subID))
		copy(message.header.subID, msg.header.subID)
	}
	if msg.header.subIDs != nil {
		message.header.subIDs = make([]byte, len(msg.header.subIDs))
		copy(message.header.subIDs, msg.header.subIDs)
	}
	if msg.header.timestamp != nil {
		message.header.timestamp = make([]byte, len(msg.header.timestamp))
		copy(message.header.timestamp, msg.header.timestamp)
	}
	if msg.header.topN != nil { topN := *msg.header.topN; message.header.topN = &topN }
	if msg.header.topic != nil {
		message.header.topic = make([]byte, len(msg.header.topic))
		copy(message.header.topic, msg.header.topic)
	}
	if msg.header.topicMatches != nil { tM := *msg.header.topicMatches; message.header.topicMatches = &tM }
	if msg.header.userID != nil {
		message.header.userID = make([]byte, len(msg.header.userID))
		copy(message.header.userID, msg.header.userID)
	}

	return message
}


// AckType is the getter for the acknowledgement type of the message. Acknowledgement type can be one of the following:
//
// - amps.AckTypeNone (for all non-ACK messages)
//
// - amps.AckTypeReceived
//
// - amps.AckTypeParsed
//
// - amps.AckTypeProcessed
//
// - amps.AckTypePersisted
//
// - amps.AckTypeCompleted
//
// - amps.AckTypeStats
//
// Example:
//
//     if ackType, _ := message.AckType(); ackType == amps.AckTypeCompleted { ... }
//
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) AckType() (int, bool) {
	if msg.header.ackType != nil { return *msg.header.ackType, true }
	return AckTypeNone, false
}

// BatchSize returns the batch size value -- the number of messages that are batched together when returning a query
// result.
//
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) BatchSize() (uint, bool) {
	if msg.header.batchSize != nil { return *msg.header.batchSize, true }
	return 0, false
}

// Bookmark returns the bookmark value -- the client-originated identifier used to mark a location in journaled messages.
//
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) Bookmark() (string, bool) { return string(msg.header.bookmark), msg.header.bookmark != nil }

// Command returns the type e.g. amps.CommandPublish, amps.CommandSOW, of the message. Can be one of the following:
//
// - amps.CommandAck
//
// - amps.CommandGroupBegin
//
// - amps.CommandGroupEnd
//
// - amps.CommandOOF
//
// - amps.CommandPublish
//
// - amps.CommandSOW
//
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) Command() (int, bool) { return msg.header.command, true }

// CommandID returns the Client-specified id. The command id is returned by the engine in responses to commands 
// to allow the client to correlate the response to the command.
//
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) CommandID() (string, bool) { return string(msg.header.commandID), msg.header.commandID != nil }

// CorrelationID returns the correlation id value, if any.
//
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) CorrelationID() (string, bool) {
	return string(msg.header.correlationID), msg.header.correlationID != nil
}

// Data returns the Message data (payload). Only amps.CommandPublish, amps.CommandSOW, and amps.CommandOOF 
// messages can have a payload.
func (msg *Message) Data() []byte { return msg.data }

// Expiration returns the SOW expiration time (in seconds) if used in publish.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) Expiration() (uint, bool) {
	if msg.header.expiration != nil { return *msg.header.expiration, true }
	return 0, false
}

// Filter returns the content filter expression.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) Filter() (string, bool) { return string(msg.header.filter), msg.header.filter != nil }

// GroupSequenceNumber returns the Group Sequence Number for each batch message of a SOW response.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) GroupSequenceNumber() (uint, bool) {
	if msg.header.groupSequenceNumber != nil { return *msg.header.groupSequenceNumber, true }
	return 0, false
}

// LeasePeriod -- for messages from a queue, returns the time at which the lease expires.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) LeasePeriod() (string, bool) { return string(msg.header.leasePeriod), msg.header.leasePeriod != nil }

// Matches returns the number of matches as a part of the acknowledgement to a SOW query.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) Matches() (uint, bool) {
	if msg.header.matches != nil { return *msg.header.matches, true }
	return 0, false
}

// MessageLength is sent to indicate the number of bytes used by the message body.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) MessageLength() (uint, bool) {
	if msg.header.messageLength != nil { return *msg.header.messageLength, true }
	return 0, false
}

// Options returns a comma-delimited list of options on a specific command.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) Options() (string, bool) { return string(msg.header.options), msg.header.options != nil }

// OrderBy returns the SOW topic key(s) by which to order SOW query.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) OrderBy() (string, bool) { return string(msg.header.orderBy), msg.header.orderBy != nil }

// QueryID returns the SOW Query identifier set by client to identify a query.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) QueryID() (string, bool) { return string(msg.header.queryID), msg.header.queryID != nil }

// Reason returns the failure message that appears when an acknowledgement returns a status of failure.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) Reason() (string, bool) { return string(msg.header.reason), msg.header.reason != nil }

// RecordsDeleted returns the number of records deleted from the SOW with a sow_delete command. Used in conjunction with 
// the stats acknowledgement message.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) RecordsDeleted() (uint, bool) {
	if msg.header.recordsDeleted != nil { return *msg.header.recordsDeleted, true }
	return 0, false
}

// RecordsInserted returns the number of records inserted into the SOW. Used in conjunction with the stats 
// acknowledgement message.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) RecordsInserted() (uint, bool) {
	if msg.header.recordsInserted != nil { return *msg.header.recordsInserted, true }
	return 0, false
}

// RecordsReturned returns number of records in the store. The value is returned in the acknowledgement to an SOW query.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) RecordsReturned() (uint, bool) {
	if msg.header.recordsReturned != nil { return *msg.header.recordsReturned, true }
	return 0, false
}

// RecordsUpdated returns the number of records updated in the SOW. Used in conjunction with the stats acknowledgement 
// message.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) RecordsUpdated() (uint, bool) {
	if msg.header.recordsUpdated != nil { return *msg.header.recordsUpdated, true }
	return 0, false
}

// SequenceID returns an integer that corresponds to the publish message sequence number. For more information see the 
// Replication section in the User Guide.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) SequenceID() (uint64, bool) {
	if msg.header.sequenceID != nil { return *msg.header.sequenceID, true }
	return 0, false
}

// SowKey returns the sow key value. A SOW key will accompany each message returned in an SOW batch. A SOW key may also 
// be added to messages coming in on a subscription when the published message matches a record in the SOW.
//
// Format:
//
// - string containing the digits of an unsigned long for AMPS-generated SOW keys
//
// OR:
//
// - arbitrary string in the base64 character set for user-provided SOW keys.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) SowKey() (string, bool) { return string(msg.header.sowKey), msg.header.sowKey != nil }

// SowKeys returns the SOW keys as a comma-delimited value.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) SowKeys() (string, bool) { return string(msg.header.sowKeys), msg.header.sowKeys != nil }

// Status returns the client status when client is monitored for heartbeats.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) Status() (string, bool) { return string(msg.header.status), msg.header.status != nil }

// SubID returns the subscription identifier set by server when processing a subscription.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) SubID() (string, bool) { return string(msg.header.subID), msg.header.subID != nil }

// SubIDs returns the list of subscription IDs - a comma-separated list of subscription ids sent from AMPS engine to 
// identify which client subscriptions match a given publish message.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) SubIDs() (string, bool) { return string(msg.header.subIDs), msg.header.subIDs != nil }

// Timestamp returns the message timestamp set by the server in a ISO-8601 date-time format.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) Timestamp() (string, bool) { return string(msg.header.timestamp), msg.header.timestamp != nil }

// TopN returns the number of records to return. 
//
// Note: If TopN is not equally divisible by the BatchSize value, then more records will be returned so that the total 
// number of records is equally divisible by the BatchSize setting.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) TopN() (uint, bool) {
	if msg.header.topN != nil { return *msg.header.topN, true }
	return 0, false
}

// Topic returns the topic value.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) Topic() (string, bool) { return string(msg.header.topic), msg.header.topic != nil }

// TopicMatches returns the number of topic matches for the SOW query. Sent in stats acknowledgement message.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) TopicMatches() (uint, bool) {
	if msg.header.topicMatches != nil { return *msg.header.topicMatches, true }
	return 0, false
}

// UserID returns the user id used to identify the user of a command.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (msg *Message) UserID() (string, bool) { return string(msg.header.userID), msg.header.userID != nil }

