package amps

// Command Types
const (
	CommandAck = iota
	CommandDeltaPublish
	CommandDeltaSubscribe
	CommandFlush
	CommandGroupBegin
	CommandGroupEnd
	commandHeartbeat
	commandLogon
	CommandOOF
	CommandPublish
	CommandSOW
	CommandSOWAndDeltaSubscribe
	CommandSOWAndSubscribe
	CommandSOWDelete
	commandStartTimer
	commandStopTimer
	CommandSubscribe
	CommandUnsubscribe
	CommandUnknown
)


// Command class encapsulates the AMPS Command entity. The Go client sends Command objects to the server and  receives
// Message objects from the server.
type Command struct {
	header *_Header
	data []byte
}



////////////////////////////////////////////////////////////////////////
//                            Internal API                            //
////////////////////////////////////////////////////////////////////////


func (com *Command) reset() {
	if com.header != nil { com.header.reset() }
	com.data = nil
}

func commandStringToInt(command string) int {
	result := CommandUnknown

	switch command {
	case "ack":
		result = CommandAck
	case "delta_publish":
		result = CommandDeltaPublish
	case "delta_subscribe":
		result = CommandDeltaSubscribe
	case "flush":
		result = CommandFlush
	case "group_begin":
		result = CommandGroupBegin
	case "group_end":
		result = CommandGroupEnd
	case "heartbeat":
		result = commandHeartbeat
	case "logon":
		result = commandLogon
	case "oof":
		result = CommandOOF
	case "p":
		fallthrough
	case "publish":
		result = CommandPublish
	case "sow":
		result = CommandSOW
	case "sow_and_delta_subscribe":
		result = CommandSOWAndDeltaSubscribe
	case "sow_and_subscribe":
		result = CommandSOWAndSubscribe
	case "sow_delete":
		result = CommandSOWDelete
	case "start_timer":
		result = commandStartTimer
	case "stop_timer":
		result = commandStopTimer
	case "subscribe":
		result = CommandSubscribe
	case "unsubscribe":
		result = CommandUnsubscribe
	}

	return result
}

func commandIntToString(command int) string {
	var result string

	switch command {
	case CommandAck:
		result = "ack"
	case CommandDeltaPublish:
		result = "delta_publish"
	case CommandDeltaSubscribe:
		result = "delta_subscribe"
	case CommandFlush:
		result = "flush"
	case CommandGroupBegin:
		result = "group_begin"
	case CommandGroupEnd:
		result = "group_end"
	case commandHeartbeat:
		result = "heartbeat"
	case commandLogon:
		result = "logon"
	case CommandOOF:
		result = "oof"
	case CommandPublish:
		result = "p"
	case CommandSOW:
		result = "sow"
	case CommandSOWAndDeltaSubscribe:
		result = "sow_and_delta_subscribe"
	case CommandSOWAndSubscribe:
		result = "sow_and_subscribe"
	case CommandSOWDelete:
		result = "sow_delete"
	case commandStartTimer:
		result = "start_timer"
	case commandStopTimer:
		result = "stop_timer"
	case CommandSubscribe:
		result = "subscribe"
	case CommandUnknown:
		result = ""
	case CommandUnsubscribe:
		result = "unsubscribe"
	}

	return result
}



////////////////////////////////////////////////////////////////////////
//                             Public API                             //
////////////////////////////////////////////////////////////////////////


// Getters

// AckType is the getter for the acknowledgement message type(s) requested by the command. Acknowledgement types can be
// detected using the bitwise AND and OR operators:
//
//     if ackType, _ := command.AckType(); (ackType & amps.AckTypeCompleted) > 0 { ... }
//
//     // or, to check if there are multiple acks set, the bitwise OR can be used as well:
//     if ackType, _ := command.AckType(); (ackType & (amps.AckTypeProcessed | amps.AckTypeCompleted)) > 0 { ... }
//
// The second returned parameter is set to true if the value exists, false otherwise.
func (com *Command) AckType() (int, bool) {
	if com.header.ackType != nil { return *com.header.ackType, true }
	return AckTypeNone, false
}

// BatchSize returns the batch size value -- the number of messages that are batched together when returning a query
// result.
//
// The second returned parameter is set to true if the value exists, false otherwise.
func (com *Command) BatchSize() (uint, bool) {
	if com.header.batchSize != nil { return *com.header.batchSize, true }
	return 0, false
}

// Bookmark returns the bookmark value -- the client-originated identifier used to mark a location in journaled messages.
//
// The second returned parameter is set to true if the value exists, false otherwise.
func (com *Command) Bookmark() (string, bool) { return string(com.header.bookmark), com.header.bookmark != nil }

// Command returns the command type e.g. 'publish', 'sow_and_subscribe', to be executed.
//
// The second returned parameter is set to true if the value exists, false otherwise.
func (com *Command) Command() (string, bool) {
	return commandIntToString(com.header.command), com.header.command >= 0 && com.header.command < CommandUnknown
}

// CommandID returns the Client-specified command id. The command id is returned by the engine in responses to commands 
// to allow the client to correlate the response to the command.
//
// The second returned parameter is set to true if the value exists, false otherwise.
func (com *Command) CommandID() (string, bool) { return string(com.header.commandID), com.header.commandID != nil }

// CorrelationID returns the correlation id value, if any.
//
// The second returned parameter is set to true if the value exists, false otherwise.
func (com *Command) CorrelationID() (string, bool) {
	return string(com.header.correlationID), com.header.correlationID != nil
}

// Data returns the Command data (message payload). Only publish, delta_publish, and sow_delete commands can have a 
// payload.
func (com *Command) Data() []byte { return com.data }

// Expiration returns the SOW expiration time (in seconds) if used in publish.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (com *Command) Expiration() (uint, bool) {
	if com.header.expiration != nil { return *com.header.expiration, true }
	return 0, false
}

// Filter returns the content filter expression.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (com *Command) Filter() (string, bool) { return string(com.header.filter), com.header.filter != nil }

// Options returns the (comma-delimited) string of options on a specific Command.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (com *Command) Options() (string, bool) { return string(com.header.options), com.header.options != nil }

// OrderBy returns the SOW topic key(s) by which to order SOW query.
//
// The second returned parameter is set to true if the value exists, false otherwise.
func (com *Command) OrderBy() (string, bool) { return string(com.header.orderBy), com.header.orderBy != nil }

// QueryID returns the SOW Query identifier set by client to identify a query.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (com *Command) QueryID() (string, bool) { return string(com.header.queryID), com.header.queryID != nil }

// SequenceID returns the sequence id value. Sequence id is an integer that corresponds to the publish message sequence 
// number. For more information see the Replication section in the User Guide.
//
// The second returned parameter is set to true if the value exists, false otherwise.
func (com *Command) SequenceID() (uint64, bool) {
	if com.header.sequenceID != nil { return *com.header.sequenceID, true }
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
func (com *Command) SowKey() (string, bool) { return string(com.header.sowKey), com.header.sowKey != nil }

// SowKeys returns the SOW keys as a comma-delimited value.
//
// The second returned parameter is set to true if the value exists, false otherwise.
func (com *Command) SowKeys() (string, bool) { return string(com.header.sowKeys), com.header.sowKeys != nil }

// SubID returns the subscription identifier set by server when processing a subscription.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (com *Command) SubID() (string, bool) { return string(com.header.subID), com.header.subID != nil }

// SubIDs returns the list of subscription IDs - a comma-separated list of subscription ids sent from AMPS engine to 
// identify which client subscriptions match a given publish message.
//
// The second returned parameter is set to true if the value exists, false otherwise.
func (com *Command) SubIDs() (string, bool) { return string(com.header.subIDs), com.header.subIDs != nil }

// TopN returns the number of records to return. 
//
// Note: If TopN is not equally divisible by the BatchSize value, then more records will be returned so that the total 
// number of records is equally divisible by the BatchSize setting.
//
// The second returned parameter is set to true if the value exists, false otherwise.
func (com *Command) TopN() (uint, bool) {
	if com.header.topN != nil { return *com.header.topN, true }
	return 0, false
}

// Topic returns the topic value.
// 
// The second returned parameter is set to true if the value exists, false otherwise.
func (com *Command) Topic() (string, bool) { return string(com.header.topic), com.header.topic != nil }


// Setters

// SetAckType sets the acknowledgements requested for the command. Multiple ack types can be combined using bitwise OR:
//    command.SetAckType(AckTypeProcessed | AckTypeCompleted)
// The acknowledgement messages will be delivered to the message handler / message stream.
//
// Returns the Command object, allowing combining several setters into a chain.
func (com *Command) SetAckType(ackType int) *Command {
	if ackType < AckTypeNone ||
	ackType > (AckTypeReceived | AckTypeParsed | AckTypeProcessed | AckTypePersisted | AckTypeCompleted | AckTypeStats) {
		com.header.ackType = nil
	} else {
		com.header.ackType = &ackType
	}
	return com
}


// AddAckType adds the ack(s) to the list of the acknowledgements requested for the command. Multiple ack types can be
// combined using bitwise OR:
//    command.AddAckType(AckTypeProcessed | AckTypeCompleted)
// The acknowledgement messages will be delivered to the message handler / message stream.
//
// Returns the Command object, allowing combining several setters into a chain.
func (com *Command) AddAckType(ackType int) *Command {
	if ackType > AckTypeNone && ackType <= AckTypeStats {
		if com.header.ackType == nil {
			com.header.ackType = &ackType
		} else {
			*com.header.ackType |= ackType
		}
	}

	return com
}

// SetBatchSize sets the number of messages that are batched together when returning a SOW query result. If it's not set,
// it is 10 by default.
//
// Returns the Command object, allowing combining several setters into a chain.
func (com *Command) SetBatchSize(batchSize uint) *Command {
	if batchSize == 0 { com.header.batchSize = nil } else { com.header.batchSize = &batchSize }
	return com
}

// SetBookmark sets the client-originated identifier used to mark a location in journaled messages.
//
// Returns the Command object, allowing combining several setters into a chain.
func (com *Command) SetBookmark(bookmark string) *Command {
	if len(bookmark) == 0 { com.header.bookmark = nil } else { com.header.bookmark = []byte(bookmark) }
	return com
}

// SetCommand sets the command type e.g. 'publish', 'sow_and_subscribe', to be executed. The following commands are
// available:
//
// - publish
//
// - delta_publish
//
// - subscribe
//
// - delta_subscribe
//
// - sow
//
// - sow_and_subscribe
//
// - sow_and_delta_subscribe
//
// - sow_delete
//
// - unsubscribe
//
// Returns the Command object, allowing combining several setters into a chain.
func (com *Command) SetCommand(command string) *Command {
	if len(command) == 0 {
		com.header.command = CommandUnknown
	} else {
		com.header.command = commandStringToInt(command)
	}

	return com
}

// SetCommandID sets the Client-specified command id. The command id is returned by the engine in responses to commands 
// to allow the client to correlate the response to the command.
//
// Returns the Command object, allowing combining several setters into a chain.
func (com *Command) SetCommandID(commandID string) *Command {
	if len(commandID) == 0 { com.header.commandID = nil } else { com.header.commandID = []byte(commandID) }
	return com
}

// SetCorrelationID sets the opaque token set by an application and returned with the message.
//
// Returns the Command object, allowing combining several setters into a chain.
func (com *Command) SetCorrelationID(correlationID string) *Command {
	if len(correlationID) == 0 {
		com.header.correlationID = nil
	} else {
		com.header.correlationID = []byte(correlationID)
	}
	return com
}

// SetData sets the command data (message payload). Makes sense only for publish, delta_publish, and sow_delete commands.
//
// Returns the Command object, allowing combining several setters into a chain.
func (com *Command) SetData(data []byte) *Command { com.data = data; return com }

// SetExpiration sets the SOW expiration time (in seconds) if used in publish.
//
// Returns the Command object, allowing combining several setters into a chain.
func (com *Command) SetExpiration(expiration uint) *Command {
	if expiration == 0 { com.header.expiration = nil } else { com.header.expiration = &expiration }
	return com
}

// SetFilter sets the content filter expression.
//
// Returns the Command object, allowing combining several setters into a chain.
func (com *Command) SetFilter(filter string) *Command {
	if len(filter) == 0 { com.header.filter = nil } else { com.header.filter = []byte(filter) }
	return com
}

// SetOptions sets the (comma-delimited) string of options on a specific Command.
//
// Returns the Command object, allowing combining several setters into a chain.
func (com *Command) SetOptions(options string) *Command {
	if len(options) == 0 { com.header.options = nil } else { com.header.options = []byte(options) }
	return com
}

// SetOrderBy sets the SOW topic key(s) by which to order SOW query.
//
// Returns the Command object, allowing combining several setters into a chain.
func (com *Command) SetOrderBy(orderBy string) *Command {
	if len(orderBy) == 0 { com.header.orderBy = nil } else { com.header.orderBy = []byte(orderBy) }
	return com
}

// SetQueryID sets the SOW Query identifier set by client to identify a query.
//
// Returns the Command object, allowing combining several setters into a chain.
func (com *Command) SetQueryID(queryID string) *Command {
	if len(queryID) == 0 { com.header.queryID = nil } else { com.header.queryID = []byte(queryID) }
	return com
}

// SetSequenceID sets the sequence id value. Sequence id is an integer that corresponds to the publish message sequence 
// number. For more information see the Replication section in the User Guide.
//
// Returns the Command object, allowing combining several setters into a chain.
func (com *Command) SetSequenceID(sequenceID uint64) *Command {
	if sequenceID == 0 { com.header.sequenceID = nil } else { com.header.sequenceID = &sequenceID }
	return com
}

// SetSowKey sets the sow key value. A SOW key will accompany each message returned in an SOW batch. A SOW key may also 
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
// Returns the Command object, allowing combining several setters into a chain.
func (com *Command) SetSowKey(sowKey string) *Command {
	if len(sowKey) == 0 { com.header.sowKey = nil } else { com.header.sowKey = []byte(sowKey) }
	return com
}

// SetSowKeys sets the SOW keys as a comma-delimited value.
//
// Returns the Command object, allowing combining several setters into a chain.
func (com *Command) SetSowKeys(sowKeys string) *Command {
	if len(sowKeys) == 0 { com.header.sowKeys = nil } else { com.header.sowKeys = []byte(sowKeys) }
	return com
}

// SetSubID sets the subscription identifier set by server when processing a subscription.
//
// Returns the Command object, allowing combining several setters into a chain.
func (com *Command) SetSubID(subID string) *Command {
	if len(subID) == 0 { com.header.subID = nil } else { com.header.subID = []byte(subID) }
	return com
}

// SetSubIDs sets the list of subscription IDs - a comma-separated list of subscription ids sent from AMPS engine to 
// identify which client subscriptions match a given publish message.
//
// Returns the Command object, allowing combining several setters into a chain.
func (com *Command) SetSubIDs(subIDs string) *Command {
	if len(subIDs) == 0 { com.header.subIDs = nil } else { com.header.subIDs = []byte(subIDs) }
	return com
}

// SetTopN sets the number of records to return. 
//
// Note: If TopN is not equally divisible by the BatchSize value, then more records will be returned so that the total 
// number of records is equally divisible by the BatchSize setting.
//
// Returns the Command object, allowing combining several setters into a chain.
// Deprecated: Provide the "top_n=<value>" value as a part of the `options` field instead.
func (com *Command) SetTopN(topN uint) *Command {
	if topN == 0 { com.header.topN = nil } else { com.header.topN = &topN }
	return com
}

// SetTopic sets the topic value.
//
// Returns the Command object, allowing combining several setters into a chain.
func (com *Command) SetTopic(topic string) *Command {
	if len(topic) == 0 { com.header.topic = nil } else { com.header.topic = []byte(topic) }
	return com
}


// NewCommand creates a new Command object and returns it.
//
// Arguments:
//
// commandName [string] - a type of the command to be created. It can be on of the following:
//
// - publish
//
// - delta_publish
//
// - subscribe
//
// - delta_subscribe
//
// - sow
//
// - sow_and_subscribe
//
// - sow_and_delta_subscribe
//
// - sow_delete
//
// - unsubscribe
func NewCommand(commandName string) *Command {
	return &Command{header: &_Header{command: commandStringToInt(commandName)}}
}
