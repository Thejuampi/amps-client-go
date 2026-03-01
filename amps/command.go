package amps

// CommandAck and related constants define protocol and client behavior values.
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

// Command stores exported state used by AMPS client APIs.
type Command struct {
	header  *_Header
	data    []byte
	timeout int
}

func (com *Command) reset() {
	if com.header != nil {
		com.header.reset()
	}
	com.data = nil
	com.timeout = 0
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

func commandBytesToInt(command []byte) int {
	switch len(command) {
	case 0:
		return CommandUnknown
	case 1:
		if command[0] == 'p' {
			return CommandPublish
		}
	case 3:
		if bytesEqualString(command, "ack") {
			return CommandAck
		}
		if bytesEqualString(command, "oof") {
			return CommandOOF
		}
		if bytesEqualString(command, "sow") {
			return CommandSOW
		}
	case 5:
		if bytesEqualString(command, "flush") {
			return CommandFlush
		}
		if bytesEqualString(command, "logon") {
			return commandLogon
		}
	case 7:
		if bytesEqualString(command, "publish") {
			return CommandPublish
		}
	case 9:
		if bytesEqualString(command, "heartbeat") {
			return commandHeartbeat
		}
		if bytesEqualString(command, "subscribe") {
			return CommandSubscribe
		}
		if bytesEqualString(command, "group_end") {
			return CommandGroupEnd
		}
	case 10:
		if bytesEqualString(command, "sow_delete") {
			return CommandSOWDelete
		}
		if bytesEqualString(command, "stop_timer") {
			return commandStopTimer
		}
	case 11:
		if bytesEqualString(command, "group_begin") {
			return CommandGroupBegin
		}
		if bytesEqualString(command, "start_timer") {
			return commandStartTimer
		}
		if bytesEqualString(command, "unsubscribe") {
			return CommandUnsubscribe
		}
	case 13:
		if bytesEqualString(command, "delta_publish") {
			return CommandDeltaPublish
		}
	case 15:
		if bytesEqualString(command, "delta_subscribe") {
			return CommandDeltaSubscribe
		}
	case 17:
		if bytesEqualString(command, "sow_and_subscribe") {
			return CommandSOWAndSubscribe
		}
	case 23:
		if bytesEqualString(command, "sow_and_delta_subscribe") {
			return CommandSOWAndDeltaSubscribe
		}
	}
	return commandStringToInt(string(command))
}

func bytesEqualString(value []byte, literal string) bool {
	if len(value) != len(literal) {
		return false
	}
	for index := 0; index < len(literal); index++ {
		if value[index] != literal[index] {
			return false
		}
	}
	return true
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

// AckType executes the exported acktype operation.
func (com *Command) AckType() (int, bool) {
	if com.header.ackType != nil {
		return *com.header.ackType, true
	}
	return AckTypeNone, false
}

// GetAckType returns the configured ack type bitset.
func (com *Command) GetAckType() int {
	value, _ := com.AckType()
	return value
}

// GetAckTypeEnum returns the configured ack type bitset.
func (com *Command) GetAckTypeEnum() int {
	return com.GetAckType()
}

// BatchSize executes the exported batchsize operation.
func (com *Command) BatchSize() (uint, bool) {
	if com.header.batchSize != nil {
		return *com.header.batchSize, true
	}
	return 0, false
}

// Bookmark executes the exported bookmark operation.
func (com *Command) Bookmark() (string, bool) {
	return string(com.header.bookmark), com.header.bookmark != nil
}

// Command executes the exported command operation.
func (com *Command) Command() (string, bool) {
	return commandIntToString(com.header.command), com.header.command >= 0 && com.header.command < CommandUnknown
}

// GetCommandEnum returns the command enum value.
func (com *Command) GetCommandEnum() int {
	if com == nil || com.header == nil {
		return CommandUnknown
	}
	return com.header.command
}

// CommandID executes the exported commandid operation.
func (com *Command) CommandID() (string, bool) {
	return string(com.header.commandID), com.header.commandID != nil
}

// CorrelationID executes the exported correlationid operation.
func (com *Command) CorrelationID() (string, bool) {
	return string(com.header.correlationID), com.header.correlationID != nil
}

// Data executes the exported data operation.
func (com *Command) Data() []byte { return com.data }

// Expiration executes the exported expiration operation.
func (com *Command) Expiration() (uint, bool) {
	if com.header.expiration != nil {
		return *com.header.expiration, true
	}
	return 0, false
}

// Filter executes the exported filter operation.
func (com *Command) Filter() (string, bool) {
	return string(com.header.filter), com.header.filter != nil
}

// Options executes the exported options operation.
func (com *Command) Options() (string, bool) {
	return string(com.header.options), com.header.options != nil
}

// OrderBy executes the exported orderby operation.
func (com *Command) OrderBy() (string, bool) {
	return string(com.header.orderBy), com.header.orderBy != nil
}

// QueryID executes the exported queryid operation.
func (com *Command) QueryID() (string, bool) {
	return string(com.header.queryID), com.header.queryID != nil
}

// SequenceID executes the exported sequenceid operation.
func (com *Command) SequenceID() (uint64, bool) {
	if com.header.sequenceID != nil {
		return *com.header.sequenceID, true
	}
	return 0, false
}

// SowKey executes the exported sowkey operation.
func (com *Command) SowKey() (string, bool) {
	return string(com.header.sowKey), com.header.sowKey != nil
}

// SowKeys executes the exported sowkeys operation.
func (com *Command) SowKeys() (string, bool) {
	return string(com.header.sowKeys), com.header.sowKeys != nil
}

// SubID executes the exported subid operation.
func (com *Command) SubID() (string, bool) { return string(com.header.subID), com.header.subID != nil }

// SubIDs executes the exported subids operation.
func (com *Command) SubIDs() (string, bool) {
	return string(com.header.subIDs), com.header.subIDs != nil
}

// TopN executes the exported topn operation.
func (com *Command) TopN() (uint, bool) {
	if com.header.topN != nil {
		return *com.header.topN, true
	}
	return 0, false
}

// Topic executes the exported topic operation.
func (com *Command) Topic() (string, bool) { return string(com.header.topic), com.header.topic != nil }

// GetMessage returns a message representation of this command.
func (com *Command) GetMessage() *Message {
	return commandToMessage(com)
}

// GetSequence returns the publish sequence id for this command.
func (com *Command) GetSequence() uint64 {
	sequence, _ := com.SequenceID()
	return sequence
}

// SetSequence sets the publish sequence id for this command.
func (com *Command) SetSequence(sequence uint64) *Command {
	return com.SetSequenceID(sequence)
}

// GetTimeout returns the command timeout value in milliseconds.
func (com *Command) GetTimeout() int {
	return com.timeout
}

// SetTimeout sets the command timeout in milliseconds.
func (com *Command) SetTimeout(timeout int) *Command {
	if timeout < 0 {
		timeout = 0
	}
	com.timeout = timeout
	return com
}

// HasProcessedAck reports whether processed ack is requested.
func (com *Command) HasProcessedAck() bool {
	ackType, hasAckType := com.AckType()
	return hasAckType && (ackType&AckTypeProcessed) != 0
}

// HasStatsAck reports whether stats ack is requested.
func (com *Command) HasStatsAck() bool {
	ackType, hasAckType := com.AckType()
	return hasAckType && (ackType&AckTypeStats) != 0
}

// IsSow reports whether command is a SOW family command.
func (com *Command) IsSow() bool {
	switch com.header.command {
	case CommandSOW, CommandSOWAndSubscribe, CommandSOWAndDeltaSubscribe:
		return true
	default:
		return false
	}
}

// IsSubscribe reports whether command is a subscribe family command.
func (com *Command) IsSubscribe() bool {
	switch com.header.command {
	case CommandSubscribe, CommandDeltaSubscribe, CommandSOWAndSubscribe, CommandSOWAndDeltaSubscribe:
		return true
	default:
		return false
	}
}

// NeedsSequenceNumber reports whether command participates in publish sequence tracking.
func (com *Command) NeedsSequenceNumber() bool {
	switch com.header.command {
	case CommandPublish, CommandDeltaPublish, CommandSOWDelete:
		return true
	default:
		return false
	}
}

// Init reinitializes this command with the provided command name.
func (com *Command) Init(commandName string) *Command {
	com.reset()
	return com.SetCommand(commandName)
}

// Reset clears command header and payload state.
func (com *Command) Reset() *Command {
	com.reset()
	return com
}

// SetIds updates command, query, and subscription identifiers.
func (com *Command) SetIds(commandID string, queryID string, subID string) *Command {
	com.SetCommandID(commandID)
	com.SetQueryID(queryID)
	com.SetSubID(subID)
	return com
}

// SetAckType sets ack type on the receiver.
func (com *Command) SetAckType(ackType int) *Command {
	if ackType < AckTypeNone ||
		ackType > (AckTypeReceived|AckTypeParsed|AckTypeProcessed|AckTypePersisted|AckTypeCompleted|AckTypeStats) {
		com.header.ackType = nil
	} else {
		com.header.ackType = &ackType
	}
	return com
}

// AddAckType adds ack type behavior on the receiver.
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

// SetBatchSize sets batch size on the receiver.
func (com *Command) SetBatchSize(batchSize uint) *Command {
	if batchSize == 0 {
		com.header.batchSize = nil
	} else {
		com.header.batchSize = &batchSize
	}
	return com
}

func assignStringBytes(destination *[]byte, value string) {
	if len(value) == 0 {
		*destination = nil
		return
	}
	*destination = append((*destination)[:0], value...)
}

// SetBookmark sets bookmark on the receiver.
func (com *Command) SetBookmark(bookmark string) *Command {
	assignStringBytes(&com.header.bookmark, bookmark)
	return com
}

// SetCommand sets command on the receiver.
func (com *Command) SetCommand(command string) *Command {
	if len(command) == 0 {
		com.header.command = CommandUnknown
	} else {
		com.header.command = commandStringToInt(command)
	}

	return com
}

// SetCommandEnum sets command enum on the receiver.
func (com *Command) SetCommandEnum(command int) *Command {
	if com == nil || com.header == nil {
		return com
	}
	com.header.command = command
	return com
}

// SetCommandID sets command id on the receiver.
func (com *Command) SetCommandID(commandID string) *Command {
	assignStringBytes(&com.header.commandID, commandID)
	return com
}

// SetCorrelationID sets correlation id on the receiver.
func (com *Command) SetCorrelationID(correlationID string) *Command {
	assignStringBytes(&com.header.correlationID, correlationID)
	return com
}

// SetData sets data on the receiver.
func (com *Command) SetData(data []byte) *Command { com.data = data; return com }

// SetExpiration sets expiration on the receiver.
func (com *Command) SetExpiration(expiration uint) *Command {
	if expiration == 0 {
		com.header.expiration = nil
	} else {
		com.header.expiration = &expiration
	}
	return com
}

// SetFilter sets filter on the receiver.
func (com *Command) SetFilter(filter string) *Command {
	assignStringBytes(&com.header.filter, filter)
	return com
}

// SetOptions sets options on the receiver.
func (com *Command) SetOptions(options string) *Command {
	assignStringBytes(&com.header.options, options)
	return com
}

// SetOrderBy sets order by on the receiver.
func (com *Command) SetOrderBy(orderBy string) *Command {
	assignStringBytes(&com.header.orderBy, orderBy)
	return com
}

// SetQueryID sets query id on the receiver.
func (com *Command) SetQueryID(queryID string) *Command {
	assignStringBytes(&com.header.queryID, queryID)
	return com
}

// SetSequenceID sets sequence id on the receiver.
func (com *Command) SetSequenceID(sequenceID uint64) *Command {
	if sequenceID == 0 {
		com.header.sequenceID = nil
	} else {
		com.header.sequenceID = &sequenceID
	}
	return com
}

// SetSowKey sets sow key on the receiver.
func (com *Command) SetSowKey(sowKey string) *Command {
	assignStringBytes(&com.header.sowKey, sowKey)
	return com
}

// SetSowKeys sets sow keys on the receiver.
func (com *Command) SetSowKeys(sowKeys string) *Command {
	assignStringBytes(&com.header.sowKeys, sowKeys)
	return com
}

// SetSubID sets sub id on the receiver.
func (com *Command) SetSubID(subID string) *Command {
	assignStringBytes(&com.header.subID, subID)
	return com
}

// SetSubIDs sets sub ids on the receiver.
func (com *Command) SetSubIDs(subIDs string) *Command {
	assignStringBytes(&com.header.subIDs, subIDs)
	return com
}

// SetTopN sets top n on the receiver.
func (com *Command) SetTopN(topN uint) *Command {
	if topN == 0 {
		com.header.topN = nil
	} else {
		com.header.topN = &topN
	}
	return com
}

// SetTopic sets topic on the receiver.
func (com *Command) SetTopic(topic string) *Command {
	assignStringBytes(&com.header.topic, topic)
	return com
}

// NewCommand returns a new Command.
func NewCommand(commandName string) *Command {
	return &Command{header: &_Header{command: commandStringToInt(commandName)}}
}
