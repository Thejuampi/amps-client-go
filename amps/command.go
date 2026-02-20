package amps

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

type Command struct {
	header *_Header
	data   []byte
}

func (com *Command) reset() {
	if com.header != nil {
		com.header.reset()
	}
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

func (com *Command) AckType() (int, bool) {
	if com.header.ackType != nil {
		return *com.header.ackType, true
	}
	return AckTypeNone, false
}

func (com *Command) BatchSize() (uint, bool) {
	if com.header.batchSize != nil {
		return *com.header.batchSize, true
	}
	return 0, false
}

func (com *Command) Bookmark() (string, bool) {
	return string(com.header.bookmark), com.header.bookmark != nil
}

func (com *Command) Command() (string, bool) {
	return commandIntToString(com.header.command), com.header.command >= 0 && com.header.command < CommandUnknown
}

func (com *Command) CommandID() (string, bool) {
	return string(com.header.commandID), com.header.commandID != nil
}

func (com *Command) CorrelationID() (string, bool) {
	return string(com.header.correlationID), com.header.correlationID != nil
}

func (com *Command) Data() []byte { return com.data }

func (com *Command) Expiration() (uint, bool) {
	if com.header.expiration != nil {
		return *com.header.expiration, true
	}
	return 0, false
}

func (com *Command) Filter() (string, bool) {
	return string(com.header.filter), com.header.filter != nil
}

func (com *Command) Options() (string, bool) {
	return string(com.header.options), com.header.options != nil
}

func (com *Command) OrderBy() (string, bool) {
	return string(com.header.orderBy), com.header.orderBy != nil
}

func (com *Command) QueryID() (string, bool) {
	return string(com.header.queryID), com.header.queryID != nil
}

func (com *Command) SequenceID() (uint64, bool) {
	if com.header.sequenceID != nil {
		return *com.header.sequenceID, true
	}
	return 0, false
}

func (com *Command) SowKey() (string, bool) {
	return string(com.header.sowKey), com.header.sowKey != nil
}

func (com *Command) SowKeys() (string, bool) {
	return string(com.header.sowKeys), com.header.sowKeys != nil
}

func (com *Command) SubID() (string, bool) { return string(com.header.subID), com.header.subID != nil }

func (com *Command) SubIDs() (string, bool) {
	return string(com.header.subIDs), com.header.subIDs != nil
}

func (com *Command) TopN() (uint, bool) {
	if com.header.topN != nil {
		return *com.header.topN, true
	}
	return 0, false
}

func (com *Command) Topic() (string, bool) { return string(com.header.topic), com.header.topic != nil }

func (com *Command) SetAckType(ackType int) *Command {
	if ackType < AckTypeNone ||
		ackType > (AckTypeReceived|AckTypeParsed|AckTypeProcessed|AckTypePersisted|AckTypeCompleted|AckTypeStats) {
		com.header.ackType = nil
	} else {
		com.header.ackType = &ackType
	}
	return com
}

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

func (com *Command) SetBatchSize(batchSize uint) *Command {
	if batchSize == 0 {
		com.header.batchSize = nil
	} else {
		com.header.batchSize = &batchSize
	}
	return com
}

func (com *Command) SetBookmark(bookmark string) *Command {
	if len(bookmark) == 0 {
		com.header.bookmark = nil
	} else {
		com.header.bookmark = []byte(bookmark)
	}
	return com
}

func (com *Command) SetCommand(command string) *Command {
	if len(command) == 0 {
		com.header.command = CommandUnknown
	} else {
		com.header.command = commandStringToInt(command)
	}

	return com
}

func (com *Command) SetCommandID(commandID string) *Command {
	if len(commandID) == 0 {
		com.header.commandID = nil
	} else {
		com.header.commandID = []byte(commandID)
	}
	return com
}

func (com *Command) SetCorrelationID(correlationID string) *Command {
	if len(correlationID) == 0 {
		com.header.correlationID = nil
	} else {
		com.header.correlationID = []byte(correlationID)
	}
	return com
}

func (com *Command) SetData(data []byte) *Command { com.data = data; return com }

func (com *Command) SetExpiration(expiration uint) *Command {
	if expiration == 0 {
		com.header.expiration = nil
	} else {
		com.header.expiration = &expiration
	}
	return com
}

func (com *Command) SetFilter(filter string) *Command {
	if len(filter) == 0 {
		com.header.filter = nil
	} else {
		com.header.filter = []byte(filter)
	}
	return com
}

func (com *Command) SetOptions(options string) *Command {
	if len(options) == 0 {
		com.header.options = nil
	} else {
		com.header.options = []byte(options)
	}
	return com
}

func (com *Command) SetOrderBy(orderBy string) *Command {
	if len(orderBy) == 0 {
		com.header.orderBy = nil
	} else {
		com.header.orderBy = []byte(orderBy)
	}
	return com
}

func (com *Command) SetQueryID(queryID string) *Command {
	if len(queryID) == 0 {
		com.header.queryID = nil
	} else {
		com.header.queryID = []byte(queryID)
	}
	return com
}

func (com *Command) SetSequenceID(sequenceID uint64) *Command {
	if sequenceID == 0 {
		com.header.sequenceID = nil
	} else {
		com.header.sequenceID = &sequenceID
	}
	return com
}

func (com *Command) SetSowKey(sowKey string) *Command {
	if len(sowKey) == 0 {
		com.header.sowKey = nil
	} else {
		com.header.sowKey = []byte(sowKey)
	}
	return com
}

func (com *Command) SetSowKeys(sowKeys string) *Command {
	if len(sowKeys) == 0 {
		com.header.sowKeys = nil
	} else {
		com.header.sowKeys = []byte(sowKeys)
	}
	return com
}

func (com *Command) SetSubID(subID string) *Command {
	if len(subID) == 0 {
		com.header.subID = nil
	} else {
		com.header.subID = []byte(subID)
	}
	return com
}

func (com *Command) SetSubIDs(subIDs string) *Command {
	if len(subIDs) == 0 {
		com.header.subIDs = nil
	} else {
		com.header.subIDs = []byte(subIDs)
	}
	return com
}

func (com *Command) SetTopN(topN uint) *Command {
	if topN == 0 {
		com.header.topN = nil
	} else {
		com.header.topN = &topN
	}
	return com
}

func (com *Command) SetTopic(topic string) *Command {
	if len(topic) == 0 {
		com.header.topic = nil
	} else {
		com.header.topic = []byte(topic)
	}
	return com
}

func NewCommand(commandName string) *Command {
	return &Command{header: &_Header{command: commandStringToInt(commandName)}}
}
