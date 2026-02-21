package amps

import "encoding/json"

type commandSnapshot struct {
	Command       string  `json:"command"`
	AckType       *int    `json:"ack_type,omitempty"`
	BatchSize     *uint   `json:"batch_size,omitempty"`
	Bookmark      string  `json:"bookmark,omitempty"`
	CommandID     string  `json:"command_id,omitempty"`
	CorrelationID string  `json:"correlation_id,omitempty"`
	Expiration    *uint   `json:"expiration,omitempty"`
	Filter        string  `json:"filter,omitempty"`
	Options       string  `json:"options,omitempty"`
	OrderBy       string  `json:"order_by,omitempty"`
	QueryID       string  `json:"query_id,omitempty"`
	SequenceID    *uint64 `json:"sequence_id,omitempty"`
	SowKey        string  `json:"sow_key,omitempty"`
	SowKeys       string  `json:"sow_keys,omitempty"`
	SubID         string  `json:"sub_id,omitempty"`
	SubIDs        string  `json:"sub_ids,omitempty"`
	Topic         string  `json:"topic,omitempty"`
	TopN          *uint   `json:"top_n,omitempty"`
	Timeout       int     `json:"timeout,omitempty"`
	Data          []byte  `json:"data,omitempty"`
}

func snapshotFromCommand(command *Command) commandSnapshot {
	if command == nil {
		return commandSnapshot{}
	}

	snapshot := commandSnapshot{Data: nil}
	if command.header != nil {
		snapshot.Command = commandIntToString(command.header.command)
		if command.header.ackType != nil {
			value := *command.header.ackType
			snapshot.AckType = &value
		}
		if command.header.batchSize != nil {
			value := *command.header.batchSize
			snapshot.BatchSize = &value
		}
		if command.header.bookmark != nil {
			snapshot.Bookmark = string(command.header.bookmark)
		}
		if command.header.commandID != nil {
			snapshot.CommandID = string(command.header.commandID)
		}
		if command.header.correlationID != nil {
			snapshot.CorrelationID = string(command.header.correlationID)
		}
		if command.header.expiration != nil {
			value := *command.header.expiration
			snapshot.Expiration = &value
		}
		if command.header.filter != nil {
			snapshot.Filter = string(command.header.filter)
		}
		if command.header.options != nil {
			snapshot.Options = string(command.header.options)
		}
		if command.header.orderBy != nil {
			snapshot.OrderBy = string(command.header.orderBy)
		}
		if command.header.queryID != nil {
			snapshot.QueryID = string(command.header.queryID)
		}
		if command.header.sequenceID != nil {
			value := *command.header.sequenceID
			snapshot.SequenceID = &value
		}
		if command.header.sowKey != nil {
			snapshot.SowKey = string(command.header.sowKey)
		}
		if command.header.sowKeys != nil {
			snapshot.SowKeys = string(command.header.sowKeys)
		}
		if command.header.subID != nil {
			snapshot.SubID = string(command.header.subID)
		}
		if command.header.subIDs != nil {
			snapshot.SubIDs = string(command.header.subIDs)
		}
		if command.header.topic != nil {
			snapshot.Topic = string(command.header.topic)
		}
		if command.header.topN != nil {
			value := *command.header.topN
			snapshot.TopN = &value
		}
	}

	if command.data != nil {
		snapshot.Data = make([]byte, len(command.data))
		copy(snapshot.Data, command.data)
	}
	snapshot.Timeout = command.timeout

	return snapshot
}

func commandFromSnapshot(snapshot commandSnapshot) *Command {
	command := NewCommand(snapshot.Command)
	if command.header == nil {
		command.header = new(_Header)
	}

	if snapshot.AckType != nil {
		value := *snapshot.AckType
		command.header.ackType = &value
	}
	if snapshot.BatchSize != nil {
		value := *snapshot.BatchSize
		command.header.batchSize = &value
	}
	if snapshot.Bookmark != "" {
		command.header.bookmark = []byte(snapshot.Bookmark)
	}
	if snapshot.CommandID != "" {
		command.header.commandID = []byte(snapshot.CommandID)
	}
	if snapshot.CorrelationID != "" {
		command.header.correlationID = []byte(snapshot.CorrelationID)
	}
	if snapshot.Expiration != nil {
		value := *snapshot.Expiration
		command.header.expiration = &value
	}
	if snapshot.Filter != "" {
		command.header.filter = []byte(snapshot.Filter)
	}
	if snapshot.Options != "" {
		command.header.options = []byte(snapshot.Options)
	}
	if snapshot.OrderBy != "" {
		command.header.orderBy = []byte(snapshot.OrderBy)
	}
	if snapshot.QueryID != "" {
		command.header.queryID = []byte(snapshot.QueryID)
	}
	if snapshot.SequenceID != nil {
		value := *snapshot.SequenceID
		command.header.sequenceID = &value
	}
	if snapshot.SowKey != "" {
		command.header.sowKey = []byte(snapshot.SowKey)
	}
	if snapshot.SowKeys != "" {
		command.header.sowKeys = []byte(snapshot.SowKeys)
	}
	if snapshot.SubID != "" {
		command.header.subID = []byte(snapshot.SubID)
	}
	if snapshot.SubIDs != "" {
		command.header.subIDs = []byte(snapshot.SubIDs)
	}
	if snapshot.Topic != "" {
		command.header.topic = []byte(snapshot.Topic)
	}
	if snapshot.TopN != nil {
		value := *snapshot.TopN
		command.header.topN = &value
	}
	if snapshot.Data != nil {
		command.data = make([]byte, len(snapshot.Data))
		copy(command.data, snapshot.Data)
	}
	command.timeout = snapshot.Timeout

	return command
}

func cloneCommand(command *Command) *Command {
	return commandFromSnapshot(snapshotFromCommand(command))
}

func marshalCommandSnapshot(command *Command) ([]byte, error) {
	snapshot := snapshotFromCommand(command)
	return json.Marshal(snapshot)
}

func unmarshalCommandSnapshot(data []byte) (*Command, error) {
	snapshot := commandSnapshot{}
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, err
	}
	return commandFromSnapshot(snapshot), nil
}
