package amps

import (
	"strconv"
	"strings"
)

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

func commandHeader(com *Command) *_Header {
	if com == nil {
		return nil
	}
	return com.header
}

func ensureCommandHeader(com *Command) *_Header {
	if com == nil {
		return nil
	}
	if com.header == nil {
		com.header = newHeader()
	}
	return com.header
}

func (com *Command) reset() {
	if com == nil {
		return
	}
	var header = ensureCommandHeader(com)
	if header != nil {
		header.reset()
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
	var header = commandHeader(com)
	if header != nil && header.ackType != nil {
		return *header.ackType, true
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
	var header = commandHeader(com)
	if header != nil && header.batchSize != nil {
		return *header.batchSize, true
	}
	return 0, false
}

// Bookmark executes the exported bookmark operation.
func (com *Command) Bookmark() (string, bool) {
	var header = commandHeader(com)
	if header == nil {
		return "", false
	}
	return string(header.bookmark), header.bookmark != nil
}

// Command executes the exported command operation.
func (com *Command) Command() (string, bool) {
	var header = commandHeader(com)
	if header == nil {
		return "", false
	}
	return commandIntToString(header.command), header.command >= 0 && header.command < CommandUnknown
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
	var header = commandHeader(com)
	if header == nil {
		return "", false
	}
	return string(header.commandID), header.commandID != nil
}

// CorrelationID executes the exported correlationid operation.
func (com *Command) CorrelationID() (string, bool) {
	var header = commandHeader(com)
	if header == nil {
		return "", false
	}
	return string(header.correlationID), header.correlationID != nil
}

// Data executes the exported data operation.
func (com *Command) Data() []byte {
	if com == nil {
		return nil
	}
	return com.data
}

// Expiration executes the exported expiration operation.
func (com *Command) Expiration() (uint, bool) {
	var header = commandHeader(com)
	if header != nil && header.expiration != nil {
		return *header.expiration, true
	}
	return 0, false
}

// Filter executes the exported filter operation.
func (com *Command) Filter() (string, bool) {
	var header = commandHeader(com)
	if header == nil {
		return "", false
	}
	return string(header.filter), header.filter != nil
}

// Options executes the exported options operation.
func (com *Command) Options() (string, bool) {
	var header = commandHeader(com)
	if header == nil {
		return "", false
	}
	return string(header.options), header.options != nil
}

// OrderBy executes the exported orderby operation.
func (com *Command) OrderBy() (string, bool) {
	var header = commandHeader(com)
	if header == nil {
		return "", false
	}
	return string(header.orderBy), header.orderBy != nil
}

// QueryID executes the exported queryid operation.
func (com *Command) QueryID() (string, bool) {
	var header = commandHeader(com)
	if header == nil {
		return "", false
	}
	return string(header.queryID), header.queryID != nil
}

// SequenceID executes the exported sequenceid operation.
func (com *Command) SequenceID() (uint64, bool) {
	var header = commandHeader(com)
	if header != nil && header.sequenceID != nil {
		return *header.sequenceID, true
	}
	return 0, false
}

// SowKey executes the exported sowkey operation.
func (com *Command) SowKey() (string, bool) {
	var header = commandHeader(com)
	if header == nil {
		return "", false
	}
	return string(header.sowKey), header.sowKey != nil
}

// SowKeys executes the exported sowkeys operation.
func (com *Command) SowKeys() (string, bool) {
	var header = commandHeader(com)
	if header == nil {
		return "", false
	}
	return string(header.sowKeys), header.sowKeys != nil
}

// SubID executes the exported subid operation.
func (com *Command) SubID() (string, bool) {
	var header = commandHeader(com)
	if header == nil {
		return "", false
	}
	return string(header.subID), header.subID != nil
}

// SubIDs executes the exported subids operation.
func (com *Command) SubIDs() (string, bool) {
	var header = commandHeader(com)
	if header == nil {
		return "", false
	}
	return string(header.subIDs), header.subIDs != nil
}

// TopN executes the exported topn operation.
func (com *Command) TopN() (uint, bool) {
	var header = commandHeader(com)
	if header != nil && header.topN != nil {
		return *header.topN, true
	}
	return 0, false
}

// Topic executes the exported topic operation.
func (com *Command) Topic() (string, bool) {
	var header = commandHeader(com)
	if header == nil {
		return "", false
	}
	return string(header.topic), header.topic != nil
}

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
	if com == nil {
		return 0
	}
	return com.timeout
}

// SetTimeout sets the command timeout in milliseconds.
func (com *Command) SetTimeout(timeout int) *Command {
	if com == nil {
		return nil
	}
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
	switch com.GetCommandEnum() {
	case CommandSOW, CommandSOWAndSubscribe, CommandSOWAndDeltaSubscribe:
		return true
	default:
		return false
	}
}

// IsSubscribe reports whether command is a subscribe family command.
func (com *Command) IsSubscribe() bool {
	switch com.GetCommandEnum() {
	case CommandSubscribe, CommandDeltaSubscribe, CommandSOWAndSubscribe, CommandSOWAndDeltaSubscribe:
		return true
	default:
		return false
	}
}

// NeedsSequenceNumber reports whether command participates in publish sequence tracking.
func (com *Command) NeedsSequenceNumber() bool {
	switch com.GetCommandEnum() {
	case CommandPublish, CommandDeltaPublish, CommandSOWDelete:
		return true
	default:
		return false
	}
}

// Init reinitializes this command with the provided command name.
func (com *Command) Init(commandName string) *Command {
	if com == nil {
		return nil
	}
	com.reset()
	return com.SetCommand(commandName)
}

// Reset clears command header and payload state.
func (com *Command) Reset() *Command {
	if com == nil {
		return nil
	}
	com.reset()
	return com
}

// SetIds updates command, query, and subscription identifiers.
func (com *Command) SetIds(commandID string, queryID string, subID string) *Command {
	if com == nil {
		return nil
	}
	com.SetCommandID(commandID)
	com.SetQueryID(queryID)
	com.SetSubID(subID)
	return com
}

// SetAckType sets ack type on the receiver.
func (com *Command) SetAckType(ackType int) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	if ackType < AckTypeNone ||
		ackType > (AckTypeReceived|AckTypeParsed|AckTypeProcessed|AckTypePersisted|AckTypeCompleted|AckTypeStats) {
		header.ackType = nil
	} else {
		header.ackType = &ackType
	}
	return com
}

// AddAckType adds ack type behavior on the receiver.
func (com *Command) AddAckType(ackType int) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	if ackType > AckTypeNone && ackType <= AckTypeStats {
		if header.ackType == nil {
			header.ackType = &ackType
		} else {
			*header.ackType |= ackType
		}
	}

	return com
}

// SetBatchSize sets batch size on the receiver.
func (com *Command) SetBatchSize(batchSize uint) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	if batchSize == 0 {
		header.batchSize = nil
	} else {
		header.batchSize = &batchSize
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

type commandOptionToken struct {
	key      string
	value    string
	hasValue bool
}

func parseCommandOptions(options string) []commandOptionToken {
	if options == "" {
		return nil
	}

	var rawTokens = splitCommandOptions(options)
	var tokens = make([]commandOptionToken, 0, len(rawTokens))
	for _, rawToken := range rawTokens {
		if key, value, found := strings.Cut(rawToken, "="); found {
			tokens = append(tokens, commandOptionToken{
				key:      key,
				value:    value,
				hasValue: true,
			})
			continue
		}
		tokens = append(tokens, commandOptionToken{key: rawToken})
	}
	return tokens
}

func splitCommandOptions(options string) []string {
	if options == "" {
		return nil
	}

	var tokens []string
	var bracketDepth int
	var tokenStart int
	for index, r := range options {
		switch r {
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		case ',':
			if bracketDepth == 0 {
				token := strings.TrimSpace(options[tokenStart:index])
				if token != "" {
					tokens = append(tokens, token)
				}
				tokenStart = index + 1
			}
		}
	}

	token := strings.TrimSpace(options[tokenStart:])
	if token != "" {
		tokens = append(tokens, token)
	}
	return tokens
}

func writeCommandOptions(tokens []commandOptionToken) string {
	if len(tokens) == 0 {
		return ""
	}

	var buffer []byte
	for _, token := range tokens {
		if token.key == "" {
			continue
		}
		if len(buffer) > 0 {
			buffer = append(buffer, ',')
		}
		buffer = append(buffer, token.key...)
		if token.hasValue {
			buffer = append(buffer, '=')
			buffer = append(buffer, token.value...)
		}
	}
	return string(buffer)
}

func getCommandOption(options string, key string) (string, bool) {
	for _, token := range parseCommandOptions(options) {
		if token.key != key {
			continue
		}
		if token.hasValue {
			return token.value, true
		}
		return "", true
	}
	return "", false
}

func setCommandFlagOption(options string, key string, enabled bool) string {
	var tokens = parseCommandOptions(options)
	var kept = make([]commandOptionToken, 0, len(tokens)+1)
	for _, token := range tokens {
		if token.key == key {
			continue
		}
		kept = append(kept, token)
	}
	if enabled {
		kept = append(kept, commandOptionToken{key: key})
	}
	return writeCommandOptions(kept)
}

func setCommandValueOption(options string, key string, value string) string {
	var tokens = parseCommandOptions(options)
	var kept = make([]commandOptionToken, 0, len(tokens)+1)
	for _, token := range tokens {
		if token.key == key {
			continue
		}
		kept = append(kept, token)
	}
	if value != "" {
		kept = append(kept, commandOptionToken{key: key, value: value, hasValue: true})
	}
	return writeCommandOptions(kept)
}

// SetBookmark sets bookmark on the receiver.
func (com *Command) SetBookmark(bookmark string) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	assignStringBytes(&header.bookmark, bookmark)
	return com
}

// SetCommand sets command on the receiver.
func (com *Command) SetCommand(command string) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	if len(command) == 0 {
		header.command = CommandUnknown
	} else {
		header.command = commandStringToInt(command)
	}

	return com
}

// SetCommandEnum sets command enum on the receiver.
func (com *Command) SetCommandEnum(command int) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	header.command = command
	return com
}

// SetCommandID sets command id on the receiver.
func (com *Command) SetCommandID(commandID string) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	assignStringBytes(&header.commandID, commandID)
	header.strictParityEscapeState = 0
	return com
}

// SetCorrelationID sets correlation id on the receiver.
func (com *Command) SetCorrelationID(correlationID string) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	assignStringBytes(&header.correlationID, correlationID)
	return com
}

// SetData sets data on the receiver.
func (com *Command) SetData(data []byte) *Command {
	if com == nil {
		return nil
	}
	com.data = data
	return com
}

// SetExpiration sets expiration on the receiver.
func (com *Command) SetExpiration(expiration uint) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	if expiration == 0 {
		header.expiration = nil
	} else {
		header.expiration = &expiration
	}
	return com
}

// SetFilter sets filter on the receiver.
func (com *Command) SetFilter(filter string) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	assignStringBytes(&header.filter, filter)
	header.strictParityEscapeState = 0
	return com
}

// SetOptions sets options on the receiver.
func (com *Command) SetOptions(options string) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	assignStringBytes(&header.options, options)
	header.strictParityEscapeState = 0
	return com
}

func (com *Command) SendKeys() (bool, bool) {
	options, hasOptions := com.Options()
	if !hasOptions {
		return false, false
	}
	_, hasOption := getCommandOption(options, OptionSendKeys)
	return hasOption, hasOption
}

func (com *Command) SetSendKeys(enabled bool) *Command {
	if com == nil {
		return nil
	}
	options, _ := com.Options()
	return com.SetOptions(setCommandFlagOption(options, OptionSendKeys, enabled))
}

func (com *Command) FullyDurable() (bool, bool) {
	options, hasOptions := com.Options()
	if !hasOptions {
		return false, false
	}
	_, hasOption := getCommandOption(options, OptionFullyDurable)
	return hasOption, hasOption
}

func (com *Command) SetFullyDurable(enabled bool) *Command {
	if com == nil {
		return nil
	}
	options, _ := com.Options()
	return com.SetOptions(setCommandFlagOption(options, OptionFullyDurable, enabled))
}

func (com *Command) MaxBacklog() (uint, bool) {
	options, hasOptions := com.Options()
	if !hasOptions {
		return 0, false
	}
	value, hasValue := getCommandOption(options, "max_backlog")
	if !hasValue {
		return 0, false
	}
	parsed, ok := parseUintValue([]byte(value))
	if !ok {
		return 0, false
	}
	return parsed, true
}

func (com *Command) SetMaxBacklog(maxBacklog uint) *Command {
	if com == nil {
		return nil
	}
	options, _ := com.Options()
	return com.SetOptions(setCommandValueOption(options, "max_backlog", strconv.FormatUint(uint64(maxBacklog), 10)))
}

func (com *Command) BookmarkNotFound() (string, bool) {
	options, hasOptions := com.Options()
	if !hasOptions {
		return "", false
	}
	return getCommandOption(options, "bookmark_not_found")
}

func (com *Command) SetBookmarkNotFound(action string) *Command {
	if com == nil {
		return nil
	}
	options, _ := com.Options()
	switch action {
	case "", "now", "epoch", "fail":
		return com.SetOptions(setCommandValueOption(options, "bookmark_not_found", action))
	default:
		return com
	}
}

func (com *Command) SetBookmarkNotFoundNow() *Command {
	return com.SetBookmarkNotFound("now")
}

func (com *Command) SetBookmarkNotFoundEpoch() *Command {
	return com.SetBookmarkNotFound("epoch")
}

func (com *Command) SetBookmarkNotFoundFail() *Command {
	return com.SetBookmarkNotFound("fail")
}

// SetOrderBy sets order by on the receiver.
func (com *Command) SetOrderBy(orderBy string) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	assignStringBytes(&header.orderBy, orderBy)
	return com
}

// SetQueryID sets query id on the receiver.
func (com *Command) SetQueryID(queryID string) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	assignStringBytes(&header.queryID, queryID)
	header.strictParityEscapeState = 0
	return com
}

// SetSequenceID sets sequence id on the receiver.
func (com *Command) SetSequenceID(sequenceID uint64) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	if sequenceID == 0 {
		header.sequenceID = nil
	} else {
		header.sequenceID = &sequenceID
	}
	return com
}

// SetSowKey sets sow key on the receiver.
func (com *Command) SetSowKey(sowKey string) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	assignStringBytes(&header.sowKey, sowKey)
	return com
}

// SetSowKeys sets sow keys on the receiver.
func (com *Command) SetSowKeys(sowKeys string) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	assignStringBytes(&header.sowKeys, sowKeys)
	return com
}

// SetSubID sets sub id on the receiver.
func (com *Command) SetSubID(subID string) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	assignStringBytes(&header.subID, subID)
	header.strictParityEscapeState = 0
	return com
}

// SetSubIDs sets sub ids on the receiver.
func (com *Command) SetSubIDs(subIDs string) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	assignStringBytes(&header.subIDs, subIDs)
	return com
}

// SetTopN sets top n on the receiver.
func (com *Command) SetTopN(topN uint) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	if topN == 0 {
		header.topN = nil
	} else {
		header.topN = &topN
	}
	return com
}

// SetTopic sets topic on the receiver.
func (com *Command) SetTopic(topic string) *Command {
	var header = ensureCommandHeader(com)
	if header == nil {
		return nil
	}
	assignStringBytes(&header.topic, topic)
	header.strictParityEscapeState = 0
	return com
}

// NewCommand returns a new Command.
func NewCommand(commandName string) *Command {
	var header = newHeader()
	header.command = commandStringToInt(commandName)
	return &Command{header: header}
}

func (com *Command) Clone() *Command {
	if com == nil {
		return nil
	}
	return cloneCommand(com)
}

func (com *Command) SetLeasePeriod(period uint) *Command {
	var h = ensureCommandHeader(com)
	if h == nil {
		return nil
	}
	if extras := ensureHeaderTextExtras(h); extras != nil {
		extras.leasePeriod = []byte(strconv.FormatUint(uint64(period), 10))
	}
	return com
}

func (com *Command) SetGroupSequenceNumber(seq uint) *Command {
	var h = ensureCommandHeader(com)
	if h == nil {
		return nil
	}
	h.groupSequenceNumber = &seq
	return com
}

func (com *Command) SetDataOnly(dataOnly bool) *Command {
	var h = ensureCommandHeader(com)
	if h == nil {
		return nil
	}
	if extras := ensureHeaderTextExtras(h); extras != nil {
		if dataOnly {
			extras.dataOnly = []byte("true")
		} else {
			extras.dataOnly = nil
		}
	}
	return com
}

func (com *Command) SetSendEmpty(sendEmpty bool) *Command {
	var h = ensureCommandHeader(com)
	if h == nil {
		return nil
	}
	if extras := ensureHeaderTextExtras(h); extras != nil {
		if sendEmpty {
			extras.sendEmpty = []byte("true")
		} else {
			extras.sendEmpty = nil
		}
	}
	return com
}

func (com *Command) SetSkipN(n uint) *Command {
	var h = ensureCommandHeader(com)
	if h == nil {
		return nil
	}
	h.skipN = &n
	return com
}

func (com *Command) SetReplace() *Command {
	return com.SetOptions(OptionReplace)
}

func (com *Command) SetMessageType(messageType string) *Command {
	var h = ensureCommandHeader(com)
	if h == nil {
		return nil
	}
	h.messageType = []byte(messageType)
	return com
}
