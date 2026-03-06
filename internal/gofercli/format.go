package gofercli

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Thejuampi/amps-client-go/amps"
)

func renderMessageFormat(format string, message *amps.Message) (string, error) {
	if message == nil {
		return "", nil
	}
	if format == "" {
		return string(message.Data()), nil
	}
	if strings.Contains(format, "{") {
		return renderBraceMessageFormat(format, message)
	}
	return renderPercentMessageFormat(format, message)
}

func renderBraceMessageFormat(format string, message *amps.Message) (string, error) {
	var builder strings.Builder
	for index := 0; index < len(format); index++ {
		switch format[index] {
		case '{':
			if index+1 < len(format) && format[index+1] == '{' {
				builder.WriteByte('{')
				index++
				continue
			}
			var end = strings.IndexByte(format[index:], '}')
			if end < 0 {
				return "", fmt.Errorf("unterminated format token")
			}
			var token = format[index+1 : index+end]
			var value, err = messageFormatValue(token, message)
			if err != nil {
				return "", err
			}
			builder.WriteString(value)
			index += end
		case '}':
			if index+1 < len(format) && format[index+1] == '}' {
				builder.WriteByte('}')
				index++
				continue
			}
			return "", fmt.Errorf("unexpected format token terminator")
		default:
			builder.WriteByte(format[index])
		}
	}
	return builder.String(), nil
}

func renderPercentMessageFormat(format string, message *amps.Message) (string, error) {
	var builder strings.Builder
	for index := 0; index < len(format); index++ {
		if format[index] != '%' {
			builder.WriteByte(format[index])
			continue
		}
		index++
		if index >= len(format) {
			return "", fmt.Errorf("dangling format token")
		}

		switch format[index] {
		case '%':
			builder.WriteByte('%')
		case 'm':
			builder.Write(message.Data())
		case 't':
			var topic, _ = message.Topic()
			builder.WriteString(topic)
		case 'b':
			var bookmark, _ = message.Bookmark()
			builder.WriteString(bookmark)
		case 'k':
			var sowKey, _ = message.SowKey()
			builder.WriteString(sowKey)
		case 's':
			var subID, _ = message.SubID()
			builder.WriteString(subID)
		case 'c':
			builder.WriteString(messageCommandName(message))
		default:
			return "", fmt.Errorf("unsupported format token %%%c", format[index])
		}
	}
	return builder.String(), nil
}

func messageFormatValue(token string, message *amps.Message) (string, error) {
	switch strings.ToLower(strings.TrimSpace(token)) {
	case "bookmark":
		var bookmark, _ = message.Bookmark()
		return bookmark, nil
	case "command":
		return messageCommandName(message), nil
	case "correlation_id":
		var correlationID, _ = message.CorrelationID()
		return correlationID, nil
	case "data":
		return string(message.Data()), nil
	case "expiration":
		var expiration, _ = message.Expiration()
		return strconv.FormatUint(uint64(expiration), 10), nil
	case "lease_period":
		var leasePeriod, _ = message.LeasePeriod()
		return leasePeriod, nil
	case "length":
		return strconv.Itoa(len(message.Data())), nil
	case "sowkey":
		var sowKey, _ = message.SowKey()
		return sowKey, nil
	case "sub_id", "subid":
		var subID, _ = message.SubID()
		return subID, nil
	case "timestamp":
		var timestamp, _ = message.Timestamp()
		return timestamp, nil
	case "topic":
		var topic, _ = message.Topic()
		return topic, nil
	case "user_id":
		var userID, _ = message.UserID()
		return userID, nil
	default:
		return "", fmt.Errorf("unsupported format token {%s}", token)
	}
}

func messageCommandName(message *amps.Message) string {
	var command, _ = message.Command()
	switch command {
	case amps.CommandPublish:
		return "publish"
	case amps.CommandSOW:
		return "sow"
	case amps.CommandSubscribe:
		return "subscribe"
	case amps.CommandSOWAndSubscribe:
		return "sow_and_subscribe"
	case amps.CommandSOWDelete:
		return "sow_delete"
	case amps.CommandDeltaPublish:
		return "delta_publish"
	case amps.CommandDeltaSubscribe:
		return "delta_subscribe"
	case amps.CommandSOWAndDeltaSubscribe:
		return "sow_and_delta_subscribe"
	default:
		return ""
	}
}
