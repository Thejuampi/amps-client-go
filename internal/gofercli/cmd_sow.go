package gofercli

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

func (app *App) runSOW(args []string) error {
	if hasHelpArg(args) {
		_, _ = io.WriteString(app.Stdout, commandHelpText("sow"))
		return nil
	}
	var config, err = parseStreamArgs("sow", args)
	if err != nil {
		return err
	}

	var client, _, connectErr = connectClient(config.ConnectionOptions, app.Env, app.Now)
	if connectErr != nil {
		return connectErr
	}
	defer func() { _ = client.Close() }()

	var copier *copyPublisher
	if config.CopyServer != "" {
		var copyOptions = config.ConnectionOptions
		copyOptions.Server = config.CopyServer
		var copyErr error
		copier, copyErr = newCopyPublisher(copyOptions, app.Env, app.Now, config.Delta)
		if copyErr != nil {
			return copyErr
		}
		defer copier.Close()
	}

	var command = buildStreamCommand("sow", config)
	command.AddAckType(amps.AckTypeCompleted)

	var done = make(chan struct{}, 1)
	var failures = make(chan error, 1)
	var count int
	var started = app.Now()

	_, err = client.ExecuteAsync(command, func(message *amps.Message) error {
		var commandType, _ = message.Command()
		switch commandType {
		case amps.CommandSOW:
			count++
			if err := app.emitMessage(message, config.Format, copier); err != nil {
				signalError(failures, err)
				signalDone(done)
			}
		case amps.CommandGroupEnd:
			signalDone(done)
		case amps.CommandAck:
			if ackType, ok := message.AckType(); ok && ackType == amps.AckTypeCompleted {
				signalDone(done)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	select {
	case err = <-failures:
		return err
	case <-done:
	case <-time.After(config.ConnectionOptions.Timeout):
		return fmt.Errorf("sow timed out")
	}

	_, _ = fmt.Fprintf(app.Stdout, "Total messages received: %d (%s/s)\n", count, formatRate(count, app.Now().Sub(started)))
	return nil
}

func (app *App) runSOWDelete(args []string) error {
	if hasHelpArg(args) {
		_, _ = io.WriteString(app.Stdout, commandHelpText("sow_delete"))
		return nil
	}

	var options = connectionOptions{Timeout: defaultTimeout}
	var topic string
	var filter string
	var keys string
	var inlineData string
	var filePath string
	var delimiterRaw = "\n"
	var parser = newFlagParser("sow_delete")
	parser.StringVar(&options.Server, "server", "", "server")
	parser.StringVar(&topic, "topic", "", "topic")
	parser.StringVar(&filter, "filter", "", "filter")
	parser.StringVar(&inlineData, "data", "", "data")
	parser.StringVar(&filePath, "file", "", "file")
	parser.StringVar(&keys, "keys", "", "keys")
	parser.StringVar(&delimiterRaw, "delimiter", "\n", "delimiter")
	registerConnectionAliases(parser, &options)
	if err := parser.Parse(args); err != nil {
		return err
	}
	if topic == "" {
		return fmt.Errorf("topic is required (-topic)")
	}

	var delimiter, delimiterErr = parseDelimiter(delimiterRaw)
	if delimiterErr != nil {
		return delimiterErr
	}

	var payloads [][]byte
	if strings.TrimSpace(filter) == "" && strings.TrimSpace(keys) == "" || payloadInputExplicit(inlineData, filePath) {
		var payloadErr error
		payloads, payloadErr = readPayloadInputs(app.Stdin, inlineData, filePath, delimiter)
		if payloadErr != nil {
			return payloadErr
		}
	}

	var mode, modeErr = selectDeleteMode(deleteModeOptions{
		Filter:   filter,
		Keys:     keys,
		Payloads: payloads,
	})
	if modeErr != nil {
		return modeErr
	}

	var client, _, connectErr = connectClient(options, app.Env, app.Now)
	if connectErr != nil {
		return connectErr
	}
	defer func() { _ = client.Close() }()

	var started = app.Now()
	var deleted uint
	switch mode {
	case deleteByFilter:
		var result, err = client.SowDelete(topic, filter)
		if err != nil {
			return err
		}
		deleted = deletedCount(result)
	case deleteByKeys:
		var result, err = client.SowDeleteByKeys(topic, keys)
		if err != nil {
			return err
		}
		deleted = deletedCount(result)
	case deleteByData:
		for _, payload := range payloads {
			var result, err = client.SowDeleteByData(topic, payload)
			if err != nil {
				return err
			}
			deleted += deletedCount(result)
		}
	}

	_, _ = fmt.Fprintf(app.Stdout, "Deleted %d records in %d ms.\n", deleted, app.Now().Sub(started).Milliseconds())
	return nil
}

func deletedCount(message *amps.Message) uint {
	if message == nil {
		return 0
	}
	var deleted, _ = message.RecordsDeleted()
	return deleted
}
