package gofercli

import (
	"fmt"
	"io"

	"github.com/Thejuampi/amps-client-go/amps"
)

func (app *App) runPublish(args []string) error {
	if hasHelpArg(args) {
		_, _ = io.WriteString(app.Stdout, commandHelpText("publish"))
		return nil
	}

	var options = connectionOptions{Timeout: defaultTimeout}
	var topic string
	var inlineData string
	var filePath string
	var delimiterRaw = "\n"
	var rateRaw string
	var copyServer string
	var delta bool

	var parser = newFlagParser("publish")
	parser.StringVar(&options.Server, "server", "", "server")
	parser.StringVar(&topic, "topic", "", "topic")
	parser.StringVar(&inlineData, "data", "", "data")
	parser.StringVar(&filePath, "file", "", "file")
	parser.StringVar(&delimiterRaw, "delimiter", "\n", "delimiter")
	parser.StringVar(&copyServer, "copy", "", "copy")
	parser.BoolVar(&delta, "delta", false, "delta")
	parser.StringVar(&rateRaw, "rate", "", "rate")
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
	var payloads, payloadErr = readPayloadInputs(app.Stdin, inlineData, filePath, delimiter)
	if payloadErr != nil {
		return payloadErr
	}
	if len(payloads) == 0 {
		return fmt.Errorf("publish requires -data, -file, or stdin payloads")
	}

	var primary, _, connectErr = connectClient(options, app.Env, app.Now)
	if connectErr != nil {
		return connectErr
	}
	defer func() { _ = primary.Close() }()

	var copier *copyPublisher
	if copyServer != "" {
		var copyOptions = options
		copyOptions.Server = copyServer
		var copyErr error
		copier, copyErr = newCopyPublisher(copyOptions, app.Env, app.Now, delta)
		if copyErr != nil {
			return copyErr
		}
		defer copier.Close()
	}

	var interval, intervalErr = parseRateInterval(rateRaw)
	if intervalErr != nil {
		return intervalErr
	}

	var started = app.Now()
	for index, payload := range payloads {
		if index > 0 && interval > 0 {
			app.Sleep(interval)
		}
		if err := publishPayload(primary, topic, payload, delta); err != nil {
			return err
		}
		if copier != nil {
			if err := copier.Publish(topic, payload); err != nil {
				return err
			}
		}
	}

	_, _ = fmt.Fprintf(app.Stdout, "Total messages published: %d (%s/s)\n", len(payloads), formatRate(len(payloads), app.Now().Sub(started)))
	return nil
}

func publishPayload(client *amps.Client, topic string, payload []byte, delta bool) error {
	if delta {
		return client.DeltaPublishBytes(topic, payload)
	}
	return client.PublishBytes(topic, payload)
}
