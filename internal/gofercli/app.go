package gofercli

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

type App struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	Env    []string
	Now    func() time.Time
	Sleep  func(time.Duration)
}

type deleteMode int

const (
	deleteByFilter deleteMode = iota
	deleteByData
	deleteByKeys
)

type deleteModeOptions struct {
	Filter   string
	Keys     string
	Payloads [][]byte
}

type streamConfig struct {
	ConnectionOptions connectionOptions
	Topic             string
	Filter            string
	Format            string
	CopyServer        string
	OrderBy           string
	TopN              uint
	BatchSize         uint
	Backlog           int
	Limit             int
	Delta             bool
	Ack               bool
}

func Main(args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer, env []string) int {
	var app = &App{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
		Env:    env,
		Now:    time.Now,
		Sleep:  time.Sleep,
	}
	return app.Run(args)
}

func (app *App) Run(args []string) int {
	if len(args) == 0 {
		_, _ = io.WriteString(app.Stderr, mainHelpText())
		return 1
	}

	var err error
	switch args[0] {
	case "help", "-help", "--help", "-h":
		err = app.runHelp(args[1:])
	case "ping":
		err = app.runPing(args[1:])
	case "publish":
		err = app.runPublish(args[1:])
	case "subscribe":
		err = app.runSubscribe(args[1:])
	case "sow":
		err = app.runSOW(args[1:])
	case "sow_and_subscribe":
		err = app.runSOWAndSubscribe(args[1:])
	case "sow_delete":
		err = app.runSOWDelete(args[1:])
	default:
		err = fmt.Errorf("unknown command %q\n\n%s", args[0], mainHelpText())
	}

	if err != nil {
		_, _ = fmt.Fprintf(app.Stderr, "gofer: %v\n", err)
		return 1
	}
	return 0
}

func (app *App) runHelp(args []string) error {
	if len(args) == 0 {
		_, _ = io.WriteString(app.Stdout, mainHelpText())
		return nil
	}
	_, _ = io.WriteString(app.Stdout, commandHelpText(args[0]))
	return nil
}

func registerConnectionAliases(parser *flagParser, options *connectionOptions) {
	parser.StringVar(&options.Selector, "type", "", "type")
	parser.StringVar(&options.Selector, "prot", "", "prot")
	parser.StringVar(&options.Selector, "proto", "", "proto")
	parser.BoolVar(&options.Secure, "secure", false, "secure")
	parser.StringVar(&options.URIScheme, "urischeme", "", "urischeme")
	parser.StringVar(&options.URIOptions, "uriopts", "", "uriopts")
	parser.StringVar(&options.Authenticator, "authenticator", "", "authenticator")
	parser.DurationVar(&options.Timeout, "timeout", defaultTimeout, "timeout")
}

func parseConnectionArgs(name string, args []string, options *connectionOptions) error {
	var parser = newFlagParser(name)
	parser.StringVar(&options.Server, "server", "", "server")
	registerConnectionAliases(parser, options)
	return parser.Parse(args)
}

func hasHelpArg(args []string) bool {
	for _, arg := range args {
		switch arg {
		case "-help", "--help", "-h":
			return true
		}
	}
	return false
}

func formatRate(count int, elapsed time.Duration) string {
	if count == 0 || elapsed <= 0 {
		return "Infinity"
	}
	return fmt.Sprintf("%.3f", float64(count)/elapsed.Seconds())
}

func buildStreamCommand(name string, config streamConfig) *amps.Command {
	var commandName = name
	if config.Delta {
		switch name {
		case "subscribe":
			commandName = "delta_subscribe"
		case "sow_and_subscribe":
			commandName = "sow_and_delta_subscribe"
		}
	}

	var command = amps.NewCommand(commandName).SetTopic(config.Topic)
	if config.Filter != "" {
		command.SetFilter(config.Filter)
	}
	if config.OrderBy != "" {
		command.SetOrderBy(config.OrderBy)
	}
	if config.TopN > 0 {
		command.SetTopN(config.TopN)
	}
	if config.BatchSize > 0 {
		command.SetBatchSize(config.BatchSize)
	}
	if options := buildCommandOptions(config); options != "" {
		command.SetOptions(options)
	}
	return command
}

func buildCommandOptions(config streamConfig) string {
	var parts []string
	if config.Backlog > 0 {
		parts = append(parts, fmt.Sprintf("max_backlog=%d", config.Backlog))
	}
	return strings.Join(parts, ",")
}

func parseStreamArgs(name string, args []string) (streamConfig, error) {
	var config streamConfig
	config.ConnectionOptions.Timeout = defaultTimeout

	var parser = newFlagParser(name)
	parser.StringVar(&config.ConnectionOptions.Server, "server", "", "server")
	parser.StringVar(&config.Topic, "topic", "", "topic")
	parser.StringVar(&config.Filter, "filter", "", "filter")
	parser.StringVar(&config.Format, "format", "", "format")
	parser.StringVar(&config.CopyServer, "copy", "", "copy")
	parser.StringVar(&config.OrderBy, "orderby", "", "orderby")
	parser.UintVar(&config.TopN, "topn", 0, "topn")
	parser.UintVar(&config.BatchSize, "batchsize", 0, "batchsize")
	parser.IntVar(&config.Backlog, "backlog", 0, "backlog")
	parser.IntVar(&config.Limit, "n", 0, "n")
	parser.BoolVar(&config.Delta, "delta", false, "delta")
	parser.BoolVar(&config.Ack, "ack", false, "ack")
	registerConnectionAliases(parser, &config.ConnectionOptions)
	if err := parser.Parse(args); err != nil {
		return config, err
	}
	if config.Topic == "" {
		return config, fmt.Errorf("topic is required (-topic)")
	}
	return config, nil
}

func selectDeleteMode(options deleteModeOptions) (deleteMode, error) {
	var modeCount int
	if strings.TrimSpace(options.Filter) != "" {
		modeCount++
	}
	if strings.TrimSpace(options.Keys) != "" {
		modeCount++
	}
	if len(options.Payloads) > 0 {
		modeCount++
	}
	if modeCount == 0 {
		return deleteByFilter, fmt.Errorf("sow_delete requires -filter, -keys, -data, -file, or stdin payloads")
	}
	if modeCount > 1 {
		return deleteByFilter, fmt.Errorf("sow_delete accepts only one of filter, keys, or payload input")
	}
	if strings.TrimSpace(options.Filter) != "" {
		return deleteByFilter, nil
	}
	if strings.TrimSpace(options.Keys) != "" {
		return deleteByKeys, nil
	}
	return deleteByData, nil
}

func publishPayload(client *amps.Client, topic string, payload []byte, delta bool) error {
	if delta {
		return client.DeltaPublishBytes(topic, payload)
	}
	return client.PublishBytes(topic, payload)
}

func deletedCount(message *amps.Message) uint {
	if message == nil {
		return 0
	}
	var deleted, _ = message.RecordsDeleted()
	return deleted
}

func signalDone(done chan<- struct{}) {
	select {
	case done <- struct{}{}:
	default:
	}
}

func signalError(target chan<- error, err error) {
	select {
	case target <- err:
	default:
	}
}

func payloadInputExplicit(inlineData string, filePath string) bool {
	return strings.TrimSpace(inlineData) != "" || strings.TrimSpace(filePath) != ""
}

func handleStreamingControlMessage(message *amps.Message, failures chan<- error, done chan<- struct{}) bool {
	if message == nil {
		return false
	}

	var commandType, _ = message.Command()
	if commandType != amps.CommandAck {
		return false
	}

	if err := message.ThrowFor(); err != nil {
		signalError(failures, err)
		signalDone(done)
	}
	return true
}

func (app *App) emitMessage(message *amps.Message, format string, copier *copyPublisher) error {
	var rendered, err = renderMessageFormat(format, message)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(app.Stdout, rendered); err != nil {
		return err
	}
	if copier != nil {
		var topic, _ = message.Topic()
		if err := copier.Publish(topic, message.Data()); err != nil {
			return err
		}
	}
	return nil
}

func (app *App) runPing(args []string) error {
	if hasHelpArg(args) {
		_, _ = io.WriteString(app.Stdout, commandHelpText("ping"))
		return nil
	}

	var options = connectionOptions{Timeout: defaultTimeout}
	if err := parseConnectionArgs("ping", args, &options); err != nil {
		return err
	}

	var client, uri, connectErr = connectClient(options, app.Env, app.Now)
	if connectErr != nil {
		return connectErr
	}
	defer func() { _ = client.Close() }()

	_, _ = fmt.Fprintf(app.Stdout, "Successfully connected to %s\n", uri)
	return nil
}

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

func (app *App) runSubscribe(args []string) error {
	if hasHelpArg(args) {
		_, _ = io.WriteString(app.Stdout, commandHelpText("subscribe"))
		return nil
	}
	var config, err = parseStreamArgs("subscribe", args)
	if err != nil {
		return err
	}
	return app.runStreamingCommand(config, "subscribe")
}

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

func (app *App) runSOWAndSubscribe(args []string) error {
	if hasHelpArg(args) {
		_, _ = io.WriteString(app.Stdout, commandHelpText("sow_and_subscribe"))
		return nil
	}
	var config, err = parseStreamArgs("sow_and_subscribe", args)
	if err != nil {
		return err
	}
	return app.runStreamingCommand(config, "sow_and_subscribe")
}

func (app *App) runStreamingCommand(config streamConfig, commandName string) error {
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

	var command = buildStreamCommand(commandName, config)
	var done = make(chan struct{}, 1)
	var failures = make(chan error, 1)
	var interrupts = make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt)
	defer signal.Stop(interrupts)

	var count int
	var lock sync.Mutex
	_, err := client.ExecuteAsync(command, func(message *amps.Message) error {
		if handleStreamingControlMessage(message, failures, done) {
			return nil
		}

		var commandType, _ = message.Command()
		if commandType != amps.CommandPublish && commandType != amps.CommandSOW && commandType != amps.CommandOOF {
			return nil
		}

		if err := app.emitMessage(message, config.Format, copier); err != nil {
			signalError(failures, err)
			signalDone(done)
			return nil
		}
		if config.Ack {
			if ackErr := message.Ack(); ackErr != nil {
				signalError(failures, ackErr)
				signalDone(done)
				return nil
			}
		}

		lock.Lock()
		count++
		var reachedLimit = config.Limit > 0 && count >= config.Limit
		lock.Unlock()
		if reachedLimit {
			signalDone(done)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if config.Limit > 0 {
		select {
		case err = <-failures:
			return err
		case <-done:
			select {
			case err = <-failures:
				return err
			default:
				return nil
			}
		case <-time.After(config.ConnectionOptions.Timeout):
			return fmt.Errorf("%s timed out", commandName)
		}
	}

	select {
	case err = <-failures:
		return err
	case <-interrupts:
		return nil
	}
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
