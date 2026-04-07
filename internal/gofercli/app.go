package gofercli

import (
	"fmt"
	"io"
	"strings"
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
	case "version", "-version", "--version":
		_, _ = fmt.Fprintf(app.Stdout, "gofer version %s\n", version)
		return 0
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
	if count == 0 && elapsed <= 0 {
		return "N/A"
	}
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
