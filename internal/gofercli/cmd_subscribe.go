package gofercli

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

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
			return nil
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
