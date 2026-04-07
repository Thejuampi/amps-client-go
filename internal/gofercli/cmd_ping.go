package gofercli

import (
	"fmt"
	"io"
)

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
