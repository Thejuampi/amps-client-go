package main

import (
	"flag"
	"fmt"
)

type integrationTestOptions struct {
	race    bool
	count   int
	shuffle string
	workers int
}

func parseTestOptions(args []string) (integrationTestOptions, error) {
	var options integrationTestOptions
	var flags = flag.NewFlagSet("fakeampsgate", flag.ContinueOnError)
	flags.BoolVar(&options.race, "race", false, "enable the race detector for integration tests")
	flags.IntVar(&options.count, "count", 1, "repeat both integration suites this many times")
	flags.StringVar(&options.shuffle, "shuffle", "off", "test shuffle setting or reproducible seed")
	flags.IntVar(&options.workers, "workers", 2, "maximum concurrent integration rounds")
	if err := flags.Parse(args); err != nil {
		return options, err
	}
	if options.count < 1 {
		return options, fmt.Errorf("count must be positive, got %d", options.count)
	}
	if options.workers < 1 {
		return options, fmt.Errorf("workers must be positive, got %d", options.workers)
	}
	if flags.NArg() != 0 {
		return options, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	return options, nil
}
