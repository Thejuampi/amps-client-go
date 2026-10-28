package main

import (
	"flag"
	"fmt"
)

type integrationTestOptions struct {
	race    bool
	count   int
	shuffle string
}

func parseTestOptions(args []string) (integrationTestOptions, error) {
	var options integrationTestOptions
	var flags = flag.NewFlagSet("fakeampsgate", flag.ContinueOnError)
	flags.BoolVar(&options.race, "race", false, "enable the race detector for integration tests")
	flags.IntVar(&options.count, "count", 1, "repeat both integration suites this many times")
	flags.StringVar(&options.shuffle, "shuffle", "off", "test shuffle setting or reproducible seed")
	if err := flags.Parse(args); err != nil {
		return options, err
	}
	if options.count < 1 {
		return options, fmt.Errorf("count must be positive, got %d", options.count)
	}
	if flags.NArg() != 0 {
		return options, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	return options, nil
}

func runIntegrationTests(root string, env []string, options integrationTestOptions, execute func(string, []string, string, ...string) error) error {
	for iteration := 1; iteration <= options.count; iteration++ {
		for _, pkg := range []string{"./amps", "./tools/fakeamps"} {
			var args = []string{"test", "-count=1"}
			if options.race {
				args = append(args, "-race")
			}
			args = append(args, pkg, "-run", "Integration", "-shuffle="+options.shuffle)
			var testEnv []string
			if pkg == "./amps" {
				testEnv = env
			}
			if err := execute(root, testEnv, "go", args...); err != nil {
				return fmt.Errorf("%s integration iteration %d/%d: %w", pkg, iteration, options.count, err)
			}
		}
	}
	return nil
}
