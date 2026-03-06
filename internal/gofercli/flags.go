package gofercli

import (
	"fmt"
	"flag"
	"io"
	"strings"
	"time"
)

type flagParser struct {
	inner     *flag.FlagSet
	boolFlags map[string]struct{}
}

func newFlagParser(name string) *flagParser {
	var set = flag.NewFlagSet(name, flag.ContinueOnError)
	set.SetOutput(io.Discard)
	return &flagParser{
		inner:     set,
		boolFlags: map[string]struct{}{},
	}
}

func (parser *flagParser) Parse(args []string) error {
	var normalized = parser.normalizeBoolArgs(args)
	if err := parser.inner.Parse(normalized); err != nil {
		return err
	}
	if parser.inner.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %s", strings.Join(parser.inner.Args(), " "))
	}
	return nil
}

func (parser *flagParser) StringVar(target *string, name string, value string, usage string) {
	parser.inner.StringVar(target, name, value, usage)
}

func (parser *flagParser) BoolVar(target *bool, name string, value bool, usage string) {
	parser.inner.BoolVar(target, name, value, usage)
	parser.boolFlags[name] = struct{}{}
}

func (parser *flagParser) IntVar(target *int, name string, value int, usage string) {
	parser.inner.IntVar(target, name, value, usage)
}

func (parser *flagParser) UintVar(target *uint, name string, value uint, usage string) {
	parser.inner.UintVar(target, name, value, usage)
}

func (parser *flagParser) DurationVar(target *time.Duration, name string, value time.Duration, usage string) {
	parser.inner.DurationVar(target, name, value, usage)
}

func (parser *flagParser) normalizeBoolArgs(args []string) []string {
	var normalized = make([]string, 0, len(args))
	for index := 0; index < len(args); index++ {
		var token = args[index]
		var name, hasValue = splitFlagToken(token)
		if _, ok := parser.boolFlags[name]; ok {
			if hasValue {
				var flagName, rawValue, _ = strings.Cut(token, "=")
				if normalizedValue, valueOK := normalizeSparkBoolValue(rawValue); valueOK {
					normalized = append(normalized, flagName+"="+normalizedValue)
					continue
				}
			}
			if index+1 < len(args) {
				if normalizedValue, valueOK := normalizeSparkBoolValue(args[index+1]); valueOK {
					normalized = append(normalized, token+"="+normalizedValue)
					index++
					continue
				}
			}
		}
		normalized = append(normalized, token)
	}
	return normalized
}

func splitFlagToken(token string) (string, bool) {
	if !strings.HasPrefix(token, "-") {
		return "", false
	}
	var trimmed = strings.TrimLeft(token, "-")
	var name, _, found = strings.Cut(trimmed, "=")
	return name, found
}

func normalizeSparkBoolValue(token string) (string, bool) {
	var value, err = parseSparkBool(token)
	if err != nil {
		return "", false
	}
	return fmt.Sprintf("%t", value), true
}
