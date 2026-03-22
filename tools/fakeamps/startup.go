package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Thejuampi/amps-client-go/internal/ampsconfig"
)

type startupMode string

const (
	startupModeRun          startupMode = "run"
	startupModeSampleConfig startupMode = "sample-config"
	startupModeVerifyConfig startupMode = "verify-config"
	startupModeDumpConfig   startupMode = "dump-config"
)

var (
	flagConfig       = flag.String("config", "", "load AMPS XML configuration from this file")
	flagSampleConfig = flag.Bool("sample-config", false, "print a sample AMPS XML configuration and exit")
	flagVerifyConfig = flag.String("verify-config", "", "verify the provided AMPS XML configuration and exit")
	flagDumpConfig   = flag.String("dump-config", "", "expand includes/environment variables and print the resulting AMPS XML configuration")
	effectiveConfig  *ampsconfig.ExpandedConfig
)

var configuredLogClosers []io.Closer

type startupOptions struct {
	Mode       startupMode
	ConfigPath string
	parseArgs  []string
	userSet    map[string]bool
}

func (options startupOptions) FlagSetByUser(name string) bool {
	if options.userSet == nil {
		return false
	}
	return options.userSet[name]
}

func parseStartupOptions(args []string) (startupOptions, error) {
	var options = startupOptions{
		Mode:    startupModeRun,
		userSet: make(map[string]bool),
	}
	var positional []string

	for index := 0; index < len(args); index++ {
		var current = args[index]
		if !strings.HasPrefix(current, "-") || current == "-" {
			positional = append(positional, current)
			continue
		}

		var name, value, hasValue = splitStartupFlag(current)
		if name == "" {
			continue
		}
		options.userSet[name] = true
		options.parseArgs = append(options.parseArgs, current)

		if !hasValue && flagConsumesNextValue(name) && index+1 < len(args) && !strings.HasPrefix(args[index+1], "-") {
			index++
			value = args[index]
			hasValue = true
			options.parseArgs = append(options.parseArgs, value)
		}

		switch name {
		case "sample-config":
			enabled := true
			if hasValue {
				parsed, err := strconv.ParseBool(strings.TrimSpace(value))
				if err != nil {
					return startupOptions{}, fmt.Errorf("sample-config must be a boolean: %w", err)
				}
				enabled = parsed
			}
			if enabled {
				options.Mode = startupModeSampleConfig
			}
		case "verify-config":
			options.Mode = startupModeVerifyConfig
			if !hasValue || strings.TrimSpace(value) == "" {
				return startupOptions{}, fmt.Errorf("verify-config requires a config path")
			}
			if err := setStartupConfigPath(&options, value); err != nil {
				return startupOptions{}, err
			}
		case "dump-config":
			options.Mode = startupModeDumpConfig
			if !hasValue || strings.TrimSpace(value) == "" {
				return startupOptions{}, fmt.Errorf("dump-config requires a config path")
			}
			if err := setStartupConfigPath(&options, value); err != nil {
				return startupOptions{}, err
			}
		case "config":
			if !hasValue || strings.TrimSpace(value) == "" {
				return startupOptions{}, fmt.Errorf("config requires a config path")
			}
			if err := setStartupConfigPath(&options, value); err != nil {
				return startupOptions{}, err
			}
		}
	}

	for _, current := range positional {
		if strings.TrimSpace(current) == "" {
			continue
		}
		if err := setStartupConfigPath(&options, current); err != nil {
			return startupOptions{}, err
		}
	}

	return options, nil
}

func splitStartupFlag(value string) (string, string, bool) {
	var trimmed = strings.TrimLeft(value, "-")
	if trimmed == "" {
		return "", "", false
	}
	if index := strings.Index(trimmed, "="); index >= 0 {
		return trimmed[:index], trimmed[index+1:], true
	}
	return trimmed, "", false
}

func setStartupConfigPath(options *startupOptions, value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if options.ConfigPath != "" && options.ConfigPath != value {
		return fmt.Errorf("multiple config paths provided: %s and %s", options.ConfigPath, value)
	}
	options.ConfigPath = value
	return nil
}

func flagConsumesNextValue(name string) bool {
	switch name {
	case "addr", "version", "journal-max", "journal-disk", "stats-interval", "write-buf", "read-buf", "latency",
		"lease", "out-depth", "sow-gc-interval", "queue-lease-interval", "auth", "peers", "repl-id",
		"redirect-uri", "admin", "sow-max", "sow-eviction", "sow-disk", "view", "action", "config",
		"verify-config", "dump-config":
		return true
	default:
		return false
	}
}

func handleConfigModes(options startupOptions) error {
	return handleConfigModesWithWriter(options, os.Stdout)
}

func handleConfigModesWithWriter(options startupOptions, stdout io.Writer) error {
	switch options.Mode {
	case startupModeSampleConfig:
		_, _ = fmt.Fprintln(stdout, ampsconfig.SampleConfig())
		return errStartupHandled
	}

	if options.ConfigPath == "" {
		return nil
	}

	var runtimeVersion string
	if options.FlagSetByUser("version") {
		runtimeVersion = *flagVersion
	}
	var expanded, err = ampsconfig.LoadFile(options.ConfigPath, ampsconfig.LoadOptions{
		RuntimeVersion:        runtimeVersion,
		DefaultRuntimeVersion: *flagVersion,
	})
	if err != nil {
		return err
	}

	switch options.Mode {
	case startupModeVerifyConfig:
		_, _ = fmt.Fprintf(stdout, "config verified: %s\n", expanded.Path)
		return errStartupHandled
	case startupModeDumpConfig:
		_, _ = fmt.Fprintln(stdout, expanded.XML)
		return errStartupHandled
	case startupModeRun:
		effectiveConfig = expanded
		return applyRuntimeConfig(expanded.Runtime, options)
	default:
		return nil
	}
}

var errStartupHandled = fmt.Errorf("startup handled")

func applyRuntimeConfig(runtimeConfig ampsconfig.RuntimeConfig, options startupOptions) error {
	if !options.FlagSetByUser("addr") {
		if runtimeConfig.Extensions.FakeAMPS.ListenAddress != "" {
			*flagAddr = runtimeConfig.Extensions.FakeAMPS.ListenAddress
		} else if len(runtimeConfig.Transports) > 0 && runtimeConfig.Transports[0].InetAddr != "" {
			*flagAddr = runtimeConfig.Transports[0].InetAddr
		}
	}
	if !options.FlagSetByUser("version") && runtimeConfig.Extensions.FakeAMPS.Version != "" {
		*flagVersion = runtimeConfig.Extensions.FakeAMPS.Version
	}
	if !options.FlagSetByUser("admin") && runtimeConfig.Admin.InetAddr != "" {
		*flagAdminAddr = runtimeConfig.Admin.InetAddr
	}
	if !options.FlagSetByUser("fanout") && runtimeConfig.Extensions.FakeAMPS.Fanout != nil {
		*flagFanout = *runtimeConfig.Extensions.FakeAMPS.Fanout
	}
	if !options.FlagSetByUser("sow") && runtimeConfig.Extensions.FakeAMPS.SOWEnabled != nil {
		*flagSOWEnabled = *runtimeConfig.Extensions.FakeAMPS.SOWEnabled
	}
	if !options.FlagSetByUser("journal") && runtimeConfig.Extensions.FakeAMPS.JournalEnabled != nil {
		*flagJournal = *runtimeConfig.Extensions.FakeAMPS.JournalEnabled
	}
	if !options.FlagSetByUser("journal-max") && (runtimeConfig.Extensions.FakeAMPS.HasJournalMax || runtimeConfig.Extensions.FakeAMPS.JournalMax != 0) {
		*flagJournalMax = runtimeConfig.Extensions.FakeAMPS.JournalMax
	}
	if !options.FlagSetByUser("journal-disk") && runtimeConfig.Extensions.FakeAMPS.JournalDisk != "" {
		*flagJournalDisk = runtimeConfig.Extensions.FakeAMPS.JournalDisk
	}
	if !options.FlagSetByUser("log-conn") && runtimeConfig.Extensions.FakeAMPS.LogConnections != nil {
		*flagLogConn = *runtimeConfig.Extensions.FakeAMPS.LogConnections
	}
	if !options.FlagSetByUser("stats") && runtimeConfig.Extensions.FakeAMPS.LogStats != nil {
		*flagLogStats = *runtimeConfig.Extensions.FakeAMPS.LogStats
	}
	if !options.FlagSetByUser("stats-interval") && (runtimeConfig.Extensions.FakeAMPS.HasStatsInterval || runtimeConfig.Extensions.FakeAMPS.StatsInterval != 0) {
		*flagStatsIvl = runtimeConfig.Extensions.FakeAMPS.StatsInterval
	}
	if !options.FlagSetByUser("write-buf") && (runtimeConfig.Extensions.FakeAMPS.HasWriteBuffer || runtimeConfig.Extensions.FakeAMPS.WriteBuffer != 0) {
		*flagWriteBuf = runtimeConfig.Extensions.FakeAMPS.WriteBuffer
	}
	if !options.FlagSetByUser("read-buf") && (runtimeConfig.Extensions.FakeAMPS.HasReadBuffer || runtimeConfig.Extensions.FakeAMPS.ReadBuffer != 0) {
		*flagReadBuf = runtimeConfig.Extensions.FakeAMPS.ReadBuffer
	}
	if !options.FlagSetByUser("nodelay") && runtimeConfig.Extensions.FakeAMPS.NoDelay != nil {
		*flagNoDelay = *runtimeConfig.Extensions.FakeAMPS.NoDelay
	}
	if !options.FlagSetByUser("latency") && (runtimeConfig.Extensions.FakeAMPS.HasLatency || runtimeConfig.Extensions.FakeAMPS.Latency != 0) {
		*flagLatency = runtimeConfig.Extensions.FakeAMPS.Latency
	}
	if !options.FlagSetByUser("queue") && runtimeConfig.Extensions.FakeAMPS.QueueEnabled != nil {
		*flagQueue = *runtimeConfig.Extensions.FakeAMPS.QueueEnabled
	}
	if !options.FlagSetByUser("lease") && (runtimeConfig.Extensions.FakeAMPS.HasLease || runtimeConfig.Extensions.FakeAMPS.Lease != 0) {
		*flagLease = runtimeConfig.Extensions.FakeAMPS.Lease
	}
	if !options.FlagSetByUser("echo") && runtimeConfig.Extensions.FakeAMPS.Echo != nil {
		*flagEcho = *runtimeConfig.Extensions.FakeAMPS.Echo
	}
	if !options.FlagSetByUser("out-depth") && (runtimeConfig.Extensions.FakeAMPS.HasOutDepth || runtimeConfig.Extensions.FakeAMPS.OutDepth != 0) {
		*flagOutDepth = runtimeConfig.Extensions.FakeAMPS.OutDepth
	}
	if !options.FlagSetByUser("sow-gc-interval") && (runtimeConfig.Extensions.FakeAMPS.HasSOWGCInterval || runtimeConfig.Extensions.FakeAMPS.SOWGCInterval != 0) {
		*flagSOWGCIvl = runtimeConfig.Extensions.FakeAMPS.SOWGCInterval
	}
	if !options.FlagSetByUser("queue-lease-interval") && (runtimeConfig.Extensions.FakeAMPS.HasQueueLeaseInterval || runtimeConfig.Extensions.FakeAMPS.QueueLeaseInterval != 0) {
		*flagLeaseIvl = runtimeConfig.Extensions.FakeAMPS.QueueLeaseInterval
	}
	if !options.FlagSetByUser("benchmark-stability") && runtimeConfig.Extensions.FakeAMPS.BenchmarkStability != nil {
		*flagBenchStable = *runtimeConfig.Extensions.FakeAMPS.BenchmarkStability
	}
	if !options.FlagSetByUser("auth") && runtimeConfig.Extensions.FakeAMPS.Auth != "" {
		*flagAuth = runtimeConfig.Extensions.FakeAMPS.Auth
	}
	if !options.FlagSetByUser("auth-challenge") && runtimeConfig.Extensions.FakeAMPS.AuthChallenge != nil {
		*flagAuthChallenge = *runtimeConfig.Extensions.FakeAMPS.AuthChallenge
	}
	if !options.FlagSetByUser("peers") && runtimeConfig.Extensions.FakeAMPS.Peers != "" {
		*flagPeers = runtimeConfig.Extensions.FakeAMPS.Peers
	}
	if !options.FlagSetByUser("repl-id") && runtimeConfig.Extensions.FakeAMPS.ReplicationID != "" {
		*flagReplID = runtimeConfig.Extensions.FakeAMPS.ReplicationID
	}
	if !options.FlagSetByUser("redirect-uri") && runtimeConfig.Extensions.FakeAMPS.RedirectURI != "" {
		*flagRedirectURI = runtimeConfig.Extensions.FakeAMPS.RedirectURI
	}
	if !options.FlagSetByUser("sow-max") && (runtimeConfig.Extensions.FakeAMPS.HasSOWMax || runtimeConfig.Extensions.FakeAMPS.SOWMax != 0) {
		*flagSOWMax = runtimeConfig.Extensions.FakeAMPS.SOWMax
	}
	if !options.FlagSetByUser("sow-eviction") && runtimeConfig.Extensions.FakeAMPS.SOWEviction != "" {
		*flagSOWEviction = runtimeConfig.Extensions.FakeAMPS.SOWEviction
	}
	if !options.FlagSetByUser("sow-disk") && runtimeConfig.Extensions.FakeAMPS.SOWDisk != "" {
		*flagSOWDisk = runtimeConfig.Extensions.FakeAMPS.SOWDisk
	}
	if !options.FlagSetByUser("view") && len(runtimeConfig.Extensions.FakeAMPS.Views) > 0 {
		flagViews = append(viewFlags(nil), runtimeConfig.Extensions.FakeAMPS.Views...)
	}
	if !options.FlagSetByUser("action") && len(runtimeConfig.Extensions.FakeAMPS.Actions) > 0 {
		flagActions = append(actionFlags(nil), runtimeConfig.Extensions.FakeAMPS.Actions...)
	}

	return configureLoggingTargets(runtimeConfig.Logging)
}

func configBenchmarkStabilityOverrides(runtimeConfig ampsconfig.RuntimeConfig) benchmarkStabilityOverrides {
	return benchmarkStabilityOverrides{
		logConnSet:            runtimeConfig.Extensions.FakeAMPS.LogConnections != nil,
		logStatsSet:           runtimeConfig.Extensions.FakeAMPS.LogStats != nil,
		sowGCIntervalSet:      runtimeConfig.Extensions.FakeAMPS.HasSOWGCInterval,
		queueLeaseIntervalSet: runtimeConfig.Extensions.FakeAMPS.HasQueueLeaseInterval,
	}
}

func configureLoggingTargets(loggingConfig ampsconfig.LoggingConfig) error {
	if len(loggingConfig.Targets) == 0 {
		return nil
	}

	closeConfiguredLogOutputs()

	var outputs []io.Writer
	var closers []io.Closer
	for _, target := range loggingConfig.Targets {
		var protocol = strings.ToLower(strings.TrimSpace(target.Protocol))
		switch protocol {
		case "", "stderr":
			outputs = append(outputs, os.Stderr)
		case "stdout":
			outputs = append(outputs, os.Stdout)
		case "file":
			if strings.TrimSpace(target.FileName) == "" {
				return fmt.Errorf("logging target protocol=file requires FileName")
			}
			if err := os.MkdirAll(filepath.Dir(target.FileName), 0o755); err != nil {
				return fmt.Errorf("create log directory: %w", err)
			}
			var file, err = os.OpenFile(target.FileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
			if err != nil {
				return fmt.Errorf("open log file: %w", err)
			}
			outputs = append(outputs, file)
			closers = append(closers, file)
		default:
			return fmt.Errorf("unsupported logging target protocol %q", target.Protocol)
		}
	}

	configuredLogClosers = closers

	if len(outputs) == 1 {
		log.SetOutput(outputs[0])
		return nil
	}
	log.SetOutput(io.MultiWriter(outputs...))
	return nil
}

func closeConfiguredLogOutputs() {
	for _, closer := range configuredLogClosers {
		_ = closer.Close()
	}
	configuredLogClosers = nil
}
