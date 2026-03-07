package ampsconfig

import (
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

var includePattern = regexp.MustCompile(`(?is)<Include(?P<attrs>[^>]*)>(?P<path>.*?)</Include>`)
var envPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)
var includeCommentDefaultPattern = regexp.MustCompile(`(?is)<ConfigIncludeCommentDefault>\s*(true|false)\s*</ConfigIncludeCommentDefault>`)

type LoadOptions struct {
	Env            map[string]string
	RuntimeVersion string
}

type ExpandedConfig struct {
	Path    string
	XML     string
	Runtime RuntimeConfig
}

type RuntimeConfig struct {
	Name                        string
	Group                       string
	Description                 string
	RequiredMinimumVersion      string
	ConfigIncludeCommentDefault bool
	Transports                  []TransportConfig
	Logging                     LoggingConfig
	Admin                       AdminConfig
	Extensions                  ExtensionsConfig
	Modules                     []ModuleConfig
}

type TransportConfig struct {
	Name        string
	Type        string
	InetAddr    string
	Protocol    string
	MessageType string
}

type LoggingConfig struct {
	Targets []LoggingTargetConfig
}

type LoggingTargetConfig struct {
	Protocol string
	FileName string
	Level    string
}

type AdminConfig struct {
	InetAddr string
	Interval time.Duration
}

type ExtensionsConfig struct {
	FakeAMPS FakeAMPSExtension
}

type FakeAMPSExtension struct {
	ListenAddress         string
	Version               string
	Fanout                *bool
	SOWEnabled            *bool
	JournalEnabled        *bool
	JournalMax            int
	HasJournalMax         bool
	JournalDisk           string
	LogConnections        *bool
	LogStats              *bool
	StatsInterval         time.Duration
	HasStatsInterval      bool
	WriteBuffer           int
	HasWriteBuffer        bool
	ReadBuffer            int
	HasReadBuffer         bool
	NoDelay               *bool
	Latency               time.Duration
	HasLatency            bool
	QueueEnabled          *bool
	Lease                 time.Duration
	HasLease              bool
	Echo                  *bool
	OutDepth              int
	HasOutDepth           bool
	SOWGCInterval         time.Duration
	HasSOWGCInterval      bool
	QueueLeaseInterval    time.Duration
	HasQueueLeaseInterval bool
	BenchmarkStability    *bool
	Auth                  string
	AuthChallenge         *bool
	Peers                 string
	ReplicationID         string
	RedirectURI           string
	SOWMax                int
	HasSOWMax             bool
	SOWEviction           string
	SOWDisk               string
	Views                 []string
	Actions               []string
	ProcessName           string
	SlowClientPolicy      string
	CrashArtifactDir      string
	ExternalLibraryPath   string
}

type xmlConfig struct {
	XMLName                     xml.Name      `xml:"AMPSConfig"`
	Name                        string        `xml:"Name"`
	Group                       string        `xml:"Group"`
	Description                 string        `xml:"Description"`
	RequiredMinimumVersion      string        `xml:"RequiredMinimumVersion"`
	ConfigIncludeCommentDefault string        `xml:"ConfigIncludeCommentDefault"`
	Transports                  xmlTransports `xml:"Transports"`
	Logging                     xmlLogging    `xml:"Logging"`
	Admin                       xmlAdmin      `xml:"Admin"`
	Modules                     xmlModules    `xml:"Modules"`
	UserDefinedFunctions        xmlUDFs       `xml:"UserDefinedFunctions"`
	Extensions                  xmlExtensions `xml:"Extensions"`
}

type xmlTransports struct {
	Items []xmlTransport `xml:"Transport"`
}

type xmlTransport struct {
	Name        string `xml:"Name"`
	Type        string `xml:"Type"`
	InetAddr    string `xml:"InetAddr"`
	Protocol    string `xml:"Protocol"`
	MessageType string `xml:"MessageType"`
}

type xmlLogging struct {
	Targets []xmlLoggingTarget `xml:"Target"`
}

type xmlLoggingTarget struct {
	Protocol string `xml:"Protocol"`
	FileName string `xml:"FileName"`
	Level    string `xml:"Level"`
}

type xmlAdmin struct {
	InetAddr string `xml:"InetAddr"`
	Interval string `xml:"Interval"`
}

type xmlModules struct {
	Items []xmlModule `xml:"Module"`
}

type xmlModule struct {
	Name    string `xml:"Name"`
	Library string `xml:"Library"`
}

type xmlUDFs struct {
	Items []xmlUDF `xml:"Function"`
}

type xmlUDF struct {
	Name   string `xml:"Name"`
	Module string `xml:"Module"`
	Symbol string `xml:"Symbol"`
}

type xmlExtensions struct {
	FakeAMPS xmlFakeAMPS `xml:"FakeAMPS"`
}

type xmlFakeAMPS struct {
	ListenAddress       string   `xml:"ListenAddress"`
	Version             string   `xml:"Version"`
	Fanout              string   `xml:"Fanout"`
	SOWEnabled          string   `xml:"SOWEnabled"`
	JournalEnabled      string   `xml:"JournalEnabled"`
	JournalMax          string   `xml:"JournalMax"`
	JournalDisk         string   `xml:"JournalDisk"`
	LogConnections      string   `xml:"LogConnections"`
	LogStats            string   `xml:"LogStats"`
	StatsInterval       string   `xml:"StatsInterval"`
	WriteBuffer         string   `xml:"WriteBuffer"`
	ReadBuffer          string   `xml:"ReadBuffer"`
	NoDelay             string   `xml:"NoDelay"`
	Latency             string   `xml:"Latency"`
	QueueEnabled        string   `xml:"QueueEnabled"`
	Lease               string   `xml:"Lease"`
	Echo                string   `xml:"Echo"`
	OutDepth            string   `xml:"OutDepth"`
	SOWGCInterval       string   `xml:"SOWGCInterval"`
	QueueLeaseInterval  string   `xml:"QueueLeaseInterval"`
	BenchmarkStability  string   `xml:"BenchmarkStability"`
	Auth                string   `xml:"Auth"`
	AuthChallenge       string   `xml:"AuthChallenge"`
	Peers               string   `xml:"Peers"`
	ReplicationID       string   `xml:"ReplicationID"`
	RedirectURI         string   `xml:"RedirectURI"`
	SOWMax              string   `xml:"SOWMax"`
	SOWEviction         string   `xml:"SOWEviction"`
	SOWDisk             string   `xml:"SOWDisk"`
	Views               []string `xml:"View"`
	Actions             []string `xml:"Action"`
	ProcessName         string   `xml:"ProcessName"`
	SlowClientPolicy    string   `xml:"SlowClientPolicy"`
	CrashArtifactDir    string   `xml:"CrashArtifactDir"`
	ExternalLibraryPath string   `xml:"ExternalLibraryPath"`
}

func LoadFile(path string, opts LoadOptions) (*ExpandedConfig, error) {
	var absolutePath, err = filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve config path: %w", err)
	}

	var expandedXML string
	expandedXML, err = expandConfigFile(absolutePath, opts, map[string]bool{absolutePath: true}, false)
	if err != nil {
		return nil, err
	}

	var document xmlConfig
	if err := xml.Unmarshal([]byte(expandedXML), &document); err != nil {
		return nil, fmt.Errorf("parse expanded XML: %w", err)
	}

	var runtime, runtimeErr = buildRuntimeConfig(document)
	if runtimeErr != nil {
		return nil, runtimeErr
	}
	if err := validateRuntimeConfig(runtime, document.UserDefinedFunctions, opts.RuntimeVersion); err != nil {
		return nil, err
	}

	return &ExpandedConfig{
		Path:    absolutePath,
		XML:     expandedXML,
		Runtime: runtime,
	}, nil
}

func SampleConfig() string {
	return strings.TrimSpace(`
<AMPSConfig>
    <Name>fakeamps-instance</Name>
    <ConfigIncludeCommentDefault>false</ConfigIncludeCommentDefault>
    <Transports>
        <Transport>
            <Name>json-tcp</Name>
            <Type>tcp</Type>
            <InetAddr>127.0.0.1:19000</InetAddr>
            <Protocol>amps</Protocol>
            <MessageType>json</MessageType>
        </Transport>
    </Transports>
    <Admin>
        <InetAddr>127.0.0.1:8085</InetAddr>
        <Interval>5s</Interval>
    </Admin>
    <Logging>
        <Target>
            <Protocol>stderr</Protocol>
            <Level>info</Level>
        </Target>
    </Logging>
    <Extensions>
        <FakeAMPS>
            <SOWEnabled>true</SOWEnabled>
            <JournalEnabled>true</JournalEnabled>
            <JournalMax>1000000</JournalMax>
            <QueueEnabled>true</QueueEnabled>
            <SOWGCInterval>30s</SOWGCInterval>
            <QueueLeaseInterval>5s</QueueLeaseInterval>
        </FakeAMPS>
    </Extensions>
</AMPSConfig>
`)
}

func expandConfigFile(path string, opts LoadOptions, stack map[string]bool, includeComments bool) (string, error) {
	var contentBytes, err = os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read config %s: %w", path, err)
	}

	var content = expandEnvironment(string(contentBytes), opts.Env)
	var commentDefault = detectIncludeCommentDefault(content, includeComments)
	var expandErr error
	var expanded = includePattern.ReplaceAllStringFunc(content, func(match string) string {
		if expandErr != nil {
			return ""
		}

		var parts = includePattern.FindStringSubmatch(match)
		if len(parts) == 0 {
			return match
		}

		var attrs = parts[1]
		var includePath = strings.TrimSpace(parts[2])
		var resolvedPath = includePath
		if !filepath.IsAbs(resolvedPath) {
			resolvedPath = filepath.Join(filepath.Dir(path), resolvedPath)
		}
		resolvedPath, expandErr = filepath.Abs(resolvedPath)
		if expandErr != nil {
			return ""
		}
		if stack[resolvedPath] {
			expandErr = fmt.Errorf("include cycle detected at %s", resolvedPath)
			return ""
		}

		var childStack = clonePathStack(stack)
		childStack[resolvedPath] = true

		var childComments = commentDefault
		if override, ok := parseIncludeCommentOverride(attrs); ok {
			childComments = override
		}

		var child string
		child, expandErr = expandConfigFile(resolvedPath, opts, childStack, childComments)
		if expandErr != nil {
			return ""
		}

		if !childComments {
			return child
		}
		return "<!-- Start " + strings.TrimSpace(match) + " -->\n" + child + "\n<!-- End " + strings.TrimSpace(match) + " -->"
	})
	if expandErr != nil {
		return "", expandErr
	}

	return expanded, nil
}

func clonePathStack(input map[string]bool) map[string]bool {
	var output = make(map[string]bool, len(input)+1)
	for key, value := range input {
		output[key] = value
	}
	return output
}

func expandEnvironment(content string, env map[string]string) string {
	return envPattern.ReplaceAllStringFunc(content, func(match string) string {
		var parts = envPattern.FindStringSubmatch(match)
		if len(parts) != 2 {
			return match
		}
		if env != nil {
			if value, ok := env[parts[1]]; ok {
				return value
			}
		}
		if value, ok := os.LookupEnv(parts[1]); ok {
			return value
		}
		return ""
	})
}

func detectIncludeCommentDefault(content string, fallback bool) bool {
	var match = includeCommentDefaultPattern.FindStringSubmatch(content)
	if len(match) != 2 {
		return fallback
	}
	return strings.EqualFold(strings.TrimSpace(match[1]), "true")
}

func parseIncludeCommentOverride(attrs string) (bool, bool) {
	var lower = strings.ToLower(attrs)
	switch {
	case strings.Contains(lower, `comment="true"`), strings.Contains(lower, `comment='true'`):
		return true, true
	case strings.Contains(lower, `comment="false"`), strings.Contains(lower, `comment='false'`):
		return false, true
	case strings.Contains(lower, `includecomment="true"`), strings.Contains(lower, `includecomment='true'`):
		return true, true
	case strings.Contains(lower, `includecomment="false"`), strings.Contains(lower, `includecomment='false'`):
		return false, true
	default:
		return false, false
	}
}

func buildRuntimeConfig(document xmlConfig) (RuntimeConfig, error) {
	var runtime = RuntimeConfig{
		Name:                        strings.TrimSpace(document.Name),
		Group:                       strings.TrimSpace(document.Group),
		Description:                 strings.TrimSpace(document.Description),
		RequiredMinimumVersion:      strings.TrimSpace(document.RequiredMinimumVersion),
		ConfigIncludeCommentDefault: strings.EqualFold(strings.TrimSpace(document.ConfigIncludeCommentDefault), "true"),
		Transports:                  make([]TransportConfig, 0, len(document.Transports.Items)),
		Logging: LoggingConfig{
			Targets: make([]LoggingTargetConfig, 0, len(document.Logging.Targets)),
		},
		Modules: make([]ModuleConfig, 0, len(document.Modules.Items)),
	}

	for _, transport := range document.Transports.Items {
		runtime.Transports = append(runtime.Transports, TransportConfig{
			Name:        strings.TrimSpace(transport.Name),
			Type:        normalizeTransportType(transport.Type),
			InetAddr:    strings.TrimSpace(transport.InetAddr),
			Protocol:    normalizeProtocolName(transport.Protocol),
			MessageType: normalizeMessageType(transport.MessageType),
		})
	}

	for _, target := range document.Logging.Targets {
		runtime.Logging.Targets = append(runtime.Logging.Targets, LoggingTargetConfig{
			Protocol: strings.TrimSpace(target.Protocol),
			FileName: strings.TrimSpace(target.FileName),
			Level:    strings.TrimSpace(target.Level),
		})
	}

	if strings.TrimSpace(document.Admin.Interval) != "" {
		var interval, err = parseInterval(document.Admin.Interval)
		if err != nil {
			return RuntimeConfig{}, fmt.Errorf("parse admin interval: %w", err)
		}
		runtime.Admin.Interval = interval
	}
	runtime.Admin.InetAddr = strings.TrimSpace(document.Admin.InetAddr)

	for _, module := range document.Modules.Items {
		runtime.Modules = append(runtime.Modules, ModuleConfig{
			Name:    strings.TrimSpace(module.Name),
			Library: strings.TrimSpace(module.Library),
		})
	}

	var extension xmlFakeAMPS = document.Extensions.FakeAMPS
	runtime.Extensions.FakeAMPS.ListenAddress = strings.TrimSpace(extension.ListenAddress)
	runtime.Extensions.FakeAMPS.Version = strings.TrimSpace(extension.Version)
	runtime.Extensions.FakeAMPS.JournalDisk = strings.TrimSpace(extension.JournalDisk)
	runtime.Extensions.FakeAMPS.Auth = strings.TrimSpace(extension.Auth)
	runtime.Extensions.FakeAMPS.Peers = strings.TrimSpace(extension.Peers)
	runtime.Extensions.FakeAMPS.ReplicationID = strings.TrimSpace(extension.ReplicationID)
	runtime.Extensions.FakeAMPS.RedirectURI = strings.TrimSpace(extension.RedirectURI)
	runtime.Extensions.FakeAMPS.SOWEviction = strings.TrimSpace(extension.SOWEviction)
	runtime.Extensions.FakeAMPS.SOWDisk = strings.TrimSpace(extension.SOWDisk)
	runtime.Extensions.FakeAMPS.Views = append(runtime.Extensions.FakeAMPS.Views, extension.Views...)
	runtime.Extensions.FakeAMPS.Actions = append(runtime.Extensions.FakeAMPS.Actions, extension.Actions...)
	runtime.Extensions.FakeAMPS.ProcessName = strings.TrimSpace(extension.ProcessName)
	runtime.Extensions.FakeAMPS.SlowClientPolicy = strings.TrimSpace(extension.SlowClientPolicy)
	runtime.Extensions.FakeAMPS.CrashArtifactDir = strings.TrimSpace(extension.CrashArtifactDir)
	runtime.Extensions.FakeAMPS.ExternalLibraryPath = strings.TrimSpace(extension.ExternalLibraryPath)

	if value, err := parseOptionalBool(extension.Fanout); err != nil {
		return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS Fanout: %w", err)
	} else {
		runtime.Extensions.FakeAMPS.Fanout = value
	}
	if value, err := parseOptionalBool(extension.SOWEnabled); err != nil {
		return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS SOWEnabled: %w", err)
	} else {
		runtime.Extensions.FakeAMPS.SOWEnabled = value
	}
	if value, err := parseOptionalBool(extension.JournalEnabled); err != nil {
		return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS JournalEnabled: %w", err)
	} else {
		runtime.Extensions.FakeAMPS.JournalEnabled = value
	}
	if value, err := parseOptionalBool(extension.LogConnections); err != nil {
		return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS LogConnections: %w", err)
	} else {
		runtime.Extensions.FakeAMPS.LogConnections = value
	}
	if value, err := parseOptionalBool(extension.LogStats); err != nil {
		return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS LogStats: %w", err)
	} else {
		runtime.Extensions.FakeAMPS.LogStats = value
	}
	if value, err := parseOptionalBool(extension.NoDelay); err != nil {
		return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS NoDelay: %w", err)
	} else {
		runtime.Extensions.FakeAMPS.NoDelay = value
	}
	if value, err := parseOptionalBool(extension.QueueEnabled); err != nil {
		return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS QueueEnabled: %w", err)
	} else {
		runtime.Extensions.FakeAMPS.QueueEnabled = value
	}
	if value, err := parseOptionalBool(extension.Echo); err != nil {
		return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS Echo: %w", err)
	} else {
		runtime.Extensions.FakeAMPS.Echo = value
	}
	if value, err := parseOptionalBool(extension.BenchmarkStability); err != nil {
		return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS BenchmarkStability: %w", err)
	} else {
		runtime.Extensions.FakeAMPS.BenchmarkStability = value
	}
	if value, err := parseOptionalBool(extension.AuthChallenge); err != nil {
		return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS AuthChallenge: %w", err)
	} else {
		runtime.Extensions.FakeAMPS.AuthChallenge = value
	}

	if strings.TrimSpace(extension.StatsInterval) != "" {
		var interval, err = parseInterval(extension.StatsInterval)
		if err != nil {
			return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS StatsInterval: %w", err)
		}
		runtime.Extensions.FakeAMPS.StatsInterval = interval
		runtime.Extensions.FakeAMPS.HasStatsInterval = true
	}
	if strings.TrimSpace(extension.Latency) != "" {
		var latency, err = parseInterval(extension.Latency)
		if err != nil {
			return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS Latency: %w", err)
		}
		runtime.Extensions.FakeAMPS.Latency = latency
		runtime.Extensions.FakeAMPS.HasLatency = true
	}
	if strings.TrimSpace(extension.Lease) != "" {
		var lease, err = parseInterval(extension.Lease)
		if err != nil {
			return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS Lease: %w", err)
		}
		runtime.Extensions.FakeAMPS.Lease = lease
		runtime.Extensions.FakeAMPS.HasLease = true
	}
	if strings.TrimSpace(extension.SOWGCInterval) != "" {
		var interval, err = parseInterval(extension.SOWGCInterval)
		if err != nil {
			return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS SOWGCInterval: %w", err)
		}
		runtime.Extensions.FakeAMPS.SOWGCInterval = interval
		runtime.Extensions.FakeAMPS.HasSOWGCInterval = true
	}
	if strings.TrimSpace(extension.QueueLeaseInterval) != "" {
		var interval, err = parseInterval(extension.QueueLeaseInterval)
		if err != nil {
			return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS QueueLeaseInterval: %w", err)
		}
		runtime.Extensions.FakeAMPS.QueueLeaseInterval = interval
		runtime.Extensions.FakeAMPS.HasQueueLeaseInterval = true
	}

	if strings.TrimSpace(extension.JournalMax) != "" {
		var value, err = parseScaledInteger(extension.JournalMax)
		if err != nil {
			return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS JournalMax: %w", err)
		}
		runtime.Extensions.FakeAMPS.JournalMax = value
		runtime.Extensions.FakeAMPS.HasJournalMax = true
	}
	if strings.TrimSpace(extension.WriteBuffer) != "" {
		var value, err = parseScaledInteger(extension.WriteBuffer)
		if err != nil {
			return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS WriteBuffer: %w", err)
		}
		runtime.Extensions.FakeAMPS.WriteBuffer = value
		runtime.Extensions.FakeAMPS.HasWriteBuffer = true
	}
	if strings.TrimSpace(extension.ReadBuffer) != "" {
		var value, err = parseScaledInteger(extension.ReadBuffer)
		if err != nil {
			return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS ReadBuffer: %w", err)
		}
		runtime.Extensions.FakeAMPS.ReadBuffer = value
		runtime.Extensions.FakeAMPS.HasReadBuffer = true
	}
	if strings.TrimSpace(extension.OutDepth) != "" {
		var value, err = parseScaledInteger(extension.OutDepth)
		if err != nil {
			return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS OutDepth: %w", err)
		}
		runtime.Extensions.FakeAMPS.OutDepth = value
		runtime.Extensions.FakeAMPS.HasOutDepth = true
	}
	if strings.TrimSpace(extension.SOWMax) != "" {
		var value, err = parseScaledInteger(extension.SOWMax)
		if err != nil {
			return RuntimeConfig{}, fmt.Errorf("parse FakeAMPS SOWMax: %w", err)
		}
		runtime.Extensions.FakeAMPS.SOWMax = value
		runtime.Extensions.FakeAMPS.HasSOWMax = true
	}

	return runtime, nil
}

type ModuleConfig struct {
	Name    string
	Library string
}

func validateRuntimeConfig(runtime RuntimeConfig, udfs xmlUDFs, runtimeVersion string) error {
	if runtime.RequiredMinimumVersion != "" && runtimeVersion != "" {
		if amps.ConvertVersionToNumber(runtimeVersion) < amps.ConvertVersionToNumber(runtime.RequiredMinimumVersion) {
			return fmt.Errorf("runtime version %s is below required minimum version %s", runtimeVersion, runtime.RequiredMinimumVersion)
		}
	}

	for _, module := range runtime.Modules {
		if module.Library != "" {
			return fmt.Errorf("unsupported custom module %q with library %q", module.Name, module.Library)
		}
		if module.Name != "" && !isSupportedBuiltInModule(module.Name) {
			return fmt.Errorf("unknown built-in module %q", module.Name)
		}
	}

	if len(udfs.Items) > 0 {
		return fmt.Errorf("unsupported custom module-backed user defined function %q", strings.TrimSpace(udfs.Items[0].Name))
	}

	if runtime.Extensions.FakeAMPS.ExternalLibraryPath != "" {
		if _, err := os.Stat(runtime.Extensions.FakeAMPS.ExternalLibraryPath); err != nil {
			return fmt.Errorf("external library path %q is unavailable: %w", runtime.Extensions.FakeAMPS.ExternalLibraryPath, err)
		}
	}
	if runtime.Extensions.FakeAMPS.CrashArtifactDir != "" {
		var info, err = os.Stat(runtime.Extensions.FakeAMPS.CrashArtifactDir)
		if err != nil {
			return fmt.Errorf("crash artifact directory %q is unavailable: %w", runtime.Extensions.FakeAMPS.CrashArtifactDir, err)
		}
		if !info.IsDir() {
			return fmt.Errorf("crash artifact directory %q is not a directory", runtime.Extensions.FakeAMPS.CrashArtifactDir)
		}
	}

	switch strings.ToLower(runtime.Extensions.FakeAMPS.SlowClientPolicy) {
	case "", "disconnect", "drop-oldest", "block":
	default:
		return fmt.Errorf("unsupported slow client policy %q", runtime.Extensions.FakeAMPS.SlowClientPolicy)
	}

	return nil
}

func isSupportedBuiltInModule(name string) bool {
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(name)), "amps-") {
		return true
	}

	switch strings.ToLower(strings.TrimSpace(name)) {
	case "":
		return true
	case "amps-default-authentication-module":
		return true
	case "amps-default-entitlement-module":
		return true
	default:
		return false
	}
}

func normalizeTransportType(value string) string {
	var normalized = strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "":
		return "tcp"
	case "ssl":
		return "tcps"
	default:
		return normalized
	}
}

func normalizeProtocolName(value string) string {
	var normalized = strings.ToLower(strings.TrimSpace(value))
	if normalized == "" || normalized == "json" {
		return "amps"
	}
	return normalized
}

func normalizeMessageType(value string) string {
	var normalized = strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return "json"
	}
	return normalized
}

func parseOptionalBool(value string) (*bool, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parsed, err := strconv.ParseBool(strings.ToLower(value))
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func parseInterval(value string) (time.Duration, error) {
	var normalized = strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return 0, nil
	}
	if normalized == "0" {
		return 0, nil
	}
	if strings.HasSuffix(normalized, "d") {
		var numeric = strings.TrimSuffix(normalized, "d")
		var days, err = strconv.ParseFloat(numeric, 64)
		if err != nil {
			return 0, err
		}
		return time.Duration(days * float64(24*time.Hour)), nil
	}
	return time.ParseDuration(normalized)
}

func parseScaledInteger(value string) (int, error) {
	var normalized = strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return 0, nil
	}

	var multiplier = 1
	switch {
	case strings.HasSuffix(normalized, "k"):
		multiplier = 1_000
		normalized = strings.TrimSuffix(normalized, "k")
	case strings.HasSuffix(normalized, "m"):
		multiplier = 1_000_000
		normalized = strings.TrimSuffix(normalized, "m")
	case strings.HasSuffix(normalized, "g"):
		multiplier = 1_000_000_000
		normalized = strings.TrimSuffix(normalized, "g")
	}

	var parsed, err = strconv.Atoi(normalized)
	if err != nil {
		return 0, err
	}
	return parsed * multiplier, nil
}
