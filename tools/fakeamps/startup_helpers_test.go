package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Thejuampi/amps-client-go/internal/ampsconfig"
)

func TestSplitStartupFlag(t *testing.T) {
	var name, value, hasValue = splitStartupFlag("--config=test.xml")
	if name != "config" || value != "test.xml" || !hasValue {
		t.Fatalf("splitStartupFlag(config=test.xml) = (%q, %q, %v)", name, value, hasValue)
	}

	name, value, hasValue = splitStartupFlag("-sample-config")
	if name != "sample-config" || value != "" || hasValue {
		t.Fatalf("splitStartupFlag(sample-config) = (%q, %q, %v)", name, value, hasValue)
	}
}

func TestFlagConsumesNextValue(t *testing.T) {
	if !flagConsumesNextValue("config") {
		t.Fatalf("flagConsumesNextValue(config) = false, want true")
	}
	if flagConsumesNextValue("sample-config") {
		t.Fatalf("flagConsumesNextValue(sample-config) = true, want false")
	}
}

func TestParseStartupOptionsSupportsEqualsSyntaxAndMissingValues(t *testing.T) {
	var options, err = parseStartupOptions([]string{"--config=test.xml", "--admin=:8085"})
	if err != nil {
		t.Fatalf("parseStartupOptions returned error: %v", err)
	}
	if options.ConfigPath != "test.xml" {
		t.Fatalf("ConfigPath = %q, want test.xml", options.ConfigPath)
	}
	if !options.FlagSetByUser("admin") {
		t.Fatalf("admin should be marked as explicitly set for equals syntax")
	}

	_, missingErr := parseStartupOptions([]string{"--verify-config"})
	if missingErr == nil {
		t.Fatalf("parseStartupOptions should fail when verify-config has no path")
	}
}

func TestParseStartupOptionsKeepsFlagsAfterPositionalConfigPath(t *testing.T) {
	var options, err = parseStartupOptions([]string{"config.xml", "-admin", ":8085", "-fanout=false"})
	if err != nil {
		t.Fatalf("parseStartupOptions returned error: %v", err)
	}

	if options.ConfigPath != "config.xml" {
		t.Fatalf("ConfigPath = %q, want config.xml", options.ConfigPath)
	}
	if len(options.parseArgs) != 3 {
		t.Fatalf("len(parseArgs) = %d, want 3", len(options.parseArgs))
	}
	if options.parseArgs[0] != "-admin" || options.parseArgs[1] != ":8085" || options.parseArgs[2] != "-fanout=false" {
		t.Fatalf("parseArgs = %v, want trailing flags without positional config path", options.parseArgs)
	}
}

func TestHandleConfigModesRunLoadsConfigAndSetsEffectiveConfig(t *testing.T) {
	restore := snapshotRuntimeFlags()
	defer restore()

	var tempDir = t.TempDir()
	var configPath = filepath.Join(tempDir, "config.xml")
	if err := os.WriteFile(configPath, []byte(strings.TrimSpace(`
<AMPSConfig>
    <Name>run-mode</Name>
    <Transports>
        <Transport>
            <InetAddr>127.0.0.1:19077</InetAddr>
        </Transport>
    </Transports>
    <Admin>
        <InetAddr>127.0.0.1:18077</InetAddr>
    </Admin>
    <Extensions>
        <FakeAMPS>
            <Version>6.3.7.0</Version>
        </FakeAMPS>
    </Extensions>
</AMPSConfig>
`)), 0o600); err != nil {
		t.Fatalf("WriteFile(config): %v", err)
	}

	var output bytes.Buffer
	var err = handleConfigModesWithWriter(startupOptions{
		Mode:       startupModeRun,
		ConfigPath: configPath,
	}, &output)
	if err != nil {
		t.Fatalf("handleConfigModesWithWriter(run) error: %v", err)
	}
	if effectiveConfig == nil || effectiveConfig.Runtime.Name != "run-mode" {
		t.Fatalf("effectiveConfig = %+v, want loaded runtime config", effectiveConfig)
	}
	if *flagAddr != "127.0.0.1:19077" {
		t.Fatalf("flagAddr = %q, want config transport address", *flagAddr)
	}
	if *flagAdminAddr != "127.0.0.1:18077" {
		t.Fatalf("flagAdminAddr = %q, want config admin address", *flagAdminAddr)
	}
	if *flagVersion != "6.3.7.0" {
		t.Fatalf("flagVersion = %q, want config version", *flagVersion)
	}
}

func TestHandleConfigModesVerifyUsesConfiguredVersionWhenVersionFlagIsUnset(t *testing.T) {
	restore := snapshotRuntimeFlags()
	defer restore()

	var tempDir = t.TempDir()
	var configPath = filepath.Join(tempDir, "config.xml")
	if err := os.WriteFile(configPath, []byte(strings.TrimSpace(`
<AMPSConfig>
    <RequiredMinimumVersion>6.4.0.0</RequiredMinimumVersion>
    <Extensions>
        <FakeAMPS>
            <Version>6.4.0.0</Version>
        </FakeAMPS>
    </Extensions>
</AMPSConfig>
`)), 0o600); err != nil {
		t.Fatalf("WriteFile(config): %v", err)
	}

	var output bytes.Buffer
	var err = handleConfigModesWithWriter(startupOptions{
		Mode:       startupModeVerifyConfig,
		ConfigPath: configPath,
	}, &output)
	if !errors.Is(err, errStartupHandled) {
		t.Fatalf("handleConfigModesWithWriter returned %v, want errStartupHandled", err)
	}
	if !strings.Contains(output.String(), "config verified:") {
		t.Fatalf("verify output = %q, want verification message", output.String())
	}
}

func TestHandleConfigModesVerifyHonorsExplicitVersionOverride(t *testing.T) {
	restore := snapshotRuntimeFlags()
	defer restore()

	*flagVersion = "6.3.1.0"

	var tempDir = t.TempDir()
	var configPath = filepath.Join(tempDir, "config.xml")
	if err := os.WriteFile(configPath, []byte(strings.TrimSpace(`
<AMPSConfig>
    <RequiredMinimumVersion>6.4.0.0</RequiredMinimumVersion>
    <Extensions>
        <FakeAMPS>
            <Version>6.4.0.0</Version>
        </FakeAMPS>
    </Extensions>
</AMPSConfig>
`)), 0o600); err != nil {
		t.Fatalf("WriteFile(config): %v", err)
	}

	var output bytes.Buffer
	var err = handleConfigModesWithWriter(startupOptions{
		Mode:       startupModeVerifyConfig,
		ConfigPath: configPath,
		userSet: map[string]bool{
			"version": true,
		},
	}, &output)
	if err == nil {
		t.Fatalf("handleConfigModesWithWriter should fail when explicit version is below the required minimum")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "required minimum version") {
		t.Fatalf("handleConfigModesWithWriter error = %v, want minimum version validation", err)
	}
}

func TestHandleConfigModesVerifyUsesDefaultVersionWhenConfigVersionIsUnset(t *testing.T) {
	restore := snapshotRuntimeFlags()
	defer restore()

	*flagVersion = "6.3.1.0"

	var tempDir = t.TempDir()
	var configPath = filepath.Join(tempDir, "config.xml")
	if err := os.WriteFile(configPath, []byte(strings.TrimSpace(`
<AMPSConfig>
    <RequiredMinimumVersion>6.4.0.0</RequiredMinimumVersion>
</AMPSConfig>
`)), 0o600); err != nil {
		t.Fatalf("WriteFile(config): %v", err)
	}

	var output bytes.Buffer
	var err = handleConfigModesWithWriter(startupOptions{
		Mode:       startupModeVerifyConfig,
		ConfigPath: configPath,
	}, &output)
	if err == nil {
		t.Fatalf("handleConfigModesWithWriter should fail when the default runtime version is below the required minimum")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "required minimum version") {
		t.Fatalf("handleConfigModesWithWriter error = %v, want minimum version validation", err)
	}
}

func TestHandleConfigModesReturnsConfigErrors(t *testing.T) {
	var output bytes.Buffer
	var err = handleConfigModesWithWriter(startupOptions{
		Mode:       startupModeDumpConfig,
		ConfigPath: filepath.Join(t.TempDir(), "missing.xml"),
	}, &output)
	if err == nil {
		t.Fatalf("handleConfigModesWithWriter should return an error for missing config files")
	}
}

func TestConfigureLoggingTargetsRejectsInvalidTargets(t *testing.T) {
	var noFileErr = configureLoggingTargets(ampsconfig.LoggingConfig{
		Targets: []ampsconfig.LoggingTargetConfig{
			{Protocol: "file"},
		},
	})
	if noFileErr == nil {
		t.Fatalf("configureLoggingTargets should fail for file targets without FileName")
	}

	var protocolErr = configureLoggingTargets(ampsconfig.LoggingConfig{
		Targets: []ampsconfig.LoggingTargetConfig{
			{Protocol: "syslog"},
		},
	})
	if protocolErr == nil {
		t.Fatalf("configureLoggingTargets should fail for unsupported protocols")
	}
}

func TestEffectiveConfigSummary(t *testing.T) {
	oldEffectiveConfig := effectiveConfig
	effectiveConfig = nil
	if summary := effectiveConfigSummary(); summary != nil {
		t.Fatalf("effectiveConfigSummary() = %#v, want nil when no config was loaded", summary)
	}

	effectiveConfig = &ampsconfig.ExpandedConfig{
		Path: "config.xml",
		Runtime: ampsconfig.RuntimeConfig{
			Name:        "summary-instance",
			Group:       "group-a",
			Description: "configured",
			Transports: []ampsconfig.TransportConfig{
				{InetAddr: "127.0.0.1:19000"},
			},
			Admin: ampsconfig.AdminConfig{
				InetAddr: "127.0.0.1:8085",
			},
		},
	}
	defer func() {
		effectiveConfig = oldEffectiveConfig
	}()

	var summary = effectiveConfigSummary()
	if summary["source"] != "config.xml" {
		t.Fatalf("summary[source] = %v, want config.xml", summary["source"])
	}
	if summary["transport_count"] != 1 {
		t.Fatalf("summary[transport_count] = %v, want 1", summary["transport_count"])
	}
}
