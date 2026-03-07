package main

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestParseStartupOptionsSupportsConfigModes(t *testing.T) {
	var configPath = filepath.Join("testdata", "config.xml")

	var sampleOptions, err = parseStartupOptions([]string{"--sample-config"})
	if err != nil {
		t.Fatalf("parseStartupOptions(sample) error: %v", err)
	}
	if sampleOptions.Mode != startupModeSampleConfig {
		t.Fatalf("sample mode = %q, want %q", sampleOptions.Mode, startupModeSampleConfig)
	}

	var verifyOptions, verifyErr = parseStartupOptions([]string{"--verify-config", configPath})
	if verifyErr != nil {
		t.Fatalf("parseStartupOptions(verify) error: %v", verifyErr)
	}
	if verifyOptions.Mode != startupModeVerifyConfig {
		t.Fatalf("verify mode = %q, want %q", verifyOptions.Mode, startupModeVerifyConfig)
	}
	if verifyOptions.ConfigPath != configPath {
		t.Fatalf("verify ConfigPath = %q, want %q", verifyOptions.ConfigPath, configPath)
	}

	var dumpOptions, dumpErr = parseStartupOptions([]string{"--dump-config", configPath})
	if dumpErr != nil {
		t.Fatalf("parseStartupOptions(dump) error: %v", dumpErr)
	}
	if dumpOptions.Mode != startupModeDumpConfig {
		t.Fatalf("dump mode = %q, want %q", dumpOptions.Mode, startupModeDumpConfig)
	}

	var runOptions, runErr = parseStartupOptions([]string{"--config", configPath, "-admin", ":8085"})
	if runErr != nil {
		t.Fatalf("parseStartupOptions(run) error: %v", runErr)
	}
	if runOptions.Mode != startupModeRun {
		t.Fatalf("run mode = %q, want %q", runOptions.Mode, startupModeRun)
	}
	if runOptions.ConfigPath != configPath {
		t.Fatalf("run ConfigPath = %q, want %q", runOptions.ConfigPath, configPath)
	}
	if !runOptions.FlagSetByUser("admin") {
		t.Fatalf("admin should be marked as explicitly set")
	}
}

func TestParseStartupOptionsAcceptsPositionalConfigPath(t *testing.T) {
	var configPath = filepath.Join("testdata", "config.xml")

	var options, err = parseStartupOptions([]string{configPath})
	if err != nil {
		t.Fatalf("parseStartupOptions returned error: %v", err)
	}

	if options.ConfigPath != configPath {
		t.Fatalf("ConfigPath = %q, want %q", options.ConfigPath, configPath)
	}
	if options.Mode != startupModeRun {
		t.Fatalf("Mode = %q, want %q", options.Mode, startupModeRun)
	}
}

func TestParseStartupOptionsRejectsConflictingConfigInputs(t *testing.T) {
	_, err := parseStartupOptions([]string{"--config", "a.xml", "b.xml"})
	if err == nil {
		t.Fatalf("parseStartupOptions should reject conflicting config inputs")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "multiple config paths") {
		t.Fatalf("parseStartupOptions error = %v, want multiple config paths", err)
	}
}
