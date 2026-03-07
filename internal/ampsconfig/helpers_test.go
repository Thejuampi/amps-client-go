package ampsconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExpandEnvironmentFallsBackToProcessEnvironment(t *testing.T) {
	const key = "AMPSCONFIG_TEST_ENV"
	if err := os.Setenv(key, "from-process-env"); err != nil {
		t.Fatalf("Setenv(%q): %v", key, err)
	}
	defer os.Unsetenv(key)

	var expanded = expandEnvironment("<Value>${AMPSCONFIG_TEST_ENV}</Value>", nil)
	if !strings.Contains(expanded, "from-process-env") {
		t.Fatalf("expandEnvironment = %q, want process env value", expanded)
	}
}

func TestDetectIncludeCommentDefaultAndOverride(t *testing.T) {
	var detected = detectIncludeCommentDefault("<AMPSConfig><ConfigIncludeCommentDefault>true</ConfigIncludeCommentDefault></AMPSConfig>", false)
	if !detected {
		t.Fatalf("detectIncludeCommentDefault should return true when explicitly enabled")
	}

	var override, ok = parseIncludeCommentOverride(` comment="false" `)
	if !ok {
		t.Fatalf("parseIncludeCommentOverride should detect attribute override")
	}
	if override {
		t.Fatalf("parseIncludeCommentOverride override = true, want false")
	}
}

func TestParseOptionalBool(t *testing.T) {
	var value, err = parseOptionalBool("TRUE")
	if err != nil {
		t.Fatalf("parseOptionalBool(TRUE) error: %v", err)
	}
	if value == nil || !*value {
		t.Fatalf("parseOptionalBool(TRUE) = %v, want true pointer", value)
	}

	var emptyValue, emptyErr = parseOptionalBool("")
	if emptyErr != nil {
		t.Fatalf("parseOptionalBool(empty) error: %v", emptyErr)
	}
	if emptyValue != nil {
		t.Fatalf("parseOptionalBool(empty) = %v, want nil", emptyValue)
	}

	_, invalidErr := parseOptionalBool("not-a-bool")
	if invalidErr == nil {
		t.Fatalf("parseOptionalBool should fail for invalid boolean text")
	}
}

func TestParseIntervalAndScaledInteger(t *testing.T) {
	var duration, durationErr = parseInterval("2D")
	if durationErr != nil {
		t.Fatalf("parseInterval(2D) error: %v", durationErr)
	}
	if duration != 48*time.Hour {
		t.Fatalf("parseInterval(2D) = %v, want 48h", duration)
	}

	var scaled, scaledErr = parseScaledInteger("3m")
	if scaledErr != nil {
		t.Fatalf("parseScaledInteger(3m) error: %v", scaledErr)
	}
	if scaled != 3_000_000 {
		t.Fatalf("parseScaledInteger(3m) = %d, want 3000000", scaled)
	}

	_, badDurationErr := parseInterval("bad")
	if badDurationErr == nil {
		t.Fatalf("parseInterval should fail for invalid duration")
	}
	_, badScaledErr := parseScaledInteger("bad")
	if badScaledErr == nil {
		t.Fatalf("parseScaledInteger should fail for invalid scaled integer")
	}
}

func TestBuildRuntimeConfigParsesExtensionOptions(t *testing.T) {
	var runtime, err = buildRuntimeConfig(xmlConfig{
		Name: "builder",
		Transports: xmlTransports{
			Items: []xmlTransport{
				{
					Type:        "SSL",
					Protocol:    "json",
					MessageType: "",
				},
			},
		},
		Extensions: xmlExtensions{
			FakeAMPS: xmlFakeAMPS{
				JournalMax:         "10k",
				StatsInterval:      "15s",
				WriteBuffer:        "64k",
				ReadBuffer:         "32k",
				Latency:            "25ms",
				Lease:              "45s",
				OutDepth:           "1024",
				SOWGCInterval:      "1m",
				QueueLeaseInterval: "2m",
				SOWMax:             "5k",
				Fanout:             "false",
			},
		},
	})
	if err != nil {
		t.Fatalf("buildRuntimeConfig returned error: %v", err)
	}

	if runtime.Transports[0].Type != "tcps" {
		t.Fatalf("transport type = %q, want tcps", runtime.Transports[0].Type)
	}
	if runtime.Transports[0].Protocol != "amps" {
		t.Fatalf("transport protocol = %q, want amps", runtime.Transports[0].Protocol)
	}
	if runtime.Transports[0].MessageType != "json" {
		t.Fatalf("transport message type = %q, want json", runtime.Transports[0].MessageType)
	}
	if !runtime.Extensions.FakeAMPS.HasJournalMax || runtime.Extensions.FakeAMPS.JournalMax != 10_000 {
		t.Fatalf("JournalMax = %d (has=%v), want 10000 set", runtime.Extensions.FakeAMPS.JournalMax, runtime.Extensions.FakeAMPS.HasJournalMax)
	}
	if !runtime.Extensions.FakeAMPS.HasStatsInterval || runtime.Extensions.FakeAMPS.StatsInterval != 15*time.Second {
		t.Fatalf("StatsInterval = %v (has=%v), want 15s set", runtime.Extensions.FakeAMPS.StatsInterval, runtime.Extensions.FakeAMPS.HasStatsInterval)
	}
	if !runtime.Extensions.FakeAMPS.HasWriteBuffer || runtime.Extensions.FakeAMPS.WriteBuffer != 64_000 {
		t.Fatalf("WriteBuffer = %d (has=%v), want 64000 set", runtime.Extensions.FakeAMPS.WriteBuffer, runtime.Extensions.FakeAMPS.HasWriteBuffer)
	}
	if !runtime.Extensions.FakeAMPS.HasReadBuffer || runtime.Extensions.FakeAMPS.ReadBuffer != 32_000 {
		t.Fatalf("ReadBuffer = %d (has=%v), want 32000 set", runtime.Extensions.FakeAMPS.ReadBuffer, runtime.Extensions.FakeAMPS.HasReadBuffer)
	}
	if !runtime.Extensions.FakeAMPS.HasLatency || runtime.Extensions.FakeAMPS.Latency != 25*time.Millisecond {
		t.Fatalf("Latency = %v (has=%v), want 25ms set", runtime.Extensions.FakeAMPS.Latency, runtime.Extensions.FakeAMPS.HasLatency)
	}
	if !runtime.Extensions.FakeAMPS.HasLease || runtime.Extensions.FakeAMPS.Lease != 45*time.Second {
		t.Fatalf("Lease = %v (has=%v), want 45s set", runtime.Extensions.FakeAMPS.Lease, runtime.Extensions.FakeAMPS.HasLease)
	}
	if !runtime.Extensions.FakeAMPS.HasOutDepth || runtime.Extensions.FakeAMPS.OutDepth != 1024 {
		t.Fatalf("OutDepth = %d (has=%v), want 1024 set", runtime.Extensions.FakeAMPS.OutDepth, runtime.Extensions.FakeAMPS.HasOutDepth)
	}
	if !runtime.Extensions.FakeAMPS.HasSOWGCInterval || runtime.Extensions.FakeAMPS.SOWGCInterval != time.Minute {
		t.Fatalf("SOWGCInterval = %v (has=%v), want 1m set", runtime.Extensions.FakeAMPS.SOWGCInterval, runtime.Extensions.FakeAMPS.HasSOWGCInterval)
	}
	if !runtime.Extensions.FakeAMPS.HasQueueLeaseInterval || runtime.Extensions.FakeAMPS.QueueLeaseInterval != 2*time.Minute {
		t.Fatalf("QueueLeaseInterval = %v (has=%v), want 2m set", runtime.Extensions.FakeAMPS.QueueLeaseInterval, runtime.Extensions.FakeAMPS.HasQueueLeaseInterval)
	}
	if !runtime.Extensions.FakeAMPS.HasSOWMax || runtime.Extensions.FakeAMPS.SOWMax != 5_000 {
		t.Fatalf("SOWMax = %d (has=%v), want 5000 set", runtime.Extensions.FakeAMPS.SOWMax, runtime.Extensions.FakeAMPS.HasSOWMax)
	}
	if runtime.Extensions.FakeAMPS.Fanout == nil || *runtime.Extensions.FakeAMPS.Fanout {
		t.Fatalf("Fanout = %v, want false pointer", runtime.Extensions.FakeAMPS.Fanout)
	}
}

func TestValidateRuntimeConfigRejectsUnsupportedStates(t *testing.T) {
	var tempDir = t.TempDir()
	var filePath = filepath.Join(tempDir, "artifact.txt")
	if err := os.WriteFile(filePath, []byte("not-a-directory"), 0o600); err != nil {
		t.Fatalf("WriteFile(filePath): %v", err)
	}

	var versionErr = validateRuntimeConfig(RuntimeConfig{
		RequiredMinimumVersion: "6.3.0.0",
	}, xmlUDFs{}, "6.2.9.0")
	if versionErr == nil {
		t.Fatalf("validateRuntimeConfig should fail when runtime version is too old")
	}

	var moduleErr = validateRuntimeConfig(RuntimeConfig{
		Modules: []ModuleConfig{{Name: "custom-module"}},
	}, xmlUDFs{}, "6.3.1.0")
	if moduleErr == nil {
		t.Fatalf("validateRuntimeConfig should fail for unknown built-in module name")
	}

	var slowClientErr = validateRuntimeConfig(RuntimeConfig{
		Extensions: ExtensionsConfig{
			FakeAMPS: FakeAMPSExtension{
				SlowClientPolicy: "mystery",
			},
		},
	}, xmlUDFs{}, "6.3.1.0")
	if slowClientErr == nil {
		t.Fatalf("validateRuntimeConfig should fail for unsupported slow client policy")
	}

	var crashDirErr = validateRuntimeConfig(RuntimeConfig{
		Extensions: ExtensionsConfig{
			FakeAMPS: FakeAMPSExtension{
				CrashArtifactDir: filePath,
			},
		},
	}, xmlUDFs{}, "6.3.1.0")
	if crashDirErr == nil {
		t.Fatalf("validateRuntimeConfig should fail when crash artifact path is not a directory")
	}
}

func TestIsSupportedBuiltInModule(t *testing.T) {
	if !isSupportedBuiltInModule("amps-default-authentication-module") {
		t.Fatalf("isSupportedBuiltInModule should accept documented amps-* module names")
	}
	if isSupportedBuiltInModule("custom-module") {
		t.Fatalf("isSupportedBuiltInModule should reject unknown custom module names")
	}
}
