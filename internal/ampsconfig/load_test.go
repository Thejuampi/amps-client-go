package ampsconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadExpandsIncludeAndEnvironmentVariables(t *testing.T) {
	var tempDir = t.TempDir()
	var includePath = filepath.Join(tempDir, "logging.xml")
	var configPath = filepath.Join(tempDir, "config.xml")

	if err := os.WriteFile(includePath, []byte(strings.TrimSpace(`
<Target>
    <Protocol>file</Protocol>
    <FileName>${ENV_LOG}</FileName>
    <Level>info</Level>
</Target>
`)), 0o600); err != nil {
		t.Fatalf("WriteFile(include): %v", err)
	}

	if err := os.WriteFile(configPath, []byte(strings.TrimSpace(`
<AMPSConfig>
    <Name>test-instance</Name>
    <Transports>
        <Transport>
            <Name>json-tcp</Name>
            <Type>tcp</Type>
            <InetAddr>19001</InetAddr>
            <Protocol>amps</Protocol>
            <MessageType>json</MessageType>
        </Transport>
    </Transports>
    <Logging>
        <Include>logging.xml</Include>
    </Logging>
</AMPSConfig>
`)), 0o600); err != nil {
		t.Fatalf("WriteFile(config): %v", err)
	}

	var expanded, err = LoadFile(configPath, LoadOptions{
		Env: map[string]string{
			"ENV_LOG": filepath.Join(tempDir, "fakeamps.log"),
		},
		RuntimeVersion: "6.3.1.0",
	})
	if err != nil {
		t.Fatalf("LoadFile returned error: %v", err)
	}

	if expanded.Runtime.Name != "test-instance" {
		t.Fatalf("Runtime.Name = %q, want test-instance", expanded.Runtime.Name)
	}
	if len(expanded.Runtime.Transports) != 1 {
		t.Fatalf("len(Runtime.Transports) = %d, want 1", len(expanded.Runtime.Transports))
	}
	if expanded.Runtime.Transports[0].InetAddr != "19001" {
		t.Fatalf("Transport.InetAddr = %q, want 19001", expanded.Runtime.Transports[0].InetAddr)
	}
	if len(expanded.Runtime.Logging.Targets) != 1 {
		t.Fatalf("len(Runtime.Logging.Targets) = %d, want 1", len(expanded.Runtime.Logging.Targets))
	}
	if !strings.Contains(expanded.XML, "fakeamps.log") {
		t.Fatalf("expanded XML should contain expanded ENV_LOG path, got %q", expanded.XML)
	}
}

func TestLoadAddsIncludeCommentsWhenEnabled(t *testing.T) {
	var tempDir = t.TempDir()
	var includePath = filepath.Join(tempDir, "target.xml")
	var configPath = filepath.Join(tempDir, "config.xml")

	if err := os.WriteFile(includePath, []byte("<Target><Protocol>stdout</Protocol></Target>"), 0o600); err != nil {
		t.Fatalf("WriteFile(include): %v", err)
	}

	if err := os.WriteFile(configPath, []byte(strings.TrimSpace(`
<AMPSConfig>
    <Name>commented</Name>
    <ConfigIncludeCommentDefault>true</ConfigIncludeCommentDefault>
    <Logging>
        <Include>target.xml</Include>
    </Logging>
</AMPSConfig>
`)), 0o600); err != nil {
		t.Fatalf("WriteFile(config): %v", err)
	}

	var expanded, err = LoadFile(configPath, LoadOptions{RuntimeVersion: "6.3.1.0"})
	if err != nil {
		t.Fatalf("LoadFile returned error: %v", err)
	}

	if !strings.Contains(expanded.XML, "<!-- Start <Include>target.xml</Include> -->") {
		t.Fatalf("expanded XML should include start comment, got %q", expanded.XML)
	}
	if !strings.Contains(expanded.XML, "<!-- End <Include>target.xml</Include> -->") {
		t.Fatalf("expanded XML should include end comment, got %q", expanded.XML)
	}
}

func TestLoadRejectsIncludeCycle(t *testing.T) {
	var tempDir = t.TempDir()
	var configA = filepath.Join(tempDir, "a.xml")
	var configB = filepath.Join(tempDir, "b.xml")

	if err := os.WriteFile(configA, []byte("<AMPSConfig><Include>b.xml</Include></AMPSConfig>"), 0o600); err != nil {
		t.Fatalf("WriteFile(a): %v", err)
	}
	if err := os.WriteFile(configB, []byte("<Include>a.xml</Include>"), 0o600); err != nil {
		t.Fatalf("WriteFile(b): %v", err)
	}

	_, err := LoadFile(configA, LoadOptions{RuntimeVersion: "6.3.1.0"})
	if err == nil {
		t.Fatalf("LoadFile should fail for include cycle")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "include cycle") {
		t.Fatalf("LoadFile error = %v, want include cycle", err)
	}
}

func TestLoadParsesUnitsAndRequiredMinimumVersion(t *testing.T) {
	var tempDir = t.TempDir()
	var configPath = filepath.Join(tempDir, "config.xml")

	if err := os.WriteFile(configPath, []byte(strings.TrimSpace(`
<AMPSConfig>
    <Name>units</Name>
    <RequiredMinimumVersion>6.3.0.0</RequiredMinimumVersion>
    <Admin>
        <InetAddr>127.0.0.1:8085</InetAddr>
        <Interval>20s</Interval>
    </Admin>
    <Extensions>
        <FakeAMPS>
            <SOWGCInterval>5m</SOWGCInterval>
            <JournalMax>10k</JournalMax>
        </FakeAMPS>
    </Extensions>
</AMPSConfig>
`)), 0o600); err != nil {
		t.Fatalf("WriteFile(config): %v", err)
	}

	var expanded, err = LoadFile(configPath, LoadOptions{RuntimeVersion: "6.3.1.0"})
	if err != nil {
		t.Fatalf("LoadFile returned error: %v", err)
	}

	if expanded.Runtime.Admin.Interval != 20*time.Second {
		t.Fatalf("Runtime.Admin.Interval = %v, want 20s", expanded.Runtime.Admin.Interval)
	}
	if expanded.Runtime.Extensions.FakeAMPS.SOWGCInterval != 5*time.Minute {
		t.Fatalf("Runtime.Extensions.FakeAMPS.SOWGCInterval = %v, want 5m", expanded.Runtime.Extensions.FakeAMPS.SOWGCInterval)
	}
	if expanded.Runtime.Extensions.FakeAMPS.JournalMax != 10_000 {
		t.Fatalf("Runtime.Extensions.FakeAMPS.JournalMax = %d, want 10000", expanded.Runtime.Extensions.FakeAMPS.JournalMax)
	}
}

func TestLoadRejectsUnknownCustomModuleAndUDF(t *testing.T) {
	var tempDir = t.TempDir()
	var configPath = filepath.Join(tempDir, "config.xml")

	if err := os.WriteFile(configPath, []byte(strings.TrimSpace(`
<AMPSConfig>
    <Name>modules</Name>
    <Modules>
        <Module>
            <Name>custom-auth</Name>
            <Library>/tmp/custom-auth.so</Library>
        </Module>
    </Modules>
    <UserDefinedFunctions>
        <Function>
            <Name>CUSTOM</Name>
            <Module>custom-auth</Module>
            <Symbol>Custom</Symbol>
        </Function>
    </UserDefinedFunctions>
</AMPSConfig>
`)), 0o600); err != nil {
		t.Fatalf("WriteFile(config): %v", err)
	}

	_, err := LoadFile(configPath, LoadOptions{RuntimeVersion: "6.3.1.0"})
	if err == nil {
		t.Fatalf("LoadFile should fail for unsupported custom modules/UDFs")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "unsupported custom module") {
		t.Fatalf("LoadFile error = %v, want unsupported custom module", err)
	}
}

func TestSampleConfigContainsMinimalAMPSShape(t *testing.T) {
	var sample = SampleConfig()
	if !strings.Contains(sample, "<AMPSConfig>") {
		t.Fatalf("sample config should contain AMPSConfig root, got %q", sample)
	}
	if !strings.Contains(sample, "<Transports>") {
		t.Fatalf("sample config should contain Transports, got %q", sample)
	}
	if !strings.Contains(sample, "<Admin>") {
		t.Fatalf("sample config should contain Admin, got %q", sample)
	}
}
