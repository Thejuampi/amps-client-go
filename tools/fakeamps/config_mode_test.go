package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHandleConfigModesSampleWritesSampleConfig(t *testing.T) {
	var output bytes.Buffer

	var err = handleConfigModesWithWriter(startupOptions{
		Mode: startupModeSampleConfig,
	}, &output)
	if !errors.Is(err, errStartupHandled) {
		t.Fatalf("handleConfigModesWithWriter returned %v, want errStartupHandled", err)
	}
	if !strings.Contains(output.String(), "<AMPSConfig>") {
		t.Fatalf("sample config output missing AMPSConfig root: %q", output.String())
	}
}

func TestHandleConfigModesVerifyLoadsConfig(t *testing.T) {
	var tempDir = t.TempDir()
	var configPath = filepath.Join(tempDir, "config.xml")
	if err := os.WriteFile(configPath, []byte(strings.TrimSpace(`
<AMPSConfig>
    <Name>verify-mode</Name>
    <Transports>
        <Transport>
            <InetAddr>127.0.0.1:19000</InetAddr>
        </Transport>
    </Transports>
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

func TestHandleConfigModesDumpWritesExpandedConfig(t *testing.T) {
	var tempDir = t.TempDir()
	var includePath = filepath.Join(tempDir, "logging.xml")
	var configPath = filepath.Join(tempDir, "config.xml")

	if err := os.WriteFile(includePath, []byte("<Target><Protocol>stdout</Protocol></Target>"), 0o600); err != nil {
		t.Fatalf("WriteFile(include): %v", err)
	}
	if err := os.WriteFile(configPath, []byte(strings.TrimSpace(`
<AMPSConfig>
    <Logging>
        <Include>logging.xml</Include>
    </Logging>
</AMPSConfig>
`)), 0o600); err != nil {
		t.Fatalf("WriteFile(config): %v", err)
	}

	var output bytes.Buffer
	var err = handleConfigModesWithWriter(startupOptions{
		Mode:       startupModeDumpConfig,
		ConfigPath: configPath,
	}, &output)
	if !errors.Is(err, errStartupHandled) {
		t.Fatalf("handleConfigModesWithWriter returned %v, want errStartupHandled", err)
	}
	if !strings.Contains(output.String(), "<Target><Protocol>stdout</Protocol></Target>") {
		t.Fatalf("dump output missing expanded include: %q", output.String())
	}
}
