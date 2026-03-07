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

func TestLoadParsesAdminDashboardConfiguration(t *testing.T) {
	var tempDir = t.TempDir()
	var certPath = filepath.Join(tempDir, "admin.crt")
	var keyPath = filepath.Join(tempDir, "admin.key")
	var configPath = filepath.Join(tempDir, "config.xml")

	if err := os.WriteFile(certPath, []byte("cert"), 0o600); err != nil {
		t.Fatalf("WriteFile(cert): %v", err)
	}
	if err := os.WriteFile(keyPath, []byte("key"), 0o600); err != nil {
		t.Fatalf("WriteFile(key): %v", err)
	}

	if err := os.WriteFile(configPath, []byte(strings.TrimSpace(`
<AMPSConfig>
    <Name>dashboard-instance</Name>
    <Transports>
        <Transport>
            <Name>json-tcp</Name>
            <Type>tcp</Type>
            <InetAddr>127.0.0.1:19000</InetAddr>
        </Transport>
    </Transports>
    <Admin>
        <InetAddr>127.0.0.1:8085</InetAddr>
        <Interval>5s</Interval>
        <FileName>${ADMIN_FILE}</FileName>
        <ExternalInetAddr>dashboard.example.com:8443</ExternalInetAddr>
        <SQLTransport>json-tcp</SQLTransport>
        <Authentication>Basic realm="FakeAMPS"</Authentication>
        <Entitlement>role-based</Entitlement>
        <AnonymousPaths>
            <Path>/</Path>
            <Path>/assets</Path>
        </AnonymousPaths>
        <SessionOptions>
            <Option>SameSite=Lax</Option>
            <Option>Secure=true</Option>
        </SessionOptions>
        <Header>X-Frame-Options: DENY</Header>
        <Header>Cache-Control: no-store</Header>
        <Certificate>`+certPath+`</Certificate>
        <PrivateKey>`+keyPath+`</PrivateKey>
        <Ciphers>
            <Cipher>TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256</Cipher>
        </Ciphers>
    </Admin>
    <Extensions>
        <FakeAMPS>
            <AdminUsers>
                <User>
                    <Username>viewer</Username>
                    <Password>viewer-pass</Password>
                    <Role>viewer</Role>
                </User>
                <User>
                    <Username>operator</Username>
                    <Password>operator-pass</Password>
                    <Role>operator</Role>
                </User>
            </AdminUsers>
        </FakeAMPS>
    </Extensions>
</AMPSConfig>
`)), 0o600); err != nil {
		t.Fatalf("WriteFile(config): %v", err)
	}

	var expanded, err = LoadFile(configPath, LoadOptions{
		Env: map[string]string{
			"ADMIN_FILE": filepath.Join(tempDir, "admin-history.json"),
		},
		RuntimeVersion: "6.3.1.0",
	})
	if err != nil {
		t.Fatalf("LoadFile returned error: %v", err)
	}

	if expanded.Runtime.Admin.FileName != filepath.Join(tempDir, "admin-history.json") {
		t.Fatalf("Runtime.Admin.FileName = %q, want expanded admin history path", expanded.Runtime.Admin.FileName)
	}
	if expanded.Runtime.Admin.ExternalInetAddr != "dashboard.example.com:8443" {
		t.Fatalf("Runtime.Admin.ExternalInetAddr = %q, want dashboard.example.com:8443", expanded.Runtime.Admin.ExternalInetAddr)
	}
	if expanded.Runtime.Admin.SQLTransport != "json-tcp" {
		t.Fatalf("Runtime.Admin.SQLTransport = %q, want json-tcp", expanded.Runtime.Admin.SQLTransport)
	}
	if expanded.Runtime.Admin.Authentication != `Basic realm="FakeAMPS"` {
		t.Fatalf("Runtime.Admin.Authentication = %q, want Basic realm", expanded.Runtime.Admin.Authentication)
	}
	if len(expanded.Runtime.Admin.AnonymousPaths) != 2 {
		t.Fatalf("len(Runtime.Admin.AnonymousPaths) = %d, want 2", len(expanded.Runtime.Admin.AnonymousPaths))
	}
	if len(expanded.Runtime.Admin.SessionOptions) != 2 {
		t.Fatalf("len(Runtime.Admin.SessionOptions) = %d, want 2", len(expanded.Runtime.Admin.SessionOptions))
	}
	if len(expanded.Runtime.Admin.Headers) != 2 {
		t.Fatalf("len(Runtime.Admin.Headers) = %d, want 2", len(expanded.Runtime.Admin.Headers))
	}
	if expanded.Runtime.Admin.Certificate != certPath {
		t.Fatalf("Runtime.Admin.Certificate = %q, want %q", expanded.Runtime.Admin.Certificate, certPath)
	}
	if expanded.Runtime.Admin.PrivateKey != keyPath {
		t.Fatalf("Runtime.Admin.PrivateKey = %q, want %q", expanded.Runtime.Admin.PrivateKey, keyPath)
	}
	if len(expanded.Runtime.Admin.Ciphers) != 1 || expanded.Runtime.Admin.Ciphers[0] != "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256" {
		t.Fatalf("Runtime.Admin.Ciphers = %v, want TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", expanded.Runtime.Admin.Ciphers)
	}
	if len(expanded.Runtime.Extensions.FakeAMPS.AdminUsers) != 2 {
		t.Fatalf("len(Runtime.Extensions.FakeAMPS.AdminUsers) = %d, want 2", len(expanded.Runtime.Extensions.FakeAMPS.AdminUsers))
	}
}

func TestLoadRejectsTLS13OnlyAdminCipherSuites(t *testing.T) {
	var tempDir = t.TempDir()
	var certPath = filepath.Join(tempDir, "admin.crt")
	var keyPath = filepath.Join(tempDir, "admin.key")
	var configPath = filepath.Join(tempDir, "config.xml")

	if err := os.WriteFile(certPath, []byte("cert"), 0o600); err != nil {
		t.Fatalf("WriteFile(cert): %v", err)
	}
	if err := os.WriteFile(keyPath, []byte("key"), 0o600); err != nil {
		t.Fatalf("WriteFile(key): %v", err)
	}

	if err := os.WriteFile(configPath, []byte(strings.TrimSpace(`
<AMPSConfig>
    <Transports>
        <Transport>
            <Name>json-tcp</Name>
            <Type>tcp</Type>
            <InetAddr>127.0.0.1:19000</InetAddr>
        </Transport>
    </Transports>
    <Admin>
        <InetAddr>127.0.0.1:8085</InetAddr>
        <Certificate>`+certPath+`</Certificate>
        <PrivateKey>`+keyPath+`</PrivateKey>
        <Ciphers>
            <Cipher>TLS_AES_128_GCM_SHA256</Cipher>
        </Ciphers>
    </Admin>
</AMPSConfig>
`)), 0o600); err != nil {
		t.Fatalf("WriteFile(config): %v", err)
	}

	_, err := LoadFile(configPath, LoadOptions{RuntimeVersion: "6.3.1.0"})
	if err == nil {
		t.Fatalf("LoadFile should fail for TLS 1.3-only admin cipher suites")
	}
	if !strings.Contains(err.Error(), "TLS 1.3") {
		t.Fatalf("LoadFile error = %v, want TLS 1.3 validation", err)
	}
}

func TestLoadRejectsAdminAuthenticationWithoutUsers(t *testing.T) {
	var tempDir = t.TempDir()
	var configPath = filepath.Join(tempDir, "config.xml")

	if err := os.WriteFile(configPath, []byte(strings.TrimSpace(`
<AMPSConfig>
    <Name>dashboard-auth</Name>
    <Transports>
        <Transport>
            <Name>json-tcp</Name>
            <Type>tcp</Type>
            <InetAddr>127.0.0.1:19000</InetAddr>
        </Transport>
    </Transports>
    <Admin>
        <InetAddr>127.0.0.1:8085</InetAddr>
        <Authentication>Basic realm="FakeAMPS"</Authentication>
    </Admin>
</AMPSConfig>
`)), 0o600); err != nil {
		t.Fatalf("WriteFile(config): %v", err)
	}

	_, err := LoadFile(configPath, LoadOptions{RuntimeVersion: "6.3.1.0"})
	if err == nil {
		t.Fatalf("LoadFile should fail when admin authentication is configured without users")
	}
	if !strings.Contains(err.Error(), "admin authentication requires at least one admin user") {
		t.Fatalf("LoadFile error = %v, want admin user validation", err)
	}
}

func TestLoadRejectsPartialAdminTLSConfiguration(t *testing.T) {
	var tempDir = t.TempDir()
	var certPath = filepath.Join(tempDir, "admin.crt")
	var keyPath = filepath.Join(tempDir, "admin.key")

	if err := os.WriteFile(certPath, []byte("cert"), 0o600); err != nil {
		t.Fatalf("WriteFile(cert): %v", err)
	}
	if err := os.WriteFile(keyPath, []byte("key"), 0o600); err != nil {
		t.Fatalf("WriteFile(key): %v", err)
	}

	for _, testCase := range []struct {
		name string
		xml  string
	}{
		{
			name: "certificate only",
			xml: `
<AMPSConfig>
    <Admin>
        <InetAddr>127.0.0.1:8085</InetAddr>
        <Certificate>` + certPath + `</Certificate>
    </Admin>
</AMPSConfig>
`,
		},
		{
			name: "private key only",
			xml: `
<AMPSConfig>
    <Admin>
        <InetAddr>127.0.0.1:8085</InetAddr>
        <PrivateKey>` + keyPath + `</PrivateKey>
    </Admin>
</AMPSConfig>
`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var configPath = filepath.Join(tempDir, strings.ReplaceAll(testCase.name, " ", "_")+".xml")
			if err := os.WriteFile(configPath, []byte(strings.TrimSpace(testCase.xml)), 0o600); err != nil {
				t.Fatalf("WriteFile(config): %v", err)
			}

			_, err := LoadFile(configPath, LoadOptions{RuntimeVersion: "6.3.1.0"})
			if err == nil {
				t.Fatalf("LoadFile should fail for partial admin TLS configuration")
			}
			if !strings.Contains(err.Error(), "admin TLS configuration requires both Certificate and PrivateKey") {
				t.Fatalf("LoadFile error = %v, want TLS pair validation", err)
			}
		})
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
