package gofercli

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

func writeLocalFile(t *testing.T, name string, data []byte) string {
	t.Helper()

	var path = filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func generatePEMFiles(t *testing.T) (string, string) {
	t.Helper()

	var privateKey, err = rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	var certificate = &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "gofercli-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	var der, certErr = x509.CreateCertificate(rand.Reader, certificate, certificate, &privateKey.PublicKey, privateKey)
	if certErr != nil {
		t.Fatalf("CreateCertificate: %v", certErr)
	}

	var certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	var keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	return writeLocalFile(t, "trust.pem", certPEM), writeLocalFile(t, "bundle.pem", append(certPEM, keyPEM...))
}

func TestSparkTLSConfigAndLookupEnv(t *testing.T) {
	var trustStore, keyStore = generatePEMFiles(t)
	var settings = parseSparkOptionsEnv(
		"-Djavax.net.ssl.trustStore=" + trustStore +
			" -Djavax.net.ssl.keyStore=" + keyStore +
			" -Djavax.net.ssl.insecureSkipVerify=true",
	)

	var tlsConfig, err = settings.TLSConfig()
	if err != nil {
		t.Fatalf("TLSConfig returned error: %v", err)
	}
	if tlsConfig == nil {
		t.Fatalf("TLSConfig returned nil config")
	}
	if tlsConfig.RootCAs == nil {
		t.Fatalf("RootCAs should be configured")
	}
	if len(tlsConfig.Certificates) != 1 {
		t.Fatalf("len(Certificates) = %d, want 1", len(tlsConfig.Certificates))
	}
	if !tlsConfig.InsecureSkipVerify {
		t.Fatalf("InsecureSkipVerify = false, want true")
	}

	var value = lookupEnv([]string{"A=1", "AMPS_SPARK_OPTS=abc", "B=2"}, "AMPS_SPARK_OPTS")
	if value != "abc" {
		t.Fatalf("lookupEnv returned %q", value)
	}
}

func TestCommandHelpTextAndCommandNames(t *testing.T) {
	var helps = []string{"ping", "publish", "subscribe", "sow", "sow_and_subscribe", "sow_delete"}
	for _, name := range helps {
		if got := commandHelpText(name); got == "" || got == mainHelpText() {
			t.Fatalf("commandHelpText(%q) returned %q", name, got)
		}
	}

	var message = amps.NewCommand("delta_publish").GetMessage()
	if got := messageCommandName(message); got != "delta_publish" {
		t.Fatalf("messageCommandName(delta_publish) = %q", got)
	}
}

func TestBuildAMPSPathAndStreamCommand(t *testing.T) {
	if got := buildAMPSPath("amps"); got != "/amps" {
		t.Fatalf("buildAMPSPath(amps) = %q", got)
	}
	if got := buildAMPSPath("fix"); got != "/amps/fix" {
		t.Fatalf("buildAMPSPath(fix) = %q", got)
	}

	var command = buildStreamCommand("sow_and_subscribe", streamConfig{
		Topic:     "orders",
		Filter:    "/id > 0",
		OrderBy:   "/id desc",
		TopN:      10,
		BatchSize: 20,
		Backlog:   3,
		Delta:     true,
	})
	if value, _ := command.Command(); value != "sow_and_delta_subscribe" {
		t.Fatalf("command = %q", value)
	}
	if value, _ := command.Options(); !strings.Contains(value, "max_backlog=3") {
		t.Fatalf("options = %q", value)
	}
}

func TestSelectDeleteModeAndHelpers(t *testing.T) {
	var mode, err = selectDeleteMode(deleteModeOptions{Keys: "k1,k2"})
	if err != nil {
		t.Fatalf("selectDeleteMode returned error: %v", err)
	}
	if mode != deleteByKeys {
		t.Fatalf("mode = %v, want %v", mode, deleteByKeys)
	}

	if _, err := selectDeleteMode(deleteModeOptions{Filter: "/id=1", Keys: "k1"}); err == nil {
		t.Fatalf("expected conflicting delete mode error")
	}
	if err := publishPayload(amps.NewClient("publish-helper"), "orders", []byte("{}"), false); err == nil {
		t.Fatalf("expected disconnected publish failure")
	}
	if deletedCount(nil) != 0 {
		t.Fatalf("deletedCount(nil) should be zero")
	}

	var nilPublisher *copyPublisher
	if err := nilPublisher.Publish("orders", []byte("{}")); err != nil {
		t.Fatalf("nil copy publisher should be a no-op: %v", err)
	}
	nilPublisher.Close()

	if err := publishPayload(amps.NewClient("publish-delta-helper"), "orders", []byte("{}"), true); err == nil {
		t.Fatalf("expected disconnected delta publish failure")
	}
}

func TestParseDelimiterReadInputAndFormatErrors(t *testing.T) {
	var delimiter, err = parseDelimiter("10")
	if err != nil {
		t.Fatalf("parseDelimiter returned error: %v", err)
	}
	if delimiter != byte('\n') {
		t.Fatalf("delimiter = %d, want %d", delimiter, byte('\n'))
	}
	if _, err := parseDelimiter("999"); err == nil {
		t.Fatalf("expected invalid delimiter error")
	}

	var payloads, readErr = readPayloadInputs(strings.NewReader("a|b"), "", "", byte('|'))
	if readErr != nil {
		t.Fatalf("readPayloadInputs returned error: %v", readErr)
	}
	if len(payloads) != 2 {
		t.Fatalf("len(payloads) = %d, want 2", len(payloads))
	}

	var archive = writeTempZIP(t, "payloads.zip", map[string]string{
		"2":  "two",
		"10": "ten",
		"1":  "one",
	})
	payloads, readErr = readPayloadInputs(strings.NewReader(""), "", archive, byte('\n'))
	if readErr != nil {
		t.Fatalf("zip readPayloadInputs returned error: %v", readErr)
	}
	if len(payloads) != 3 {
		t.Fatalf("len(payloads) = %d, want 3", len(payloads))
	}
	if string(payloads[0]) != "one" || string(payloads[1]) != "two" || string(payloads[2]) != "ten" {
		t.Fatalf("payload order = %q, %q, %q", payloads[0], payloads[1], payloads[2])
	}

	if _, err := renderMessageFormat("{unknown}", amps.NewCommand("publish").GetMessage()); err == nil {
		t.Fatalf("expected unsupported format token error")
	}
	if got := formatRate(0, 0); got != "Infinity" {
		t.Fatalf("formatRate(0,0) = %q", got)
	}
}
