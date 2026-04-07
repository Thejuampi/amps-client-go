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
	"github.com/Thejuampi/amps-client-go/internal/gofercli/testutil"
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

func TestMessageCommandNameVariants(t *testing.T) {
	var tests = []struct {
		command string
		want    string
	}{
		{command: "publish", want: "publish"},
		{command: "sow", want: "sow"},
		{command: "subscribe", want: "subscribe"},
		{command: "sow_and_subscribe", want: "sow_and_subscribe"},
		{command: "sow_delete", want: "sow_delete"},
		{command: "delta_publish", want: "delta_publish"},
		{command: "delta_subscribe", want: "delta_subscribe"},
		{command: "sow_and_delta_subscribe", want: "sow_and_delta_subscribe"},
		{command: "ack", want: ""},
	}

	for _, test := range tests {
		var message = amps.NewCommand(test.command).GetMessage()
		if got := messageCommandName(message); got != test.want {
			t.Fatalf("messageCommandName(%s) = %q, want %q", test.command, got, test.want)
		}
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

	var archive = testutil.WriteTempZIP(t, "payloads.zip", map[string]string{
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
	if got := formatRate(0, 0); got != "N/A" {
		t.Fatalf("formatRate(0,0) = %q, want N/A", got)
	}
	if got := formatRate(0, time.Second); got != "Infinity" {
		t.Fatalf("formatRate(0,1s) = %q, want Infinity", got)
	}
}

func TestRenderMessageFormatLegacyAndBraceEdgeCases(t *testing.T) {
	var message = amps.NewCommand("publish").
		SetTopic("orders").
		SetBookmark("1|2|").
		SetSubID("sub-2").
		SetData([]byte("payload")).
		GetMessage()

	var rendered, err = renderMessageFormat("%c|%t|%b|%s|%m|%%", message)
	if err != nil {
		t.Fatalf("legacy renderMessageFormat returned error: %v", err)
	}
	if rendered != "publish|orders|1|2||sub-2|payload|%" {
		t.Fatalf("legacy rendered = %q", rendered)
	}

	rendered, err = renderMessageFormat("{{{topic}}}|{sub_id}|{lease_period}|{timestamp}|{user_id}", message)
	if err != nil {
		t.Fatalf("brace renderMessageFormat returned error: %v", err)
	}
	if rendered != "{orders}|sub-2|||" {
		t.Fatalf("brace rendered = %q", rendered)
	}

	if _, err := renderMessageFormat("{unterminated", message); err == nil {
		t.Fatalf("expected unterminated brace token error")
	}
	if _, err := renderMessageFormat("broken}{data}", message); err == nil {
		t.Fatalf("expected stray brace token error")
	}
	if rendered, err = renderMessageFormat("{data}", nil); err != nil || rendered != "" {
		t.Fatalf("nil message render = %q, err = %v", rendered, err)
	}
}

func TestFlagParserAndSparkBoolHelpers(t *testing.T) {
	if value, err := parseSparkBool("yes"); err != nil || !value {
		t.Fatalf("parseSparkBool(yes) = %t, %v", value, err)
	}
	if value, err := parseSparkBool("no"); err != nil || value {
		t.Fatalf("parseSparkBool(no) = %t, %v", value, err)
	}
	if _, err := parseSparkBool("maybe"); err == nil {
		t.Fatalf("expected parseSparkBool error for maybe")
	}

	var options connectionOptions
	if err := parseConnectionArgs("ping", []string{"-server", "localhost:9007", "-secure=yes"}, &options); err != nil {
		t.Fatalf("parseConnectionArgs secure=yes returned error: %v", err)
	}
	if !options.Secure {
		t.Fatalf("secure=yes should enable secure mode")
	}

	if err := parseConnectionArgs("ping", []string{"-server", "localhost:9007", "extra"}, &options); err == nil {
		t.Fatalf("expected parseConnectionArgs to reject unexpected positional args")
	}
}

func TestNaturalLessNonDigitAndPrefixComparisons(t *testing.T) {
	var tests = []struct {
		left  string
		right string
		want  bool
	}{
		{left: "abc_1", right: "abc_2", want: true},
		{left: "abc_2", right: "abc_1", want: false},
		{left: "a", right: "ab", want: true},
		{left: "01_a", right: "01_b", want: true},
	}
	for _, test := range tests {
		if got := naturalLess(test.left, test.right); got != test.want {
			t.Fatalf("naturalLess(%q, %q) = %t, want %t", test.left, test.right, got, test.want)
		}
	}
}

func TestParseSparkOptionsEnvSkipsNonDFlagsAndPasswords(t *testing.T) {
	var settings = parseSparkOptionsEnv("-other=value -Djavax.net.ssl.trustStorePassword=secret123 -Djavax.net.ssl.keyStorePassword=keypass")
	if settings.TrustStorePassword != "secret123" {
		t.Fatalf("TrustStorePassword = %q", settings.TrustStorePassword)
	}
	if settings.KeyStorePassword != "keypass" {
		t.Fatalf("KeyStorePassword = %q", settings.KeyStorePassword)
	}
}

func TestParseSparkOptionsEnvSkipsNonDAndMissingEquals(t *testing.T) {
	var settings = parseSparkOptionsEnv("-other=value -Djavax.net.ssl.trustStore=C:\\ca.pem -Dflag_without_value")
	if settings.TrustStorePath != "C:\\ca.pem" {
		t.Fatalf("TrustStorePath = %q", settings.TrustStorePath)
	}
	if settings.KeyStorePath != "" {
		t.Fatalf("KeyStorePath should be empty, got %q", settings.KeyStorePath)
	}
}

func TestTLSConfigErrors(t *testing.T) {
	_, err := sparkOptionsEnv{TrustStorePath: "nonexistent.pem"}.TLSConfig()
	if err == nil {
		t.Fatalf("expected trust store read error")
	}

	_, err = sparkOptionsEnv{KeyStorePath: "nonexistent.pem"}.TLSConfig()
	if err == nil {
		t.Fatalf("expected key store read error")
	}

	var trustStore, _ = generatePEMFiles(t)
	_, err = sparkOptionsEnv{KeyStorePath: trustStore}.TLSConfig()
	if err == nil {
		t.Fatalf("expected key store parse error for cert-only PEM")
	}
}

func TestBuildConnectionURIErrors(t *testing.T) {
	_, err := buildConnectionURI(connectionOptions{})
	if err == nil {
		t.Fatalf("expected empty server error")
	}

	_, err = buildConnectionURI(connectionOptions{Server: "://bad"})
	if err == nil {
		t.Fatalf("expected parse error for bad URI")
	}

	var uri string
	uri, err = buildConnectionURI(connectionOptions{Server: "localhost:9007", Secure: true})
	if err != nil {
		t.Fatalf("secure connection URI error: %v", err)
	}
	if !strings.HasPrefix(uri, "tcps://") {
		t.Fatalf("secure uri = %q, want tcps scheme", uri)
	}
}

func TestMergeURIOptionsAndNormalizeErrors(t *testing.T) {
	_, err := mergeURIOptions("%%not-query%%", "send_buffer=1")
	if err == nil {
		t.Fatalf("expected parse error for bad query string")
	}

	_, err = mergeURIOptions("", "no_equals_sign")
	if err == nil {
		t.Fatalf("expected error for option without =")
	}

	var merged, mergeErr = mergeURIOptions("", "")
	if mergeErr != nil {
		t.Fatalf("empty merge returned error: %v", mergeErr)
	}
	if len(merged) != 0 {
		t.Fatalf("empty merge should be empty")
	}
}

func TestParseDelimiterEmptyAndSingleChar(t *testing.T) {
	var delimiter, err = parseDelimiter("")
	if err != nil {
		t.Fatalf("empty delimiter error: %v", err)
	}
	if delimiter != byte('\n') {
		t.Fatalf("empty delimiter = %d, want newline", delimiter)
	}

	delimiter, err = parseDelimiter(",")
	if err != nil {
		t.Fatalf("single char delimiter error: %v", err)
	}
	if delimiter != byte(',') {
		t.Fatalf("delimiter = %d, want comma", delimiter)
	}
}

func TestReadPayloadInputsEmptyAndFileError(t *testing.T) {
	var payloads, err = readPayloadInputs(strings.NewReader(""), "", "", byte('\n'))
	if err != nil {
		t.Fatalf("empty stdin error: %v", err)
	}
	if len(payloads) != 0 {
		t.Fatalf("empty stdin should return nil, got %d payloads", len(payloads))
	}

	_, err = readPayloadInputs(strings.NewReader(""), "", "nonexistent_file.txt", byte('\n'))
	if err == nil {
		t.Fatalf("expected file not found error")
	}
}

func TestParseRateIntervalErrors(t *testing.T) {
	_, err := parseRateInterval("notanumber")
	if err == nil {
		t.Fatalf("expected parse error for non-numeric rate")
	}

	_, err = parseRateInterval("0")
	if err == nil {
		t.Fatalf("expected error for zero rate")
	}
}
