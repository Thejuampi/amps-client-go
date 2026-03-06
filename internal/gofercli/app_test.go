package gofercli

import (
	"bytes"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

func TestBuildConnectionURIDefaultsBareServerToAMPSJSON(t *testing.T) {
	var uri, err = buildConnectionURI(connectionOptions{
		Server: "localhost:9007",
	})
	if err != nil {
		t.Fatalf("buildConnectionURI returned error: %v", err)
	}
	if uri != "tcp://localhost:9007/amps/json" {
		t.Fatalf("uri = %q, want %q", uri, "tcp://localhost:9007/amps/json")
	}
}

func TestBuildConnectionURIPrefersURISchemeOverSecure(t *testing.T) {
	var uri, err = buildConnectionURI(connectionOptions{
		Server:    "localhost:9007",
		Secure:    true,
		URIScheme: "tcp",
	})
	if err != nil {
		t.Fatalf("buildConnectionURI returned error: %v", err)
	}
	if uri != "tcp://localhost:9007/amps/json" {
		t.Fatalf("uri = %q, want %q", uri, "tcp://localhost:9007/amps/json")
	}
}

func TestBuildConnectionURIMergesURIOptions(t *testing.T) {
	var uri, err = buildConnectionURI(connectionOptions{
		Server:     "tcp://localhost:9007/amps/json?tcp_nodelay=true",
		URIOptions: "send_buffer=8192,receive_buffer=4096,nodelay=false",
	})
	if err != nil {
		t.Fatalf("buildConnectionURI returned error: %v", err)
	}
	if !strings.Contains(uri, "tcp_nodelay=true") {
		t.Fatalf("uri missing existing query: %q", uri)
	}
	if !strings.Contains(uri, "tcp_sndbuf=8192") {
		t.Fatalf("uri missing translated send buffer query: %q", uri)
	}
	if !strings.Contains(uri, "tcp_rcvbuf=4096") {
		t.Fatalf("uri missing translated receive buffer query: %q", uri)
	}
	if !strings.Contains(uri, "tcp_nodelay=false") {
		t.Fatalf("uri missing translated nodelay query: %q", uri)
	}
	var parsed, parseErr = url.Parse(uri)
	if parseErr != nil {
		t.Fatalf("Parse returned error: %v", parseErr)
	}
	var query = parsed.Query()
	if _, exists := query["send_buffer"]; exists {
		t.Fatalf("uri still contains untranslated send_buffer alias: %q", uri)
	}
	if _, exists := query["receive_buffer"]; exists {
		t.Fatalf("uri still contains untranslated receive_buffer alias: %q", uri)
	}
	if _, exists := query["nodelay"]; exists {
		t.Fatalf("uri still contains untranslated nodelay alias: %q", uri)
	}
}

type errReader struct{}

func (errReader) Read(_ []byte) (int, error) {
	return 0, errors.New("stdin should not be read")
}

func TestRunSOWDeleteFilterDoesNotReadStdin(t *testing.T) {
	var broker = startBroker(t)
	var app = &App{
		Stdin:  errReader{},
		Stdout: &bytes.Buffer{},
		Stderr: &bytes.Buffer{},
		Now:    time.Now,
		Sleep:  time.Sleep,
	}

	var err = app.runSOWDelete([]string{
		"-server", broker.uri,
		"-topic", "orders.delete.filter",
		"-filter", "/id = 1",
	})
	if err != nil {
		t.Fatalf("runSOWDelete returned error: %v", err)
	}
}

func TestParseSparkOptionsEnvRecognizesTLSSettings(t *testing.T) {
	var settings = parseSparkOptionsEnv("-Djavax.net.ssl.trustStore=C:\\ca.pem -Djavax.net.ssl.keyStore=C:\\client.pem -Djavax.net.ssl.insecureSkipVerify=true")
	if settings.TrustStorePath != "C:\\ca.pem" {
		t.Fatalf("TrustStorePath = %q", settings.TrustStorePath)
	}
	if settings.KeyStorePath != "C:\\client.pem" {
		t.Fatalf("KeyStorePath = %q", settings.KeyStorePath)
	}
	if !settings.InsecureSkipVerify {
		t.Fatalf("InsecureSkipVerify = false, want true")
	}
}

func TestParseSparkOptionsEnvRespectsQuotedPaths(t *testing.T) {
	var settings = parseSparkOptionsEnv(`-Djavax.net.ssl.trustStore="C:\Program Files\site\ca.pem" -Djavax.net.ssl.keyStore="C:\Program Files\site\client.pem"`)
	if settings.TrustStorePath != `C:\Program Files\site\ca.pem` {
		t.Fatalf("TrustStorePath = %q", settings.TrustStorePath)
	}
	if settings.KeyStorePath != `C:\Program Files\site\client.pem` {
		t.Fatalf("KeyStorePath = %q", settings.KeyStorePath)
	}
}

func TestResolveAuthenticatorSupportsDocumentedFactoryNames(t *testing.T) {
	var auth, err = resolveAuthenticator("com.crankuptheamps.spark.DefaultAuthenticatorFactory")
	if err != nil {
		t.Fatalf("resolveAuthenticator(default fqcn) returned error: %v", err)
	}
	if auth != nil {
		t.Fatalf("default authenticator should resolve to nil")
	}

	auth, err = resolveAuthenticator("com.crankuptheamps.spark.KerberosAuthenticatorFactory")
	if err != nil {
		t.Fatalf("resolveAuthenticator(kerberos fqcn) returned error: %v", err)
	}
	if auth == nil {
		t.Fatalf("kerberos authenticator should resolve to a concrete authenticator")
	}
}

func TestResolveAuthenticatorRejectsUnsupportedFactory(t *testing.T) {
	_, err := resolveAuthenticator("com.example.CustomAuthenticatorFactory")
	if err == nil {
		t.Fatalf("expected unsupported authenticator error")
	}
}

func TestRenderMessageFormat(t *testing.T) {
	var message = amps.NewCommand("publish").
		SetTopic("orders").
		SetBookmark("1|1|").
		SetCorrelationID("corr-1").
		SetExpiration(25).
		SetSubID("sub-1").
		SetCommandID("cid-1").
		SetSowKey("k-1").
		SetData([]byte(`{"id":1}`)).
		GetMessage()

	var rendered, err = renderMessageFormat("{command}|{topic}|{bookmark}|{correlation_id}|{expiration}|{length}|{sowkey}|{data}", message)
	if err != nil {
		t.Fatalf("renderMessageFormat returned error: %v", err)
	}
	if rendered != `publish|orders|1|1||corr-1|25|8|k-1|{"id":1}` {
		t.Fatalf("rendered = %q", rendered)
	}
}

func TestParseConnectionArgsSupportsSparkSecureValues(t *testing.T) {
	var tests = []struct {
		name string
		args []string
		want bool
	}{
		{name: "bare", args: []string{"-server", "localhost:9007", "-secure"}, want: true},
		{name: "true", args: []string{"-server", "localhost:9007", "-secure", "true"}, want: true},
		{name: "one", args: []string{"-server", "localhost:9007", "-secure", "1"}, want: true},
		{name: "false", args: []string{"-server", "localhost:9007", "-secure", "false"}, want: false},
		{name: "zero", args: []string{"-server", "localhost:9007", "-secure", "0"}, want: false},
		{name: "no", args: []string{"-server", "localhost:9007", "-secure", "no"}, want: false},
	}

	for _, test := range tests {
		var options connectionOptions
		if err := parseConnectionArgs("ping", test.args, &options); err != nil {
			t.Fatalf("%s: parseConnectionArgs returned error: %v", test.name, err)
		}
		if options.Secure != test.want {
			t.Fatalf("%s: Secure = %t, want %t", test.name, options.Secure, test.want)
		}
	}
}

func TestSplitPayloadsUsesDelimiterAndDropsEmptyTail(t *testing.T) {
	var payloads = splitPayloads([]byte("one\ntwo\n"), byte('\n'))
	if len(payloads) != 2 {
		t.Fatalf("len(payloads) = %d, want 2", len(payloads))
	}
	if string(payloads[0]) != "one" {
		t.Fatalf("payloads[0] = %q", payloads[0])
	}
	if string(payloads[1]) != "two" {
		t.Fatalf("payloads[1] = %q", payloads[1])
	}
}

func TestParseRateIntervalSupportsFractionalRates(t *testing.T) {
	var interval, err = parseRateInterval("0.5")
	if err != nil {
		t.Fatalf("parseRateInterval returned error: %v", err)
	}
	if interval != 2*time.Second {
		t.Fatalf("interval = %s, want %s", interval, 2*time.Second)
	}
}

func TestSelectDeleteMode(t *testing.T) {
	var mode, err = selectDeleteMode(deleteModeOptions{Filter: "/id = 7"})
	if err != nil {
		t.Fatalf("selectDeleteMode returned error: %v", err)
	}
	if mode != deleteByFilter {
		t.Fatalf("mode = %v, want %v", mode, deleteByFilter)
	}
}

func normalizeGoldenText(value string) string {
	return strings.ReplaceAll(value, "\r\n", "\n")
}

func TestHelpTextMatchesGolden(t *testing.T) {
	var mainHelp, err = os.ReadFile(filepath.Join("testdata", "help_main.golden"))
	if err != nil {
		t.Fatalf("ReadFile main help: %v", err)
	}
	if got := normalizeGoldenText(mainHelpText()); got != normalizeGoldenText(string(mainHelp)) {
		t.Fatalf("mainHelpText mismatch\n--- got ---\n%s\n--- want ---\n%s", got, string(mainHelp))
	}

	var publishHelp, publishErr = os.ReadFile(filepath.Join("testdata", "help_publish.golden"))
	if publishErr != nil {
		t.Fatalf("ReadFile publish help: %v", publishErr)
	}
	if got := normalizeGoldenText(commandHelpText("publish")); got != normalizeGoldenText(string(publishHelp)) {
		t.Fatalf("publish help mismatch\n--- got ---\n%s\n--- want ---\n%s", got, string(publishHelp))
	}
}
