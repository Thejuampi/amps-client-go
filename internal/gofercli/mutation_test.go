package gofercli

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Thejuampi/amps-client-go/amps"
)

func TestFormatRateZeroCountPositiveElapsedReturnsInfinity(t *testing.T) {
	var got = formatRate(0, time.Second)
	if got != "Infinity" {
		t.Fatalf("formatRate(0, 1s) = %q, want Infinity", got)
	}
}

func TestBuildStreamCommandDeltaSOWAndSubscribeProducesDeltaCommand(t *testing.T) {
	var command = buildStreamCommand("sow_and_subscribe", streamConfig{Topic: "t", Delta: true})
	var name, _ = command.Command()
	if name != "sow_and_delta_subscribe" {
		t.Fatalf("command = %q, want sow_and_delta_subscribe", name)
	}
}

func TestBuildStreamCommandNoDeltaSubscribeKeepsName(t *testing.T) {
	var command = buildStreamCommand("subscribe", streamConfig{Topic: "t", Delta: false})
	var name, _ = command.Command()
	if name != "subscribe" {
		t.Fatalf("command = %q, want subscribe", name)
	}
}

func TestBuildStreamCommandNoDeltaSOWAndSubscribeKeepsName(t *testing.T) {
	var command = buildStreamCommand("sow_and_subscribe", streamConfig{Topic: "t", Delta: false})
	var name, _ = command.Command()
	if name != "sow_and_subscribe" {
		t.Fatalf("command = %q, want sow_and_subscribe", name)
	}
}

func TestBuildCommandOptionsZeroBacklogReturnsEmpty(t *testing.T) {
	var got = buildCommandOptions(streamConfig{Backlog: 0})
	if got != "" {
		t.Fatalf("buildCommandOptions(0) = %q, want empty", got)
	}
}

func TestBuildCommandOptionsPositiveBacklogIncludesValue(t *testing.T) {
	var got = buildCommandOptions(streamConfig{Backlog: 7})
	if got != "max_backlog=7" {
		t.Fatalf("buildCommandOptions(7) = %q, want max_backlog=7", got)
	}
}

func TestSelectDeleteModeByDataPayloads(t *testing.T) {
	var mode, err = selectDeleteMode(deleteModeOptions{Payloads: [][]byte{[]byte("{}")}})
	if err != nil {
		t.Fatalf("selectDeleteMode by data returned error: %v", err)
	}
	if mode != deleteByData {
		t.Fatalf("mode = %v, want %v", mode, deleteByData)
	}
}

func TestSelectDeleteModeByKeysReturnsKeysMode(t *testing.T) {
	var mode, err = selectDeleteMode(deleteModeOptions{Keys: "k1"})
	if err != nil {
		t.Fatalf("selectDeleteMode by keys returned error: %v", err)
	}
	if mode != deleteByKeys {
		t.Fatalf("mode = %v, want %v", mode, deleteByKeys)
	}
}

func TestPayloadInputExplicitVariations(t *testing.T) {
	if !payloadInputExplicit("data", "") {
		t.Fatalf("payloadInputExplicit(data, empty) should be true")
	}
	if !payloadInputExplicit("", "file.txt") {
		t.Fatalf("payloadInputExplicit(empty, file) should be true")
	}
	if payloadInputExplicit("  ", "") {
		t.Fatalf("payloadInputExplicit(whitespace, empty) should be false")
	}
	if payloadInputExplicit("", "  ") {
		t.Fatalf("payloadInputExplicit(empty, whitespace) should be false")
	}
	if payloadInputExplicit("", "") {
		t.Fatalf("payloadInputExplicit(empty, empty) should be false")
	}
}

func TestSignalDoneNonBlockingWhenChannelFull(t *testing.T) {
	var done = make(chan struct{}, 1)
	signalDone(done)
	signalDone(done)
	if len(done) != 1 {
		t.Fatalf("channel should have exactly 1 item, got %d", len(done))
	}
}

func TestSignalErrorNonBlockingWhenChannelFull(t *testing.T) {
	var target = make(chan error, 1)
	signalError(target, errors.New("first"))
	signalError(target, errors.New("second"))
	var err = <-target
	if err.Error() != "first" {
		t.Fatalf("expected first error, got %v", err)
	}
}

func TestEmitMessageFormatErrorReturnsError(t *testing.T) {
	var app = &App{
		Stdout: &bytes.Buffer{},
		Now:    time.Now,
		Sleep:  time.Sleep,
	}
	var message = amps.NewCommand("publish").SetTopic("t").SetData([]byte("{}")).GetMessage()
	if err := app.emitMessage(message, "{unknown_field}", nil); err == nil {
		t.Fatalf("expected unsupported format token error")
	}
}

func TestBuildAMPSPathVariants(t *testing.T) {
	if got := buildAMPSPath(""); got != "/amps/json" {
		t.Fatalf("buildAMPSPath('') = %q, want /amps/json", got)
	}
	if got := buildAMPSPath("json"); got != "/amps/json" {
		t.Fatalf("buildAMPSPath(json) = %q, want /amps/json", got)
	}
	if got := buildAMPSPath("amps"); got != "/amps" {
		t.Fatalf("buildAMPSPath(amps) = %q, want /amps", got)
	}
	if got := buildAMPSPath("fix"); got != "/amps/fix" {
		t.Fatalf("buildAMPSPath(fix) = %q, want /amps/fix", got)
	}
	if got := buildAMPSPath("XML"); got != "/amps/xml" {
		t.Fatalf("buildAMPSPath(XML) = %q, want /amps/xml", got)
	}
}

func TestBuildConnectionURIWithSelectorOverridesPath(t *testing.T) {
	var uri, err = buildConnectionURI(connectionOptions{
		Server:   "localhost:9007",
		Selector: "fix",
	})
	if err != nil {
		t.Fatalf("buildConnectionURI returned error: %v", err)
	}
	if !strings.HasSuffix(uri, "/amps/fix") {
		t.Fatalf("uri = %q, want path /amps/fix", uri)
	}
}

func TestBuildConnectionURIPathPreservedWhenSet(t *testing.T) {
	var uri, err = buildConnectionURI(connectionOptions{
		Server: "tcp://localhost:9007/custom/path",
	})
	if err != nil {
		t.Fatalf("buildConnectionURI returned error: %v", err)
	}
	if !strings.Contains(uri, "/custom/path") {
		t.Fatalf("uri = %q, want /custom/path preserved", uri)
	}
}

func TestBuildConnectionURIEmptyHostError(t *testing.T) {
	_, err := buildConnectionURI(connectionOptions{Server: "tcp://"})
	if err == nil {
		t.Fatalf("expected empty host error")
	}
	if !strings.Contains(err.Error(), "host") {
		t.Fatalf("error = %q", err.Error())
	}
}

func TestNormalizeURIOptionKeyPassthrough(t *testing.T) {
	var got = normalizeURIOptionKey("custom_key")
	if got != "custom_key" {
		t.Fatalf("normalizeURIOptionKey(custom_key) = %q", got)
	}
}

func TestLookupEnvNotFoundReturnsEmpty(t *testing.T) {
	var got = lookupEnv([]string{"A=1", "B=2"}, "MISSING")
	if got != "" {
		t.Fatalf("lookupEnv for missing key = %q, want empty", got)
	}
}

func TestLookupEnvEmptyEnvReturnsEmpty(t *testing.T) {
	var got = lookupEnv(nil, "ANY")
	if got != "" {
		t.Fatalf("lookupEnv with nil env = %q, want empty", got)
	}
}

func TestRenderPercentFormatDanglingPercentError(t *testing.T) {
	var message = amps.NewCommand("publish").SetData([]byte("x")).GetMessage()
	_, err := renderMessageFormat("prefix%", message)
	if err == nil {
		t.Fatalf("expected dangling format token error")
	}
	if !strings.Contains(err.Error(), "dangling") {
		t.Fatalf("error = %q", err.Error())
	}
}

func TestRenderPercentFormatUnknownTokenError(t *testing.T) {
	var message = amps.NewCommand("publish").SetData([]byte("x")).GetMessage()
	_, err := renderMessageFormat("%z", message)
	if err == nil {
		t.Fatalf("expected unsupported format token error")
	}
	if !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("error = %q", err.Error())
	}
}

func TestReadPayloadInputsInlineDataReturnsSinglePayload(t *testing.T) {
	var payloads, err = readPayloadInputs(nil, `{"id":1}`, "", byte('\n'))
	if err != nil {
		t.Fatalf("readPayloadInputs returned error: %v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("len(payloads) = %d, want 1", len(payloads))
	}
	if string(payloads[0]) != `{"id":1}` {
		t.Fatalf("payload = %q", payloads[0])
	}
}

func TestReadPayloadInputsEmptyDataReturnsNil(t *testing.T) {
	var payloads, err = readPayloadInputs(strings.NewReader(""), "", "", byte('\n'))
	if err != nil {
		t.Fatalf("readPayloadInputs returned error: %v", err)
	}
	if payloads != nil {
		t.Fatalf("payloads = %v, want nil", payloads)
	}
}

func TestNaturalLessNumericComparison(t *testing.T) {
	if !naturalLess("file_2.txt", "file_10.txt") {
		t.Fatalf("naturalLess(2, 10) should be true")
	}
	if naturalLess("file_10.txt", "file_2.txt") {
		t.Fatalf("naturalLess(10, 2) should be false")
	}
	if !naturalLess("file_002.txt", "file_10.txt") {
		t.Fatalf("naturalLess(002, 10) should be true")
	}
}

func TestNextNaturalTokenEmpty(t *testing.T) {
	var token, isDigit, rest = nextNaturalToken("")
	if token != "" {
		t.Fatalf("token = %q, want empty", token)
	}
	if isDigit {
		t.Fatalf("isDigit should be false for empty")
	}
	if rest != "" {
		t.Fatalf("rest = %q, want empty", rest)
	}
}

func TestNextNaturalTokenAllDigits(t *testing.T) {
	var token, isDigit, rest = nextNaturalToken("123abc")
	if token != "123" {
		t.Fatalf("token = %q, want 123", token)
	}
	if !isDigit {
		t.Fatalf("isDigit should be true")
	}
	if rest != "abc" {
		t.Fatalf("rest = %q, want abc", rest)
	}
}

func TestNextNaturalTokenAllLetters(t *testing.T) {
	var token, isDigit, rest = nextNaturalToken("abc123")
	if token != "abc" {
		t.Fatalf("token = %q, want abc", token)
	}
	if isDigit {
		t.Fatalf("isDigit should be false for letters")
	}
	if rest != "123" {
		t.Fatalf("rest = %q, want 123", rest)
	}
}

func TestResolveAuthenticatorShortForms(t *testing.T) {
	var auth, err = resolveAuthenticator("default")
	if err != nil {
		t.Fatalf("resolveAuthenticator(default) returned error: %v", err)
	}
	if auth != nil {
		t.Fatalf("default authenticator should be nil")
	}

	auth, err = resolveAuthenticator("kerberos")
	if err != nil {
		t.Fatalf("resolveAuthenticator(kerberos) returned error: %v", err)
	}
	if auth == nil {
		t.Fatalf("kerberos authenticator should not be nil")
	}
}

func TestParseDelimiterNumericByteValue(t *testing.T) {
	var delimiter, err = parseDelimiter("48")
	if err != nil {
		t.Fatalf("parseDelimiter(48) returned error: %v", err)
	}
	if delimiter != 48 {
		t.Fatalf("delimiter = %d, want 48", delimiter)
	}

	delimiter, err = parseDelimiter("255")
	if err != nil {
		t.Fatalf("parseDelimiter(255) returned error: %v", err)
	}
	if delimiter != 255 {
		t.Fatalf("delimiter = %d, want 255", delimiter)
	}
}

func TestPublishPayloadDeltaTrueCallsDeltaPublish(t *testing.T) {
	var client = amps.NewClient("delta-unit-test")
	var err = publishPayload(client, "topic", []byte("{}"), true)
	if err == nil {
		t.Fatalf("expected error from disconnected client delta publish")
	}
}

func TestPublishPayloadDeltaFalseCallsPublish(t *testing.T) {
	var client = amps.NewClient("publish-unit-test")
	var err = publishPayload(client, "topic", []byte("{}"), false)
	if err == nil {
		t.Fatalf("expected error from disconnected client publish")
	}
}

func TestRunVersionCommandAllVariants(t *testing.T) {
	for _, args := range [][]string{{"version"}, {"--version"}, {"-version"}} {
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		var app = &App{
			Stdout: &stdout,
			Stderr: &stderr,
			Now:    time.Now,
			Sleep:  time.Sleep,
		}
		var code = app.Run(args)
		if code != 0 {
			t.Fatalf("%v: exit code = %d, want 0", args, code)
		}
		if !strings.HasPrefix(stdout.String(), "gofer version ") {
			t.Fatalf("%v: stdout = %q", args, stdout.String())
		}
		if stderr.Len() > 0 {
			t.Fatalf("%v: stderr should be empty, got %q", args, stderr.String())
		}
	}
}

func TestRunUnknownCommandReturnsError(t *testing.T) {
	var stderr bytes.Buffer
	var app = &App{
		Stdout: io.Discard,
		Stderr: &stderr,
		Now:    time.Now,
		Sleep:  time.Sleep,
	}
	var code = app.Run([]string{"nonexistent"})
	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "unknown command") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestIsZIPArchiveRejectsNonZIP(t *testing.T) {
	if isZIPArchive([]byte("PK\x00\x00")) {
		t.Fatalf("wrong magic byte 3 should not be ZIP")
	}
	if isZIPArchive([]byte("PK\x03")) {
		t.Fatalf("too short should not be ZIP")
	}
	if isZIPArchive(nil) {
		t.Fatalf("nil should not be ZIP")
	}
	if isZIPArchive([]byte{}) {
		t.Fatalf("empty should not be ZIP")
	}
}

func TestIsZIPArchiveAcceptsValidMagic(t *testing.T) {
	if !isZIPArchive([]byte("PK\x03\x04")) {
		t.Fatalf("valid ZIP magic should be detected")
	}
	if !isZIPArchive([]byte("PK\x03\x04extra data")) {
		t.Fatalf("valid ZIP magic with data should be detected")
	}
}

func TestSplitPayloadsAllEmptyReturnsEmpty(t *testing.T) {
	var payloads = splitPayloads([]byte("\n\n\n"), byte('\n'))
	if len(payloads) != 0 {
		t.Fatalf("len(payloads) = %d, want 0", len(payloads))
	}
}

func TestSplitPayloadsSingleNonEmpty(t *testing.T) {
	var payloads = splitPayloads([]byte("hello"), byte('\n'))
	if len(payloads) != 1 {
		t.Fatalf("len(payloads) = %d, want 1", len(payloads))
	}
	if string(payloads[0]) != "hello" {
		t.Fatalf("payload = %q", payloads[0])
	}
}

func TestRunHelpWithCommandReturnsSpecificHelp(t *testing.T) {
	var stdout bytes.Buffer
	var app = &App{
		Stdout: &stdout,
		Stderr: io.Discard,
		Now:    time.Now,
		Sleep:  time.Sleep,
	}
	var code = app.Run([]string{"help", "ping"})
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if !strings.Contains(stdout.String(), "gofer ping") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunHelpWithUnknownCommandReturnsMainHelp(t *testing.T) {
	var stdout bytes.Buffer
	var app = &App{
		Stdout: &stdout,
		Stderr: io.Discard,
		Now:    time.Now,
		Sleep:  time.Sleep,
	}
	var code = app.Run([]string{"help", "unknown"})
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if !strings.Contains(stdout.String(), "spark-compatible") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestHasHelpArgRecognizesAllForms(t *testing.T) {
	if !hasHelpArg([]string{"-help"}) {
		t.Fatalf("-help should be recognized")
	}
	if !hasHelpArg([]string{"--help"}) {
		t.Fatalf("--help should be recognized")
	}
	if !hasHelpArg([]string{"-h"}) {
		t.Fatalf("-h should be recognized")
	}
	if hasHelpArg([]string{"-server", "localhost"}) {
		t.Fatalf("no help args should return false")
	}
}

func TestFormatRateZeroCountZeroElapsed(t *testing.T) {
	if got := formatRate(0, 0); got != "N/A" {
		t.Fatalf("formatRate(0, 0) = %q, want N/A", got)
	}
}

func TestFormatRateZeroCountNegativeElapsedReturnsNA(t *testing.T) {
	if got := formatRate(0, -time.Second); got != "N/A" {
		t.Fatalf("formatRate(0, -1s) = %q, want N/A", got)
	}
}

func TestFormatRateNormalRate(t *testing.T) {
	if got := formatRate(10, 2*time.Second); got != "5.000" {
		t.Fatalf("formatRate(10, 2s) = %q, want 5.000", got)
	}
}

func TestBuildConnectionURISecureSetsTCPS(t *testing.T) {
	var uri, err = buildConnectionURI(connectionOptions{
		Server: "localhost:9007",
		Secure: true,
	})
	if err != nil {
		t.Fatalf("buildConnectionURI returned error: %v", err)
	}
	if !strings.HasPrefix(uri, "tcps://") {
		t.Fatalf("uri = %q, want tcps scheme", uri)
	}
}

func TestBuildConnectionURIURISchemeOverridesSecure(t *testing.T) {
	var uri, err = buildConnectionURI(connectionOptions{
		Server:    "localhost:9007",
		Secure:    true,
		URIScheme: "tcp",
	})
	if err != nil {
		t.Fatalf("buildConnectionURI returned error: %v", err)
	}
	if !strings.HasPrefix(uri, "tcp://") {
		t.Fatalf("uri = %q, want tcp scheme (override)", uri)
	}
}

func TestBuildConnectionURIEmptyServerError(t *testing.T) {
	_, err := buildConnectionURI(connectionOptions{Server: "   "})
	if err == nil {
		t.Fatalf("expected empty server error for whitespace")
	}
}

func TestSplitURIOptionsCommaAndAmpersand(t *testing.T) {
	var tokens = splitURIOptions("a=1,b=2&c=3")
	if len(tokens) != 3 {
		t.Fatalf("len(tokens) = %d, want 3: %v", len(tokens), tokens)
	}
}

func TestSplitURIOptionsEmptyReturnsNil(t *testing.T) {
	var tokens = splitURIOptions("  ")
	if tokens != nil {
		t.Fatalf("tokens = %v, want nil", tokens)
	}
}

func TestParseSparkBoolVariants(t *testing.T) {
	var tests = []struct {
		input string
		want  bool
	}{
		{"", true},
		{"true", true},
		{"1", true},
		{"yes", true},
		{"TRUE", true},
		{"false", false},
		{"0", false},
		{"no", false},
		{"FALSE", false},
	}
	for _, test := range tests {
		var got, err = parseSparkBool(test.input)
		if err != nil {
			t.Fatalf("parseSparkBool(%q) returned error: %v", test.input, err)
		}
		if got != test.want {
			t.Fatalf("parseSparkBool(%q) = %t, want %t", test.input, got, test.want)
		}
	}
}

func TestParseSparkBoolInvalidValue(t *testing.T) {
	_, err := parseSparkBool("invalid")
	if err == nil {
		t.Fatalf("expected error for invalid boolean value")
	}
}

func TestRenderBraceFormatAllTokens(t *testing.T) {
	var message = amps.NewCommand("publish").
		SetTopic("orders").
		SetBookmark("bm1").
		SetCorrelationID("corr1").
		SetExpiration(30).
		SetSubID("sub1").
		SetSowKey("key1").
		SetData([]byte("payload")).
		GetMessage()

	var rendered, err = renderMessageFormat("{command}|{topic}|{bookmark}|{correlation_id}|{expiration}|{length}|{sowkey}|{sub_id}|{data}", message)
	if err != nil {
		t.Fatalf("renderMessageFormat returned error: %v", err)
	}
	if !strings.Contains(rendered, "publish") {
		t.Fatalf("missing command in rendered: %q", rendered)
	}
	if !strings.Contains(rendered, "orders") {
		t.Fatalf("missing topic in rendered: %q", rendered)
	}
	if !strings.Contains(rendered, "bm1") {
		t.Fatalf("missing bookmark in rendered: %q", rendered)
	}
	if !strings.Contains(rendered, "corr1") {
		t.Fatalf("missing correlation_id in rendered: %q", rendered)
	}
	if !strings.Contains(rendered, "30") {
		t.Fatalf("missing expiration in rendered: %q", rendered)
	}
	if !strings.Contains(rendered, "7") {
		t.Fatalf("missing length in rendered: %q", rendered)
	}
	if !strings.Contains(rendered, "key1") {
		t.Fatalf("missing sowkey in rendered: %q", rendered)
	}
	if !strings.Contains(rendered, "sub1") {
		t.Fatalf("missing sub_id in rendered: %q", rendered)
	}
	if !strings.Contains(rendered, "payload") {
		t.Fatalf("missing data in rendered: %q", rendered)
	}
}

func TestRenderPercentFormatAllTokens(t *testing.T) {
	var message = amps.NewCommand("publish").
		SetTopic("orders").
		SetBookmark("bm1").
		SetSubID("sub1").
		SetSowKey("key1").
		SetData([]byte("payload")).
		GetMessage()

	var rendered, err = renderMessageFormat("%c|%t|%b|%k|%s|%m|%%", message)
	if err != nil {
		t.Fatalf("renderMessageFormat returned error: %v", err)
	}
	if rendered != "publish|orders|bm1|key1|sub1|payload|%" {
		t.Fatalf("rendered = %q", rendered)
	}
}

func TestRenderBraceFormatEscapedBraces(t *testing.T) {
	var message = amps.NewCommand("publish").
		SetTopic("t").
		SetData([]byte("d")).
		GetMessage()

	var rendered, err = renderMessageFormat("{{topic}}", message)
	if err != nil {
		t.Fatalf("renderMessageFormat returned error: %v", err)
	}
	if rendered != "{topic}" {
		t.Fatalf("rendered = %q, want {topic}", rendered)
	}
}

func TestRenderBraceFormatStrayBraceError(t *testing.T) {
	var message = amps.NewCommand("publish").GetMessage()
	_, err := renderMessageFormat("{data}extra}", message)
	if err == nil {
		t.Fatalf("expected stray brace error")
	}
}

func TestRenderBraceFormatUnterminatedTokenError(t *testing.T) {
	var message = amps.NewCommand("publish").GetMessage()
	_, err := renderMessageFormat("{unterminated", message)
	if err == nil {
		t.Fatalf("expected unterminated token error")
	}
}
