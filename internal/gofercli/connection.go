package gofercli

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/Thejuampi/amps-client-go/amps"
)

const defaultTimeout = 10 * time.Second

type connectionOptions struct {
	Server        string
	Selector      string
	URIScheme     string
	URIOptions    string
	Authenticator string
	Secure        bool
	Timeout       time.Duration
}

type sparkOptionsEnv struct {
	TrustStorePath     string
	TrustStorePassword string
	KeyStorePath       string
	KeyStorePassword   string
	InsecureSkipVerify bool
}

func parseSparkOptionsEnv(raw string) (settings sparkOptionsEnv) {
	for _, token := range splitSparkOptionTokens(raw) {
		if !strings.HasPrefix(token, "-D") {
			continue
		}
		var entry = strings.TrimPrefix(token, "-D")
		var key, value, found = strings.Cut(entry, "=")
		if !found {
			continue
		}
		switch strings.TrimSpace(key) {
		case "javax.net.ssl.trustStore":
			settings.TrustStorePath = value
		case "javax.net.ssl.trustStorePassword":
			settings.TrustStorePassword = value
		case "javax.net.ssl.keyStore":
			settings.KeyStorePath = value
		case "javax.net.ssl.keyStorePassword":
			settings.KeyStorePassword = value
		case "javax.net.ssl.insecureSkipVerify":
			settings.InsecureSkipVerify = strings.EqualFold(value, "true")
		}
	}
	return settings
}

func splitSparkOptionTokens(raw string) []string {
	var tokens []string
	var builder strings.Builder
	var quote rune
	for _, value := range raw {
		switch {
		case quote == 0 && unicode.IsSpace(value):
			if builder.Len() > 0 {
				tokens = append(tokens, builder.String())
				builder.Reset()
			}
		case quote == 0 && (value == '"' || value == '\''):
			quote = value
		case quote != 0 && value == quote:
			quote = 0
		default:
			builder.WriteRune(value)
		}
	}
	if builder.Len() > 0 {
		tokens = append(tokens, builder.String())
	}
	return tokens
}

func (settings sparkOptionsEnv) TLSConfig() (*tls.Config, error) {
	if settings == (sparkOptionsEnv{}) {
		return nil, nil
	}

	var config = &tls.Config{
		InsecureSkipVerify: settings.InsecureSkipVerify, // #nosec G402 -- explicit spark compatibility option
		MinVersion:         tls.VersionTLS12,
	}

	if settings.TrustStorePath != "" {
		var roots = x509.NewCertPool()
		var pemData, err = os.ReadFile(settings.TrustStorePath)
		if err != nil {
			return nil, fmt.Errorf("read trust store: %w", err)
		}
		if !roots.AppendCertsFromPEM(pemData) {
			return nil, fmt.Errorf("trust store %q must contain PEM certificates", settings.TrustStorePath)
		}
		config.RootCAs = roots
	}

	if settings.KeyStorePath != "" {
		var pemData, err = os.ReadFile(settings.KeyStorePath)
		if err != nil {
			return nil, fmt.Errorf("read key store: %w", err)
		}
		var certificate, errCert = tls.X509KeyPair(pemData, pemData)
		if errCert != nil {
			return nil, fmt.Errorf("key store %q must contain PEM certificate and private key data: %w", settings.KeyStorePath, errCert)
		}
		config.Certificates = []tls.Certificate{certificate}
	}

	return config, nil
}

func buildConnectionURI(options connectionOptions) (string, error) {
	var server = strings.TrimSpace(options.Server)
	if server == "" {
		return "", fmt.Errorf("server is required (-server)")
	}

	var parsed *url.URL
	var err error
	if strings.Contains(server, "://") {
		parsed, err = url.Parse(server)
		if err != nil {
			return "", fmt.Errorf("parse server URI: %w", err)
		}
	} else {
		parsed = &url.URL{Host: server}
	}

	if parsed.Host == "" {
		return "", fmt.Errorf("server must include host[:port]")
	}

	if options.URIScheme != "" {
		parsed.Scheme = strings.ToLower(strings.TrimSpace(options.URIScheme))
	} else if options.Secure {
		parsed.Scheme = "tcps"
	} else if parsed.Scheme == "" {
		parsed.Scheme = "tcp"
	}

	if parsed.Path == "" || options.Selector != "" {
		parsed.Path = buildAMPSPath(options.Selector)
	}

	var query, queryErr = mergeURIOptions(parsed.RawQuery, options.URIOptions)
	if queryErr != nil {
		return "", queryErr
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func buildAMPSPath(selector string) string {
	var normalized = strings.ToLower(strings.TrimSpace(selector))
	switch normalized {
	case "", "json":
		return "/amps/json"
	case "amps":
		return "/amps"
	default:
		return "/amps/" + normalized
	}
}

func mergeURIOptions(existing string, extra string) (url.Values, error) {
	var merged = url.Values{}
	if existing != "" {
		var parsed, err = url.ParseQuery(existing)
		if err != nil {
			return nil, fmt.Errorf("parse URI query: %w", err)
		}
		for key, values := range parsed {
			for _, value := range values {
				merged.Add(key, value)
			}
		}
	}

	for _, token := range splitURIOptions(extra) {
		if token == "" {
			continue
		}
		var key, value, found = strings.Cut(token, "=")
		if !found {
			return nil, fmt.Errorf("invalid uri option %q", token)
		}
		merged.Add(normalizeURIOptionKey(key), strings.TrimSpace(value))
	}

	return merged, nil
}

func normalizeURIOptionKey(raw string) string {
	var key = strings.ToLower(strings.TrimSpace(raw))
	switch key {
	case "send_buffer":
		return "tcp_sndbuf"
	case "receive_buffer":
		return "tcp_rcvbuf"
	case "nodelay":
		return "tcp_nodelay"
	default:
		return key
	}
}

func splitURIOptions(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	return strings.FieldsFunc(raw, func(r rune) bool {
		return r == '&' || r == ','
	})
}

func connectClient(options connectionOptions, env []string, now func() time.Time) (*amps.Client, string, error) {
	if options.Timeout <= 0 {
		options.Timeout = defaultTimeout
	}

	var uri, err = buildConnectionURI(options)
	if err != nil {
		return nil, "", err
	}

	var parsed, parseErr = url.Parse(uri)
	if parseErr != nil {
		return nil, "", fmt.Errorf("parse normalized URI: %w", parseErr)
	}
	if parsed.Scheme != "tcp" && parsed.Scheme != "tcps" {
		return nil, uri, fmt.Errorf("unsupported URI scheme %q", parsed.Scheme)
	}

	var auth, authErr = resolveAuthenticator(options.Authenticator)
	if authErr != nil {
		return nil, uri, authErr
	}

	var tlsConfig, tlsErr = parseSparkOptionsEnv(lookupEnv(env, "AMPS_SPARK_OPTS")).TLSConfig()
	if tlsErr != nil {
		return nil, uri, tlsErr
	}

	var clientName = fmt.Sprintf("gofer-%d-%d", os.Getpid(), now().UnixNano())
	var client = amps.NewClient(clientName)
	if tlsConfig != nil {
		client.SetTLSConfig(tlsConfig)
	}
	if err := client.Connect(uri); err != nil {
		return nil, uri, err
	}

	var params amps.LogonParams
	params.Timeout = uint(options.Timeout.Milliseconds()) // #nosec G115 -- bounded CLI duration converted to milliseconds
	params.Authenticator = auth

	if auth != nil || params.Timeout > 0 {
		if err := client.Logon(params); err != nil {
			_ = client.Close()
			return nil, uri, err
		}
	} else {
		if err := client.Logon(); err != nil {
			_ = client.Close()
			return nil, uri, err
		}
	}

	return client, uri, nil
}

func lookupEnv(env []string, key string) string {
	var prefix = key + "="
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			return strings.TrimPrefix(entry, prefix)
		}
	}
	return ""
}

func parseSparkBool(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "true", "1", "yes":
		return true, nil
	case "false", "0", "no":
		return false, nil
	default:
		var value, err = strconv.ParseBool(raw)
		if err != nil {
			return false, fmt.Errorf("invalid boolean value %q", raw)
		}
		return value, nil
	}
}
