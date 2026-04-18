package main

import (
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/Thejuampi/amps-client-go/internal/ampsconfig"
)

func TestStartAdminServerNoAddr(t *testing.T) {
	if err := startAdminServer(""); err != nil {
		t.Fatalf("startAdminServer(\"\") error: %v", err)
	}
}

func TestStartAdminServerRejectsPartialTLSConfig(t *testing.T) {
	var tempDir = t.TempDir()
	var certPath = tempDir + "/admin.crt"
	if err := os.WriteFile(certPath, []byte("cert"), 0o600); err != nil {
		t.Fatalf("WriteFile(cert): %v", err)
	}

	var oldEffectiveConfig = effectiveConfig
	effectiveConfig = &ampsconfig.ExpandedConfig{
		Runtime: ampsconfig.RuntimeConfig{
			Admin: ampsconfig.AdminConfig{
				Certificate: certPath,
			},
		},
	}
	defer func() {
		effectiveConfig = oldEffectiveConfig
	}()

	var err = startAdminServer("127.0.0.1:0")
	if err == nil {
		t.Fatalf("startAdminServer should fail when only one TLS file is configured")
	}
	if !strings.Contains(err.Error(), "requires both certificate and private key") {
		t.Fatalf("startAdminServer error = %v, want TLS pair validation", err)
	}
}

func TestNewAdminServerAppliesHeadersAndCipherSuites(t *testing.T) {
	var server, err = newAdminHTTPServer("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), ampsconfig.AdminConfig{
		Headers: []string{
			"X-Frame-Options: DENY",
			"Cache-Control: no-store",
		},
		Ciphers: []string{
			"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
		},
	})
	if err != nil {
		t.Fatalf("newAdminHTTPServer returned error: %v", err)
	}

	var response = httptest.NewRecorder()
	server.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if response.Header().Get("X-Frame-Options") != "DENY" {
		t.Fatalf("X-Frame-Options = %q, want DENY", response.Header().Get("X-Frame-Options"))
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", response.Header().Get("Cache-Control"))
	}
	if server.TLSConfig == nil {
		t.Fatalf("server.TLSConfig = nil, want configured TLS config")
	}
	if len(server.TLSConfig.CipherSuites) != 1 || server.TLSConfig.CipherSuites[0] != tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 {
		t.Fatalf("server.TLSConfig.CipherSuites = %v, want configured cipher suite", server.TLSConfig.CipherSuites)
	}
	if server.TLSConfig.MaxVersion != tls.VersionTLS12 {
		t.Fatalf("server.TLSConfig.MaxVersion = %d, want TLS 1.2 for TLS 1.2-only suites", server.TLSConfig.MaxVersion)
	}
}

func TestBuildAdminTLSConfigRejectsTLS13CipherSuites(t *testing.T) {
	_, err := buildAdminTLSConfig([]string{"TLS_AES_128_GCM_SHA256"})
	if err == nil {
		t.Fatalf("buildAdminTLSConfig should reject TLS 1.3-only cipher suites")
	}
	if !strings.Contains(err.Error(), "TLS 1.3") {
		t.Fatalf("buildAdminTLSConfig error = %v, want TLS 1.3 enforcement error", err)
	}
}

func TestBuildAdminTLSConfigRejectsInsecureCipherSuites(t *testing.T) {
	var insecureSuiteName string
	for _, suite := range tls.InsecureCipherSuites() {
		insecureSuiteName = suite.Name
		break
	}
	if insecureSuiteName == "" {
		t.Fatalf("tls.InsecureCipherSuites returned no suites to validate")
	}

	_, err := buildAdminTLSConfig([]string{insecureSuiteName})
	if err == nil {
		t.Fatalf("buildAdminTLSConfig should reject insecure cipher suites")
	}
	if !strings.Contains(err.Error(), "insecure") {
		t.Fatalf("buildAdminTLSConfig error = %v, want insecure cipher suite error", err)
	}
}

func TestJSONResponse(t *testing.T) {
	rr := httptest.NewRecorder()
	jsonResponse(rr, map[string]string{"ok": "yes"})
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"ok": "yes"`) {
		t.Fatalf("unexpected json body: %s", rr.Body.String())
	}
}

func TestHandleAdminStatusAndStats(t *testing.T) {
	oldEffectiveConfig := effectiveConfig
	effectiveConfig = &ampsconfig.ExpandedConfig{
		Path: "testdata/config.xml",
		Runtime: ampsconfig.RuntimeConfig{
			Name: "configured-instance",
			Transports: []ampsconfig.TransportConfig{
				{InetAddr: "127.0.0.1:19000"},
			},
		},
	}
	defer func() {
		effectiveConfig = oldEffectiveConfig
	}()

	rr := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/admin/status", nil)
	handleAdminStatus(rr, request)
	if rr.Code != http.StatusOK {
		t.Fatalf("status endpoint expected 200 got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"source": "testdata/config.xml"`) {
		t.Fatalf("status endpoint should expose effective config source, got %s", rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"name": "configured-instance"`) {
		t.Fatalf("status endpoint should expose effective config name, got %s", rr.Body.String())
	}

	rr = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/admin/stats", nil)
	handleAdminStats(rr, request)
	if rr.Code != http.StatusOK {
		t.Fatalf("stats endpoint expected 200 got %d", rr.Code)
	}
}

func TestHandleAdminSOWEndpoints(t *testing.T) {
	oldSow := sow
	sow = newSOWCache()
	defer func() { sow = oldSow }()

	sow.upsert("orders", "k1", []byte(`{"id":1}`), "bm1", "ts1", 1, 0)

	// GET /admin/sow
	rr := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/admin/sow", nil)
	handleAdminSOW(rr, request)
	if rr.Code != http.StatusOK {
		t.Fatalf("sow endpoint expected 200 got %d", rr.Code)
	}

	// GET /admin/sow/orders
	rr = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/admin/sow/orders", nil)
	handleAdminSOWTopic(rr, request)
	if rr.Code != http.StatusOK {
		t.Fatalf("sow topic get expected 200 got %d", rr.Code)
	}

	// DELETE /admin/sow/orders
	rr = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodDelete, "/admin/sow/orders", nil)
	handleAdminSOWTopic(rr, request)
	if rr.Code != http.StatusOK {
		t.Fatalf("sow topic delete expected 200 got %d", rr.Code)
	}
}

func TestHandleAdminSubscriptionsViewsActionsJournal(t *testing.T) {
	// Prepare subscription
	connA, connB := net.Pipe()
	defer connA.Close()
	defer connB.Close()
	sub := &subscription{conn: connA, subID: "s1", topic: "orders"}
	registerSubscription("orders", sub)
	defer unregisterSubscription("orders", sub)

	// Prepare views and actions
	resetViewsForTest()
	defer resetViewsForTest()
	registerView(&viewDef{name: "v1", sources: []string{"orders"}})

	resetActionsForTest()
	defer resetActionsForTest()
	registerAction(actionDef{trigger: triggerOnPublish, topicMatch: "orders", action: actionLog})

	// Prepare journal
	oldJournal := journal
	journal = newMessageJournal(10)
	defer func() { journal = oldJournal }()

	rr := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/admin/subscriptions", nil)
	handleAdminSubscriptions(rr, request)
	if rr.Code != http.StatusOK {
		t.Fatalf("subscriptions endpoint expected 200 got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/admin/views", nil)
	handleAdminViews(rr, request)
	if rr.Code != http.StatusOK {
		t.Fatalf("views endpoint expected 200 got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/admin/actions", nil)
	handleAdminActions(rr, request)
	if rr.Code != http.StatusOK {
		t.Fatalf("actions endpoint expected 200 got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/admin/journal", nil)
	handleAdminJournal(rr, request)
	if rr.Code != http.StatusOK {
		t.Fatalf("journal endpoint expected 200 got %d", rr.Code)
	}
}
