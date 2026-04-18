package main

import (
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Thejuampi/amps-client-go/internal/ampsconfig"
)

const adminSessionCookieName = "fakeamps_admin_session"

type monitoringServiceOptions struct {
	Admin ampsconfig.AdminConfig
	Users []ampsconfig.AdminUserConfig
	Now   func() time.Time
}

type monitoringService struct {
	admin    ampsconfig.AdminConfig
	users    map[string]ampsconfig.AdminUserConfig
	sessions map[string]adminSession
	history  *metricsHistoryStore
	now      func() time.Time
	mu       sync.RWMutex
	sampleMu sync.Mutex
	sampleCh chan struct{}
	doneCh   chan struct{}
}

type adminSession struct {
	ID        string
	Username  string
	Role      string
	ExpiresAt time.Time
}

type adminPrincipal struct {
	Username string `json:"username"`
	Role     string `json:"role"`
}

type transportDescriptor struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	InetAddr    string `json:"inet_addr"`
	Protocol    string `json:"protocol"`
	MessageType string `json:"message_type"`
	Enabled     bool   `json:"enabled"`
}

type replicationDescriptor struct {
	ID        string `json:"id"`
	Address   string `json:"address"`
	Connected bool   `json:"connected"`
	Mode      string `json:"mode"`
}

type sqlWorkspaceOptions struct {
	TopN     *int   `json:"top_n"`
	OrderBy  string `json:"order_by"`
	Bookmark string `json:"bookmark"`
	Delta    bool   `json:"delta"`
	Live     bool   `json:"live"`
}

type sqlWorkspaceRequest struct {
	Type      string              `json:"type"`
	RequestID string              `json:"request_id"`
	Mode      string              `json:"mode"`
	Action    string              `json:"action"`
	Topic     string              `json:"topic"`
	Filter    string              `json:"filter"`
	Options   sqlWorkspaceOptions `json:"options"`
}

func newMonitoringService(options monitoringServiceOptions) *monitoringService {
	var admin = options.Admin
	if isZeroAdminConfig(admin) && effectiveConfig != nil {
		admin = effectiveConfig.Runtime.Admin
	}
	if admin.Interval <= 0 {
		admin.Interval = 5 * time.Second
	}

	var users = options.Users
	if len(users) == 0 && effectiveConfig != nil {
		users = effectiveConfig.Runtime.Extensions.FakeAMPS.AdminUsers
	}

	var userMap = make(map[string]ampsconfig.AdminUserConfig, len(users))
	for _, user := range users {
		userMap[user.Username] = user
	}

	var now = options.Now
	if now == nil {
		now = time.Now
	}

	var service = &monitoringService{
		admin:    admin,
		users:    userMap,
		sessions: make(map[string]adminSession),
		history:  newMetricsHistoryStore(metricsHistoryOptions{Retention: 15 * time.Minute}),
		now:      now,
	}
	service.initializeTransports()
	if service.admin.FileName != "" {
		if err := service.history.LoadFile(service.admin.FileName); err != nil && !errors.Is(err, fs.ErrNotExist) {
			log.Printf("fakeamps: load admin history %s failed: %v", service.admin.FileName, err)
		}
	}
	return service
}

func (service *monitoringService) initializeTransports() {
	monitoringTransports.Reset(service.configuredTransports())
}

func (service *monitoringService) StartSampling() {
	if service == nil {
		return
	}

	service.sampleMu.Lock()
	if service.sampleCh != nil {
		service.sampleMu.Unlock()
		return
	}
	service.sampleCh = make(chan struct{})
	service.doneCh = make(chan struct{})
	var stopCh = service.sampleCh
	var doneCh = service.doneCh
	var interval = service.admin.Interval
	service.sampleMu.Unlock()

	service.sampleMetrics()

	go func() {
		defer close(doneCh)

		var ticker = time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				service.sampleMetrics()
			case <-stopCh:
				return
			}
		}
	}()
}

func (service *monitoringService) Close() {
	if service == nil {
		return
	}

	service.sampleMu.Lock()
	var stopCh = service.sampleCh
	var doneCh = service.doneCh
	service.sampleCh = nil
	service.doneCh = nil
	service.sampleMu.Unlock()

	if stopCh == nil {
		return
	}

	close(stopCh)
	<-doneCh
}

func isZeroAdminConfig(config ampsconfig.AdminConfig) bool {
	return config.InetAddr == "" &&
		config.Interval == 0 &&
		config.FileName == "" &&
		config.ExternalInetAddr == "" &&
		config.SQLTransport == "" &&
		config.Authentication == "" &&
		config.Entitlement == "" &&
		config.Certificate == "" &&
		config.PrivateKey == "" &&
		len(config.AnonymousPaths) == 0 &&
		len(config.SessionOptions) == 0 &&
		len(config.Headers) == 0 &&
		len(config.Ciphers) == 0
}

func (service *monitoringService) Handler() http.Handler {
	var mux = http.NewServeMux()
	mux.HandleFunc("/", service.handleDashboard)
	mux.HandleFunc("/assets/dashboard.css", service.handleDashboardCSS)
	mux.HandleFunc("/assets/dashboard.js", service.handleDashboardJS)
	mux.HandleFunc("/amps", service.withViewerAccess(service.handleAMPSRoot))
	mux.HandleFunc("/amps/", service.handleAMPSRoutes)
	mux.HandleFunc("/admin/status", service.withViewerAccess(handleAdminStatus))
	mux.HandleFunc("/admin/stats", service.withViewerAccess(handleAdminStats))
	mux.HandleFunc("/admin/sow", service.withViewerAccess(handleAdminSOW))
	mux.HandleFunc("/admin/sow/", service.withViewerAccess(handleAdminSOWTopic))
	mux.HandleFunc("/admin/subscriptions", service.withViewerAccess(handleAdminSubscriptions))
	mux.HandleFunc("/admin/views", service.withViewerAccess(handleAdminViews))
	mux.HandleFunc("/admin/actions", service.withViewerAccess(handleAdminActions))
	mux.HandleFunc("/admin/journal", service.withViewerAccess(handleAdminJournal))
	return mux
}

func (service *monitoringService) handleAMPSRoutes(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/amps/session":
		service.withViewerAccess(service.handleSessionState)(w, r)
	case "/amps/session/login":
		service.handleSessionLogin(w, r)
	case "/amps/session/logout":
		service.handleSessionLogout(w, r)
	case "/amps/host":
		service.withViewerAccess(service.handleHost)(w, r)
	case "/amps/instance":
		service.withViewerAccess(service.handleInstance)(w, r)
	case "/amps/instance/history":
		service.withViewerAccess(service.handleInstanceHistory)(w, r)
	case "/amps/instance/topics":
		service.withViewerAccess(service.handleInstanceTopics)(w, r)
	case "/amps/instance/subscriptions":
		service.withViewerAccess(service.handleInstanceSubscriptions)(w, r)
	case "/amps/instance/sow":
		service.withViewerAccess(service.handleInstanceSOW)(w, r)
	case "/amps/instance/views":
		service.withViewerAccess(service.handleInstanceViews)(w, r)
	case "/amps/instance/actions":
		service.withViewerAccess(service.handleInstanceActions)(w, r)
	case "/amps/instance/replication":
		service.withViewerAccess(service.handleInstanceReplication)(w, r)
	case "/amps/instance/clients":
		service.withViewerAccess(service.handleInstanceClients)(w, r)
	case "/amps/administrator":
		service.withViewerAccess(service.handleAdministrator)(w, r)
	case "/amps/administrator/diagnostics":
		service.withOperatorAccess(service.handleDiagnostics)(w, r)
	case "/amps/administrator/minidump":
		service.withOperatorAccess(service.handleUnsupportedAction)(w, r)
	case "/amps/administrator/sow/clear":
		service.withOperatorAccess(service.handleSOWClear)(w, r)
	case "/amps/sql/ws":
		service.withViewerAccess(service.handleSQLWebSocket)(w, r)
	default:
		switch {
		case strings.HasPrefix(r.URL.Path, "/amps/administrator/clients/") && strings.HasSuffix(r.URL.Path, "/disconnect"):
			service.withOperatorAccess(service.handleClientDisconnect)(w, r)
		case strings.HasPrefix(r.URL.Path, "/amps/administrator/transports/") &&
			(strings.HasSuffix(r.URL.Path, "/enable") || strings.HasSuffix(r.URL.Path, "/disable")):
			service.withOperatorAccess(service.handleTransportAction)(w, r)
		case strings.HasPrefix(r.URL.Path, "/amps/administrator/replication/") &&
			(strings.HasSuffix(r.URL.Path, "/upgrade") || strings.HasSuffix(r.URL.Path, "/downgrade")):
			service.withOperatorAccess(service.handleReplicationAction)(w, r)
		default:
			http.NotFound(w, r)
		}
	}
}

func (service *monitoringService) withViewerAccess(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, ok := service.authorizeRequest(w, r)
		if !ok {
			return
		}
		next(w, r)
	}
}

func (service *monitoringService) withOperatorAccess(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		principal, ok := service.authorizeRequest(w, r)
		if !ok {
			return
		}
		if principal.Role != "operator" {
			jsonError(w, http.StatusForbidden, "operator role required")
			return
		}
		next(w, r)
	}
}

func (service *monitoringService) authorizeRequest(w http.ResponseWriter, r *http.Request) (adminPrincipal, bool) {
	if service.authEnabled() {
		if principal, ok := service.sessionPrincipal(r); ok {
			return principal, true
		}
		if principal, ok := service.basicPrincipal(r); ok {
			return principal, true
		}
	}
	if service.pathAllowedAnonymously(r.URL.Path) {
		return adminPrincipal{Role: "viewer"}, true
	}
	if !service.authEnabled() {
		return adminPrincipal{Role: "operator"}, true
	}

	w.Header().Set("WWW-Authenticate", `Basic realm="`+service.authRealm()+`"`)
	jsonError(w, http.StatusUnauthorized, "authentication required")
	return adminPrincipal{}, false
}

func (service *monitoringService) authEnabled() bool {
	return strings.TrimSpace(service.admin.Authentication) != ""
}

func (service *monitoringService) authRealm() string {
	var authentication = strings.TrimSpace(service.admin.Authentication)
	if authentication == "" {
		return "FakeAMPS"
	}

	var lower = strings.ToLower(authentication)
	var marker = `realm="`
	var index = strings.Index(lower, marker)
	if index < 0 {
		return "FakeAMPS"
	}
	var start = index + len(marker)
	var end = strings.Index(authentication[start:], `"`)
	if end < 0 {
		return "FakeAMPS"
	}
	return authentication[start : start+end]
}

func (service *monitoringService) pathAllowedAnonymously(path string) bool {
	if path == "/" || strings.HasPrefix(path, "/assets/") || path == "/amps/session/login" {
		return true
	}
	for _, allowed := range service.admin.AnonymousPaths {
		var trimmed = strings.TrimSpace(allowed)
		if trimmed == "" {
			continue
		}
		if trimmed == "/" {
			if path == "/" {
				return true
			}
			continue
		}
		if trimmed == path {
			return true
		}
		if strings.HasSuffix(trimmed, "/") && strings.HasPrefix(path, trimmed) {
			return true
		}
		if strings.HasPrefix(path, trimmed+"/") {
			return true
		}
	}
	return false
}

func (service *monitoringService) basicPrincipal(r *http.Request) (adminPrincipal, bool) {
	var username, password, ok = r.BasicAuth()
	if !ok {
		return adminPrincipal{}, false
	}
	var user, exists = service.users[username]
	if !exists || user.Password != password {
		return adminPrincipal{}, false
	}
	return adminPrincipal{Username: user.Username, Role: user.Role}, true
}

func (service *monitoringService) sessionPrincipal(r *http.Request) (adminPrincipal, bool) {
	var cookie, err = r.Cookie(adminSessionCookieName)
	if err != nil {
		return adminPrincipal{}, false
	}

	service.mu.RLock()
	var session, ok = service.sessions[cookie.Value]
	service.mu.RUnlock()
	if !ok {
		return adminPrincipal{}, false
	}
	if service.now().After(session.ExpiresAt) {
		service.mu.Lock()
		delete(service.sessions, cookie.Value)
		service.mu.Unlock()
		return adminPrincipal{}, false
	}
	return adminPrincipal{Username: session.Username, Role: session.Role}, true
}

func (service *monitoringService) handleSessionLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var payload struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		jsonError(w, http.StatusBadRequest, "invalid login payload")
		return
	}

	var user, ok = service.users[payload.Username]
	if !ok || user.Password != payload.Password {
		jsonError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	var sessionID, err = randomToken()
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to create session")
		return
	}

	var expiresAt = service.now().Add(8 * time.Hour)
	service.mu.Lock()
	service.sessions[sessionID] = adminSession{
		ID:        sessionID,
		Username:  user.Username,
		Role:      user.Role,
		ExpiresAt: expiresAt,
	}
	service.mu.Unlock()

	http.SetCookie(w, service.sessionCookie(sessionID, expiresAt))
	jsonResponse(w, adminPrincipal{
		Username: user.Username,
		Role:     user.Role,
	})
}

func (service *monitoringService) sessionCookie(sessionID string, expiresAt time.Time) *http.Cookie {
	var cookie = &http.Cookie{
		Name:     adminSessionCookieName,
		Value:    sessionID,
		Path:     "/",
		HttpOnly: true,
		Expires:  expiresAt,
		SameSite: http.SameSiteLaxMode,
	}
	for _, option := range service.admin.SessionOptions {
		var lower = strings.ToLower(strings.TrimSpace(option))
		switch {
		case lower == "secure=true":
			cookie.Secure = true
		case lower == "samesite=strict":
			cookie.SameSite = http.SameSiteStrictMode
		case lower == "samesite=none":
			cookie.SameSite = http.SameSiteNoneMode
		case lower == "samesite=lax":
			cookie.SameSite = http.SameSiteLaxMode
		}
	}
	return cookie
}

func (service *monitoringService) expiredSessionCookie() *http.Cookie {
	var cookie = service.sessionCookie("", time.Unix(0, 0))
	cookie.Value = ""
	cookie.MaxAge = -1
	return cookie
}

func (service *monitoringService) handleSessionLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if cookie, err := r.Cookie(adminSessionCookieName); err == nil {
		service.mu.Lock()
		delete(service.sessions, cookie.Value)
		service.mu.Unlock()
	}

	http.SetCookie(w, service.expiredSessionCookie())
	jsonResponse(w, map[string]string{"status": "logged_out"})
}

func (service *monitoringService) handleSessionState(w http.ResponseWriter, r *http.Request) {
	if principal, ok := service.sessionPrincipal(r); ok {
		jsonResponse(w, principal)
		return
	}
	if principal, ok := service.basicPrincipal(r); ok {
		jsonResponse(w, principal)
		return
	}
	if !service.authEnabled() {
		jsonResponse(w, adminPrincipal{
			Username: "open",
			Role:     "operator",
		})
		return
	}
	jsonError(w, http.StatusUnauthorized, "no active session")
}

func (service *monitoringService) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	writeDashboardAsset(w, "dashboard/index.html")
}

func (service *monitoringService) handleDashboardCSS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/css; charset=utf-8")
	writeDashboardAsset(w, "dashboard/dashboard.css")
}

func (service *monitoringService) handleDashboardJS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	writeDashboardAsset(w, "dashboard/dashboard.js")
}

func (service *monitoringService) handleAMPSRoot(w http.ResponseWriter, r *http.Request) {
	service.sampleMetrics()
	jsonResponse(w, map[string]interface{}{
		"kind": "root",
		"resources": map[string]string{
			"host":          "/amps/host",
			"instance":      "/amps/instance",
			"topics":        "/amps/instance/topics",
			"administrator": "/amps/administrator",
			"session":       "/amps/session",
			"sql":           "/amps/sql/ws",
		},
		"external_inet_addr": firstNonEmpty(service.admin.ExternalInetAddr, service.admin.InetAddr),
	})
}

func (service *monitoringService) handleHost(w http.ResponseWriter, r *http.Request) {
	service.sampleMetrics()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	jsonResponse(w, map[string]interface{}{
		"kind":       "host",
		"sampled_at": service.now().UTC(),
		"memory": map[string]interface{}{
			"alloc_mb":       mem.Alloc / 1024 / 1024,
			"total_alloc_mb": mem.TotalAlloc / 1024 / 1024,
			"sys_mb":         mem.Sys / 1024 / 1024,
			"num_gc":         mem.NumGC,
		},
		"runtime": map[string]interface{}{
			"goroutines": runtime.NumGoroutine(),
			"gomaxprocs": runtime.GOMAXPROCS(0),
			"num_cpu":    runtime.NumCPU(),
		},
		"system": map[string]interface{}{
			"goos":   runtime.GOOS,
			"goarch": runtime.GOARCH,
		},
	})
}

func (service *monitoringService) handleInstance(w http.ResponseWriter, r *http.Request) {
	service.sampleMetrics()

	var counts = service.instanceCounts()
	jsonResponse(w, map[string]interface{}{
		"kind":       "instance",
		"sampled_at": service.now().UTC(),
		"name":       configuredInstanceName(),
		"version":    *flagVersion,
		"counts":     counts,
		"status": map[string]interface{}{
			"uptime_ms":      time.Since(serverStartTime).Milliseconds(),
			"redirect_uri":   *flagRedirectURI,
			"auth_mode":      firstNonEmpty(service.admin.Authentication, "open"),
			"sql_transport":  service.admin.SQLTransport,
			"external_addr":  service.admin.ExternalInetAddr,
			"admin_listener": firstNonEmpty(service.admin.InetAddr, *flagAdminAddr),
		},
		"transports":  service.transportDescriptors(),
		"replication": service.replicationDescriptors(),
	})
}

func configuredInstanceName() string {
	if effectiveConfig != nil && strings.TrimSpace(effectiveConfig.Runtime.Name) != "" {
		return effectiveConfig.Runtime.Name
	}
	return "fakeamps"
}

func (service *monitoringService) handleInstanceHistory(w http.ResponseWriter, r *http.Request) {
	service.sampleMetrics()

	var metric = strings.TrimSpace(r.URL.Query().Get("metric"))
	if metric == "" {
		jsonError(w, http.StatusBadRequest, "metric is required")
		return
	}

	var start = service.now().Add(-15 * time.Minute)
	var end = service.now()
	if rawStart := strings.TrimSpace(r.URL.Query().Get("start")); rawStart != "" {
		var parsed, err = time.Parse(time.RFC3339, rawStart)
		if err != nil {
			jsonError(w, http.StatusBadRequest, "invalid start")
			return
		}
		start = parsed
	}
	if rawEnd := strings.TrimSpace(r.URL.Query().Get("end")); rawEnd != "" {
		var parsed, err = time.Parse(time.RFC3339, rawEnd)
		if err != nil {
			jsonError(w, http.StatusBadRequest, "invalid end")
			return
		}
		end = parsed
	}
	jsonResponse(w, service.history.Range(metric, start, end))
}

func (service *monitoringService) handleInstanceTopics(w http.ResponseWriter, r *http.Request) {
	service.sampleMetrics()

	var search = strings.TrimSpace(r.URL.Query().Get("search"))
	var topics = service.topicInventory(search)
	jsonResponse(w, map[string]interface{}{
		"count":  len(topics),
		"topics": topics,
	})
}

func (service *monitoringService) handleInstanceSubscriptions(w http.ResponseWriter, r *http.Request) {
	handleAdminSubscriptions(w, r)
}

func (service *monitoringService) handleInstanceSOW(w http.ResponseWriter, r *http.Request) {
	handleAdminSOW(w, r)
}

func (service *monitoringService) handleInstanceViews(w http.ResponseWriter, r *http.Request) {
	handleAdminViews(w, r)
}

func (service *monitoringService) handleInstanceActions(w http.ResponseWriter, r *http.Request) {
	handleAdminActions(w, r)
}

func (service *monitoringService) handleInstanceReplication(w http.ResponseWriter, r *http.Request) {
	service.sampleMetrics()
	jsonResponse(w, service.replicationDescriptors())
}

func (service *monitoringService) handleInstanceClients(w http.ResponseWriter, r *http.Request) {
	service.sampleMetrics()
	jsonResponse(w, map[string]interface{}{
		"count":   len(monitoringClients.List()),
		"clients": monitoringClients.List(),
	})
}

func (service *monitoringService) handleAdministrator(w http.ResponseWriter, r *http.Request) {
	service.sampleMetrics()
	jsonResponse(w, map[string]interface{}{
		"actions": []map[string]string{
			{
				"id":      "diagnostics",
				"method":  http.MethodPost,
				"path":    "/amps/administrator/diagnostics",
				"minRole": "operator",
			},
			{
				"id":      "minidump",
				"method":  http.MethodPost,
				"path":    "/amps/administrator/minidump",
				"minRole": "operator",
			},
		},
	})
}

func (service *monitoringService) handleDiagnostics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var snapshot = map[string]interface{}{
		"time":          service.now().UTC(),
		"connections":   service.instanceCounts()["connections_current"],
		"subscriptions": service.instanceCounts()["subscriptions"],
		"replication":   len(replicaPeers),
	}
	log.Printf("fakeamps: diagnostics requested: %+v", snapshot)
	jsonResponse(w, map[string]interface{}{
		"status":   "ok",
		"snapshot": snapshot,
	})
}

func (service *monitoringService) handleUnsupportedAction(w http.ResponseWriter, r *http.Request) {
	jsonError(w, http.StatusNotImplemented, "unsupported in fakeamps")
}

func (service *monitoringService) handleClientDisconnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var clientID = strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/amps/administrator/clients/"), "/disconnect")
	if !monitoringClients.Disconnect(clientID) {
		jsonError(w, http.StatusNotFound, "client not found")
		return
	}

	jsonResponse(w, map[string]string{
		"status": "disconnected",
		"id":     clientID,
	})
}

func (service *monitoringService) handleTransportAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var suffix = "enable"
	if strings.HasSuffix(r.URL.Path, "/disable") {
		suffix = "disable"
	}
	var transportID = strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/amps/administrator/transports/"), "/"+suffix)
	var enabled = suffix == "enable"
	if !monitoringTransports.SetEnabled(transportID, enabled) {
		jsonError(w, http.StatusNotFound, "transport not found")
		return
	}
	jsonResponse(w, map[string]interface{}{
		"status":  "updated",
		"id":      transportID,
		"enabled": enabled,
	})
}

func (service *monitoringService) handleReplicationAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var suffix = "upgrade"
	if strings.HasSuffix(r.URL.Path, "/downgrade") {
		suffix = "downgrade"
	}
	var peerID = strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/amps/administrator/replication/"), "/"+suffix)
	if !service.setReplicationMode(peerID, suffix+"d") {
		jsonError(w, http.StatusNotFound, "replication peer not found")
		return
	}
	jsonResponse(w, map[string]interface{}{
		"status": "updated",
		"id":     peerID,
		"mode":   suffix + "d",
	})
}

func (service *monitoringService) handleSOWClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if sow == nil {
		jsonResponse(w, map[string]interface{}{"status": "ok", "records_cleared": 0})
		return
	}

	var topic = strings.TrimSpace(r.URL.Query().Get("topic"))
	if topic == "" {
		var cleared, removed = clearAllSOWRecords("clear")
		notifyWorkspaceRemovals(removed)
		jsonResponse(w, map[string]interface{}{
			"status":          "cleared",
			"records_cleared": cleared,
		})
		return
	}

	var cleared, removed, ok = clearSOWTopicRecords(topic, "clear")
	if !ok {
		jsonResponse(w, map[string]interface{}{
			"status":          "not_found",
			"topic":           topic,
			"records_cleared": 0,
		})
		return
	}

	notifyWorkspaceRemovals(removed)
	jsonResponse(w, map[string]interface{}{
		"status":          "cleared",
		"topic":           topic,
		"records_cleared": cleared,
	})
}

func (service *monitoringService) handleSQLWebSocket(w http.ResponseWriter, r *http.Request) {
	var hijacker, ok = w.(http.Hijacker)
	if !ok {
		jsonError(w, http.StatusInternalServerError, "websocket unsupported")
		return
	}
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		jsonError(w, http.StatusBadRequest, "expected websocket upgrade")
		return
	}
	if service.admin.SQLTransport != "" && !service.hasTransport(service.admin.SQLTransport) {
		jsonError(w, http.StatusServiceUnavailable, "configured SQL transport is unavailable")
		return
	}

	var conn, rw, err = hijacker.Hijack()
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "websocket hijack failed")
		return
	}

	var acceptRaw = sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + websocketAcceptGUID))
	var accept = base64.StdEncoding.EncodeToString(acceptRaw[:])

	_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
	_, _ = rw.WriteString("Upgrade: websocket\r\n")
	_, _ = rw.WriteString("Connection: Upgrade\r\n")
	_, _ = rw.WriteString("Sec-WebSocket-Accept: " + accept + "\r\n\r\n")
	_ = rw.Flush()

	var ws = &websocketConn{
		Conn:   conn,
		reader: rw.Reader,
	}
	defer func() {
		if closeErr := ws.Close(); closeErr != nil {
			log.Printf("fakeamps: websocket close failed: %v", closeErr)
		}
	}()
	defer workspaceSessions.Remove(ws)

	for {
		var payload, opcode, readErr = ws.readFrame()
		if readErr != nil {
			return
		}
		switch opcode {
		case 0x8:
			return
		case 0x9:
			_ = ws.writeControlFrame(0xA, payload)
			continue
		case 0x1, 0x2:
			service.handleSQLWorkspaceMessage(ws, payload)
		}
	}
}

func (service *monitoringService) handleSQLWorkspaceMessage(ws *websocketConn, payload []byte) {
	var request sqlWorkspaceRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		_, _ = ws.WriteText(mustJSON(map[string]string{
			"type":  "error",
			"error": "invalid_payload",
		}))
		return
	}

	if request.Type == "" {
		_, _ = ws.WriteText(service.handleSQLWorkspacePayload(payload))
		return
	}

	switch request.Type {
	case "run":
		service.handleSQLWorkspaceRun(ws, request)
	case "stop":
		service.handleSQLWorkspaceStop(ws, request)
	default:
		_, _ = ws.WriteText(mustJSON(map[string]interface{}{
			"type":       "workspace_error",
			"request_id": request.RequestID,
			"error":      "unsupported_request_type",
		}))
	}
}

func (service *monitoringService) handleSQLWorkspacePayload(payload []byte) []byte {
	var request sqlWorkspaceRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return mustJSON(map[string]string{
			"type":  "error",
			"error": "invalid_payload",
		})
	}

	switch request.Action {
	case "publish", "delta_publish":
		return mustJSON(map[string]string{
			"type":  "error",
			"error": "read_only",
		})
	case "sow":
		return mustJSON(map[string]interface{}{
			"type":    "sow_result",
			"topic":   request.Topic,
			"filter":  request.Filter,
			"records": service.collectSOWRecords(request.Topic, request.Filter),
		})
	case "subscribe":
		return mustJSON(map[string]interface{}{
			"type":   "subscribe_ready",
			"topic":  request.Topic,
			"filter": request.Filter,
		})
	case "sow_and_subscribe":
		return mustJSON(map[string]interface{}{
			"type":    "sow_and_subscribe_result",
			"topic":   request.Topic,
			"filter":  request.Filter,
			"records": service.collectSOWRecords(request.Topic, request.Filter),
		})
	default:
		return mustJSON(map[string]string{
			"type":  "error",
			"error": "unsupported_action",
		})
	}
}

func (service *monitoringService) handleSQLWorkspaceRun(ws *websocketConn, request sqlWorkspaceRequest) {
	var mode = normalizeWorkspaceMode(request.Mode)
	if mode == "" {
		_, _ = ws.WriteText(mustJSON(map[string]interface{}{
			"type":       "workspace_error",
			"request_id": request.RequestID,
			"error":      "unsupported_action",
		}))
		return
	}

	workspaceSessions.Remove(ws)

	var readyMessage = mustJSON(map[string]interface{}{
		"type":       "workspace_ready",
		"request_id": request.RequestID,
		"mode":       mode,
		"topic":      request.Topic,
		"filter":     request.Filter,
		"delta":      request.Options.Delta,
		"live":       request.Options.Live,
	})

	switch mode {
	case "sow":
		var rows = service.collectSOWRows(
			request.Topic,
			request.Filter,
			workspaceTopN(request.Options),
			request.Options.OrderBy,
			request.Options.Bookmark,
		)
		_, _ = ws.WriteText(readyMessage)
		_, _ = ws.WriteText(mustJSON(map[string]interface{}{
			"type":       "workspace_snapshot",
			"request_id": request.RequestID,
			"mode":       mode,
			"rows":       rows,
		}))
		_, _ = ws.WriteText(mustJSON(map[string]interface{}{
			"type":       "workspace_complete",
			"request_id": request.RequestID,
			"reason":     "snapshot_complete",
		}))
	case "subscribe":
		var rows = []map[string]interface{}{}
		var snapshotMessage = mustJSON(map[string]interface{}{
			"type":       "workspace_snapshot",
			"request_id": request.RequestID,
			"mode":       mode,
			"rows":       rows,
		})
		if request.Options.Live {
			var query = workspaceLiveQuery{
				RequestID: request.RequestID,
				Mode:      mode,
				Topic:     request.Topic,
				Filter:    request.Filter,
				Options:   request.Options,
			}
			if workspaceQueryUsesSnapshots(query) {
				query.Signature = workspaceRowsSignature(rows)
			}
			if err := writeLiveWorkspaceBootstrap(ws, query, readyMessage, snapshotMessage); err != nil {
				workspaceSessions.Remove(ws)
			}
			return
		}
		_, _ = ws.WriteText(readyMessage)
		_, _ = ws.WriteText(snapshotMessage)
		_, _ = ws.WriteText(mustJSON(map[string]interface{}{
			"type":       "workspace_complete",
			"request_id": request.RequestID,
			"reason":     "snapshot_complete",
		}))
	case "sow_and_subscribe":
		var rows = service.collectSOWRows(
			request.Topic,
			request.Filter,
			workspaceTopN(request.Options),
			request.Options.OrderBy,
			request.Options.Bookmark,
		)
		var snapshotMessage = mustJSON(map[string]interface{}{
			"type":       "workspace_snapshot",
			"request_id": request.RequestID,
			"mode":       mode,
			"rows":       rows,
		})
		if request.Options.Live {
			var query = workspaceLiveQuery{
				RequestID: request.RequestID,
				Mode:      mode,
				Topic:     request.Topic,
				Filter:    request.Filter,
				Options:   request.Options,
			}
			if workspaceQueryUsesSnapshots(query) {
				query.Signature = workspaceRowsSignature(rows)
			}
			if err := writeLiveWorkspaceBootstrap(ws, query, readyMessage, snapshotMessage); err != nil {
				workspaceSessions.Remove(ws)
			}
			return
		}
		_, _ = ws.WriteText(readyMessage)
		_, _ = ws.WriteText(snapshotMessage)
		_, _ = ws.WriteText(mustJSON(map[string]interface{}{
			"type":       "workspace_complete",
			"request_id": request.RequestID,
			"reason":     "snapshot_complete",
		}))
	}
}

func writeLiveWorkspaceBootstrap(ws *websocketConn, query workspaceLiveQuery, readyMessage []byte, snapshotMessage []byte) error {
	if ws == nil {
		return nil
	}

	ws.writeMu.Lock()
	defer ws.writeMu.Unlock()

	workspaceSessions.Set(ws, query)
	if _, err := ws.writeDataFrameLocked(0x1, readyMessage); err != nil {
		return err
	}
	if _, err := ws.writeDataFrameLocked(0x1, snapshotMessage); err != nil {
		return err
	}
	return nil
}

func (service *monitoringService) handleSQLWorkspaceStop(ws *websocketConn, request sqlWorkspaceRequest) {
	if workspaceSessions.Stop(ws, request.RequestID) {
		_, _ = ws.WriteText(mustJSON(map[string]interface{}{
			"type":       "workspace_complete",
			"request_id": request.RequestID,
			"reason":     "stopped",
		}))
		return
	}

	_, _ = ws.WriteText(mustJSON(map[string]interface{}{
		"type":       "workspace_error",
		"request_id": request.RequestID,
		"error":      "no_active_query",
	}))
}

func (service *monitoringService) collectSOWRecords(topic string, filter string) []map[string]interface{} {
	if sow == nil || topic == "" {
		return []map[string]interface{}{}
	}

	var snapshotRecords = workspaceSnapshotRecords(topic, filter, -1, "", "")
	var records = make([]map[string]interface{}, 0, len(snapshotRecords))
	for _, record := range snapshotRecords {
		records = append(records, map[string]interface{}{
			"topic":     record.topic,
			"sow_key":   record.sowKey,
			"bookmark":  record.bookmark,
			"timestamp": record.timestamp,
			"payload":   json.RawMessage(record.payload),
		})
	}
	return records
}

func (service *monitoringService) collectSOWRows(topic string, filter string, topN int, orderBy string, bookmark string) []map[string]interface{} {
	if sow == nil || topic == "" {
		return []map[string]interface{}{}
	}

	var snapshotRecords = workspaceSnapshotRecords(topic, filter, topN, orderBy, bookmark)
	var rows = make([]map[string]interface{}, 0, len(snapshotRecords))
	for _, record := range snapshotRecords {
		rows = append(rows, workspaceRow(record.topic, record.sowKey, record.bookmark, record.timestamp, "record", record.payload))
	}
	return rows
}

func (service *monitoringService) sampleMetrics() {
	var now = service.now().UTC()
	for metric, value := range service.metricSnapshot() {
		service.history.Add(metric, now, value)
	}
	if service.admin.FileName != "" {
		if err := service.history.SaveFile(service.admin.FileName); err != nil {
			log.Printf("fakeamps: persist admin history %s failed: %v", service.admin.FileName, err)
		}
	}
}

func (service *monitoringService) metricSnapshot() map[string]float64 {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	var counts = service.instanceCounts()
	return map[string]float64{
		"instance.connections.current": float64(counts["connections_current"]),
		"instance.subscriptions.count": float64(counts["subscriptions"]),
		"instance.sow.records":         float64(counts["sow_records"]),
		"host.memory.alloc_mb":         float64(mem.Alloc / 1024 / 1024),
		"host.runtime.goroutines":      float64(runtime.NumGoroutine()),
	}
}

func (service *monitoringService) instanceCounts() map[string]int64 {
	var subscriptions int64
	topicSubscribers.Range(func(_ interface{}, value interface{}) bool {
		var set = value.(*subscriberSet)
		set.mu.RLock()
		subscriptions += int64(len(set.subs))
		set.mu.RUnlock()
		return true
	})

	var sowRecords int64
	if sow != nil {
		sow.topics.Range(func(_ interface{}, value interface{}) bool {
			var topic = value.(*topicSOW)
			topic.mu.RLock()
			sowRecords += int64(len(topic.records))
			topic.mu.RUnlock()
			return true
		})
	}

	var journalEntries int64
	if journal != nil {
		journal.mu.RLock()
		journalEntries = int64(journal.count)
		journal.mu.RUnlock()
	}

	return map[string]int64{
		"connections_accepted": int64(globalConnectionsAccepted.Load()),
		"connections_current":  globalConnectionsCurrent.Load(),
		"subscriptions":        subscriptions,
		"sow_records":          sowRecords,
		"journal_entries":      journalEntries,
		"views":                int64(service.viewCount()),
		"actions":              int64(service.actionCount()),
		"replication_peers":    int64(len(replicaPeers)),
	}
}

func (service *monitoringService) transportDescriptors() []transportDescriptor {
	var current = monitoringTransports.List()
	if len(current) > 0 {
		return current
	}
	return service.configuredTransports()
}

func (service *monitoringService) configuredTransports() []transportDescriptor {
	if effectiveConfig != nil && len(effectiveConfig.Runtime.Transports) > 0 {
		var out = make([]transportDescriptor, 0, len(effectiveConfig.Runtime.Transports))
		for index, transport := range effectiveConfig.Runtime.Transports {
			out = append(out, transportDescriptor{
				ID:          firstNonEmpty(transport.Name, "transport-"+strconv.Itoa(index+1)),
				Name:        firstNonEmpty(transport.Name, "transport-"+strconv.Itoa(index+1)),
				Type:        transport.Type,
				InetAddr:    transport.InetAddr,
				Protocol:    transport.Protocol,
				MessageType: transport.MessageType,
				Enabled:     true,
			})
		}
		return out
	}

	return []transportDescriptor{
		{
			ID:          "primary",
			Name:        "primary",
			Type:        "tcp",
			InetAddr:    *flagAddr,
			Protocol:    "amps",
			MessageType: "json",
			Enabled:     true,
		},
	}
}

func primaryTransportID() string {
	if effectiveConfig != nil && len(effectiveConfig.Runtime.Transports) > 0 {
		if id, ok := transportIDForListenerAddr(*flagAddr, effectiveConfig.Runtime.Transports); ok {
			return id
		}
		return firstNonEmpty(effectiveConfig.Runtime.Transports[0].Name, "transport-1")
	}
	return "primary"
}

func transportIDForListenerAddr(listenerAddr string, transports []ampsconfig.TransportConfig) (string, bool) {
	listenerAddr = strings.TrimSpace(listenerAddr)
	for index, transport := range transports {
		if strings.TrimSpace(transport.InetAddr) != listenerAddr {
			continue
		}
		return firstNonEmpty(transport.Name, "transport-"+strconv.Itoa(index+1)), true
	}
	return "", false
}

func (service *monitoringService) hasTransport(name string) bool {
	return monitoringTransports.Exists(name) && monitoringTransports.Enabled(name)
}

func (service *monitoringService) replicationDescriptors() []replicationDescriptor {
	var out = make([]replicationDescriptor, 0, len(replicaPeers))
	for index, peer := range replicaPeers {
		peer.mu.Lock()
		var alive = peer.alive
		var mode = firstNonEmpty(peer.mode, "active")
		peer.mu.Unlock()
		out = append(out, replicationDescriptor{
			ID:        "peer-" + strconv.Itoa(index+1),
			Address:   peer.addr,
			Connected: alive,
			Mode:      mode,
		})
	}
	return out
}

func (service *monitoringService) setReplicationMode(id string, mode string) bool {
	for index, peer := range replicaPeers {
		var currentID = "peer-" + strconv.Itoa(index+1)
		if currentID != id {
			continue
		}
		peer.mu.Lock()
		var previousMode = firstNonEmpty(peer.mode, "active")
		if previousMode != "downgraded" || mode == "downgraded" {
			peer.mode = mode
		}
		peer.mu.Unlock()
		if previousMode == "downgraded" && mode != "downgraded" {
			go func(currentPeer *peerConn, peerID string) {
				if err := currentPeer.resumeDeferredPublishes(mode); err != nil {
					log.Printf("fakeamps: replication resume for %s failed: %v", peerID, err)
				}
			}(peer, currentID)
		}
		return true
	}
	return false
}

func (service *monitoringService) viewCount() int {
	viewsMu.RLock()
	defer viewsMu.RUnlock()
	return len(views)
}

func (service *monitoringService) actionCount() int {
	actionsMu.RLock()
	defer actionsMu.RUnlock()
	return len(actions)
}

func normalizeWorkspaceMode(raw string) string {
	switch strings.TrimSpace(raw) {
	case "sow":
		return "sow"
	case "subscribe":
		return "subscribe"
	case "sow_and_subscribe":
		return "sow_and_subscribe"
	default:
		return ""
	}
}

func workspaceTopN(options sqlWorkspaceOptions) int {
	if options.TopN == nil {
		return -1
	}
	return *options.TopN
}

func randomToken() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func jsonError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}

func mustJSON(value interface{}) []byte {
	var payload, err = json.Marshal(value)
	if err != nil {
		return []byte(`{"type":"error","error":"marshal_failure"}`)
	}
	return payload
}

func writeDashboardAsset(w http.ResponseWriter, path string) {
	var payload, err = fs.ReadFile(dashboardAssets, path)
	if err != nil {
		http.Error(w, "dashboard asset unavailable", http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(payload)
}

type workspaceRemovedRecord struct {
	Topic    string
	SOWKey   string
	Bookmark string
	Reason   string
}

func clearAllSOWRecords(reason string) (int, []workspaceRemovedRecord) {
	if sow == nil {
		return 0, nil
	}

	var cleared int
	var removed []workspaceRemovedRecord
	for _, topic := range sow.allTopics() {
		var raw, ok = sow.topics.Load(topic)
		if !ok {
			continue
		}
		var current = raw.(*topicSOW)
		current.mu.Lock()
		cleared += len(current.records)
		for _, record := range current.records {
			removed = append(removed, workspaceRemovedRecord{
				Topic:    topic,
				SOWKey:   record.sowKey,
				Bookmark: record.bookmark,
				Reason:   reason,
			})
		}
		current.records = make(map[string]*sowRecord)
		current.mu.Unlock()
	}
	return cleared, removed
}

func clearSOWTopicRecords(topic string, reason string) (int, []workspaceRemovedRecord, bool) {
	if sow == nil {
		return 0, nil, false
	}

	var raw, ok = sow.topics.Load(topic)
	if !ok {
		return 0, nil, false
	}

	var current = raw.(*topicSOW)
	current.mu.Lock()
	var cleared = len(current.records)
	var removed = make([]workspaceRemovedRecord, 0, len(current.records))
	for _, record := range current.records {
		removed = append(removed, workspaceRemovedRecord{
			Topic:    topic,
			SOWKey:   record.sowKey,
			Bookmark: record.bookmark,
			Reason:   reason,
		})
	}
	current.records = make(map[string]*sowRecord)
	current.mu.Unlock()
	return cleared, removed, true
}

func notifyWorkspaceRemovals(records []workspaceRemovedRecord) {
	var stats connStats
	for _, record := range records {
		workspaceSessions.NotifyRemove(record.Topic, record.SOWKey, record.Bookmark, record.Reason)
		if *flagFanout {
			fanoutOOFWithReason(nil, record.Topic, record.SOWKey, record.Bookmark, record.Reason, &stats)
		}
	}
}

func notifyExpiredSOWRecords(records []sowRecord) {
	if len(records) == 0 {
		return
	}

	var removed = make([]workspaceRemovedRecord, 0, len(records))
	for _, record := range records {
		removed = append(removed, workspaceRemovedRecord{
			Topic:    record.topic,
			SOWKey:   record.sowKey,
			Bookmark: record.bookmark,
			Reason:   "expire",
		})
	}
	notifyWorkspaceRemovals(removed)
}

func startAdminServer(addr string) error {
	if addr == "" {
		return nil
	}
	adminAddr = addr

	var service = newMonitoringService(monitoringServiceOptions{})
	if err := validateAdminTLSPair(service.admin.Certificate, service.admin.PrivateKey); err != nil {
		return err
	}
	var server, err = newAdminHTTPServer(addr, service.Handler(), service.admin)
	if err != nil {
		return err
	}
	service.StartSampling()

	go func() {
		log.Printf("fakeamps: admin API listening on %s", addr)
		var serveErr error
		if service.admin.Certificate != "" && service.admin.PrivateKey != "" {
			serveErr = server.ListenAndServeTLS(service.admin.Certificate, service.admin.PrivateKey)
		} else {
			serveErr = server.ListenAndServe()
		}
		if serveErr != nil && serveErr != http.ErrServerClosed {
			log.Printf("fakeamps: admin API error: %v", serveErr)
		}
	}()

	return nil
}

func newAdminHTTPServer(addr string, handler http.Handler, adminConfig ampsconfig.AdminConfig) (*http.Server, error) {
	var tlsConfig, err = buildAdminTLSConfig(adminConfig.Ciphers)
	if err != nil {
		return nil, err
	}

	return &http.Server{
		Addr:      addr,
		Handler:   applyAdminHeaders(handler, adminConfig.Headers),
		TLSConfig: tlsConfig,
	}, nil
}

func applyAdminHeaders(handler http.Handler, headers []string) http.Handler {
	var parsed = parseAdminHeaders(headers)
	if len(parsed) == 0 {
		return handler
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for key, values := range parsed {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		handler.ServeHTTP(w, r)
	})
}

func parseAdminHeaders(headers []string) map[string][]string {
	var parsed = make(map[string][]string)
	for _, header := range headers {
		var name, value, ok = strings.Cut(header, ":")
		if !ok {
			continue
		}
		name = strings.TrimSpace(name)
		value = strings.TrimSpace(value)
		if name == "" || value == "" {
			continue
		}
		parsed[name] = append(parsed[name], value)
	}
	return parsed
}

func buildAdminTLSConfig(cipherNames []string) (*tls.Config, error) {
	if len(cipherNames) == 0 {
		return nil, nil
	}

	var suiteByName = make(map[string]*tls.CipherSuite)
	for _, suite := range tls.CipherSuites() {
		suiteByName[suite.Name] = suite
	}
	var insecureSuiteByName = make(map[string]struct{})
	for _, suite := range tls.InsecureCipherSuites() {
		insecureSuiteByName[suite.Name] = struct{}{}
	}

	var config = &tls.Config{}
	for _, name := range cipherNames {
		var suiteName = strings.TrimSpace(name)
		var suite, ok = suiteByName[suiteName]
		if !ok {
			if _, insecure := insecureSuiteByName[suiteName]; insecure {
				return nil, fmt.Errorf("insecure admin cipher %q is not allowed", name)
			}
			return nil, fmt.Errorf("unsupported admin cipher %q", name)
		}

		var supported bool
		for _, version := range suite.SupportedVersions {
			if version <= tls.VersionTLS12 {
				config.CipherSuites = append(config.CipherSuites, suite.ID)
				supported = true
				break
			}
		}
		if !supported {
			return nil, fmt.Errorf("admin cipher %q targets TLS 1.3, which Go cannot restrict explicitly", name)
		}
	}

	config.MaxVersion = tls.VersionTLS12
	return config, nil
}

func validateAdminTLSPair(certificatePath string, privateKeyPath string) error {
	if certificatePath == "" && privateKeyPath == "" {
		return nil
	}
	if certificatePath == "" || privateKeyPath == "" {
		return fmt.Errorf("admin TLS configuration requires both certificate and private key")
	}
	return nil
}
