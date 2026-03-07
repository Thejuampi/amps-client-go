package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Thejuampi/amps-client-go/internal/ampsconfig"
)

func TestMonitoringServiceServesDashboardShellAndAMPSTree(t *testing.T) {
	var service = newMonitoringService(monitoringServiceOptions{})
	var handler = service.Handler()

	var response = httptest.NewRecorder()
	var request = httptest.NewRequest(http.MethodGet, "/", nil)
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200", response.Code)
	}
	if !strings.Contains(response.Body.String(), `<div id="app"></div>`) {
		t.Fatalf("GET / body = %q, want dashboard shell", response.Body.String())
	}
	if strings.Contains(response.Body.String(), "Galvanometer") {
		t.Fatalf("GET / body = %q, should not contain Galvanometer branding", response.Body.String())
	}

	response = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/amps", nil)
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET /amps status = %d, want 200", response.Code)
	}
	if !strings.Contains(response.Body.String(), `"/amps/host"`) {
		t.Fatalf("GET /amps body = %q, want host resource", response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"/amps/instance"`) {
		t.Fatalf("GET /amps body = %q, want instance resource", response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"/amps/administrator"`) {
		t.Fatalf("GET /amps body = %q, want administrator resource", response.Body.String())
	}
}

func TestMonitoringServiceRequiresSessionAndEnforcesRoles(t *testing.T) {
	var service = newMonitoringService(monitoringServiceOptions{
		Admin: ampsconfig.AdminConfig{
			Authentication: `Basic realm="FakeAMPS"`,
			AnonymousPaths: []string{"/"},
		},
		Users: []ampsconfig.AdminUserConfig{
			{Username: "viewer", Password: "viewer-pass", Role: "viewer"},
			{Username: "operator", Password: "operator-pass", Role: "operator"},
		},
	})
	var server = httptest.NewServer(service.Handler())
	defer server.Close()

	var unauthenticated, err = http.Get(server.URL + "/amps/instance")
	if err != nil {
		t.Fatalf("GET /amps/instance error: %v", err)
	}
	defer unauthenticated.Body.Close()
	if unauthenticated.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /amps/instance status = %d, want 401", unauthenticated.StatusCode)
	}
	if !strings.Contains(unauthenticated.Header.Get("WWW-Authenticate"), `Basic realm="FakeAMPS"`) {
		t.Fatalf("WWW-Authenticate = %q, want Basic realm header", unauthenticated.Header.Get("WWW-Authenticate"))
	}

	var viewerClient = newSessionClient(t)
	loginAdminSession(t, viewerClient, server.URL, "viewer", "viewer-pass")

	var viewerResponse, viewerErr = viewerClient.Get(server.URL + "/amps/instance")
	if viewerErr != nil {
		t.Fatalf("viewer GET /amps/instance error: %v", viewerErr)
	}
	defer viewerResponse.Body.Close()
	if viewerResponse.StatusCode != http.StatusOK {
		t.Fatalf("viewer GET /amps/instance status = %d, want 200", viewerResponse.StatusCode)
	}

	var viewerDiagnosticsRequest, requestErr = http.NewRequest(http.MethodPost, server.URL+"/amps/administrator/diagnostics", http.NoBody)
	if requestErr != nil {
		t.Fatalf("NewRequest(viewer diagnostics): %v", requestErr)
	}
	var viewerDiagnosticsResponse, diagnosticsErr = viewerClient.Do(viewerDiagnosticsRequest)
	if diagnosticsErr != nil {
		t.Fatalf("viewer POST /amps/administrator/diagnostics error: %v", diagnosticsErr)
	}
	defer viewerDiagnosticsResponse.Body.Close()
	if viewerDiagnosticsResponse.StatusCode != http.StatusForbidden {
		t.Fatalf("viewer POST /amps/administrator/diagnostics status = %d, want 403", viewerDiagnosticsResponse.StatusCode)
	}

	var operatorClient = newSessionClient(t)
	loginAdminSession(t, operatorClient, server.URL, "operator", "operator-pass")

	var operatorDiagnosticsRequest, operatorRequestErr = http.NewRequest(http.MethodPost, server.URL+"/amps/administrator/diagnostics", http.NoBody)
	if operatorRequestErr != nil {
		t.Fatalf("NewRequest(operator diagnostics): %v", operatorRequestErr)
	}
	var operatorDiagnosticsResponse, operatorErr = operatorClient.Do(operatorDiagnosticsRequest)
	if operatorErr != nil {
		t.Fatalf("operator POST /amps/administrator/diagnostics error: %v", operatorErr)
	}
	defer operatorDiagnosticsResponse.Body.Close()
	if operatorDiagnosticsResponse.StatusCode != http.StatusOK {
		t.Fatalf("operator POST /amps/administrator/diagnostics status = %d, want 200", operatorDiagnosticsResponse.StatusCode)
	}
}

func TestMonitoringServiceAuthenticationWithoutUsersFailsClosed(t *testing.T) {
	var service = newMonitoringService(monitoringServiceOptions{
		Admin: ampsconfig.AdminConfig{
			Authentication: `Basic realm="FakeAMPS"`,
			AnonymousPaths: []string{"/"},
		},
	})
	var handler = service.Handler()

	var response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/amps/instance", nil))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("GET /amps/instance status = %d, want 401 when auth is configured without users", response.Code)
	}
}

func TestMonitoringServiceSessionStateReportsOpenOperatorAccess(t *testing.T) {
	var service = newMonitoringService(monitoringServiceOptions{})
	var handler = service.Handler()

	var response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/amps/session", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /amps/session status = %d, want 200 in open-admin mode", response.Code)
	}
	var principal adminPrincipal
	if err := json.Unmarshal(response.Body.Bytes(), &principal); err != nil {
		t.Fatalf("json.Unmarshal(open session): %v", err)
	}
	if principal.Role != "operator" {
		t.Fatalf("GET /amps/session role = %q, want operator", principal.Role)
	}
}

func TestMonitoringServiceAnonymousPathsStayViewerOnly(t *testing.T) {
	var service = newMonitoringService(monitoringServiceOptions{
		Admin: ampsconfig.AdminConfig{
			Authentication: `Basic realm="FakeAMPS"`,
			AnonymousPaths: []string{"/", "/amps"},
		},
		Users: []ampsconfig.AdminUserConfig{
			{Username: "operator", Password: "operator-pass", Role: "operator"},
		},
	})
	var handler = service.Handler()

	var response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/amps/administrator/diagnostics", nil))
	if response.Code != http.StatusForbidden {
		t.Fatalf("POST /amps/administrator/diagnostics status = %d, want 403 for anonymous viewer access", response.Code)
	}
}

func TestMonitoringServiceAnonymousPathsStillHonorOperatorCredentials(t *testing.T) {
	var service = newMonitoringService(monitoringServiceOptions{
		Admin: ampsconfig.AdminConfig{
			Authentication: `Basic realm="FakeAMPS"`,
			AnonymousPaths: []string{"/", "/amps"},
		},
		Users: []ampsconfig.AdminUserConfig{
			{Username: "operator", Password: "operator-pass", Role: "operator"},
		},
	})
	var handler = service.Handler()

	var request = httptest.NewRequest(http.MethodPost, "/amps/administrator/diagnostics", nil)
	request.SetBasicAuth("operator", "operator-pass")
	var response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("POST /amps/administrator/diagnostics with operator credentials status = %d, want 200", response.Code)
	}
}

func TestMonitoringServiceSupportsAnonymousPathsAndAdminAliases(t *testing.T) {
	var service = newMonitoringService(monitoringServiceOptions{
		Admin: ampsconfig.AdminConfig{
			Authentication: `Basic realm="FakeAMPS"`,
			AnonymousPaths: []string{"/", "/amps/host"},
		},
		Users: []ampsconfig.AdminUserConfig{
			{Username: "operator", Password: "operator-pass", Role: "operator"},
		},
	})
	var handler = service.Handler()

	var hostResponse = httptest.NewRecorder()
	var hostRequest = httptest.NewRequest(http.MethodGet, "/amps/host", nil)
	handler.ServeHTTP(hostResponse, hostRequest)
	if hostResponse.Code != http.StatusOK {
		t.Fatalf("GET /amps/host status = %d, want 200", hostResponse.Code)
	}

	var aliasResponse = httptest.NewRecorder()
	var aliasRequest = httptest.NewRequest(http.MethodGet, "/admin/status", nil)
	aliasRequest.SetBasicAuth("operator", "operator-pass")
	handler.ServeHTTP(aliasResponse, aliasRequest)
	if aliasResponse.Code != http.StatusOK {
		t.Fatalf("GET /admin/status status = %d, want 200", aliasResponse.Code)
	}
	if !strings.Contains(aliasResponse.Body.String(), `"server": "fakeamps"`) {
		t.Fatalf("GET /admin/status body = %q, want fakeamps status payload", aliasResponse.Body.String())
	}
}

func TestMonitoringServiceSQLWebSocketIsReadOnlyAndCanQuerySOW(t *testing.T) {
	var oldSOW = sow
	sow = newSOWCache()
	defer func() {
		sow = oldSOW
	}()

	sow.upsert("orders", "k1", []byte(`{"id":1,"status":"open"}`), "bm1", "ts1", 1, 0)

	var service = newMonitoringService(monitoringServiceOptions{})
	var server = httptest.NewServer(service.Handler())
	defer server.Close()

	var websocketConn = dialAdminWebSocket(t, server.URL, "/amps/sql/ws", nil)
	defer websocketConn.Close()

	writeTextWebSocketFrame(t, websocketConn, []byte(`{"action":"sow","topic":"orders"}`))
	var opcode, sowResponse = readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if opcode != 0x1 {
		t.Fatalf("sow websocket opcode = 0x%x, want text frame", opcode)
	}
	if !strings.Contains(string(sowResponse), `"type":"sow_result"`) {
		t.Fatalf("sow websocket response = %s, want sow_result", string(sowResponse))
	}
	if !strings.Contains(string(sowResponse), `"status":"open"`) {
		t.Fatalf("sow websocket response = %s, want sow payload", string(sowResponse))
	}

	writeTextWebSocketFrame(t, websocketConn, []byte(`{"action":"publish","topic":"orders","payload":{"id":2}}`))
	_, publishResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(publishResponse), `"error":"read_only"`) {
		t.Fatalf("publish websocket response = %s, want read_only error", string(publishResponse))
	}
}

func TestMonitoringServiceWorkspaceTopZeroReturnsNoRows(t *testing.T) {
	var oldSOW = sow
	sow = newSOWCache()
	defer func() {
		sow = oldSOW
	}()

	sow.upsert("orders", "k1", []byte(`{"id":1,"status":"open"}`), "bm1", "ts1", 1, 0)
	sow.upsert("orders", "k2", []byte(`{"id":2,"status":"open"}`), "bm2", "ts2", 2, 0)

	var service = newMonitoringService(monitoringServiceOptions{})
	var initialRows = service.collectSOWRows("orders", "", 0, "", "")
	if len(initialRows) != 0 {
		t.Fatalf("collectSOWRows(top_n=0) returned %d rows, want none", len(initialRows))
	}

	var topZero = 0

	var liveRows = workspaceSnapshotRows(workspaceLiveQuery{
		RequestID: "req-top-zero",
		Mode:      "sow_and_subscribe",
		Topic:     "orders",
		Options: sqlWorkspaceOptions{
			TopN: &topZero,
			Live: true,
		},
	})
	if len(liveRows) != 0 {
		t.Fatalf("workspaceSnapshotRows(top_n=0) returned %d rows, want none", len(liveRows))
	}
}

func TestMonitoringServiceTopicsEndpointAggregatesKnownTopicsAndFiltersSearch(t *testing.T) {
	var oldSOW = sow
	sow = newSOWCache()
	defer func() {
		sow = oldSOW
	}()

	resetTopicConfigsForTest()
	defer resetTopicConfigsForTest()
	resetTopicSubscribersForTest()
	defer resetTopicSubscribersForTest()
	resetViewsForTest()
	defer resetViewsForTest()
	resetActionsForTest()
	defer resetActionsForTest()

	sow.upsert("orders", "k1", []byte(`{"id":1}`), "bm1", "ts1", 1, 0)
	getOrSetTopicMessageType("orders", "json")
	getOrSetTopicMessageType("quotes", "xml")

	var subscriberConn, subscriberPeer = net.Pipe()
	defer subscriberConn.Close()
	defer subscriberPeer.Close()
	registerSubscription("queue://alerts", &subscription{
		conn:  subscriberConn,
		subID: "sub-1",
		topic: "queue://alerts",
	})

	registerView(&viewDef{name: "orders_view", sources: []string{"orders"}})
	registerAction(actionDef{
		trigger:    triggerOnPublish,
		topicMatch: "orders",
		action:     actionRoute,
		target:     "orders_archive",
	})

	var service = newMonitoringService(monitoringServiceOptions{})
	var handler = service.Handler()

	var response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/amps/instance/topics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /amps/instance/topics status = %d, want 200", response.Code)
	}
	if !strings.Contains(response.Body.String(), `"name": "orders"`) {
		t.Fatalf("GET /amps/instance/topics body = %q, want orders topic", response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"name": "quotes"`) {
		t.Fatalf("GET /amps/instance/topics body = %q, want quotes topic", response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"name": "queue://alerts"`) {
		t.Fatalf("GET /amps/instance/topics body = %q, want queue topic", response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"name": "orders_view"`) {
		t.Fatalf("GET /amps/instance/topics body = %q, want view topic", response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"name": "orders_archive"`) {
		t.Fatalf("GET /amps/instance/topics body = %q, want action target topic", response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"subscription_count": 1`) {
		t.Fatalf("GET /amps/instance/topics body = %q, want subscription count", response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"sources": [`) {
		t.Fatalf("GET /amps/instance/topics body = %q, want sources list", response.Body.String())
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/amps/instance/topics?search=archive", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /amps/instance/topics?search=archive status = %d, want 200", response.Code)
	}
	if !strings.Contains(response.Body.String(), `"name": "orders_archive"`) {
		t.Fatalf("GET /amps/instance/topics?search=archive body = %q, want archive topic", response.Body.String())
	}
	if strings.Contains(response.Body.String(), `"name": "quotes"`) {
		t.Fatalf("GET /amps/instance/topics?search=archive body = %q, should not contain quotes topic", response.Body.String())
	}
}

func TestMonitoringServiceWorkspaceRunProtocolSupportsOptionsAndLiveUpdates(t *testing.T) {
	var oldSOW = sow
	var oldJournal = journal
	sow = newSOWCache()
	journal = newMessageJournal(64)
	defer func() {
		sow = oldSOW
		journal = oldJournal
	}()

	resetTopicConfigsForTest()
	defer resetTopicConfigsForTest()
	resetTopicSubscribersForTest()
	defer resetTopicSubscribersForTest()
	resetWorkspaceSessionsForTest()
	defer resetWorkspaceSessionsForTest()

	sow.upsert("orders", "k1", []byte(`{"id":1,"status":"open"}`), "bm1", "ts1", 1, 0)
	sow.upsert("orders", "k2", []byte(`{"id":2,"status":"open"}`), "bm2", "ts2", 2, 0)
	sow.upsert("orders", "k3", []byte(`{"id":3,"status":"closed"}`), "bm3", "ts3", 3, 0)
	getOrSetTopicMessageType("orders", "json")

	var service = newMonitoringService(monitoringServiceOptions{})
	var server = httptest.NewServer(service.Handler())
	defer server.Close()

	var websocketConn = dialAdminWebSocket(t, server.URL, "/amps/sql/ws", nil)
	defer websocketConn.Close()

	writeTextWebSocketFrame(t, websocketConn, []byte(`{"action":"sow","topic":"orders","filter":"/id > 1"}`))
	_, legacyResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(legacyResponse), `"type":"sow_result"`) {
		t.Fatalf("legacy websocket response = %s, want sow_result", string(legacyResponse))
	}

	var publisherListener, listenErr = net.Listen("tcp", "127.0.0.1:0")
	if listenErr != nil {
		t.Fatalf("net.Listen() error: %v", listenErr)
	}
	defer publisherListener.Close()

	var publisherDone = make(chan struct{})
	go func() {
		defer close(publisherDone)
		var conn, acceptErr = publisherListener.Accept()
		if acceptErr != nil {
			return
		}
		handleConnection(conn)
	}()

	var publisher, dialErr = net.Dial("tcp", publisherListener.Addr().String())
	if dialErr != nil {
		t.Fatalf("net.Dial(publisher) error: %v", dialErr)
	}
	defer publisher.Close()

	var publisherReadDone = make(chan struct{})
	go func() {
		defer close(publisherReadDone)
		_, _ = io.Copy(io.Discard, publisher)
	}()

	sendFrame(t, publisher, buildCommandFrame(`{"c":"logon","cid":"log-1","a":"processed","client_name":"workspace-publisher"}`, nil))

	writeTextWebSocketFrame(t, websocketConn, []byte(`{"type":"run","request_id":"req-1","mode":"sow_and_subscribe","topic":"orders","filter":"/status = 'open'","options":{"top_n":1,"order_by":"id DESC","bookmark":"bm1","delta":true,"live":true}}`))

	_, readyResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(readyResponse), `"type":"workspace_ready"`) {
		t.Fatalf("workspace ready response = %s, want workspace_ready", string(readyResponse))
	}
	if !strings.Contains(string(readyResponse), `"request_id":"req-1"`) {
		t.Fatalf("workspace ready response = %s, want request id", string(readyResponse))
	}
	if !strings.Contains(string(readyResponse), `"delta":true`) {
		t.Fatalf("workspace ready response = %s, want delta option", string(readyResponse))
	}

	_, snapshotResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(snapshotResponse), `"type":"workspace_snapshot"`) {
		t.Fatalf("workspace snapshot response = %s, want workspace_snapshot", string(snapshotResponse))
	}
	if !strings.Contains(string(snapshotResponse), `"id":2`) {
		t.Fatalf("workspace snapshot response = %s, want filtered snapshot row", string(snapshotResponse))
	}
	if strings.Contains(string(snapshotResponse), `"id":1`) || strings.Contains(string(snapshotResponse), `"id":3`) {
		t.Fatalf("workspace snapshot response = %s, should honor bookmark/filter/top_n options", string(snapshotResponse))
	}

	sendFrame(t, publisher, buildCommandFrame(`{"c":"publish","cid":"pub-1","t":"orders","a":"processed","k":"k4","mt":"json"}`, []byte(`{"id":4,"status":"open"}`)))

	_, rowResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(rowResponse), `"type":"workspace_snapshot"`) {
		t.Fatalf("workspace row response = %s, want refreshed workspace_snapshot", string(rowResponse))
	}
	if !strings.Contains(string(rowResponse), `"id":4`) || strings.Contains(string(rowResponse), `"id":2`) {
		t.Fatalf("workspace row response = %s, want refreshed top_n snapshot with only id 4", string(rowResponse))
	}

	sendFrame(t, publisher, buildCommandFrame(`{"c":"publish","cid":"pub-2","t":"orders","a":"processed","k":"k4","mt":"json"}`, []byte(`{"id":4,"status":"closed"}`)))

	_, mismatchRemoveResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(mismatchRemoveResponse), `"type":"workspace_snapshot"`) {
		t.Fatalf("workspace mismatch remove response = %s, want refreshed workspace_snapshot", string(mismatchRemoveResponse))
	}
	if !strings.Contains(string(mismatchRemoveResponse), `"id":2`) || strings.Contains(string(mismatchRemoveResponse), `"id":4`) {
		t.Fatalf("workspace mismatch remove response = %s, want snapshot to fall back to id 2", string(mismatchRemoveResponse))
	}

	sendFrame(t, publisher, buildCommandFrame(`{"c":"publish","cid":"pub-3","t":"orders","a":"processed","k":"k5","mt":"json"}`, []byte(`{"id":5,"status":"closed"}`)))

	sendFrame(t, publisher, buildCommandFrame(`{"c":"sow_delete","cid":"del-1","t":"orders","k":"k4","a":"processed"}`, nil))

	writeTextWebSocketFrame(t, websocketConn, []byte(`{"type":"stop","request_id":"req-1"}`))
	_, completeResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(completeResponse), `"type":"workspace_complete"`) {
		t.Fatalf("workspace complete response = %s, want workspace_complete", string(completeResponse))
	}

	_ = websocketConn.Close()
	_ = publisher.Close()
	_ = publisherListener.Close()

	select {
	case <-publisherDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("workspace publisher handler did not exit in time")
	}

	select {
	case <-publisherReadDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("workspace publisher drain did not exit in time")
	}
}

func TestMonitoringServiceWorkspaceRunProtocolCanDisableLiveUpdates(t *testing.T) {
	var oldSOW = sow
	var oldJournal = journal
	sow = newSOWCache()
	journal = newMessageJournal(64)
	defer func() {
		sow = oldSOW
		journal = oldJournal
	}()

	resetTopicConfigsForTest()
	defer resetTopicConfigsForTest()
	resetTopicSubscribersForTest()
	defer resetTopicSubscribersForTest()
	resetWorkspaceSessionsForTest()
	defer resetWorkspaceSessionsForTest()

	sow.upsert("orders", "k1", []byte(`{"id":1,"status":"open"}`), "bm1", "ts1", 1, 0)
	getOrSetTopicMessageType("orders", "json")

	var service = newMonitoringService(monitoringServiceOptions{})
	var server = httptest.NewServer(service.Handler())
	defer server.Close()

	var websocketConn = dialAdminWebSocket(t, server.URL, "/amps/sql/ws", nil)
	defer websocketConn.Close()

	var publisherListener, listenErr = net.Listen("tcp", "127.0.0.1:0")
	if listenErr != nil {
		t.Fatalf("net.Listen() error: %v", listenErr)
	}
	defer publisherListener.Close()

	var publisherDone = make(chan struct{})
	go func() {
		defer close(publisherDone)
		var conn, acceptErr = publisherListener.Accept()
		if acceptErr != nil {
			return
		}
		handleConnection(conn)
	}()

	var publisher, dialErr = net.Dial("tcp", publisherListener.Addr().String())
	if dialErr != nil {
		t.Fatalf("net.Dial(publisher) error: %v", dialErr)
	}
	defer publisher.Close()

	var publisherReadDone = make(chan struct{})
	go func() {
		defer close(publisherReadDone)
		_, _ = io.Copy(io.Discard, publisher)
	}()

	sendFrame(t, publisher, buildCommandFrame(`{"c":"logon","cid":"log-disable-live","a":"processed","client_name":"workspace-publisher"}`, nil))

	writeTextWebSocketFrame(t, websocketConn, []byte(`{"type":"run","request_id":"req-disable-live","mode":"sow_and_subscribe","topic":"orders","filter":"/status = 'open'","options":{"live":false}}`))

	_, readyResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(readyResponse), `"live":false`) {
		t.Fatalf("workspace ready response = %s, want live=false", string(readyResponse))
	}

	_, snapshotResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(snapshotResponse), `"type":"workspace_snapshot"`) {
		t.Fatalf("workspace snapshot response = %s, want workspace_snapshot", string(snapshotResponse))
	}

	_, completeResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(completeResponse), `"type":"workspace_complete"`) {
		t.Fatalf("workspace complete response = %s, want workspace_complete", string(completeResponse))
	}

	sendFrame(t, publisher, buildCommandFrame(`{"c":"publish","cid":"pub-disable-live","t":"orders","a":"processed","k":"k2","mt":"json"}`, []byte(`{"id":2,"status":"open"}`)))

	if _, unexpected, err := tryReadWebSocketFrame(websocketConn, 150*time.Millisecond); err == nil {
		t.Fatalf("unexpected websocket frame after live=false run: %s", string(unexpected))
	}

	_ = websocketConn.Close()
	_ = publisher.Close()
	_ = publisherListener.Close()

	select {
	case <-publisherDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("workspace publisher handler did not exit in time")
	}

	select {
	case <-publisherReadDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("workspace publisher drain did not exit in time")
	}
}

func TestMonitoringServiceWorkspaceLiveUpdatesIgnoreFanoutFlag(t *testing.T) {
	var oldFanout = *flagFanout
	*flagFanout = false
	defer func() {
		*flagFanout = oldFanout
	}()

	var oldSOW = sow
	var oldJournal = journal
	sow = newSOWCache()
	journal = newMessageJournal(64)
	defer func() {
		sow = oldSOW
		journal = oldJournal
	}()

	resetTopicConfigsForTest()
	defer resetTopicConfigsForTest()
	resetTopicSubscribersForTest()
	defer resetTopicSubscribersForTest()
	resetWorkspaceSessionsForTest()
	defer resetWorkspaceSessionsForTest()

	sow.upsert("orders", "k1", []byte(`{"id":1,"status":"open"}`), "bm1", "ts1", 1, 0)
	getOrSetTopicMessageType("orders", "json")

	var service = newMonitoringService(monitoringServiceOptions{})
	var server = httptest.NewServer(service.Handler())
	defer server.Close()

	var websocketConn = dialAdminWebSocket(t, server.URL, "/amps/sql/ws", nil)
	defer websocketConn.Close()

	var publisherListener, listenErr = net.Listen("tcp", "127.0.0.1:0")
	if listenErr != nil {
		t.Fatalf("net.Listen() error: %v", listenErr)
	}
	defer publisherListener.Close()

	var publisherDone = make(chan struct{})
	go func() {
		defer close(publisherDone)
		var conn, acceptErr = publisherListener.Accept()
		if acceptErr != nil {
			return
		}
		handleConnection(conn)
	}()

	var publisher, dialErr = net.Dial("tcp", publisherListener.Addr().String())
	if dialErr != nil {
		t.Fatalf("net.Dial(publisher) error: %v", dialErr)
	}
	defer publisher.Close()

	var publisherReadDone = make(chan struct{})
	go func() {
		defer close(publisherReadDone)
		_, _ = io.Copy(io.Discard, publisher)
	}()

	sendFrame(t, publisher, buildCommandFrame(`{"c":"logon","cid":"log-fanout-disabled","a":"processed","client_name":"workspace-publisher"}`, nil))

	writeTextWebSocketFrame(t, websocketConn, []byte(`{"type":"run","request_id":"req-fanout-disabled","mode":"sow_and_subscribe","topic":"orders","options":{"live":true}}`))

	_, readyResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(readyResponse), `"type":"workspace_ready"`) {
		t.Fatalf("workspace ready response = %s, want workspace_ready", string(readyResponse))
	}

	_, snapshotResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(snapshotResponse), `"type":"workspace_snapshot"`) {
		t.Fatalf("workspace snapshot response = %s, want workspace_snapshot", string(snapshotResponse))
	}

	sendFrame(t, publisher, buildCommandFrame(`{"c":"publish","cid":"pub-fanout-disabled","t":"orders","a":"processed","k":"k2","mt":"json"}`, []byte(`{"id":2,"status":"open"}`)))

	_, rowResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(rowResponse), `"type":"workspace_row"`) {
		t.Fatalf("workspace row response = %s, want workspace_row", string(rowResponse))
	}
	if !strings.Contains(string(rowResponse), `"sow_key":"k2"`) {
		t.Fatalf("workspace row response = %s, want published sow key", string(rowResponse))
	}

	sendFrame(t, publisher, buildCommandFrame(`{"c":"sow_delete","cid":"del-fanout-disabled","t":"orders","k":"k2","a":"processed"}`, nil))

	_, removeResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(removeResponse), `"type":"workspace_remove"`) {
		t.Fatalf("workspace remove response = %s, want workspace_remove", string(removeResponse))
	}
	if !strings.Contains(string(removeResponse), `"reason":"delete"`) {
		t.Fatalf("workspace remove response = %s, want delete reason", string(removeResponse))
	}

	_ = websocketConn.Close()
	_ = publisher.Close()
	_ = publisherListener.Close()

	select {
	case <-publisherDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("workspace publisher handler did not exit in time")
	}

	select {
	case <-publisherReadDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("workspace publisher drain did not exit in time")
	}
}

func TestMonitoringServiceListsAndDisconnectsClients(t *testing.T) {
	monitoringClients.Reset()
	defer monitoringClients.Reset()
	monitoringTransports.Reset([]transportDescriptor{{
		ID:          "primary",
		Name:        "primary",
		Type:        "tcp",
		InetAddr:    *flagAddr,
		Protocol:    "amps",
		MessageType: "json",
		Enabled:     true,
	}})

	var service = newMonitoringService(monitoringServiceOptions{})
	var clientConn, peerConn = net.Pipe()
	defer peerConn.Close()

	var clientID = monitoringClients.Register(clientConn, "127.0.0.1:19000", "tcp", "primary", nil)
	monitoringClients.UpdateLogon(clientID, "viewer", "dashboard-viewer")
	defer monitoringClients.Remove(clientID)

	var handler = service.Handler()

	var listResponse = httptest.NewRecorder()
	var listRequest = httptest.NewRequest(http.MethodGet, "/amps/instance/clients", nil)
	handler.ServeHTTP(listResponse, listRequest)
	if listResponse.Code != http.StatusOK {
		t.Fatalf("GET /amps/instance/clients status = %d, want 200", listResponse.Code)
	}
	if !strings.Contains(listResponse.Body.String(), clientID) {
		t.Fatalf("GET /amps/instance/clients body = %q, want registered client id", listResponse.Body.String())
	}

	var disconnectResponse = httptest.NewRecorder()
	var disconnectRequest = httptest.NewRequest(http.MethodPost, "/amps/administrator/clients/"+clientID+"/disconnect", nil)
	handler.ServeHTTP(disconnectResponse, disconnectRequest)
	if disconnectResponse.Code != http.StatusOK {
		t.Fatalf("POST /amps/administrator/clients/%s/disconnect status = %d, want 200", clientID, disconnectResponse.Code)
	}

	var buffer = make([]byte, 1)
	if _, err := peerConn.Read(buffer); err == nil {
		t.Fatalf("expected peer connection read to fail after disconnect action")
	}
}

func TestMonitoringServiceSupportsTransportReplicationAndSOWActions(t *testing.T) {
	monitoringClients.Reset()
	defer monitoringClients.Reset()

	var oldSOW = sow
	var oldPeers = replicaPeers
	var originalTransports = monitoringTransports.List()
	sow = newSOWCache()
	replicaPeers = []*peerConn{{addr: "127.0.0.1:19001", mode: "active"}}
	defer func() {
		sow = oldSOW
		replicaPeers = oldPeers
		monitoringTransports.Reset(originalTransports)
	}()

	sow.upsert("orders", "k1", []byte(`{"id":1}`), "bm1", "ts1", 1, 0)

	var service = newMonitoringService(monitoringServiceOptions{})
	var handler = service.Handler()

	var disableTransportResponse = httptest.NewRecorder()
	var disableTransportRequest = httptest.NewRequest(http.MethodPost, "/amps/administrator/transports/primary/disable", nil)
	handler.ServeHTTP(disableTransportResponse, disableTransportRequest)
	if disableTransportResponse.Code != http.StatusOK {
		t.Fatalf("POST /amps/administrator/transports/primary/disable status = %d, want 200", disableTransportResponse.Code)
	}

	var instanceResponse = httptest.NewRecorder()
	var instanceRequest = httptest.NewRequest(http.MethodGet, "/amps/instance", nil)
	handler.ServeHTTP(instanceResponse, instanceRequest)
	if !strings.Contains(instanceResponse.Body.String(), `"enabled": false`) {
		t.Fatalf("GET /amps/instance body = %q, want disabled transport", instanceResponse.Body.String())
	}

	var downgradeResponse = httptest.NewRecorder()
	var downgradeRequest = httptest.NewRequest(http.MethodPost, "/amps/administrator/replication/peer-1/downgrade", nil)
	handler.ServeHTTP(downgradeResponse, downgradeRequest)
	if downgradeResponse.Code != http.StatusOK {
		t.Fatalf("POST /amps/administrator/replication/peer-1/downgrade status = %d, want 200", downgradeResponse.Code)
	}

	var replicationResponse = httptest.NewRecorder()
	var replicationRequest = httptest.NewRequest(http.MethodGet, "/amps/instance/replication", nil)
	handler.ServeHTTP(replicationResponse, replicationRequest)
	if !strings.Contains(replicationResponse.Body.String(), `"mode": "downgraded"`) {
		t.Fatalf("GET /amps/instance/replication body = %q, want downgraded mode", replicationResponse.Body.String())
	}

	var localPeer, remotePeer = net.Pipe()
	defer localPeer.Close()
	defer remotePeer.Close()
	replicaPeers[0].conn = localPeer
	replicaPeers[0].alive = true

	if err := replicaPeers[0].writeReplicatedPublish("orders", []byte(`{"id":2}`), "json", "k2", false); err != nil {
		t.Fatalf("writeReplicatedPublish(downgraded) error: %v", err)
	}

	if !service.setReplicationMode("peer-1", "upgraded") {
		t.Fatalf("setReplicationMode(peer-1, upgraded) = false, want true")
	}

	var peerHeader = readPeerHeader(t, remotePeer, 2*time.Second)
	if peerHeader.c != "publish" || peerHeader.t != "orders" || peerHeader.k != "k2" {
		t.Fatalf("upgrade should flush deferred publish, got header %+v", peerHeader)
	}

	var sowClearResponse = httptest.NewRecorder()
	var sowClearRequest = httptest.NewRequest(http.MethodPost, "/amps/administrator/sow/clear?topic=orders", nil)
	handler.ServeHTTP(sowClearResponse, sowClearRequest)
	if sowClearResponse.Code != http.StatusOK {
		t.Fatalf("POST /amps/administrator/sow/clear status = %d, want 200", sowClearResponse.Code)
	}

	var sowStatusResponse = httptest.NewRecorder()
	var sowStatusRequest = httptest.NewRequest(http.MethodGet, "/admin/sow/orders", nil)
	handler.ServeHTTP(sowStatusResponse, sowStatusRequest)
	if !strings.Contains(sowStatusResponse.Body.String(), `"records": []`) {
		t.Fatalf("GET /admin/sow/orders body = %q, want cleared records", sowStatusResponse.Body.String())
	}
}

func TestMonitoringServiceUpgradeDefersNewReplicationPublishesUntilBacklogFlushes(t *testing.T) {
	var oldPeers = replicaPeers
	var oldReplID = replID
	replID = "node-test"

	var localPeer, remotePeer = net.Pipe()
	defer func() {
		_ = localPeer.Close()
		_ = remotePeer.Close()
		replicaPeers = oldPeers
		replID = oldReplID
	}()

	replicaPeers = []*peerConn{{
		addr:       "127.0.0.1:19001",
		conn:       localPeer,
		alive:      true,
		mode:       "downgraded",
		stopCh:     make(chan struct{}),
		pendingAck: make(map[string]chan headerFields),
	}}

	var service = newMonitoringService(monitoringServiceOptions{})
	if err := replicaPeers[0].writeReplicatedPublish("orders", []byte(`{"id":1}`), "json", "k1", true); err != nil {
		t.Fatalf("writeReplicatedPublish(backlog) error: %v", err)
	}
	if !service.setReplicationMode("peer-1", "upgraded") {
		t.Fatalf("setReplicationMode(peer-1, upgraded) = false, want true")
	}

	var firstHeader = readPeerHeader(t, remotePeer, 2*time.Second)
	if firstHeader.k != "k1" || firstHeader.cid == "" {
		t.Fatalf("first replicated header = %+v, want deferred k1 with cid", firstHeader)
	}

	var publishErrCh = make(chan error, 1)
	go func() {
		publishErrCh <- replicaPeers[0].writeReplicatedPublish("orders", []byte(`{"id":2}`), "json", "k2", false)
	}()

	if header, err := tryReadPeerHeader(remotePeer, 150*time.Millisecond); err == nil {
		t.Fatalf("read replicated header before sync ack = %+v, want no new frame before backlog flush", header)
	}

	replicaPeers[0].handlePeerAck(headerFields{
		cid:    firstHeader.cid,
		status: "success",
	})

	var secondHeader = readPeerHeader(t, remotePeer, 2*time.Second)
	if secondHeader.k != "k2" {
		t.Fatalf("second replicated header = %+v, want k2 after backlog flush", secondHeader)
	}

	if err := <-publishErrCh; err != nil {
		t.Fatalf("writeReplicatedPublish(new) error: %v", err)
	}
}

func TestMetricsHistoryStoreSupportsRangesAndSnapshots(t *testing.T) {
	var start = time.Date(2026, time.March, 6, 12, 0, 0, 0, time.UTC)
	var store = newMetricsHistoryStore(metricsHistoryOptions{
		Retention: 10 * time.Minute,
	})

	store.Add("instance.messages.in", start, 1)
	store.Add("instance.messages.in", start.Add(time.Minute), 2)
	store.Add("instance.messages.in", start.Add(2*time.Minute), 3)

	var samples = store.Range("instance.messages.in", start.Add(30*time.Second), start.Add(90*time.Second))
	if len(samples) != 1 {
		t.Fatalf("len(samples) = %d, want 1", len(samples))
	}
	if samples[0].Value != 2 {
		t.Fatalf("samples[0].Value = %v, want 2", samples[0].Value)
	}

	var snapshot, ok = store.SnapshotAt("instance.messages.in", start.Add(89*time.Second))
	if !ok {
		t.Fatalf("SnapshotAt returned ok=false, want true")
	}
	if snapshot.Value != 2 {
		t.Fatalf("snapshot.Value = %v, want 2", snapshot.Value)
	}
}

func TestMonitoringServiceLoadsPersistedHistoryFromAdminFile(t *testing.T) {
	var historyPath = t.TempDir() + "/history.json"
	var start = time.Date(2026, time.March, 6, 12, 0, 0, 0, time.UTC)
	var payload = `{"instance.connections.current":[{"timestamp":"2026-03-06T12:00:00Z","value":7}]}`
	if err := os.WriteFile(historyPath, []byte(payload), 0o600); err != nil {
		t.Fatalf("WriteFile(historyPath): %v", err)
	}

	var service = newMonitoringService(monitoringServiceOptions{
		Admin: ampsconfig.AdminConfig{
			FileName: historyPath,
		},
		Now: func() time.Time {
			return start.Add(5 * time.Minute)
		},
	})

	var response = httptest.NewRecorder()
	var request = httptest.NewRequest(http.MethodGet, "/amps/instance/history?metric=instance.connections.current&start=2026-03-06T11:59:00Z&end=2026-03-06T12:01:00Z", nil)
	service.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET /amps/instance/history status = %d, want 200", response.Code)
	}
	if !strings.Contains(response.Body.String(), `"value": 7`) {
		t.Fatalf("GET /amps/instance/history body = %q, want persisted sample", response.Body.String())
	}
}

func TestMetricsHistoryStoreSerializesConcurrentSaves(t *testing.T) {
	var store = newMetricsHistoryStore(metricsHistoryOptions{Retention: time.Minute})
	store.Add("instance.connections.current", time.Date(2026, time.March, 6, 12, 0, 0, 0, time.UTC), 9)

	var oldWriteFile = metricsHistoryWriteFile
	var oldMkdirAll = metricsHistoryMkdirAll
	defer func() {
		metricsHistoryWriteFile = oldWriteFile
		metricsHistoryMkdirAll = oldMkdirAll
	}()

	metricsHistoryMkdirAll = func(string, os.FileMode) error {
		return nil
	}

	var entered = make(chan struct{}, 2)
	var release = make(chan struct{})
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32

	metricsHistoryWriteFile = func(string, []byte, os.FileMode) error {
		var current = inFlight.Add(1)
		for {
			var observed = maxInFlight.Load()
			if current <= observed {
				break
			}
			if maxInFlight.CompareAndSwap(observed, current) {
				break
			}
		}
		entered <- struct{}{}
		<-release
		inFlight.Add(-1)
		return nil
	}

	var errCh = make(chan error, 2)
	go func() {
		errCh <- store.SaveFile(t.TempDir() + "/history.json")
	}()
	go func() {
		errCh <- store.SaveFile(t.TempDir() + "/history.json")
	}()

	<-entered
	time.Sleep(50 * time.Millisecond)
	close(release)

	for range 2 {
		if err := <-errCh; err != nil {
			t.Fatalf("SaveFile returned error: %v", err)
		}
	}

	if maxInFlight.Load() != 1 {
		t.Fatalf("max concurrent writes = %d, want 1", maxInFlight.Load())
	}
}

func TestMonitoringServiceAuxiliaryRoutesAndErrors(t *testing.T) {
	var service = newMonitoringService(monitoringServiceOptions{})
	var handler = service.Handler()

	var cssResponse = httptest.NewRecorder()
	handler.ServeHTTP(cssResponse, httptest.NewRequest(http.MethodGet, "/assets/dashboard.css", nil))
	if cssResponse.Code != http.StatusOK {
		t.Fatalf("GET /assets/dashboard.css status = %d, want 200", cssResponse.Code)
	}
	if !strings.Contains(cssResponse.Body.String(), ":root") {
		t.Fatalf("GET /assets/dashboard.css body = %q, want stylesheet content", cssResponse.Body.String())
	}
	if !strings.Contains(cssResponse.Body.String(), ".tab-list") {
		t.Fatalf("GET /assets/dashboard.css body = %q, want tab styling", cssResponse.Body.String())
	}

	var jsResponse = httptest.NewRecorder()
	handler.ServeHTTP(jsResponse, httptest.NewRequest(http.MethodGet, "/assets/dashboard.js", nil))
	if jsResponse.Code != http.StatusOK {
		t.Fatalf("GET /assets/dashboard.js status = %d, want 200", jsResponse.Code)
	}
	if !strings.Contains(jsResponse.Body.String(), "refreshAll") {
		t.Fatalf("GET /assets/dashboard.js body = %q, want dashboard script content", jsResponse.Body.String())
	}
	if strings.Contains(jsResponse.Body.String(), "Galvanometer") {
		t.Fatalf("GET /assets/dashboard.js body = %q, should not contain Galvanometer branding", jsResponse.Body.String())
	}
	if !strings.Contains(jsResponse.Body.String(), "fake broker monitoring") {
		t.Fatalf("GET /assets/dashboard.js body = %q, want fakeamps-specific copy", jsResponse.Body.String())
	}
	if !strings.Contains(jsResponse.Body.String(), "activeTab") {
		t.Fatalf("GET /assets/dashboard.js body = %q, want tab state for single-view navigation", jsResponse.Body.String())
	}
	for _, label := range []string{"Overview", "Metrics", "Host", "Transports", "Replication", "Workspace", "Admin"} {
		if !strings.Contains(jsResponse.Body.String(), label) {
			t.Fatalf("GET /assets/dashboard.js body = %q, want tab label %q", jsResponse.Body.String(), label)
		}
	}
	for _, snippet := range []string{"/amps/instance/topics", "topic-search", "workspace-results", "workspace-table", "live-status", "Advanced options"} {
		if !strings.Contains(jsResponse.Body.String(), snippet) {
			t.Fatalf("GET /assets/dashboard.js body = %q, want workspace snippet %q", jsResponse.Body.String(), snippet)
		}
	}

	var sessionResponse = httptest.NewRecorder()
	handler.ServeHTTP(sessionResponse, httptest.NewRequest(http.MethodGet, "/amps/session", nil))
	if sessionResponse.Code != http.StatusOK {
		t.Fatalf("GET /amps/session status = %d, want 200", sessionResponse.Code)
	}
	if !strings.Contains(sessionResponse.Body.String(), `"role": "operator"`) {
		t.Fatalf("GET /amps/session body = %q, want operator session payload", sessionResponse.Body.String())
	}

	var logoutResponse = httptest.NewRecorder()
	handler.ServeHTTP(logoutResponse, httptest.NewRequest(http.MethodPost, "/amps/session/logout", nil))
	if logoutResponse.Code != http.StatusOK {
		t.Fatalf("POST /amps/session/logout status = %d, want 200", logoutResponse.Code)
	}

	var adminResponse = httptest.NewRecorder()
	handler.ServeHTTP(adminResponse, httptest.NewRequest(http.MethodGet, "/amps/administrator", nil))
	if adminResponse.Code != http.StatusOK {
		t.Fatalf("GET /amps/administrator status = %d, want 200", adminResponse.Code)
	}
	if !strings.Contains(adminResponse.Body.String(), `"diagnostics"`) {
		t.Fatalf("GET /amps/administrator body = %q, want diagnostics action", adminResponse.Body.String())
	}

	var unsupportedResponse = httptest.NewRecorder()
	handler.ServeHTTP(unsupportedResponse, httptest.NewRequest(http.MethodPost, "/amps/administrator/minidump", nil))
	if unsupportedResponse.Code != http.StatusNotImplemented {
		t.Fatalf("POST /amps/administrator/minidump status = %d, want 501", unsupportedResponse.Code)
	}
}

func TestMonitoringServiceLoginValidationAndHistoryErrors(t *testing.T) {
	var service = newMonitoringService(monitoringServiceOptions{
		Admin: ampsconfig.AdminConfig{
			Authentication: `Basic realm="FakeAMPS"`,
			SessionOptions: []string{"Secure=true", "SameSite=Strict"},
		},
		Users: []ampsconfig.AdminUserConfig{
			{Username: "operator", Password: "operator-pass", Role: "operator"},
		},
	})
	var handler = service.Handler()

	var methodResponse = httptest.NewRecorder()
	handler.ServeHTTP(methodResponse, httptest.NewRequest(http.MethodGet, "/amps/session/login", nil))
	if methodResponse.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /amps/session/login status = %d, want 405", methodResponse.Code)
	}

	var invalidPayloadResponse = httptest.NewRecorder()
	handler.ServeHTTP(invalidPayloadResponse, httptest.NewRequest(http.MethodPost, "/amps/session/login", strings.NewReader("{")))
	if invalidPayloadResponse.Code != http.StatusBadRequest {
		t.Fatalf("POST /amps/session/login invalid payload status = %d, want 400", invalidPayloadResponse.Code)
	}

	var badCredsResponse = httptest.NewRecorder()
	handler.ServeHTTP(badCredsResponse, httptest.NewRequest(http.MethodPost, "/amps/session/login", strings.NewReader(`{"username":"operator","password":"wrong"}`)))
	if badCredsResponse.Code != http.StatusUnauthorized {
		t.Fatalf("POST /amps/session/login bad creds status = %d, want 401", badCredsResponse.Code)
	}

	var goodLoginResponse = httptest.NewRecorder()
	var goodLoginRequest = httptest.NewRequest(http.MethodPost, "/amps/session/login", strings.NewReader(`{"username":"operator","password":"operator-pass"}`))
	handler.ServeHTTP(goodLoginResponse, goodLoginRequest)
	if goodLoginResponse.Code != http.StatusOK {
		t.Fatalf("POST /amps/session/login good creds status = %d, want 200", goodLoginResponse.Code)
	}
	var cookies = goodLoginResponse.Result().Cookies()
	if len(cookies) == 0 || !cookies[0].Secure || cookies[0].SameSite != http.SameSiteStrictMode {
		t.Fatalf("login cookie = %+v, want secure strict cookie", cookies)
	}

	var missingMetricResponse = httptest.NewRecorder()
	var missingMetricRequest = httptest.NewRequest(http.MethodGet, "/amps/instance/history", nil)
	missingMetricRequest.SetBasicAuth("operator", "operator-pass")
	handler.ServeHTTP(missingMetricResponse, missingMetricRequest)
	if missingMetricResponse.Code != http.StatusBadRequest {
		t.Fatalf("GET /amps/instance/history missing metric status = %d, want 400", missingMetricResponse.Code)
	}

	var invalidStartResponse = httptest.NewRecorder()
	var invalidStartRequest = httptest.NewRequest(http.MethodGet, "/amps/instance/history?metric=host.memory.alloc_mb&start=bad", nil)
	invalidStartRequest.SetBasicAuth("operator", "operator-pass")
	handler.ServeHTTP(invalidStartResponse, invalidStartRequest)
	if invalidStartResponse.Code != http.StatusBadRequest {
		t.Fatalf("GET /amps/instance/history invalid start status = %d, want 400", invalidStartResponse.Code)
	}
}

func TestMonitoringServiceProxyEndpointsAndSOWClearAll(t *testing.T) {
	monitoringClients.Reset()
	defer monitoringClients.Reset()

	var oldSOW = sow
	var oldJournal = journal
	sow = newSOWCache()
	journal = newMessageJournal(10)
	defer func() {
		sow = oldSOW
		journal = oldJournal
	}()

	sow.upsert("orders", "k1", []byte(`{"id":1}`), "bm1", "ts1", 1, 0)

	var connA, connB = net.Pipe()
	defer connA.Close()
	defer connB.Close()
	var sub = &subscription{conn: connA, subID: "sub-1", topic: "orders"}
	registerSubscription("orders", sub)
	defer unregisterSubscription("orders", sub)

	resetViewsForTest()
	defer resetViewsForTest()
	registerView(&viewDef{name: "orders_view", sources: []string{"orders"}})

	resetActionsForTest()
	defer resetActionsForTest()
	registerAction(actionDef{trigger: triggerOnPublish, topicMatch: "orders", action: actionLog})

	var service = newMonitoringService(monitoringServiceOptions{})
	var handler = service.Handler()

	for _, path := range []string{"/amps/instance/subscriptions", "/amps/instance/sow", "/amps/instance/views", "/amps/instance/actions"} {
		var response = httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", path, response.Code)
		}
	}

	var clearResponse = httptest.NewRecorder()
	handler.ServeHTTP(clearResponse, httptest.NewRequest(http.MethodPost, "/amps/administrator/sow/clear", nil))
	if clearResponse.Code != http.StatusOK {
		t.Fatalf("POST /amps/administrator/sow/clear status = %d, want 200", clearResponse.Code)
	}
	if !strings.Contains(clearResponse.Body.String(), `"records_cleared": 1`) {
		t.Fatalf("POST /amps/administrator/sow/clear body = %q, want cleared count", clearResponse.Body.String())
	}
}

func TestMonitoringServiceSOWClearNotifiesWorkspaceSubscribers(t *testing.T) {
	resetWorkspaceSessionsForTest()
	defer resetWorkspaceSessionsForTest()

	var oldSOW = sow
	sow = newSOWCache()
	defer func() {
		sow = oldSOW
	}()

	sow.upsert("orders", "k1", []byte(`{"id":1,"status":"open"}`), "bm1", "ts1", 1, 0)

	var service = newMonitoringService(monitoringServiceOptions{})
	var server = httptest.NewServer(service.Handler())
	defer server.Close()

	var websocketConn = dialAdminWebSocket(t, server.URL, "/amps/sql/ws", nil)
	defer websocketConn.Close()

	writeTextWebSocketFrame(t, websocketConn, []byte(`{"type":"run","request_id":"req-clear","mode":"sow_and_subscribe","topic":"orders","options":{"live":true}}`))

	_, readyResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(readyResponse), `"type":"workspace_ready"`) {
		t.Fatalf("workspace ready response = %s, want workspace_ready", string(readyResponse))
	}

	_, snapshotResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(snapshotResponse), `"sow_key":"k1"`) {
		t.Fatalf("workspace snapshot response = %s, want existing row", string(snapshotResponse))
	}

	var clearResponse, clearErr = http.Post(server.URL+"/amps/administrator/sow/clear?topic=orders", "application/json", http.NoBody)
	if clearErr != nil {
		t.Fatalf("POST /amps/administrator/sow/clear error: %v", clearErr)
	}
	defer clearResponse.Body.Close()
	if clearResponse.StatusCode != http.StatusOK {
		t.Fatalf("POST /amps/administrator/sow/clear status = %d, want 200", clearResponse.StatusCode)
	}

	_, removeResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(removeResponse), `"type":"workspace_remove"`) {
		t.Fatalf("workspace remove response = %s, want workspace_remove", string(removeResponse))
	}
	if !strings.Contains(string(removeResponse), `"sow_key":"k1"`) {
		t.Fatalf("workspace remove response = %s, want removed sow key", string(removeResponse))
	}
}

func TestMonitoringServiceSOWClearSendsOOFToDeltaSubscribers(t *testing.T) {
	var oldSOW = sow
	var oldJournal = journal
	sow = newSOWCache()
	journal = newMessageJournal(64)
	defer func() {
		sow = oldSOW
		journal = oldJournal
	}()

	resetTopicConfigsForTest()
	defer resetTopicConfigsForTest()
	resetTopicSubscribersForTest()
	defer resetTopicSubscribersForTest()

	var service = newMonitoringService(monitoringServiceOptions{})
	var adminServer = httptest.NewServer(service.Handler())
	defer adminServer.Close()

	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen(): %v", err)
	}
	defer listener.Close()

	var done = make(chan struct{}, 2)
	for index := 0; index < 2; index++ {
		go func() {
			var conn, acceptErr = listener.Accept()
			if acceptErr != nil {
				done <- struct{}{}
				return
			}
			handleConnection(conn)
			done <- struct{}{}
		}()
	}

	var subscriber, subscriberErr = net.Dial("tcp", listener.Addr().String())
	if subscriberErr != nil {
		t.Fatalf("net.Dial(subscriber): %v", subscriberErr)
	}
	defer subscriber.Close()

	var publisher, publisherErr = net.Dial("tcp", listener.Addr().String())
	if publisherErr != nil {
		t.Fatalf("net.Dial(publisher): %v", publisherErr)
	}
	defer publisher.Close()

	sendFrame(t, subscriber, buildCommandFrame(`{"c":"logon","cid":"log-sub","a":"processed","client_name":"admin-clear-subscriber"}`, nil))
	_ = readFrameBody(t, subscriber, 500*time.Millisecond)

	sendFrame(t, subscriber, buildCommandFrame(`{"c":"delta_subscribe","cid":"sub-1","sub_id":"d1","t":"orders","a":"processed"}`, nil))
	_ = readFrameBody(t, subscriber, 500*time.Millisecond)

	sendFrame(t, publisher, buildCommandFrame(`{"c":"logon","cid":"log-pub","a":"processed","client_name":"admin-clear-publisher"}`, nil))
	_ = readFrameBody(t, publisher, 500*time.Millisecond)

	sendFrame(t, publisher, buildCommandFrame(`{"c":"publish","cid":"pub-1","t":"orders","a":"processed","k":"order-1","mt":"json"}`, []byte(`{"id":1,"status":"open"}`)))
	_ = readFrameBody(t, publisher, 500*time.Millisecond)
	for attempt := 0; attempt < 4; attempt++ {
		var body = readFrameBody(t, subscriber, 500*time.Millisecond)
		if strings.Contains(body, `"c":"p"`) && strings.Contains(body, `"k":"order-1"`) {
			break
		}
	}

	var clearResponse, clearErr = http.Post(adminServer.URL+"/amps/administrator/sow/clear?topic=orders", "application/json", http.NoBody)
	if clearErr != nil {
		t.Fatalf("POST /amps/administrator/sow/clear error: %v", clearErr)
	}
	defer clearResponse.Body.Close()
	if clearResponse.StatusCode != http.StatusOK {
		t.Fatalf("POST /amps/administrator/sow/clear status = %d, want 200", clearResponse.StatusCode)
	}

	var foundClearReasonOOF bool
	var receivedBodies []string
	for attempt := 0; attempt < 6; attempt++ {
		var body, ok = tryReadFrameBody(subscriber, 500*time.Millisecond)
		if !ok {
			continue
		}
		receivedBodies = append(receivedBodies, body)
		if strings.Contains(body, `"c":"oof"`) && strings.Contains(body, `"reason":"clear"`) && strings.Contains(body, `"k":"order-1"`) {
			foundClearReasonOOF = true
			break
		}
	}

	if !foundClearReasonOOF {
		t.Fatalf("expected OOF frame with clear reason after admin SOW clear, got %v", receivedBodies)
	}

	_ = publisher.Close()
	_ = subscriber.Close()
	for count := 0; count < 2; count++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("handleConnection did not exit in time")
		}
	}
}

func TestMonitoringHelpersAndPersistencePaths(t *testing.T) {
	var service = newMonitoringService(monitoringServiceOptions{
		Admin: ampsconfig.AdminConfig{
			Authentication: "Basic",
			AnonymousPaths: []string{"/", "/public/"},
		},
	})
	if service.authRealm() != "FakeAMPS" {
		t.Fatalf("authRealm() = %q, want FakeAMPS fallback", service.authRealm())
	}
	if !service.pathAllowedAnonymously("/public/assets") {
		t.Fatalf("pathAllowedAnonymously(/public/assets) = false, want true for configured prefix")
	}
	if service.pathAllowedAnonymously("/amps/private") {
		t.Fatalf("pathAllowedAnonymously(/amps/private) = true, want false")
	}

	monitoringTransports.Reset([]transportDescriptor{{
		ID:          "primary",
		Name:        "primary",
		Type:        "tcp",
		InetAddr:    "127.0.0.1:19000",
		Protocol:    "amps",
		MessageType: "json",
		Enabled:     true,
	}})
	defer monitoringTransports.Reset([]transportDescriptor{{
		ID:          "primary",
		Name:        "primary",
		Type:        "tcp",
		InetAddr:    "127.0.0.1:19000",
		Protocol:    "amps",
		MessageType: "json",
		Enabled:     true,
	}})
	if !monitoringTransports.Exists("primary") {
		t.Fatalf("monitoringTransports.Exists(primary) = false, want true")
	}
	if monitoringTransports.Exists("missing") {
		t.Fatalf("monitoringTransports.Exists(missing) = true, want false")
	}
	if !service.hasTransport("primary") {
		t.Fatalf("service.hasTransport(primary) = false, want true")
	}
	if !monitoringTransports.SetEnabled("primary", false) {
		t.Fatalf("monitoringTransports.SetEnabled(primary, false) = false, want true")
	}
	if service.hasTransport("primary") {
		t.Fatalf("service.hasTransport(primary) = true, want false when disabled")
	}
	monitoringTransports.Reset([]transportDescriptor{{
		ID:          "transport-1",
		Name:        "named-primary",
		Type:        "tcp",
		InetAddr:    "127.0.0.1:19000",
		Protocol:    "amps",
		MessageType: "json",
		Enabled:     true,
	}})
	if !monitoringTransports.SetEnabled("named-primary", false) {
		t.Fatalf("monitoringTransports.SetEnabled(named-primary, false) = false, want true by name")
	}

	var store = newMetricsHistoryStore(metricsHistoryOptions{Retention: time.Minute})
	store.Add("instance.connections.current", time.Date(2026, time.March, 6, 12, 0, 0, 0, time.UTC), 9)
	var path = t.TempDir() + "/persisted.json"
	if err := store.SaveFile(path); err != nil {
		t.Fatalf("SaveFile(path) error: %v", err)
	}

	var loaded = newMetricsHistoryStore(metricsHistoryOptions{Retention: time.Minute})
	if err := loaded.LoadFile(path); err != nil {
		t.Fatalf("LoadFile(path) error: %v", err)
	}
	if samples := loaded.Range("instance.connections.current", time.Date(2026, time.March, 6, 11, 59, 0, 0, time.UTC), time.Date(2026, time.March, 6, 12, 1, 0, 0, time.UTC)); len(samples) != 1 {
		t.Fatalf("len(loaded samples) = %d, want 1", len(samples))
	}

	var emptyStore = newMetricsHistoryStore(metricsHistoryOptions{})
	if err := emptyStore.LoadFile(t.TempDir() + "/missing.json"); err == nil {
		t.Fatalf("LoadFile(missing) error = nil, want missing file error")
	}
}

func TestMonitoringServiceStartSamplingCollectsMetricsWithoutRequests(t *testing.T) {
	var historyPath = t.TempDir() + "/history.json"
	var service = newMonitoringService(monitoringServiceOptions{
		Admin: ampsconfig.AdminConfig{
			Interval: 20 * time.Millisecond,
			FileName: historyPath,
		},
	})
	service.StartSampling()
	defer service.Close()

	var deadline = time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		var samples = service.history.Range(
			"instance.connections.current",
			time.Now().Add(-time.Minute),
			time.Now().Add(time.Minute),
		)
		if len(samples) > 0 {
			if _, err := os.Stat(historyPath); err != nil {
				t.Fatalf("os.Stat(historyPath) error: %v", err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("StartSampling did not record metrics without request traffic")
}

func TestMonitoringServiceSessionLifecycleAndHelperBranches(t *testing.T) {
	var service = newMonitoringService(monitoringServiceOptions{
		Admin: ampsconfig.AdminConfig{
			Authentication: `Basic realm="FakeAMPS"`,
		},
		Users: []ampsconfig.AdminUserConfig{
			{Username: "viewer", Password: "viewer-pass", Role: "viewer"},
		},
	})
	var server = httptest.NewServer(service.Handler())
	defer server.Close()

	var client = newSessionClient(t)
	loginAdminSession(t, client, server.URL, "viewer", "viewer-pass")

	var sessionResponse, sessionErr = client.Get(server.URL + "/amps/session")
	if sessionErr != nil {
		t.Fatalf("GET /amps/session error: %v", sessionErr)
	}
	defer sessionResponse.Body.Close()
	if sessionResponse.StatusCode != http.StatusOK {
		t.Fatalf("GET /amps/session status = %d, want 200", sessionResponse.StatusCode)
	}

	var logoutRequest, requestErr = http.NewRequest(http.MethodPost, server.URL+"/amps/session/logout", nil)
	if requestErr != nil {
		t.Fatalf("NewRequest(logout): %v", requestErr)
	}
	var logoutResponse, logoutErr = client.Do(logoutRequest)
	if logoutErr != nil {
		t.Fatalf("POST /amps/session/logout error: %v", logoutErr)
	}
	defer logoutResponse.Body.Close()
	if logoutResponse.StatusCode != http.StatusOK {
		t.Fatalf("POST /amps/session/logout status = %d, want 200", logoutResponse.StatusCode)
	}

	var afterLogoutResponse, afterLogoutErr = client.Get(server.URL + "/amps/session")
	if afterLogoutErr != nil {
		t.Fatalf("GET /amps/session after logout error: %v", afterLogoutErr)
	}
	defer afterLogoutResponse.Body.Close()
	if afterLogoutResponse.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /amps/session after logout status = %d, want 401", afterLogoutResponse.StatusCode)
	}
}

func TestMonitoringServiceErrorBranchesAndHelpers(t *testing.T) {
	var service = newMonitoringService(monitoringServiceOptions{})
	var handler = service.Handler()

	var dashboard404 = httptest.NewRecorder()
	service.handleDashboard(dashboard404, httptest.NewRequest(http.MethodGet, "/missing", nil))
	if dashboard404.Code != http.StatusNotFound {
		t.Fatalf("handleDashboard(/missing) status = %d, want 404", dashboard404.Code)
	}

	var missingAsset = httptest.NewRecorder()
	writeDashboardAsset(missingAsset, "dashboard/missing.js")
	if missingAsset.Code != http.StatusInternalServerError {
		t.Fatalf("writeDashboardAsset(missing) status = %d, want 500", missingAsset.Code)
	}

	if string(mustJSON(make(chan int))) != `{"type":"error","error":"marshal_failure"}` {
		t.Fatalf("mustJSON(chan) should return marshal_failure payload")
	}

	if !strings.Contains(string(service.handleSQLWorkspacePayload([]byte(`{"action":"subscribe","topic":"orders"}`))), `"subscribe_ready"`) {
		t.Fatalf("subscribe workspace payload should return subscribe_ready")
	}
	if !strings.Contains(string(service.handleSQLWorkspacePayload([]byte(`{"action":"sow_and_subscribe","topic":"orders"}`))), `"sow_and_subscribe_result"`) {
		t.Fatalf("sow_and_subscribe workspace payload should return sow_and_subscribe_result")
	}
	if !strings.Contains(string(service.handleSQLWorkspacePayload([]byte(`{"action":"mystery"}`))), `"unsupported_action"`) {
		t.Fatalf("unsupported workspace payload should return unsupported_action")
	}

	for _, testCase := range []struct {
		method string
		path   string
		status int
	}{
		{method: http.MethodGet, path: "/amps/administrator/clients/missing/disconnect", status: http.StatusMethodNotAllowed},
		{method: http.MethodPost, path: "/amps/administrator/clients/missing/disconnect", status: http.StatusNotFound},
		{method: http.MethodGet, path: "/amps/administrator/transports/missing/disable", status: http.StatusMethodNotAllowed},
		{method: http.MethodPost, path: "/amps/administrator/transports/missing/disable", status: http.StatusNotFound},
		{method: http.MethodGet, path: "/amps/administrator/replication/missing/downgrade", status: http.StatusMethodNotAllowed},
		{method: http.MethodPost, path: "/amps/administrator/replication/missing/downgrade", status: http.StatusNotFound},
	} {
		var response = httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(testCase.method, testCase.path, nil))
		if response.Code != testCase.status {
			t.Fatalf("%s %s status = %d, want %d", testCase.method, testCase.path, response.Code, testCase.status)
		}
	}

	var oldEffectiveConfig = effectiveConfig
	effectiveConfig = &ampsconfig.ExpandedConfig{
		Runtime: ampsconfig.RuntimeConfig{
			Name: "configured-monitoring",
			Transports: []ampsconfig.TransportConfig{
				{
					Name:        "xml-primary",
					Type:        "tcp",
					InetAddr:    "127.0.0.1:19099",
					Protocol:    "amps",
					MessageType: "json",
				},
			},
		},
	}
	defer func() {
		effectiveConfig = oldEffectiveConfig
	}()

	var configuredService = newMonitoringService(monitoringServiceOptions{})
	var configured = configuredService.configuredTransports()
	if len(configured) != 1 || configured[0].ID != "xml-primary" {
		t.Fatalf("configuredTransports() = %+v, want xml-primary transport", configured)
	}
	if configuredInstanceName() != "configured-monitoring" {
		t.Fatalf("configuredInstanceName() = %q, want configured-monitoring", configuredInstanceName())
	}
	if primaryTransportID() != "xml-primary" {
		t.Fatalf("primaryTransportID() = %q, want xml-primary", primaryTransportID())
	}
	if configuredService.setReplicationMode("missing", "upgraded") {
		t.Fatalf("setReplicationMode(missing) = true, want false")
	}
}

func TestPrimaryTransportIDMatchesListenerOverride(t *testing.T) {
	var oldEffectiveConfig = effectiveConfig
	var oldAddr = *flagAddr
	effectiveConfig = &ampsconfig.ExpandedConfig{
		Runtime: ampsconfig.RuntimeConfig{
			Transports: []ampsconfig.TransportConfig{
				{
					Name:     "xml-primary",
					InetAddr: "127.0.0.1:19000",
				},
				{
					Name:     "xml-secondary",
					InetAddr: "127.0.0.1:19001",
				},
			},
		},
	}
	*flagAddr = "127.0.0.1:19001"
	defer func() {
		effectiveConfig = oldEffectiveConfig
		*flagAddr = oldAddr
	}()

	if primaryTransportID() != "xml-secondary" {
		t.Fatalf("primaryTransportID() = %q, want xml-secondary", primaryTransportID())
	}
}

func TestDashboardAssetGuardsWorkspaceMessagesByRequestID(t *testing.T) {
	var asset, err = dashboardAssets.ReadFile("dashboard/dashboard.js")
	if err != nil {
		t.Fatalf("dashboardAssets.ReadFile() error: %v", err)
	}

	var script = string(asset)
	if !strings.Contains(script, "message.request_id && message.request_id !== state.workspaceRequestId") {
		t.Fatalf("dashboard.js should ignore stale workspace frames by request_id")
	}
}

func TestNotifyExpiredSOWRecordsSendsWorkspaceRemove(t *testing.T) {
	var oldSOW = sow
	sow = newSOWCache()
	defer func() {
		sow = oldSOW
	}()

	resetWorkspaceSessionsForTest()
	defer resetWorkspaceSessionsForTest()

	sow.upsert("orders", "k-expire", []byte(`{"id":1,"status":"open"}`), "bm-expire", "ts-expire", 1, 5*time.Millisecond)
	time.Sleep(15 * time.Millisecond)

	var service = newMonitoringService(monitoringServiceOptions{})
	var server = httptest.NewServer(service.Handler())
	defer server.Close()

	var websocketConn = dialAdminWebSocket(t, server.URL, "/amps/sql/ws", nil)
	defer websocketConn.Close()

	writeTextWebSocketFrame(t, websocketConn, []byte(`{"type":"run","request_id":"req-expire","mode":"sow_and_subscribe","topic":"orders","options":{"live":true}}`))

	_, _ = readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	_, _ = readWebSocketFrame(t, websocketConn, 500*time.Millisecond)

	notifyExpiredSOWRecords(sow.gcExpiredRecords())

	_, removeResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(removeResponse), `"type":"workspace_remove"`) {
		t.Fatalf("workspace expire response = %s, want workspace_remove", string(removeResponse))
	}
	if !strings.Contains(string(removeResponse), `"reason":"expire"`) {
		t.Fatalf("workspace expire response = %s, want expire reason", string(removeResponse))
	}
}

func TestApplyReplicatedPublishNotifiesLiveWorkspaceSessions(t *testing.T) {
	var oldSOW = sow
	var oldJournal = journal
	sow = newSOWCache()
	journal = newMessageJournal(64)
	defer func() {
		sow = oldSOW
		journal = oldJournal
	}()

	resetWorkspaceSessionsForTest()
	defer resetWorkspaceSessionsForTest()

	var service = newMonitoringService(monitoringServiceOptions{})
	var server = httptest.NewServer(service.Handler())
	defer server.Close()

	var websocketConn = dialAdminWebSocket(t, server.URL, "/amps/sql/ws", nil)
	defer websocketConn.Close()

	writeTextWebSocketFrame(t, websocketConn, []byte(`{"type":"run","request_id":"req-repl","mode":"sow_and_subscribe","topic":"orders","options":{"live":true}}`))

	_, readyResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(readyResponse), `"type":"workspace_ready"`) {
		t.Fatalf("workspace ready response = %s, want workspace_ready", string(readyResponse))
	}

	_, snapshotResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(snapshotResponse), `"type":"workspace_snapshot"`) {
		t.Fatalf("workspace snapshot response = %s, want workspace_snapshot", string(snapshotResponse))
	}

	applyReplicatedPublish(headerFields{c: "publish", t: "orders", mt: "json", k: "repl-1"}, []byte(`{"id":1,"status":"replicated"}`))

	_, rowResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(rowResponse), `"type":"workspace_row"`) {
		t.Fatalf("workspace row response = %s, want workspace_row", string(rowResponse))
	}
	if !strings.Contains(string(rowResponse), `"sow_key":"repl-1"`) {
		t.Fatalf("workspace row response = %s, want replicated sow key", string(rowResponse))
	}
}

func TestWorkspaceSnapshotRowsIncludeWildcardTopics(t *testing.T) {
	var oldSOW = sow
	sow = newSOWCache()
	defer func() {
		sow = oldSOW
	}()

	sow.upsert("orders.us", "us-1", []byte(`{"id":1,"region":"US"}`), "bm-us", "ts-us", 1, 0)
	sow.upsert("orders.eu", "eu-1", []byte(`{"id":2,"region":"EU"}`), "bm-eu", "ts-eu", 2, 0)

	var topN = 10
	var rows = workspaceSnapshotRows(workspaceLiveQuery{
		Topic: "orders.>",
		Options: sqlWorkspaceOptions{
			TopN: &topN,
		},
	})
	if len(rows) != 2 {
		t.Fatalf("len(workspaceSnapshotRows wildcard) = %d, want 2", len(rows))
	}

	var topics = []string{rows[0]["topic"].(string), rows[1]["topic"].(string)}
	if !(topics[0] == "orders.eu" && topics[1] == "orders.us" || topics[0] == "orders.us" && topics[1] == "orders.eu") {
		t.Fatalf("workspaceSnapshotRows topics = %v, want wildcard-matched topics", topics)
	}
}

func TestWorkspaceEvictionSendsWorkspaceRemove(t *testing.T) {
	var oldSOW = sow
	var oldJournal = journal
	sow = newSOWCacheWithEviction(1, evictionOldest)
	journal = newMessageJournal(64)
	defer func() {
		sow = oldSOW
		journal = oldJournal
	}()

	resetTopicConfigsForTest()
	defer resetTopicConfigsForTest()
	resetTopicSubscribersForTest()
	defer resetTopicSubscribersForTest()
	resetWorkspaceSessionsForTest()
	defer resetWorkspaceSessionsForTest()

	sow.upsert("orders", "k1", []byte(`{"id":1,"status":"open"}`), "bm1", "ts1", 1, 0)
	getOrSetTopicMessageType("orders", "json")

	var service = newMonitoringService(monitoringServiceOptions{})
	var server = httptest.NewServer(service.Handler())
	defer server.Close()

	var websocketConn = dialAdminWebSocket(t, server.URL, "/amps/sql/ws", nil)
	defer websocketConn.Close()

	writeTextWebSocketFrame(t, websocketConn, []byte(`{"type":"run","request_id":"req-evicted","mode":"sow_and_subscribe","topic":"orders","options":{"live":true}}`))

	_, _ = readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	_, snapshotResponse := readWebSocketFrame(t, websocketConn, 500*time.Millisecond)
	if !strings.Contains(string(snapshotResponse), `"sow_key":"k1"`) {
		t.Fatalf("workspace snapshot response = %s, want existing row", string(snapshotResponse))
	}

	var publisherListener, listenErr = net.Listen("tcp", "127.0.0.1:0")
	if listenErr != nil {
		t.Fatalf("net.Listen() error: %v", listenErr)
	}
	defer publisherListener.Close()

	var publisherDone = make(chan struct{})
	go func() {
		defer close(publisherDone)
		var conn, acceptErr = publisherListener.Accept()
		if acceptErr != nil {
			return
		}
		handleConnection(conn)
	}()

	var publisher, dialErr = net.Dial("tcp", publisherListener.Addr().String())
	if dialErr != nil {
		t.Fatalf("net.Dial(publisher) error: %v", dialErr)
	}
	defer publisher.Close()

	var publisherReadDone = make(chan struct{})
	go func() {
		defer close(publisherReadDone)
		_, _ = io.Copy(io.Discard, publisher)
	}()

	sendFrame(t, publisher, buildCommandFrame(`{"c":"logon","cid":"log-evicted","a":"processed","client_name":"workspace-publisher"}`, nil))
	sendFrame(t, publisher, buildCommandFrame(`{"c":"publish","cid":"pub-evicted","t":"orders","a":"processed","k":"k2","mt":"json"}`, []byte(`{"id":2,"status":"open"}`)))

	var foundRow bool
	var foundRemove bool
	var frames []string
	for attempt := 0; attempt < 4; attempt++ {
		if _, payload, err := tryReadWebSocketFrame(websocketConn, 500*time.Millisecond); err == nil {
			var body = string(payload)
			frames = append(frames, body)
			if strings.Contains(body, `"type":"workspace_row"`) && strings.Contains(body, `"sow_key":"k2"`) {
				foundRow = true
			}
			if strings.Contains(body, `"type":"workspace_remove"`) && strings.Contains(body, `"sow_key":"k1"`) && strings.Contains(body, `"reason":"evicted"`) {
				foundRemove = true
			}
			if foundRow && foundRemove {
				break
			}
		}
	}

	if !foundRow || !foundRemove {
		t.Fatalf("expected workspace row and eviction remove frames, got %v", frames)
	}

	_ = websocketConn.Close()
	_ = publisher.Close()
	_ = publisherListener.Close()

	select {
	case <-publisherDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("workspace publisher handler did not exit in time")
	}

	select {
	case <-publisherReadDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("workspace publisher drain did not exit in time")
	}
}

func TestNotifyWorkspaceRemovalsDoesNotFanoutOOFWhenFanoutDisabled(t *testing.T) {
	var oldFanout = *flagFanout
	*flagFanout = false
	defer func() {
		*flagFanout = oldFanout
	}()

	resetTopicSubscribersForTest()
	defer resetTopicSubscribersForTest()

	var subscriberConn, subscriberPeer = net.Pipe()
	defer subscriberConn.Close()
	defer subscriberPeer.Close()

	var writer = newConnWriter(subscriberConn, &connStats{})
	defer writer.close()

	registerSubscription("orders", &subscription{
		conn:    subscriberConn,
		writer:  writer,
		subID:   "delta-sub",
		topic:   "orders",
		isDelta: true,
	})

	notifyWorkspaceRemovals([]workspaceRemovedRecord{{
		Topic:    "orders",
		SOWKey:   "order-1",
		Bookmark: "1|1|",
		Reason:   "clear",
	}})

	if body, ok := tryReadFrameBody(subscriberPeer, 150*time.Millisecond); ok {
		t.Fatalf("received unexpected delta OOF with fanout disabled: %s", body)
	}
}

func newSessionClient(t *testing.T) *http.Client {
	t.Helper()

	var jar, err = cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar.New() error: %v", err)
	}

	return &http.Client{Jar: jar}
}

func loginAdminSession(t *testing.T, client *http.Client, baseURL string, username string, password string) {
	t.Helper()

	var payload, err = json.Marshal(map[string]string{
		"username": username,
		"password": password,
	})
	if err != nil {
		t.Fatalf("json.Marshal(login payload): %v", err)
	}

	var response, loginErr = client.Post(baseURL+"/amps/session/login", "application/json", bytes.NewReader(payload))
	if loginErr != nil {
		t.Fatalf("POST /amps/session/login error: %v", loginErr)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		var body, _ = io.ReadAll(response.Body)
		t.Fatalf("POST /amps/session/login status = %d, want 200; body=%s", response.StatusCode, string(body))
	}
}

func dialAdminWebSocket(t *testing.T, baseURL string, path string, headers map[string]string) net.Conn {
	t.Helper()

	var address = strings.TrimPrefix(baseURL, "http://")
	var conn, err = net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("net.Dial(%q) error: %v", address, err)
	}

	var request bytes.Buffer
	request.WriteString("GET " + path + " HTTP/1.1\r\n")
	request.WriteString("Host: " + address + "\r\n")
	request.WriteString("Upgrade: websocket\r\n")
	request.WriteString("Connection: Upgrade\r\n")
	request.WriteString("Sec-WebSocket-Version: 13\r\n")
	request.WriteString("Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n")
	for key, value := range headers {
		request.WriteString(key + ": " + value + "\r\n")
	}
	request.WriteString("\r\n")

	if _, err := conn.Write(request.Bytes()); err != nil {
		t.Fatalf("write websocket handshake: %v", err)
	}

	var handshakeResponse = make([]byte, 1024)
	if err := conn.SetReadDeadline(time.Now().Add(750 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline() error: %v", err)
	}
	var bytesRead, readErr = conn.Read(handshakeResponse)
	if readErr != nil {
		t.Fatalf("read websocket handshake: %v", readErr)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear read deadline: %v", err)
	}
	if !strings.Contains(string(handshakeResponse[:bytesRead]), "101 Switching Protocols") {
		t.Fatalf("websocket handshake response = %q, want 101 Switching Protocols", string(handshakeResponse[:bytesRead]))
	}

	return conn
}

func writeTextWebSocketFrame(t *testing.T, conn net.Conn, payload []byte) {
	t.Helper()

	var header []byte
	var length = len(payload)
	if length <= 125 {
		header = []byte{0x81, byte(0x80 | length)}
	} else if length <= 65535 {
		header = []byte{0x81, 0x80 | 126, byte(length >> 8), byte(length)}
	} else {
		header = make([]byte, 10)
		header[0] = 0x81
		header[1] = 0x80 | 127
		binary.BigEndian.PutUint64(header[2:], uint64(length))
	}

	var mask = []byte{0x01, 0x02, 0x03, 0x04}
	var masked = make([]byte, len(payload))
	var index int
	for index = 0; index < len(payload); index++ {
		masked[index] = payload[index] ^ mask[index%4]
	}

	if _, err := conn.Write(header); err != nil {
		t.Fatalf("write websocket header: %v", err)
	}
	if _, err := conn.Write(mask); err != nil {
		t.Fatalf("write websocket mask: %v", err)
	}
	if _, err := conn.Write(masked); err != nil {
		t.Fatalf("write websocket payload: %v", err)
	}
}

func readWebSocketFrame(t *testing.T, conn net.Conn, timeout time.Duration) (byte, []byte) {
	t.Helper()

	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatalf("failed to set read deadline: %v", err)
	}
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
	}()

	var hdr [2]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		t.Fatalf("failed to read ws frame header: %v", err)
	}

	var opcode = hdr[0] & 0x0f
	var length = int(hdr[1] & 0x7f)
	if length == 126 {
		var ext [2]byte
		if _, err := io.ReadFull(conn, ext[:]); err != nil {
			t.Fatalf("failed to read ws frame ext16: %v", err)
		}
		length = int(binary.BigEndian.Uint16(ext[:]))
	} else if length == 127 {
		var ext [8]byte
		if _, err := io.ReadFull(conn, ext[:]); err != nil {
			t.Fatalf("failed to read ws frame ext64: %v", err)
		}
		length = int(binary.BigEndian.Uint64(ext[:]))
	}

	var masked = (hdr[1] & 0x80) != 0
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(conn, mask[:]); err != nil {
			t.Fatalf("failed to read ws frame mask: %v", err)
		}
	}

	var payload = make([]byte, length)
	if _, err := io.ReadFull(conn, payload); err != nil {
		t.Fatalf("failed to read ws frame payload: %v", err)
	}

	if masked {
		var index int
		for index = 0; index < len(payload); index++ {
			payload[index] ^= mask[index%4]
		}
	}

	return opcode, payload
}

func tryReadWebSocketFrame(conn net.Conn, timeout time.Duration) (byte, []byte, error) {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return 0, nil, err
	}
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
	}()

	var hdr [2]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return 0, nil, err
	}

	var opcode = hdr[0] & 0x0f
	var length = int(hdr[1] & 0x7f)
	if length == 126 {
		var ext [2]byte
		if _, err := io.ReadFull(conn, ext[:]); err != nil {
			return 0, nil, err
		}
		length = int(binary.BigEndian.Uint16(ext[:]))
	} else if length == 127 {
		var ext [8]byte
		if _, err := io.ReadFull(conn, ext[:]); err != nil {
			return 0, nil, err
		}
		length = int(binary.BigEndian.Uint64(ext[:]))
	}

	var masked = (hdr[1] & 0x80) != 0
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(conn, mask[:]); err != nil {
			return 0, nil, err
		}
	}

	var payload = make([]byte, length)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return 0, nil, err
	}

	if masked {
		var index int
		for index = 0; index < len(payload); index++ {
			payload[index] ^= mask[index%4]
		}
	}

	return opcode, payload, nil
}

func resetTopicConfigsForTest() {
	topicConfigsMu.Lock()
	topicConfigs = make(map[string]*topicConfig)
	topicConfigsMu.Unlock()
}

func resetTopicSubscribersForTest() {
	topicSubscribers.Range(func(key, _ interface{}) bool {
		topicSubscribers.Delete(key)
		return true
	})
}

func resetWorkspaceSessionsForTest() {
	workspaceSessions.mu.Lock()
	workspaceSessions.queries = make(map[*websocketConn]workspaceLiveQuery)
	workspaceSessions.mu.Unlock()
}
