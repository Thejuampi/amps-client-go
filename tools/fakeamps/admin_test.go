package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStartAdminServerNoAddr(t *testing.T) {
	startAdminServer("")
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
	rr := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/admin/status", nil)
	handleAdminStatus(rr, request)
	if rr.Code != http.StatusOK {
		t.Fatalf("status endpoint expected 200 got %d", rr.Code)
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
