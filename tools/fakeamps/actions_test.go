package main

import (
	"strings"
	"testing"
)

func resetActionsForTest() {
	actionsMu.Lock()
	actions = nil
	actionsMu.Unlock()
}

func TestParseActionDef(t *testing.T) {
	valid := parseActionDef("on-publish:orders:route:orders_archive")
	if valid == nil {
		t.Fatalf("expected valid action definition")
	}
	if valid.trigger != triggerOnPublish || valid.action != actionRoute {
		t.Fatalf("unexpected parsed action: %+v", valid)
	}

	invalid := parseActionDef("bad:orders:route")
	if invalid != nil {
		t.Fatalf("expected invalid action to return nil")
	}
}

func TestInjectField(t *testing.T) {
	payload := []byte(`{"id":1}`)
	out := injectField(payload, "extra", `"x"`)
	text := string(out)
	if !strings.Contains(text, `"extra":"x"`) {
		t.Fatalf("expected injected field in output: %s", text)
	}

	nonJSON := []byte("plain")
	unchanged := injectField(nonJSON, "k", `"v"`)
	if string(unchanged) != "plain" {
		t.Fatalf("expected non-json payload unchanged")
	}
}

func TestFireOnDeliverTransform(t *testing.T) {
	resetActionsForTest()
	defer resetActionsForTest()

	registerAction(actionDef{
		trigger:    triggerOnDeliver,
		topicMatch: "orders",
		action:     actionTransform,
		target:     "add_timestamp",
	})

	payload := []byte(`{"id":1}`)
	out := fireOnDeliver("orders", payload, "sub-1")
	if !strings.Contains(string(out), `"_delivered_at"`) {
		t.Fatalf("expected transformed payload to include _delivered_at: %s", string(out))
	}
}

func TestFireOnPublishRoute(t *testing.T) {
	resetActionsForTest()
	defer resetActionsForTest()

	oldSow := sow
	oldJournal := journal
	sow = newSOWCache()
	journal = newMessageJournal(100)
	defer func() {
		sow = oldSow
		journal = oldJournal
	}()

	registerAction(actionDef{
		trigger:    triggerOnPublish,
		topicMatch: "orders",
		action:     actionRoute,
		target:     "orders_archive",
	})

	fireOnPublish("orders", []byte(`{"id":1,"status":"ok"}`), "bm1")

	result := sow.query("orders_archive", "", -1, "")
	if result.totalCount != 1 {
		t.Fatalf("expected routed message in archive topic, got count=%d", result.totalCount)
	}
}
