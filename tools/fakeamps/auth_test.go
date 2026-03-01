package main

import "testing"

func resetAuthForTest() {
	auth = &authStore{
		users:        make(map[string]string),
		entitlements: make(map[string]*topicEntitlement),
		defaultAllow: true,
	}
}

func TestAuthStoreAuthenticate(t *testing.T) {
	store := &authStore{
		users:        make(map[string]string),
		entitlements: make(map[string]*topicEntitlement),
		defaultAllow: false,
	}
	store.addUser("u1", "p1", nil)

	if !store.authenticate("u1", "p1") {
		t.Fatalf("expected authenticate success")
	}
	if store.authenticate("u1", "bad") {
		t.Fatalf("expected authenticate failure for bad password")
	}
}

func TestAuthStoreAuthorize(t *testing.T) {
	store := &authStore{
		users: make(map[string]string),
		entitlements: map[string]*topicEntitlement{
			"u1": {
				publishAllow:   []string{"orders.>"},
				publishDeny:    []string{"orders.secret"},
				subscribeAllow: []string{"orders"},
				sowAllow:       []string{"orders"},
			},
		},
		defaultAllow: false,
	}

	if !store.authorize("u1", "publish", "orders.us") {
		t.Fatalf("expected publish allow")
	}
	if store.authorize("u1", "publish", "orders.secret") {
		t.Fatalf("expected publish deny")
	}
	if !store.authorize("u1", "subscribe", "orders") {
		t.Fatalf("expected subscribe allow")
	}
	if store.authorize("u1", "sow", "customers") {
		t.Fatalf("expected sow deny on non-allowed topic")
	}
}

func TestConfigureAuthAndLogonHelpers(t *testing.T) {
	resetAuthForTest()
	defer resetAuthForTest()

	configureAuth("alice:pwd,bob:secret")

	ok := authenticateLogon("alice", "pwd")
	if !ok.success {
		t.Fatalf("expected alice auth success")
	}

	fail := authenticateLogon("alice", "bad")
	if fail.success {
		t.Fatalf("expected bad password failure")
	}

	if authorizeCommand("alice", "publish", "orders") {
		t.Fatalf("expected publish authorization to be denied without entitlements")
	}
}
