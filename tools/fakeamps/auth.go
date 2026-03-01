package main

import (
	"log"
	"strings"
	"sync"
)

// ---------------------------------------------------------------------------
// Authentication & Entitlements
//
// Real AMPS supports pluggable authentication modules and fine-grained
// entitlements (per-topic read/write/subscribe permissions). This
// implementation provides:
//   - Username/password authentication (configurable via flags)
//   - Per-topic entitlements (publish, subscribe, sow, sow_delete)
//   - Default: accept all (no auth required) — suitable for perf testing
//   - When enabled: validate credentials on logon, check perms per command
// ---------------------------------------------------------------------------

var (
	flagAuthEnabled = false // set via init from flag
	flagAuthFile    = ""    // path to auth config file (future)
)

func init() {
	// Auth flags are registered but disabled by default for perf testing.
	// Enable with -auth to require valid credentials.
}

// authStore holds the server's authentication and entitlement state.
type authStore struct {
	mu           sync.RWMutex
	users        map[string]string            // username → password
	entitlements map[string]*topicEntitlement // username → entitlements
	defaultAllow bool                         // when true, unknown users are allowed
}

type topicEntitlement struct {
	publishAllow   []string // topic patterns allowed for publish
	publishDeny    []string // topic patterns denied for publish
	subscribeAllow []string // topic patterns allowed for subscribe
	subscribeDeny  []string
	sowAllow       []string
	sowDeny        []string
}

var auth = &authStore{
	users:        make(map[string]string),
	entitlements: make(map[string]*topicEntitlement),
	defaultAllow: true, // permissive by default
}

// addUser registers a user with password and optional entitlements.
func (a *authStore) addUser(username, password string, ent *topicEntitlement) {
	a.mu.Lock()
	a.users[username] = password
	if ent != nil {
		a.entitlements[username] = ent
	}
	a.mu.Unlock()
}

// authenticate checks username/password. Returns true if auth succeeds.
func (a *authStore) authenticate(username, password string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.defaultAllow && len(a.users) == 0 {
		return true
	}

	expected, exists := a.users[username]
	if !exists {
		return a.defaultAllow
	}
	return password == expected
}

// authorize checks if a user is allowed to perform an operation on a topic.
func (a *authStore) authorize(username, operation, topic string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.defaultAllow && len(a.entitlements) == 0 {
		return true
	}

	ent, exists := a.entitlements[username]
	if !exists {
		return a.defaultAllow
	}

	var allowList, denyList []string
	switch operation {
	case "publish", "delta_publish":
		allowList = ent.publishAllow
		denyList = ent.publishDeny
	case "subscribe", "delta_subscribe", "sow_and_subscribe", "sow_and_delta_subscribe":
		allowList = ent.subscribeAllow
		denyList = ent.subscribeDeny
	case "sow", "sow_delete":
		allowList = ent.sowAllow
		denyList = ent.sowDeny
	default:
		return true
	}

	// Check deny list first.
	for _, pattern := range denyList {
		if topicMatches(topic, pattern) {
			return false
		}
	}

	// Check allow list (if empty, allow all).
	if len(allowList) == 0 {
		return true
	}
	for _, pattern := range allowList {
		if topicMatches(topic, pattern) {
			return true
		}
	}

	return false
}

// logonResult represents the outcome of a logon authentication attempt.
type logonResult struct {
	success bool
	reason  string
}

func authenticateLogon(userID, password string) logonResult {
	if !auth.authenticate(userID, password) {
		log.Printf("fakeamps: auth failed for user=%q", userID)
		return logonResult{success: false, reason: "authentication failed"}
	}
	return logonResult{success: true}
}

func authorizeCommand(userID, command, topic string) bool {
	return auth.authorize(userID, command, topic)
}

// ---------------------------------------------------------------------------
// configureAuth sets up authentication from flag values.
// Called from main if -auth is enabled.
// ---------------------------------------------------------------------------

func configureAuth(userPassPairs string) {
	if userPassPairs == "" {
		return
	}
	auth.defaultAllow = false
	// Format: "user1:pass1,user2:pass2"
	for _, pair := range strings.Split(userPassPairs, ",") {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) == 2 {
			auth.addUser(parts[0], parts[1], nil)
			log.Printf("fakeamps: auth user registered: %s", parts[0])
		}
	}
}
