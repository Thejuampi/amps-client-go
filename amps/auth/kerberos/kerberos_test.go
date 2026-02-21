package kerberos

import (
	"strings"
	"testing"
)

func TestKerberosAuthenticatorCoverage(t *testing.T) {
	auth := NewAuthenticator(Config{
		ServicePrincipal: "amps/service",
		Realm:            "EXAMPLE.COM",
		StaticToken:      "token-1",
	})

	token, err := auth.Authenticate("user", "pass")
	if err != nil {
		t.Fatalf("authenticate failed: %v", err)
	}
	if token != "token-1" {
		t.Fatalf("unexpected token: %q", token)
	}
	retryToken, err := auth.Retry("user", "pass")
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if retryToken != "token-1" {
		t.Fatalf("unexpected retry token: %q", retryToken)
	}
	auth.Completed("user", "pass", "success")

	typed := auth.(*kerberosAuthenticator)
	caps := typed.Capabilities()
	if caps.Mechanism != "kerberos" || !caps.SupportsOS || len(caps.Limitations) == 0 {
		t.Fatalf("unexpected capability set: %+v", caps)
	}

	beginToken, err := typed.Begin("user", "pass")
	if err != nil || beginToken != "token-1" {
		t.Fatalf("begin failed: token=%q err=%v", beginToken, err)
	}
	continueToken, err := typed.Continue("user", "challenge")
	if err != nil || continueToken != "token-1" {
		t.Fatalf("continue failed: token=%q err=%v", continueToken, err)
	}
}

func TestKerberosAuthenticatorErrorPaths(t *testing.T) {
	auth := NewAuthenticator(Config{})
	if _, err := auth.Authenticate("user", "pass"); err == nil || !strings.Contains(err.Error(), "provider") {
		t.Fatalf("expected missing provider error, got %v", err)
	}
}
