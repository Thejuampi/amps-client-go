package main

import "testing"

func TestWithToolchainEnvAppendsWhenMissing(t *testing.T) {
	var env = withToolchainEnv([]string{"GOOS=windows"}, "go1.25.9+auto")
	if len(env) != 2 {
		t.Fatalf("len(env) = %d, want 2", len(env))
	}
	if env[1] != "GOTOOLCHAIN=go1.25.9+auto" {
		t.Fatalf("env[1] = %q, want appended toolchain", env[1])
	}
}

func TestWithToolchainEnvReplacesExistingValue(t *testing.T) {
	var env = withToolchainEnv([]string{"GOOS=windows", "GOTOOLCHAIN=go1.26.0"}, "go1.25.9+auto")
	if len(env) != 2 {
		t.Fatalf("len(env) = %d, want 2", len(env))
	}
	if env[1] != "GOTOOLCHAIN=go1.25.9+auto" {
		t.Fatalf("env[1] = %q, want replaced toolchain", env[1])
	}
}
