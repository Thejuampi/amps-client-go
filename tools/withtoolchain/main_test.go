package main

import (
	"strings"
	"testing"
)

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

func TestValidateGoCommandArgsRejectsNonGoExecutables(t *testing.T) {
	if _, err := validateGoCommandArgs([]string{"git", "status"}); err == nil {
		t.Fatalf("validateGoCommandArgs should reject non-go executables")
	}
}

func TestValidateGoCommandArgsTrimsGoPrefix(t *testing.T) {
	got, err := validateGoCommandArgs([]string{"go", "test", "./..."})
	if err != nil {
		t.Fatalf("validateGoCommandArgs returned error: %v", err)
	}
	if strings.Join(got, " ") != "test ./..." {
		t.Fatalf("validateGoCommandArgs returned %q, want \"test ./...\"", strings.Join(got, " "))
	}
}
