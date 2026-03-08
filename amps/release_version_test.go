package amps

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestReleaseVersionMarkers(t *testing.T) {
	rawVersion, err := os.ReadFile("../VERSION")
	if err != nil {
		t.Fatalf("ReadFile(VERSION) failed: %v", err)
	}

	expectedVersion := strings.TrimSpace(string(rawVersion))
	if !regexp.MustCompile(`^\d+\.\d+\.\d+$`).MatchString(expectedVersion) {
		t.Fatalf("VERSION = %q, want semver X.Y.Z", expectedVersion)
	}

	if ClientVersion != expectedVersion {
		t.Fatalf("ClientVersion = %q, want %q", ClientVersion, expectedVersion)
	}
	if string(clientVersionBytes) != expectedVersion {
		t.Fatalf("clientVersionBytes = %q, want %q", string(clientVersionBytes), expectedVersion)
	}
}
