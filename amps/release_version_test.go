package amps

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestReleaseVersionMarkers(t *testing.T) {
	const expectedVersion = "0.5.2"

	if ClientVersion != expectedVersion {
		t.Fatalf("ClientVersion = %q, want %q", ClientVersion, expectedVersion)
	}
	if string(clientVersionBytes) != expectedVersion {
		t.Fatalf("clientVersionBytes = %q, want %q", string(clientVersionBytes), expectedVersion)
	}

	rawVersion, err := os.ReadFile("../VERSION")
	if err != nil {
		t.Fatalf("ReadFile(VERSION) failed: %v", err)
	}

	version := strings.TrimSpace(string(rawVersion))
	if version != expectedVersion {
		t.Fatalf("VERSION = %q, want %q", version, expectedVersion)
	}
	if !regexp.MustCompile(`^\d+\.\d+\.\d+$`).MatchString(version) {
		t.Fatalf("VERSION = %q, want semver X.Y.Z", version)
	}
}
