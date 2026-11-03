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

func TestReadmeReleaseVersion(t *testing.T) {
	var readme, err = os.ReadFile("../README.md")
	if err != nil {
		t.Fatal(err)
	}
	var version = regexp.MustCompile("(?m)^Version: `([^`]+)`\\r?$").FindSubmatch(readme)
	if len(version) != 2 || string(version[1]) != ClientVersion {
		t.Fatalf("README release version = %q, want %q", version, ClientVersion)
	}
}

func TestChangelogReleaseVersion(t *testing.T) {
	var changelog, err = os.ReadFile("../CHANGELOG.md")
	if err != nil {
		t.Fatal(err)
	}
	var version = regexp.MustCompile(`(?m)^## \[(\d+\.\d+\.\d+)\] - \d{4}-\d{2}-\d{2}\r?$`).FindSubmatch(changelog)
	if len(version) != 2 || string(version[1]) != ClientVersion {
		t.Fatalf("latest changelog release = %q, want %q", version, ClientVersion)
	}
}

func TestReleaseWorkflowAcceptsPreparedVersion(t *testing.T) {
	var workflow, err = os.ReadFile("../.github/workflows/release.yml")
	if err != nil {
		t.Fatal(err)
	}
	var content = string(workflow)
	for _, required := range []string{
		"id: version_files",
		`echo "changed=false" >> "$GITHUB_OUTPUT"`,
		`echo "changed=true" >> "$GITHUB_OUTPUT"`,
		"steps.version_files.outputs.changed == 'true'",
	} {
		if !strings.Contains(content, required) {
			t.Errorf("release cannot publish an already prepared version: missing %q", required)
		}
	}
	if strings.Contains(content, "Release version files did not change.") {
		t.Fatal("prepared releases must not fail because version markers already match")
	}
}

func TestReleaseWorkflowValidatesPreparedMarkersBeforeGates(t *testing.T) {
	var workflow, err = os.ReadFile("../.github/workflows/release.yml")
	if err != nil {
		t.Fatal(err)
	}
	var content = string(workflow)
	var update = strings.Index(content, "- name: Update version files")
	var validation = strings.Index(content, "- name: Validate prepared release markers")
	var gates = strings.Index(content, "- name: Release gates")
	if update < 0 || validation <= update || gates <= validation {
		t.Fatal("release marker validation must run after version updates and before release gates")
	}
	var step = content[validation:gates]
	var command = "go test ./amps -run '^(TestReleaseVersionMarkers|TestReadmeReleaseVersion|TestChangelogReleaseVersion)$' -count=1"
	if !strings.Contains(step, command) {
		t.Fatal("release preflight must run all three release marker tests without cached results")
	}
	if strings.Contains(step, "continue-on-error:") || strings.Contains(step, "if:") || strings.Contains(step, "||") {
		t.Fatal("release marker validation must be unconditional and blocking")
	}
}
