package main

import (
	"os"
	"path/filepath"
	"testing"
)

func writeCoverageProfile(t *testing.T, lines ...string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "coverage.out")
	content := "mode: set\n"
	for _, line := range lines {
		content += line + "\n"
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile(%q): %v", path, err)
	}
	return path
}

func TestParseProfileDeduplicatesRepeatedBlocks(t *testing.T) {
	profilePath := writeCoverageProfile(
		t,
		"github.com/Thejuampi/amps-client-go/amps/command.go:10.1,12.2 2 0",
		"github.com/Thejuampi/amps-client-go/amps/command.go:10.1,12.2 2 1",
		"github.com/Thejuampi/amps-client-go/amps/command.go:13.1,14.2 1 0",
		"github.com/Thejuampi/amps-client-go/amps/errors.go:20.1,21.2 3 1",
	)

	files, err := parseProfile(profilePath)
	if err != nil {
		t.Fatalf("parseProfile(%q): %v", profilePath, err)
	}

	commandCoverage, ok := files["github.com/Thejuampi/amps-client-go/amps/command.go"]
	if !ok {
		t.Fatalf("expected command.go coverage entry, got %#v", files)
	}
	if commandCoverage.total != 3 || commandCoverage.covered != 2 {
		t.Fatalf("unexpected deduplicated command coverage: %+v", commandCoverage)
	}

	errorsCoverage, ok := files["github.com/Thejuampi/amps-client-go/amps/errors.go"]
	if !ok {
		t.Fatalf("expected errors.go coverage entry, got %#v", files)
	}
	if errorsCoverage.total != 3 || errorsCoverage.covered != 3 {
		t.Fatalf("unexpected errors coverage: %+v", errorsCoverage)
	}
}

func TestPureFileThresholdOverrides(t *testing.T) {
	if got := pureFileThresholds["amps/header.go"]; got != 99.5 {
		t.Fatalf("pureFileThresholds[header.go] = %.1f, want 99.5", got)
	}
	if got := pureFileThresholds["amps/message_stream.go"]; got != 99.7 {
		t.Fatalf("pureFileThresholds[message_stream.go] = %.1f, want 99.7", got)
	}
}
