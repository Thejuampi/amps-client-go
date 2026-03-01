package main

import (
	"flag"
	"testing"
	"time"
)

func TestViewFlagsSetAndString(t *testing.T) {
	var v viewFlags
	if err := v.Set("a:b"); err != nil {
		t.Fatalf("unexpected error from viewFlags.Set: %v", err)
	}
	if len(v) != 1 || v[0] != "a:b" {
		t.Fatalf("unexpected viewFlags value: %v", v)
	}
	if v.String() == "" {
		t.Fatalf("expected non-empty String()")
	}
}

func TestActionFlagsSetAndString(t *testing.T) {
	var a actionFlags
	if err := a.Set("on-publish:orders:log"); err != nil {
		t.Fatalf("unexpected error from actionFlags.Set: %v", err)
	}
	if len(a) != 1 || a[0] != "on-publish:orders:log" {
		t.Fatalf("unexpected actionFlags value: %v", a)
	}
	if a.String() == "" {
		t.Fatalf("expected non-empty String()")
	}
}

func TestApplyBenchmarkStabilitySettingsDisabled(t *testing.T) {
	var settings = benchmarkStabilitySettings{
		logConn:            true,
		logStats:           true,
		sowGCInterval:      30 * time.Second,
		queueLeaseInterval: 5 * time.Second,
	}

	var got = applyBenchmarkStabilitySettings(false, settings, benchmarkStabilityOverrides{})
	if got != settings {
		t.Fatalf("expected stability settings to remain unchanged: got=%+v want=%+v", got, settings)
	}
}

func TestApplyBenchmarkStabilitySettingsEnabledWithoutOverrides(t *testing.T) {
	var settings = benchmarkStabilitySettings{
		logConn:            true,
		logStats:           true,
		sowGCInterval:      30 * time.Second,
		queueLeaseInterval: 5 * time.Second,
	}
	var expected = benchmarkStabilitySettings{
		logConn:            false,
		logStats:           false,
		sowGCInterval:      5 * time.Minute,
		queueLeaseInterval: 30 * time.Second,
	}

	var got = applyBenchmarkStabilitySettings(true, settings, benchmarkStabilityOverrides{})
	if got != expected {
		t.Fatalf("unexpected benchmark stability defaults: got=%+v want=%+v", got, expected)
	}
}

func TestApplyBenchmarkStabilitySettingsEnabledWithOverrides(t *testing.T) {
	var settings = benchmarkStabilitySettings{
		logConn:            true,
		logStats:           true,
		sowGCInterval:      30 * time.Second,
		queueLeaseInterval: 5 * time.Second,
	}
	var overrides = benchmarkStabilityOverrides{
		logConnSet:            true,
		logStatsSet:           true,
		sowGCIntervalSet:      true,
		queueLeaseIntervalSet: true,
	}

	var got = applyBenchmarkStabilitySettings(true, settings, overrides)
	if got != settings {
		t.Fatalf("expected explicit settings to remain unchanged: got=%+v want=%+v", got, settings)
	}
}

func TestFlagSetByUserSet(t *testing.T) {
	var fs = flag.NewFlagSet("fakeamps-test", flag.ContinueOnError)
	_ = fs.Bool("log-conn", true, "")
	if err := fs.Parse([]string{"-log-conn=false"}); err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if !flagSetByUser(fs, "log-conn") {
		t.Fatalf("expected flagSetByUser to report log-conn as set")
	}
}

func TestFlagSetByUserUnset(t *testing.T) {
	var fs = flag.NewFlagSet("fakeamps-test", flag.ContinueOnError)
	_ = fs.Bool("stats", false, "")
	if err := fs.Parse([]string{}); err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if flagSetByUser(fs, "stats") {
		t.Fatalf("expected flagSetByUser to report stats as unset")
	}
}
