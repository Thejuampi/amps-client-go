package main

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestIntegrationTestOptions(t *testing.T) {
	var tests = []struct {
		name string
		args []string
		want integrationTestOptions
	}{
		{"default", nil, integrationTestOptions{count: 1, shuffle: "off"}},
		{"stress", []string{"-race", "-count=20", "-shuffle=12345"}, integrationTestOptions{race: true, count: 20, shuffle: "12345"}},
		{"shuffle", []string{"-shuffle=on", "-count=2"}, integrationTestOptions{count: 2, shuffle: "on"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got, err = parseTestOptions(test.args)
			if err != nil || got != test.want {
				t.Fatalf("options=(%+v, %v), want %+v", got, err, test.want)
			}
		})
	}
}

func TestIntegrationTestOptionsRejectInvalidCount(t *testing.T) {
	for _, count := range []string{"0", "-1", "bad"} {
		t.Run(count, func(t *testing.T) {
			if _, err := parseTestOptions([]string{"-count=" + count}); err == nil {
				t.Fatal("invalid iteration count accepted")
			}
		})
	}
}

func TestIntegrationTestOptionsRejectUnexpectedArguments(t *testing.T) {
	if _, err := parseTestOptions([]string{"-race", "unexpected"}); err == nil {
		t.Fatal("unexpected positional argument accepted")
	}
}

func TestIntegrationDefaultRunsBothSuitesOnce(t *testing.T) {
	var options, err = parseTestOptions(nil)
	if err != nil {
		t.Fatal(err)
	}
	var calls [][]string
	var execute = func(_ string, _ []string, _ string, args ...string) error {
		calls = append(calls, args)
		return nil
	}
	if err = runIntegrationTests("root", nil, options, execute); err != nil {
		t.Fatal(err)
	}
	var want = [][]string{
		{"test", "-count=1", "./amps", "-run", "Integration", "-shuffle=off"},
		{"test", "-count=1", "./tools/fakeamps", "-run", "Integration", "-shuffle=off"},
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("commands=%q, want %q", calls, want)
	}
}

func TestIntegrationStressRepeatsBothSuitesWithRaceAndShuffle(t *testing.T) {
	var options = integrationTestOptions{race: true, count: 2, shuffle: "12345"}
	var calls [][]string
	var env = []string{"AMPS_TEST_URI=tcp://127.0.0.1:1/amps/json"}
	var execute = func(root string, gotEnv []string, name string, args ...string) error {
		if root != "root" || name != "go" {
			t.Fatalf("unexpected invocation: %q %q", root, name)
		}
		if args[3] == "./amps" && !reflect.DeepEqual(gotEnv, env) {
			t.Fatalf("integration endpoint not supplied: %v", gotEnv)
		}
		calls = append(calls, args)
		return nil
	}
	if err := runIntegrationTests("root", env, options, execute); err != nil {
		t.Fatal(err)
	}
	var want [][]string
	for range 2 {
		for _, pkg := range []string{"./amps", "./tools/fakeamps"} {
			want = append(want, []string{"test", "-count=1", "-race", pkg, "-run", "Integration", "-shuffle=12345"})
		}
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("commands=%q, want %q", calls, want)
	}
}

func TestIntegrationStressStopsAtFirstFailure(t *testing.T) {
	for _, failingCall := range []int{1, 2, 3, 4} {
		var failure = errors.New("integration failure")
		var calls int
		var execute = func(string, []string, string, ...string) error {
			calls++
			if calls == failingCall {
				return failure
			}
			return nil
		}
		var err = runIntegrationTests("root", nil, integrationTestOptions{count: 2}, execute)
		if !errors.Is(err, failure) || calls != failingCall {
			t.Fatalf("failure %d: err=%v calls=%d", failingCall, err, calls)
		}
	}
}

func TestStressMakeTargetIncludesFakeAMPSAndIntegration(t *testing.T) {
	var cwd, err = os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	var root string
	root, err = repoRootFrom(cwd)
	if err != nil {
		t.Fatal(err)
	}
	var makefile []byte
	makefile, err = os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	var content = string(makefile)
	var recipe = strings.Split(strings.Split(content, "\nstress-check:")[1], "\n\n")[0]
	if !strings.Contains(content, "STRESS_PKG ?= ./...") || !strings.Contains(content, "STRESS_COUNT ?= 20") || !strings.Contains(recipe, "test -race -shuffle=$(STRESS_SHUFFLE) -count=$(STRESS_COUNT) $(STRESS_PKG) -skip Integration") || !strings.Contains(recipe, "./tools/fakeampsgate -race -count=$(STRESS_COUNT) -shuffle=$(STRESS_SHUFFLE)") {
		t.Fatal("stress-check must cover all packages and repeat real integration with race and shuffle")
	}
}
