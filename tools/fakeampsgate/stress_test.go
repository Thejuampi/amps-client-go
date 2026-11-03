package main

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestGateTestOptions(t *testing.T) {
	var tests = []struct {
		name string
		args []string
		want integrationTestOptions
	}{
		{"default", nil, integrationTestOptions{count: 1, shuffle: "off", workers: 2}},
		{"stress", []string{"-race", "-count=20", "-shuffle=12345", "-workers=3"}, integrationTestOptions{race: true, count: 20, shuffle: "12345", workers: 3}},
		{"shuffle", []string{"-shuffle=on", "-count=2", "-workers=1"}, integrationTestOptions{count: 2, shuffle: "on", workers: 1}},
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

func TestGateTestOptionsRejectInvalidCount(t *testing.T) {
	for _, count := range []string{"0", "-1", "bad"} {
		t.Run(count, func(t *testing.T) {
			if _, err := parseTestOptions([]string{"-count=" + count}); err == nil {
				t.Fatal("invalid iteration count accepted")
			}
		})
	}
}

func TestGateTestOptionsRejectInvalidWorkers(t *testing.T) {
	for _, workers := range []string{"0", "-1", "bad"} {
		t.Run(workers, func(t *testing.T) {
			if _, err := parseTestOptions([]string{"-workers=" + workers}); err == nil {
				t.Fatal("invalid worker count accepted")
			}
		})
	}
}

func TestGateTestOptionsRejectUnexpectedArguments(t *testing.T) {
	if _, err := parseTestOptions([]string{"-race", "unexpected"}); err == nil {
		t.Fatal("unexpected positional argument accepted")
	}
}

func TestGateDefaultRunsBothSuitesOnce(t *testing.T) {
	var options, err = parseTestOptions(nil)
	if err != nil {
		t.Fatal(err)
	}
	var calls [][]string
	var start = func(int) (integrationWorker, error) {
		return integrationWorker{cleanup: func() {}}, nil
	}
	var execute = func(command integrationCommand) error {
		calls = append(calls, command.args)
		return nil
	}
	if err = runIntegrationTests("root", options, 1, start, execute, nil); err != nil {
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

func TestGateSequentialRunsBothSuitesPerRound(t *testing.T) {
	for _, count := range []int{1, 2, 20} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			var options = integrationTestOptions{race: true, count: count, shuffle: "12345", workers: 1}
			var env = []string{"AMPS_TEST_URI=tcp://127.0.0.1:1/amps/json"}
			var calls []integrationCommand
			var starts, cleanups int
			var start = func(int) (integrationWorker, error) {
				starts++
				return integrationWorker{env: env, cleanup: func() { cleanups++ }}, nil
			}
			var execute = func(command integrationCommand) error {
				calls = append(calls, command)
				return nil
			}
			if err := runIntegrationTests("root", options, 1, start, execute, nil); err != nil {
				t.Fatal(err)
			}
			if starts != 1 || cleanups != 1 || len(calls) != count*2 {
				t.Fatalf("count=%d starts=%d cleanups=%d calls=%d", count, starts, cleanups, len(calls))
			}
			for iteration := 1; iteration <= count; iteration++ {
				for index, pkg := range []string{"./amps", "./tools/fakeamps"} {
					var command = calls[(iteration-1)*2+index]
					var wantArgs = []string{"test", "-count=1", "-race", pkg, "-run", "Integration", "-shuffle=12345"}
					if command.worker != 1 || command.iteration != iteration || command.total != count || command.dir != "root" || command.name != "go" || command.step != pkg || !reflect.DeepEqual(command.args, wantArgs) {
						t.Fatalf("iteration %d command %+v, want args %q", iteration, command, wantArgs)
					}
					if pkg == "./amps" && !reflect.DeepEqual(command.env, env) || pkg != "./amps" && command.env != nil {
						t.Fatalf("iteration %d %s env=%v", iteration, pkg, command.env)
					}
				}
			}
		})
	}
}

func TestGateStressStopsAtFirstFailureSequential(t *testing.T) {
	for _, failingCall := range []int{1, 2, 3, 4} {
		var failure = errors.New("integration failure")
		var calls, cleanups int
		var start = func(int) (integrationWorker, error) {
			return integrationWorker{cleanup: func() { cleanups++ }}, nil
		}
		var execute = func(integrationCommand) error {
			calls++
			if calls == failingCall {
				return failure
			}
			return nil
		}
		var err = runIntegrationTests("root", integrationTestOptions{count: 2, workers: 1}, 1, start, execute, nil)
		if !errors.Is(err, failure) || calls != failingCall || cleanups != 1 {
			t.Fatalf("failure %d: err=%v calls=%d cleanups=%d", failingCall, err, calls, cleanups)
		}
	}
}

func TestStressMakeTargetCoversBrokerAndRoundRunner(t *testing.T) {
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
	if !strings.Contains(content, "STRESS_PKG ?= ./...") || !strings.Contains(content, "STRESS_COUNT ?= 20") || !strings.Contains(content, "STRESS_WORKERS ?= 2") || !strings.Contains(recipe, "test -race -shuffle=$(STRESS_SHUFFLE) -count=$(STRESS_COUNT) $(STRESS_PKG) -skip Integration") || !strings.Contains(recipe, "./tools/fakeampsgate -race -count=$(STRESS_COUNT) -shuffle=$(STRESS_SHUFFLE) -workers=$(STRESS_WORKERS)") {
		t.Fatal("stress-check must cover all packages, repeat integration with race and shuffle, and pass the configurable worker count")
	}
}
