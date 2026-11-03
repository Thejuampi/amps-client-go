package main

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestGateWorkerLimit(t *testing.T) {
	for _, test := range []struct {
		count, requested, cpu, procs, want int
	}{
		{1, 2, 20, 20, 1},
		{2, 2, 20, 20, 2},
		{20, 2, 20, 20, 2},
		{20, 1, 20, 20, 1},
		{20, 50, 4, 20, 4},
		{20, 50, 20, 3, 3},
		{3, 50, 20, 20, 3},
	} {
		if got := effectiveWorkerCount(test.count, test.requested, test.cpu, test.procs); got != test.want {
			t.Fatalf("limit(%+v)=%d, want %d", test, got, test.want)
		}
	}
}

func TestGateRejectsInvalidInternalWorkerCount(t *testing.T) {
	var start = func(int) (integrationWorker, error) {
		t.Fatal("worker started for invalid count")
		return integrationWorker{}, nil
	}
	for _, workers := range []int{0, -1, 3} {
		if err := runIntegrationTests("root", integrationTestOptions{count: 2}, workers, start, nil, nil); err == nil {
			t.Fatalf("accepted worker count %d", workers)
		}
	}
}

func TestGateParallelCompletesTwoRounds(t *testing.T) {
	var mutex sync.Mutex
	var calls []integrationCommand
	var started = make(chan integrationCommand, 2)
	var release = make(chan struct{})
	var done = make(chan error, 1)
	var start = func(int) (integrationWorker, error) {
		return integrationWorker{cleanup: func() {}}, nil
	}
	var execute = func(command integrationCommand) error {
		mutex.Lock()
		calls = append(calls, command)
		mutex.Unlock()
		if command.step == "./amps" {
			started <- command
			<-release
		}
		return nil
	}
	go func() {
		done <- runIntegrationTests("root", integrationTestOptions{count: 2}, 2, start, execute, nil)
	}()
	var first = receiveCommand(t, started)
	var second = receiveCommand(t, started)
	if first.worker == second.worker || first.iteration == second.iteration {
		t.Fatalf("expected two independent rounds, got %+v and %+v", first, second)
	}
	close(release)
	if err := receiveError(t, done); err != nil {
		t.Fatal(err)
	}
	var steps = make(map[int][]string)
	for _, call := range calls {
		steps[call.iteration] = append(steps[call.iteration], call.step)
	}
	for iteration := 1; iteration <= 2; iteration++ {
		if !reflect.DeepEqual(steps[iteration], []string{"./amps", "./tools/fakeamps"}) {
			t.Fatalf("round %d steps=%v", iteration, steps[iteration])
		}
	}
	if len(calls) != 4 {
		t.Fatalf("calls=%v, want exactly four", calls)
	}
}

func TestGateParallelCompletesTwentyIsolatedRounds(t *testing.T) {
	var mutex sync.Mutex
	var calls []integrationCommand
	var cleanups []int
	var active, maxActive int
	var started = make(chan integrationCommand, 2)
	var release = make(chan struct{})
	var done = make(chan error, 1)
	var start = func(worker int) (integrationWorker, error) {
		var env = []string{fmt.Sprintf("AMPS_TEST_URI=worker-%d-primary", worker), fmt.Sprintf("AMPS_TEST_FAILOVER_URIS=worker-%d-primary,worker-%d-secondary", worker, worker)}
		return integrationWorker{env: env, cleanup: func() {
			mutex.Lock()
			defer mutex.Unlock()
			cleanups = append(cleanups, worker)
		}}, nil
	}
	var execute = func(command integrationCommand) error {
		mutex.Lock()
		calls = append(calls, command)
		active++
		if active > maxActive {
			maxActive = active
		}
		mutex.Unlock()
		if command.step == "./amps" && command.iteration <= 2 {
			started <- command
			<-release
		}
		mutex.Lock()
		active--
		mutex.Unlock()
		return nil
	}
	go func() {
		done <- runIntegrationTests("root", integrationTestOptions{count: 20, workers: 2, race: true, shuffle: "on"}, 2, start, execute, nil)
	}()
	var first = receiveCommand(t, started)
	var second = receiveCommand(t, started)
	if first.worker == second.worker || first.iteration == second.iteration {
		t.Fatalf("expected two independent rounds in flight, got %+v and %+v", first, second)
	}
	mutex.Lock()
	var beforeRelease = len(calls)
	mutex.Unlock()
	if beforeRelease != 2 {
		t.Fatalf("started %d commands with two blocked workers", beforeRelease)
	}
	close(release)
	if err := receiveError(t, done); err != nil {
		t.Fatal(err)
	}
	if maxActive != 2 || len(calls) != 40 || !reflect.DeepEqual(cleanups, []int{2, 1}) {
		t.Fatalf("max active=%d calls=%d cleanups=%v", maxActive, len(calls), cleanups)
	}
	var rounds = make(map[int][]integrationCommand)
	for _, command := range calls {
		rounds[command.iteration] = append(rounds[command.iteration], command)
		if command.total != 20 || command.dir != "root" || command.name != "go" {
			t.Fatalf("invalid command context: %+v", command)
		}
		var wantArgs = []string{"test", "-count=1", "-race", command.step, "-run", "Integration", "-shuffle=on"}
		if !reflect.DeepEqual(command.args, wantArgs) {
			t.Fatalf("command args=%v want=%v", command.args, wantArgs)
		}
	}
	for iteration := 1; iteration <= 20; iteration++ {
		var pair = rounds[iteration]
		if len(pair) != 2 || pair[0].step != "./amps" || pair[1].step != "./tools/fakeamps" || pair[0].worker != pair[1].worker {
			t.Fatalf("round %d commands=%+v", iteration, pair)
		}
		var worker = pair[0].worker
		var wantEnv = []string{fmt.Sprintf("AMPS_TEST_URI=worker-%d-primary", worker), fmt.Sprintf("AMPS_TEST_FAILOVER_URIS=worker-%d-primary,worker-%d-secondary", worker, worker)}
		if !reflect.DeepEqual(pair[0].env, wantEnv) || pair[1].env != nil {
			t.Fatalf("round %d env=%v, %v", iteration, pair[0].env, pair[1].env)
		}
	}
}

func TestGateFailureStopsNewRoundsAndDrainsInflight(t *testing.T) {
	var failure = errors.New("first integration failure")
	var mutex sync.Mutex
	var calls []integrationCommand
	var cleanups []int
	var started = make(chan integrationCommand, 2)
	var fail = make(chan struct{})
	var release = make(chan struct{})
	var done = make(chan error, 1)
	var reported = make(chan struct{})
	var output = &failureNotifier{reported: reported}
	var start = func(worker int) (integrationWorker, error) {
		return integrationWorker{cleanup: func() {
			mutex.Lock()
			defer mutex.Unlock()
			cleanups = append(cleanups, worker)
		}}, nil
	}
	var execute = func(command integrationCommand) error {
		mutex.Lock()
		calls = append(calls, command)
		mutex.Unlock()
		if command.step == "./amps" {
			started <- command
			if command.iteration == 1 {
				<-fail
				return failure
			}
			<-release
		}
		return nil
	}
	go func() {
		done <- runIntegrationTests("root", integrationTestOptions{count: 20, workers: 2}, 2, start, execute, output)
	}()
	receiveCommand(t, started)
	receiveCommand(t, started)
	close(fail)
	select {
	case <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("failure was not recorded")
	}
	select {
	case err := <-done:
		t.Fatalf("returned before in-flight round completed: %v", err)
	default:
	}
	mutex.Lock()
	if len(cleanups) != 0 {
		t.Fatalf("cleaned brokers before in-flight round completed: %v", cleanups)
	}
	mutex.Unlock()
	close(release)
	if err := receiveError(t, done); !errors.Is(err, failure) {
		t.Fatalf("error=%v, want original failure", err)
	}
	if len(calls) != 3 || !reflect.DeepEqual(cleanups, []int{2, 1}) {
		t.Fatalf("calls=%+v cleanups=%v", calls, cleanups)
	}
	for _, command := range calls {
		if command.iteration > 2 || command.iteration == 1 && command.step != "./amps" {
			t.Fatalf("unexpected command after failure: %+v", command)
		}
	}
}

func TestGateConcurrentFailuresPreserveFirstFailure(t *testing.T) {
	var first = errors.New("first failure")
	var second = errors.New("later failure")
	var started = make(chan integrationCommand, 2)
	var failFirst = make(chan struct{})
	var failSecond = make(chan struct{})
	var reported = make(chan struct{})
	var done = make(chan error, 1)
	var start = func(int) (integrationWorker, error) {
		return integrationWorker{cleanup: func() {}}, nil
	}
	var execute = func(command integrationCommand) error {
		started <- command
		if command.iteration == 1 {
			<-failFirst
			return first
		}
		<-failSecond
		return second
	}
	go func() {
		done <- runIntegrationTests("root", integrationTestOptions{count: 20}, 2, start, execute, &failureNotifier{reported: reported})
	}()
	receiveCommand(t, started)
	receiveCommand(t, started)
	close(failFirst)
	select {
	case <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("first failure was not recorded")
	}
	close(failSecond)
	if err := receiveError(t, done); !errors.Is(err, first) || errors.Is(err, second) {
		t.Fatalf("error=%v, want first failure", err)
	}
}

type failureNotifier struct {
	once     sync.Once
	reported chan struct{}
}

func (writer *failureNotifier) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("draining in-flight rounds")) {
		writer.once.Do(func() { close(writer.reported) })
	}
	return len(p), nil
}

func receiveCommand(t *testing.T, ch <-chan integrationCommand) integrationCommand {
	t.Helper()
	select {
	case command := <-ch:
		return command
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for command")
		return integrationCommand{}
	}
}

func receiveError(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for execution")
		return nil
	}
}

func TestGateWorkerStartupFailureCleansEarlierWorkers(t *testing.T) {
	var failure = errors.New("startup failure")
	var cleanups, calls []int
	var start = func(worker int) (integrationWorker, error) {
		if worker == 2 {
			return integrationWorker{}, failure
		}
		return integrationWorker{cleanup: func() { cleanups = append(cleanups, worker) }}, nil
	}
	var execute = func(command integrationCommand) error {
		calls = append(calls, command.worker)
		return nil
	}
	var err = runIntegrationTests("root", integrationTestOptions{count: 2, workers: 2}, 2, start, execute, nil)
	if !errors.Is(err, failure) || !reflect.DeepEqual(cleanups, []int{1}) || len(calls) != 0 {
		t.Fatalf("error=%v cleanups=%v calls=%v", err, cleanups, calls)
	}
}

func TestGateWorkerProcessStartupCleanup(t *testing.T) {
	for _, test := range []struct {
		name                   string
		failReserve, failStart int
		wantStops              []string
	}{
		{"reserve primary", 1, 0, nil},
		{"start primary", 0, 1, nil},
		{"reserve secondary", 2, 0, []string{"127.0.0.1:1"}},
		{"start secondary", 0, 2, []string{"127.0.0.1:1"}},
		{"success", 0, 0, []string{"127.0.0.1:2", "127.0.0.1:1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var failure = errors.New("broker failure")
			var reserves, starts int
			var stops []string
			var ops = fakeAMPSProcessOps{
				reserve: func() (string, error) {
					reserves++
					if reserves == test.failReserve {
						return "", failure
					}
					return fmt.Sprintf("127.0.0.1:%d", reserves), nil
				},
				start: func(root, binary, addr string) (*fakeAMPSProcess, error) {
					starts++
					if root != "root" || binary != "binary" {
						t.Fatalf("unexpected startup: %s %s", root, binary)
					}
					if starts == test.failStart {
						return nil, failure
					}
					return &fakeAMPSProcess{addr: addr}, nil
				},
				stop: func(process *fakeAMPSProcess) { stops = append(stops, process.addr) },
			}
			var worker, err = startIntegrationWorker("root", "binary", []string{"UNCHANGED=1"}, ops)
			if test.failReserve != 0 || test.failStart != 0 {
				if !errors.Is(err, failure) {
					t.Fatalf("error=%v, want original failure", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				var want = []string{"UNCHANGED=1", "AMPS_TEST_URI=tcp://127.0.0.1:1/amps/json", "AMPS_TEST_FAILOVER_URIS=tcp://127.0.0.1:1/amps/json,tcp://127.0.0.1:2/amps/json"}
				if !reflect.DeepEqual(worker.env, want) {
					t.Fatalf("env=%v, want %v", worker.env, want)
				}
				worker.cleanup()
			}
			if !reflect.DeepEqual(stops, test.wantStops) {
				t.Fatalf("stops=%v, want %v", stops, test.wantStops)
			}
		})
	}
}

func TestGateWorkerEnvironmentsDoNotAlias(t *testing.T) {
	var base = make([]string, 1, 8)
	base[0] = "EXISTING=1"
	var next int
	var ops = fakeAMPSProcessOps{
		reserve: func() (string, error) {
			next++
			return fmt.Sprintf("127.0.0.1:%d", next), nil
		},
		start: func(_, _, addr string) (*fakeAMPSProcess, error) { return &fakeAMPSProcess{addr: addr}, nil },
		stop:  func(*fakeAMPSProcess) {},
	}
	var first, err = startIntegrationWorker("root", "binary", base, ops)
	if err != nil {
		t.Fatal(err)
	}
	defer first.cleanup()
	var second integrationWorker
	second, err = startIntegrationWorker("root", "binary", base, ops)
	if err != nil {
		t.Fatal(err)
	}
	defer second.cleanup()
	if first.env[1] != "AMPS_TEST_URI=tcp://127.0.0.1:1/amps/json" || second.env[1] != "AMPS_TEST_URI=tcp://127.0.0.1:3/amps/json" || first.env[2] == second.env[2] {
		t.Fatalf("worker environments alias: %v, %v", first.env, second.env)
	}
}

func TestLoggedCommandIncludesContextAndDurationWithoutEnvironment(t *testing.T) {
	var output bytes.Buffer
	var failure = errors.New("command failure")
	var command = integrationCommand{worker: 2, iteration: 3, total: 20, step: "./amps", dir: "root", name: "go", args: []string{"test", "./amps"}, env: []string{"SECRET=do-not-log"}}
	var execute = func(dir string, env []string, name string, args ...string) error {
		if dir != command.dir || name != command.name || !reflect.DeepEqual(env, command.env) || !reflect.DeepEqual(args, command.args) {
			t.Fatalf("wrong command invocation: %s %v %s %v", dir, env, name, args)
		}
		return failure
	}
	var err = runLoggedCommand(&output, command, execute)
	var log = output.String()
	if !errors.Is(err, failure) || !strings.Contains(log, "worker=2") || !strings.Contains(log, "round=3/20") || !strings.Contains(log, "step=./amps") || !strings.Contains(log, "duration=") || !strings.Contains(log, "failed") || strings.Contains(log, "do-not-log") {
		t.Fatalf("error=%v log=%q", err, log)
	}
}

func TestLoggedCommandReportsSuccess(t *testing.T) {
	var output bytes.Buffer
	var command = integrationCommand{step: "build", name: "go"}
	var execute = func(string, []string, string, ...string) error { return nil }
	if err := runLoggedCommand(&output, command, execute); err != nil || !strings.Contains(output.String(), "ok duration=") {
		t.Fatalf("error=%v log=%q", err, output.String())
	}
}
