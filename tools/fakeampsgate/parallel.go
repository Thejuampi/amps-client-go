package main

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

type integrationWorker struct {
	env     []string
	cleanup func()
}

type integrationCommand struct {
	worker    int
	iteration int
	total     int
	step      string
	dir       string
	env       []string
	name      string
	args      []string
}

type fakeAMPSProcessOps struct {
	reserve func() (string, error)
	start   func(string, string, string) (*fakeAMPSProcess, error)
	stop    func(*fakeAMPSProcess)
}

func effectiveWorkerCount(count, requested, cpu, procs int) int {
	return min(count, requested, cpu, procs)
}

func startIntegrationWorker(root, binary string, baseEnv []string, ops fakeAMPSProcessOps) (integrationWorker, error) {
	var primaryAddr, err = ops.reserve()
	if err != nil {
		return integrationWorker{}, err
	}
	var primary *fakeAMPSProcess
	primary, err = ops.start(root, binary, primaryAddr)
	if err != nil {
		return integrationWorker{}, err
	}
	var secondaryAddr string
	secondaryAddr, err = ops.reserve()
	if err != nil {
		ops.stop(primary)
		return integrationWorker{}, err
	}
	var secondary *fakeAMPSProcess
	secondary, err = ops.start(root, binary, secondaryAddr)
	if err != nil {
		ops.stop(primary)
		return integrationWorker{}, err
	}
	var primaryURI = fakeAMPSURI(primary.addr)
	var secondaryURI = fakeAMPSURI(secondary.addr)
	var env = make([]string, len(baseEnv), len(baseEnv)+2)
	copy(env, baseEnv)
	env = append(env, "AMPS_TEST_URI="+primaryURI, "AMPS_TEST_FAILOVER_URIS="+primaryURI+","+secondaryURI)
	return integrationWorker{
		env: env,
		cleanup: func() {
			ops.stop(secondary)
			ops.stop(primary)
		},
	}, nil
}

func runIntegrationTests(root string, options integrationTestOptions, workerCount int, start func(int) (integrationWorker, error), execute func(integrationCommand) error, output io.Writer) error {
	if workerCount < 1 || workerCount > options.count {
		return fmt.Errorf("invalid worker count %d for %d rounds", workerCount, options.count)
	}
	if output == nil {
		output = io.Discard
	}
	var workers = make([]integrationWorker, 0, workerCount)
	defer func() {
		for index := len(workers) - 1; index >= 0; index-- {
			workers[index].cleanup()
		}
	}()
	for id := 1; id <= workerCount; id++ {
		var worker, err = start(id)
		if err != nil {
			return fmt.Errorf("start integration worker %d: %w", id, err)
		}
		workers = append(workers, worker)
	}

	var mutex sync.Mutex
	var nextIteration = 1
	var firstErr error
	var done sync.WaitGroup
	done.Add(workerCount)
	for index, worker := range workers {
		go func(id int, worker integrationWorker) {
			defer done.Done()
			for {
				mutex.Lock()
				if firstErr != nil || nextIteration > options.count {
					mutex.Unlock()
					return
				}
				var iteration = nextIteration
				nextIteration++
				mutex.Unlock()

				var err = runIntegrationIteration(root, options, id, iteration, worker.env, execute)
				if err != nil {
					mutex.Lock()
					if firstErr == nil {
						firstErr = err
						_, _ = fmt.Fprintf(output, "[worker=%d round=%d/%d] failure; draining in-flight rounds\n", id, iteration, options.count)
					}
					mutex.Unlock()
					return
				}
			}
		}(index+1, worker)
	}
	done.Wait()
	return firstErr
}

func runIntegrationIteration(root string, options integrationTestOptions, worker, iteration int, env []string, execute func(integrationCommand) error) error {
	for _, pkg := range []string{"./amps", "./tools/fakeamps"} {
		var args = []string{"test", "-count=1"}
		if options.race {
			args = append(args, "-race")
		}
		args = append(args, pkg, "-run", "Integration", "-shuffle="+options.shuffle)
		var testEnv []string
		if pkg == "./amps" {
			testEnv = env
		}
		var command = integrationCommand{worker: worker, iteration: iteration, total: options.count, step: pkg, dir: root, env: testEnv, name: "go", args: args}
		if err := execute(command); err != nil {
			return fmt.Errorf("%s integration iteration %d/%d: %w", pkg, iteration, options.count, err)
		}
	}
	return nil
}

func runLoggedCommand(output io.Writer, command integrationCommand, execute func(string, []string, string, ...string) error) error {
	_, _ = fmt.Fprintf(output, "[worker=%d round=%d/%d step=%s] start: %s %s\n", command.worker, command.iteration, command.total, command.step, command.name, strings.Join(command.args, " "))
	var started = time.Now()
	var err = execute(command.dir, command.env, command.name, command.args...)
	var status = "ok"
	if err != nil {
		status = "failed"
	}
	_, _ = fmt.Fprintf(output, "[worker=%d round=%d/%d step=%s] %s duration=%s\n", command.worker, command.iteration, command.total, command.step, status, time.Since(started).Round(time.Millisecond))
	return err
}
