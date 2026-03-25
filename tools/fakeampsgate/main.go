package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type fakeAMPSProcess struct {
	addr string
	cmd  *exec.Cmd
}

func main() {
	var err = run()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var workingDir, err = os.Getwd()
	if err != nil {
		return fmt.Errorf("get working directory: %w", err)
	}

	var root string
	root, err = repoRootFrom(workingDir)
	if err != nil {
		return err
	}

	var tempDir string
	tempDir, err = os.MkdirTemp("", "fakeampsgate-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	var binaryPath = filepath.Join(tempDir, "fakeamps"+executableExtension())
	err = runCommand(root, nil, "go", "build", "-o", binaryPath, "./tools/fakeamps")
	if err != nil {
		return fmt.Errorf("build fakeamps binary: %w", err)
	}

	var primaryAddr string
	primaryAddr, err = reserveLoopbackAddress()
	if err != nil {
		return err
	}

	var secondaryAddr string
	secondaryAddr, err = reserveLoopbackAddress()
	if err != nil {
		return err
	}

	var primary *fakeAMPSProcess
	primary, err = startFakeAMPSProcess(root, binaryPath, primaryAddr)
	if err != nil {
		return err
	}
	defer stopFakeAMPSProcess(primary)

	var secondary *fakeAMPSProcess
	secondary, err = startFakeAMPSProcess(root, binaryPath, secondaryAddr)
	if err != nil {
		return err
	}
	defer stopFakeAMPSProcess(secondary)

	var primaryURI = fakeAMPSURI(primary.addr)
	var secondaryURI = fakeAMPSURI(secondary.addr)
	var integrationEnv = append(
		os.Environ(),
		"AMPS_TEST_URI="+primaryURI,
		"AMPS_TEST_FAILOVER_URIS="+strings.Join([]string{primaryURI, secondaryURI}, ","),
	)

	err = runCommand(root, integrationEnv, "go", "test", "-count=1", "./amps", "-run", "Integration")
	if err != nil {
		return fmt.Errorf("amps integration tests: %w", err)
	}

	err = runCommand(root, nil, "go", "test", "-count=1", "./tools/fakeamps", "-run", "Integration")
	if err != nil {
		return fmt.Errorf("fakeamps integration tests: %w", err)
	}

	return nil
}

func repoRootFrom(start string) (string, error) {
	var dir = start
	for {
		var goModPath = filepath.Join(dir, "go.mod")
		var info os.FileInfo
		var err error
		info, err = os.Stat(goModPath)
		if err == nil && !info.IsDir() {
			return dir, nil
		}

		var parent = filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("could not find repository root from %s", start)
		}
		dir = parent
	}
}

func executableExtension() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}

func fakeAMPSURI(addr string) string {
	return "tcp://" + addr + "/amps/json"
}

func reserveLoopbackAddress() (string, error) {
	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("reserve loopback address: %w", err)
	}
	defer func() {
		_ = listener.Close()
	}()

	return listener.Addr().String(), nil
}

func startFakeAMPSProcess(root string, binaryPath string, addr string) (*fakeAMPSProcess, error) {
	var cmd = exec.Command(binaryPath, "-addr", addr, "-echo=true") // #nosec G204 -- binary path and arguments are constructed locally without shell expansion
	cmd.Dir = root
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	var err = cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("start fakeamps on %s: %w", addr, err)
	}

	err = waitForTCPReady(addr, 10*time.Second)
	if err != nil {
		stopFakeAMPSProcess(&fakeAMPSProcess{addr: addr, cmd: cmd})
		return nil, err
	}

	return &fakeAMPSProcess{addr: addr, cmd: cmd}, nil
}

func stopFakeAMPSProcess(process *fakeAMPSProcess) {
	if process == nil || process.cmd == nil || process.cmd.Process == nil {
		return
	}

	_ = process.cmd.Process.Kill()
	_ = process.cmd.Wait()
}

func waitForTCPReady(addr string, timeout time.Duration) error {
	var deadline = time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var conn net.Conn
		var err error
		conn, err = net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}

	return fmt.Errorf("fakeamps did not become ready at %s within %v", addr, timeout)
}

func runCommand(dir string, env []string, name string, args ...string) error {
	_, _ = fmt.Fprintf(os.Stdout, "+ %s %s\n", name, strings.Join(args, " "))

	var cmd = exec.Command(name, args...) // #nosec G204 -- arguments are passed directly without shell expansion
	cmd.Dir = dir
	if env != nil {
		cmd.Env = env
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	var err = cmd.Run()
	if err != nil {
		return fmt.Errorf("command failed: %s %s: %w", name, strings.Join(args, " "), err)
	}

	return nil
}
