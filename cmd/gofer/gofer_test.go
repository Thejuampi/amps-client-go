package main

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

type builtBinaries struct {
	fakeamps string
	gofer    string
	tmpDir   string
}

type fakeBroker struct {
	addr string
	uri  string
	cmd  *exec.Cmd
}

var (
	buildOnce sync.Once
	buildErr  error
	binaries  builtBinaries
)

func repoRoot(t *testing.T) string {
	t.Helper()

	var dir, err = os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}
		var parent = filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find repo root from %s", dir)
		}
		dir = parent
	}
}

func ensureBinaries(t *testing.T) builtBinaries {
	t.Helper()

	buildOnce.Do(func() {
		var root = repoRoot(t)
		var tempDir, err = os.MkdirTemp("", "gofer-bin-*")
		if err != nil {
			buildErr = err
			return
		}

		var ext string
		if runtime.GOOS == "windows" {
			ext = ".exe"
		}

		binaries = builtBinaries{
			fakeamps: filepath.Join(tempDir, "fakeamps"+ext),
			gofer:    filepath.Join(tempDir, "gofer"+ext),
			tmpDir:   tempDir,
		}

		var buildFakeamps = exec.Command("go", "build", "-o", binaries.fakeamps, "./tools/fakeamps")
		buildFakeamps.Dir = root
		if out, err := buildFakeamps.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("build fakeamps: %w\n%s", err, out)
			return
		}

		var buildGofer = exec.Command("go", "build", "-o", binaries.gofer, "./cmd/gofer")
		buildGofer.Dir = root
		if out, err := buildGofer.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("build gofer: %w\n%s", err, out)
			return
		}
	})

	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return binaries
}

func startFakeBroker(t *testing.T) *fakeBroker {
	t.Helper()

	var built = ensureBinaries(t)
	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var addr = listener.Addr().String()
	_ = listener.Close()

	var cmd = exec.Command(built.fakeamps, "-addr", addr)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start fakeamps: %v", err)
	}

	var deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var conn, dialErr = net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	var broker = &fakeBroker{
		addr: addr,
		uri:  "tcp://" + addr + "/amps/json",
		cmd:  cmd,
	}
	t.Cleanup(func() {
		if broker.cmd != nil && broker.cmd.Process != nil {
			_ = broker.cmd.Process.Kill()
			_ = broker.cmd.Wait()
		}
	})
	return broker
}

func runGofer(t *testing.T, stdin string, env []string, args ...string) (string, string, int) {
	t.Helper()

	var built = ensureBinaries(t)
	var cmd = exec.Command(built.gofer, args...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	var err = cmd.Run()
	var exitCode int
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("exec gofer: %v", err)
		}
		exitCode = exitErr.ExitCode()
	}

	return stdout.String(), stderr.String(), exitCode
}

func writeFile(t *testing.T, name string, data []byte) string {
	t.Helper()

	var path = filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func writeZipFile(t *testing.T, name string, files map[string]string) string {
	t.Helper()

	var path = filepath.Join(t.TempDir(), name)
	var buffer bytes.Buffer
	var archive = zip.NewWriter(&buffer)
	for fileName, body := range files {
		var fileWriter, err = archive.Create(fileName)
		if err != nil {
			t.Fatalf("zip create %s: %v", fileName, err)
		}
		if _, err := io.WriteString(fileWriter, body); err != nil {
			t.Fatalf("zip write %s: %v", fileName, err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	if err := os.WriteFile(path, buffer.Bytes(), 0600); err != nil {
		t.Fatalf("write zip: %v", err)
	}
	return path
}

func TestPingUsesSparkStyleSuccessOutput(t *testing.T) {
	var broker = startFakeBroker(t)

	var stdout, stderr, code = runGofer(t, "", nil,
		"ping",
		"-server", broker.addr,
		"-type", "json",
	)
	if code != 0 {
		t.Fatalf("ping exit code = %d, stderr = %s", code, stderr)
	}
	var want = "Successfully connected to " + broker.uri
	if !strings.Contains(stdout, want) {
		t.Fatalf("stdout = %q, want substring %q", stdout, want)
	}
}

func TestPublishFromDelimitedFileAndSOW(t *testing.T) {
	var broker = startFakeBroker(t)
	var input = writeFile(t, "orders.txt", []byte("{\"id\":1}\n{\"id\":2}\n"))

	_, stderr, code := runGofer(t, "", nil,
		"publish",
		"-server", broker.uri,
		"-topic", "orders.file",
		"-file", input,
	)
	if code != 0 {
		t.Fatalf("publish exit code = %d, stderr = %s", code, stderr)
	}

	var stdout string
	stdout, stderr, code = runGofer(t, "", nil,
		"sow",
		"-server", broker.uri,
		"-topic", "orders.file",
	)
	if code != 0 {
		t.Fatalf("sow exit code = %d, stderr = %s", code, stderr)
	}
	if !strings.Contains(stdout, "\"id\":1") {
		t.Fatalf("sow missing first payload: %q", stdout)
	}
	if !strings.Contains(stdout, "\"id\":2") {
		t.Fatalf("sow missing second payload: %q", stdout)
	}
	if !strings.Contains(stdout, "Total messages received: 2") {
		t.Fatalf("sow missing summary: %q", stdout)
	}
}

func TestPublishFromZIPAndCopyToSecondaryBroker(t *testing.T) {
	var source = startFakeBroker(t)
	var copyTarget = startFakeBroker(t)
	var archive = writeZipFile(t, "orders.zip", map[string]string{
		"0001.json": "{\"id\":1,\"source\":\"zip\"}",
		"0002.json": "{\"id\":2,\"source\":\"zip\"}",
	})

	_, stderr, code := runGofer(t, "", nil,
		"publish",
		"-server", source.uri,
		"-copy", copyTarget.uri,
		"-topic", "orders.copy",
		"-file", archive,
	)
	if code != 0 {
		t.Fatalf("publish exit code = %d, stderr = %s", code, stderr)
	}

	var stdout string
	stdout, stderr, code = runGofer(t, "", nil,
		"sow",
		"-server", copyTarget.uri,
		"-topic", "orders.copy",
	)
	if code != 0 {
		t.Fatalf("copy target sow exit code = %d, stderr = %s", code, stderr)
	}
	if !strings.Contains(stdout, "\"id\":1") || !strings.Contains(stdout, "\"id\":2") {
		t.Fatalf("copy target sow output = %q", stdout)
	}
}

func TestSubscribeSupportsQueueAckAndBacklog(t *testing.T) {
	var broker = startFakeBroker(t)
	var done = make(chan struct {
		stdout string
		stderr string
		code   int
	}, 1)

	go func() {
		var stdout, stderr, code = runGofer(t, "", nil,
			"subscribe",
			"-server", broker.uri,
			"-topic", "queue://orders.queue",
			"-ack",
			"-backlog", "1",
			"-n", "2",
		)
		done <- struct {
			stdout string
			stderr string
			code   int
		}{stdout: stdout, stderr: stderr, code: code}
	}()

	time.Sleep(750 * time.Millisecond)

	for _, payload := range []string{`{"id":1}`, `{"id":2}`} {
		_, stderr, code := runGofer(t, "", nil,
			"publish",
			"-server", broker.uri,
			"-topic", "queue://orders.queue",
			"-data", payload,
		)
		if code != 0 {
			t.Fatalf("publish payload %s failed: code=%d stderr=%s", payload, code, stderr)
		}
	}

	select {
	case result := <-done:
		if result.code != 0 {
			t.Fatalf("subscribe exit code = %d, stderr = %s", result.code, result.stderr)
		}
		if !strings.Contains(result.stdout, "\"id\":1") || !strings.Contains(result.stdout, "\"id\":2") {
			t.Fatalf("subscribe stdout = %q", result.stdout)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("subscribe timed out")
	}
}

func TestSOWDeleteSupportsPayloadInput(t *testing.T) {
	var broker = startFakeBroker(t)

	_, stderr, code := runGofer(t, "", nil,
		"publish",
		"-server", broker.uri,
		"-topic", "orders.delete",
		"-data", `{"id":7,"delete":true}`,
	)
	if code != 0 {
		t.Fatalf("seed publish exit code = %d, stderr = %s", code, stderr)
	}

	var payloadFile = writeFile(t, "delete.json", []byte(`{"id":7}`))
	var stdout string
	stdout, stderr, code = runGofer(t, "", nil,
		"sow_delete",
		"-server", broker.uri,
		"-topic", "orders.delete",
		"-file", payloadFile,
	)
	if code != 0 {
		t.Fatalf("sow_delete exit code = %d, stderr = %s", code, stderr)
	}
	if !strings.Contains(stdout, "Deleted 1 records") {
		t.Fatalf("sow_delete stdout = %q", stdout)
	}
}
