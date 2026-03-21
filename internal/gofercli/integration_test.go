package gofercli

import (
	"archive/zip"
	"bytes"
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

type builtFakeamps struct {
	path   string
	tmpDir string
}

type broker struct {
	addr string
	uri  string
	cmd  *exec.Cmd
}

var (
	fakeampsBuildOnce sync.Once
	fakeampsBuildErr  error
	fakeampsBinary    builtFakeamps
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

func ensureFakeampsBinary(t *testing.T) builtFakeamps {
	t.Helper()

	fakeampsBuildOnce.Do(func() {
		var root = repoRoot(t)
		var tempDir, err = os.MkdirTemp("", "gofercli-fakeamps-*")
		if err != nil {
			fakeampsBuildErr = err
			return
		}

		var ext string
		if runtime.GOOS == "windows" {
			ext = ".exe"
		}

		fakeampsBinary = builtFakeamps{
			path:   filepath.Join(tempDir, "fakeamps"+ext),
			tmpDir: tempDir,
		}

		var build = exec.Command("go", "build", "-o", fakeampsBinary.path, "./tools/fakeamps")
		build.Dir = root
		if out, err := build.CombinedOutput(); err != nil {
			fakeampsBuildErr = fmt.Errorf("build fakeamps: %w\n%s", err, out)
		}
	})

	if fakeampsBuildErr != nil {
		t.Fatal(fakeampsBuildErr)
	}
	return fakeampsBinary
}

func startBroker(t *testing.T) *broker {
	t.Helper()
	return startBrokerWithArgs(t)
}

func startBrokerWithArgs(t *testing.T, extraArgs ...string) *broker {
	t.Helper()

	var built = ensureFakeampsBinary(t)
	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var addr = listener.Addr().String()
	_ = listener.Close()

	var args = []string{"-addr", addr}
	args = append(args, extraArgs...)
	var cmd = exec.Command(built.path, args...)
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

	var broker = &broker{
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

func runMain(t *testing.T, stdin string, env []string, args ...string) (string, string, int) {
	t.Helper()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var code = Main(args, strings.NewReader(stdin), &stdout, &stderr, env)
	return stdout.String(), stderr.String(), code
}

func writeTempFile(t *testing.T, name string, data []byte) string {
	t.Helper()

	var path = filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func writeTempZIP(t *testing.T, name string, files map[string]string) string {
	t.Helper()

	var buffer bytes.Buffer
	var archive = zip.NewWriter(&buffer)
	for fileName, body := range files {
		var handle, err = archive.Create(fileName)
		if err != nil {
			t.Fatalf("zip create %s: %v", fileName, err)
		}
		if _, err := io.WriteString(handle, body); err != nil {
			t.Fatalf("zip write %s: %v", fileName, err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return writeTempFile(t, name, buffer.Bytes())
}

func TestIntegrationMainHelpAndUnknownCommand(t *testing.T) {
	var stdout, stderr, code = runMain(t, "", nil, "help")
	if code != 0 {
		t.Fatalf("help exit code = %d, stderr = %q", code, stderr)
	}
	if !strings.Contains(stdout, "spark-compatible") {
		t.Fatalf("help stdout = %q", stdout)
	}

	_, stderr, code = runMain(t, "", nil, "unknown")
	if code == 0 {
		t.Fatalf("unknown command should fail")
	}
	if !strings.Contains(stderr, "unknown command") {
		t.Fatalf("stderr = %q", stderr)
	}

	stdout, stderr, code = runMain(t, "", nil, "help", "publish")
	if code != 0 {
		t.Fatalf("help publish exit code = %d, stderr = %q", code, stderr)
	}
	if !strings.Contains(stdout, "gofer publish") {
		t.Fatalf("help publish stdout = %q", stdout)
	}
}

func TestIntegrationMainPingAndUnsupportedScheme(t *testing.T) {
	var broker = startBroker(t)

	var stdout, stderr, code = runMain(t, "", nil,
		"ping",
		"-server", broker.addr,
		"-type", "json",
	)
	if code != 0 {
		t.Fatalf("ping exit code = %d, stderr = %q", code, stderr)
	}
	if !strings.Contains(stdout, "Successfully connected to "+broker.uri) {
		t.Fatalf("stdout = %q", stdout)
	}

	stdout, stderr, code = runMain(t, "", nil,
		"ping",
		"-server", broker.addr,
		"-secure", "false",
	)
	if code != 0 {
		t.Fatalf("ping with secure=false exit code = %d, stderr = %q", code, stderr)
	}
	if !strings.Contains(stdout, "Successfully connected to "+broker.uri) {
		t.Fatalf("stdout = %q", stdout)
	}

	_, stderr, code = runMain(t, "", nil,
		"ping",
		"-server", broker.addr,
		"-urischeme", "wss",
	)
	if code == 0 {
		t.Fatalf("unsupported scheme should fail")
	}
	if !strings.Contains(stderr, `unsupported URI scheme "wss"`) {
		t.Fatalf("stderr = %q", stderr)
	}
}

func TestIntegrationMainPublishSOWAndDeleteFlows(t *testing.T) {
	var broker = startBroker(t)
	var input = writeTempFile(t, "orders.txt", []byte("{\"id\":1}\n{\"id\":2}\n"))

	_, stderr, code := runMain(t, "", nil,
		"publish",
		"-server", broker.uri,
		"-topic", "orders.flow",
		"-file", input,
	)
	if code != 0 {
		t.Fatalf("publish exit code = %d, stderr = %q", code, stderr)
	}

	var stdout string
	stdout, stderr, code = runMain(t, "", nil,
		"sow",
		"-server", broker.uri,
		"-topic", "orders.flow",
		"-format", "{topic}|{data}",
	)
	if code != 0 {
		t.Fatalf("sow exit code = %d, stderr = %q", code, stderr)
	}
	if !strings.Contains(stdout, `orders.flow|{"id":1}`) {
		t.Fatalf("formatted sow stdout = %q", stdout)
	}
	if !strings.Contains(stdout, "Total messages received: 2") {
		t.Fatalf("sow summary stdout = %q", stdout)
	}

	var deletePayload = writeTempFile(t, "delete.txt", []byte("{\"id\":1}"))
	stdout, stderr, code = runMain(t, "", nil,
		"sow_delete",
		"-server", broker.uri,
		"-topic", "orders.flow",
		"-file", deletePayload,
	)
	if code != 0 {
		t.Fatalf("sow_delete exit code = %d, stderr = %q", code, stderr)
	}
	if !strings.Contains(stdout, "Deleted 1 records") {
		t.Fatalf("sow_delete stdout = %q", stdout)
	}
}

func TestIntegrationMainCommandHelpFlagsAndDeltaPublish(t *testing.T) {
	var commands = []string{"ping", "publish", "subscribe", "sow", "sow_and_subscribe", "sow_delete"}
	for _, command := range commands {
		var stdout, stderr, code = runMain(t, "", nil, command, "-help")
		if code != 0 {
			t.Fatalf("%s -help exit code = %d, stderr = %q", command, code, stderr)
		}
		if !strings.Contains(stdout, "gofer "+command) {
			t.Fatalf("%s -help stdout = %q", command, stdout)
		}
	}

	var broker = startBroker(t)
	_, stderr, code := runMain(t, "", nil,
		"publish",
		"-server", broker.uri,
		"-topic", "orders.delta",
		"-delta",
		"-data", `{"id":9,"delta":true}`,
	)
	if code != 0 {
		t.Fatalf("delta publish exit code = %d, stderr = %q", code, stderr)
	}

	var stdout string
	stdout, stderr, code = runMain(t, "", nil,
		"sow",
		"-server", broker.uri,
		"-topic", "orders.delta",
	)
	if code != 0 {
		t.Fatalf("delta sow exit code = %d, stderr = %q", code, stderr)
	}
	if !strings.Contains(stdout, `"delta":true`) {
		t.Fatalf("delta sow stdout = %q", stdout)
	}
}

func TestIntegrationMainCopyAndZIPPublish(t *testing.T) {
	var source = startBroker(t)
	var target = startBroker(t)
	var archive = writeTempZIP(t, "orders.zip", map[string]string{
		"0001.json": "{\"id\":1,\"copy\":true}",
		"0002.json": "{\"id\":2,\"copy\":true}",
	})

	_, stderr, code := runMain(t, "", nil,
		"publish",
		"-server", source.uri,
		"-copy", target.uri,
		"-topic", "orders.copy",
		"-file", archive,
	)
	if code != 0 {
		t.Fatalf("publish exit code = %d, stderr = %q", code, stderr)
	}

	var stdout string
	stdout, stderr, code = runMain(t, "", nil,
		"sow",
		"-server", target.uri,
		"-topic", "orders.copy",
	)
	if code != 0 {
		t.Fatalf("copy sow exit code = %d, stderr = %q", code, stderr)
	}
	if !strings.Contains(stdout, "\"id\":1") || !strings.Contains(stdout, "\"id\":2") {
		t.Fatalf("copy sow stdout = %q", stdout)
	}
}

func TestIntegrationMainSubscribeQueueAckBacklog(t *testing.T) {
	var broker = startBroker(t)
	var done = make(chan struct {
		stdout string
		stderr string
		code   int
	}, 1)

	go func() {
		var stdout, stderr, code = runMain(t, "", nil,
			"subscribe",
			"-server", broker.uri,
			"-topic", "queue://orders.sub",
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
		_, stderr, code := runMain(t, "", nil,
			"publish",
			"-server", broker.uri,
			"-topic", "queue://orders.sub",
			"-data", payload,
		)
		if code != 0 {
			t.Fatalf("publish payload %s failed: code=%d stderr=%q", payload, code, stderr)
		}
	}

	select {
	case result := <-done:
		if result.code != 0 {
			t.Fatalf("subscribe exit code = %d, stderr = %q", result.code, result.stderr)
		}
		if !strings.Contains(result.stdout, `"id":1`) || !strings.Contains(result.stdout, `"id":2`) {
			t.Fatalf("subscribe stdout = %q", result.stdout)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("subscribe timed out")
	}
}

func TestIntegrationMainSubscribeReportsAckFailure(t *testing.T) {
	var broker = startBrokerWithArgs(t, "-auth", "alice:secret")

	_, stderr, code := runMain(t, "", nil,
		"subscribe",
		"-server", "tcp://alice:secret@"+broker.addr+"/amps/json",
		"-topic", "orders.denied",
		"-n", "1",
		"-timeout", "1500ms",
	)
	if code == 0 {
		t.Fatalf("subscribe should fail when broker returns failed ack")
	}
	var lower = strings.ToLower(stderr)
	if !strings.Contains(lower, "not entitled") && !strings.Contains(lower, "notentitlederror") {
		t.Fatalf("stderr = %q", stderr)
	}
}

func TestIntegrationMainSOWAndSubscribeAndFormatFailure(t *testing.T) {
	var broker = startBroker(t)

	_, stderr, code := runMain(t, "", nil,
		"publish",
		"-server", broker.uri,
		"-topic", "orders.sowsub",
		"-data", `{"id":1,"seed":true}`,
	)
	if code != 0 {
		t.Fatalf("seed publish exit code = %d, stderr = %q", code, stderr)
	}

	var done = make(chan struct {
		stdout string
		stderr string
		code   int
	}, 1)
	go func() {
		var stdout, stderr, code = runMain(t, "", nil,
			"sow_and_subscribe",
			"-server", broker.uri,
			"-topic", "orders.sowsub",
			"-n", "2",
		)
		done <- struct {
			stdout string
			stderr string
			code   int
		}{stdout: stdout, stderr: stderr, code: code}
	}()

	time.Sleep(750 * time.Millisecond)

	_, stderr, code = runMain(t, "", nil,
		"publish",
		"-server", broker.uri,
		"-topic", "orders.sowsub",
		"-data", `{"id":2,"live":true}`,
	)
	if code != 0 {
		t.Fatalf("live publish exit code = %d, stderr = %q", code, stderr)
	}

	select {
	case result := <-done:
		if result.code != 0 {
			t.Fatalf("sow_and_subscribe exit code = %d, stderr = %q", result.code, result.stderr)
		}
		if !strings.Contains(result.stdout, `"id":1`) || !strings.Contains(result.stdout, `"id":2`) {
			t.Fatalf("sow_and_subscribe stdout = %q", result.stdout)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("sow_and_subscribe timed out")
	}

	_, stderr, code = runMain(t, "", nil,
		"subscribe",
		"-help",
	)
	if code != 0 {
		t.Fatalf("subscribe -help exit code = %d, stderr = %q", code, stderr)
	}

	var invalidDone = make(chan struct {
		stderr string
		code   int
	}, 1)
	go func() {
		_, invalidStderr, invalidCode := runMain(t, "", nil,
			"subscribe",
			"-server", broker.uri,
			"-topic", "orders.sowsub",
			"-format", "{unknown}",
			"-n", "1",
		)
		invalidDone <- struct {
			stderr string
			code   int
		}{stderr: invalidStderr, code: invalidCode}
	}()

	time.Sleep(750 * time.Millisecond)

	_, stderr, code = runMain(t, "", nil,
		"publish",
		"-server", broker.uri,
		"-topic", "orders.sowsub",
		"-data", `{"id":3,"invalid":true}`,
	)
	if code != 0 {
		t.Fatalf("invalid-format publish exit code = %d, stderr = %q", code, stderr)
	}

	select {
	case result := <-invalidDone:
		if result.code == 0 {
			t.Fatalf("subscribe with invalid format should fail")
		}
		if !strings.Contains(result.stderr, "unsupported format token") {
			t.Fatalf("stderr = %q", result.stderr)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("invalid format subscribe timed out")
	}
}
