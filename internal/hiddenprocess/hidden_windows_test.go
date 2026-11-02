package hiddenprocess

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestHideConsoleWindowsConfiguration(t *testing.T) {
	const noWindow uint32 = 0x08000000
	const newConsole uint32 = 0x00000010
	const detachedProcess uint32 = 0x00000008

	t.Run("new attributes", func(t *testing.T) {
		var cmd = &exec.Cmd{}
		HideConsole(cmd)
		if cmd.SysProcAttr == nil || !cmd.SysProcAttr.HideWindow || cmd.SysProcAttr.CreationFlags != noWindow {
			t.Fatalf("attributes = %+v, want HideWindow and CREATE_NO_WINDOW", cmd.SysProcAttr)
		}
	})

	for _, test := range []struct {
		name  string
		flags uint32
	}{
		{name: "existing flags", flags: 0},
		{name: "new console", flags: newConsole},
		{name: "detached process", flags: detachedProcess},
		{name: "both conflicts", flags: newConsole | detachedProcess},
	} {
		t.Run(test.name, func(t *testing.T) {
			var attrs = &syscall.SysProcAttr{
				CmdLine:                    "preserved command line",
				CreationFlags:              syscall.CREATE_NEW_PROCESS_GROUP | test.flags,
				Token:                      syscall.Token(123),
				NoInheritHandles:           true,
				AdditionalInheritedHandles: []syscall.Handle{42, 43},
			}
			var expected = *attrs
			expected.HideWindow = true
			expected.CreationFlags = syscall.CREATE_NEW_PROCESS_GROUP | noWindow
			var cmd = &exec.Cmd{SysProcAttr: attrs}
			HideConsole(cmd)
			HideConsole(cmd)
			if cmd.SysProcAttr != attrs || !reflect.DeepEqual(*cmd.SysProcAttr, expected) {
				t.Fatalf("attributes = %+v, want %+v with original pointer", cmd.SysProcAttr, expected)
			}
		})
	}
}

func TestHiddenProcessHasNoConsole(t *testing.T) {
	if os.Getenv("HIDDENPROCESS_TEST_CHILD") == "1" {
		requireNativeProbeExitOptions()
		var window, _, _ = syscall.NewLazyDLL("kernel32.dll").NewProc("GetConsoleWindow").Call()
		_, _ = fmt.Fprint(os.Stdout, window)
		os.Exit(0)
	}
	var executable, err = os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var cmd = exec.CommandContext(ctx, executable, "-test.run=^TestHiddenProcessHasNoConsole$")
	cmd.WaitDelay = time.Second
	cmd.Env = nativeProbeEnvironment("HIDDENPROCESS_TEST_CHILD=1")
	HideConsole(cmd)
	var output []byte
	output, err = cmd.CombinedOutput()
	if err != nil || string(output) != "0" {
		t.Fatalf("child output = %q, error = %v; want console handle 0", output, err)
	}
}

func TestHiddenProcessGrandchildHasNoConsole(t *testing.T) {
	var mode = os.Getenv("HIDDENPROCESS_TEST_GRANDCHILD")
	if mode == "grandchild" {
		requireNativeProbeExitOptions()
		var window, _, _ = syscall.NewLazyDLL("kernel32.dll").NewProc("GetConsoleWindow").Call()
		_, _ = fmt.Fprint(os.Stdout, window)
		os.Exit(0)
	}
	var executable, err = os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var cmd = exec.CommandContext(ctx, executable, "-test.run=^TestHiddenProcessGrandchildHasNoConsole$")
	cmd.WaitDelay = time.Second
	if mode == "child" {
		// Keep the probe invisible even if Windows creates a new console.
		cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
		cmd.Env = nativeProbeEnvironment("HIDDENPROCESS_TEST_GRANDCHILD=grandchild")
		var output []byte
		output, err = cmd.CombinedOutput()
		if err != nil {
			os.Exit(1)
		}
		_, _ = fmt.Fprint(os.Stdout, string(output))
		os.Exit(0)
	}
	cmd.Env = nativeProbeEnvironment("HIDDENPROCESS_TEST_GRANDCHILD=child")
	HideConsole(cmd)
	var output []byte
	output, err = cmd.CombinedOutput()
	if err != nil || string(output) != "0" {
		t.Fatalf("grandchild output = %q, error = %v; want console handle 0", output, err)
	}
}

func nativeProbeEnvironment(marker string) []string {
	// These isolated probes only query a native handle and exit; there is no
	// concurrent Go work that needs the race detector's exit grace period.
	return append(os.Environ(), marker, "GORACE="+os.Getenv("GORACE")+" atexit_sleep_ms=0")
}

func TestNativeProbeEnvironmentPreservesOptions(t *testing.T) {
	const raceOptions = "history_size=4 atexit_sleep_ms=2000"
	t.Setenv("GORACE", raceOptions)
	t.Setenv("HIDDENPROCESS_ENV_PRESERVED", "preserved")
	var environment = map[string]string{}
	for _, entry := range nativeProbeEnvironment("HIDDENPROCESS_TEST_CHILD=1") {
		var key, value, _ = strings.Cut(entry, "=")
		environment[key] = value
	}
	if environment["GORACE"] != raceOptions+" atexit_sleep_ms=0" || environment["HIDDENPROCESS_ENV_PRESERVED"] != "preserved" || environment["HIDDENPROCESS_TEST_CHILD"] != "1" {
		t.Fatalf("probe options were not preserved: GORACE=%q, inherited=%q, marker=%q", environment["GORACE"], environment["HIDDENPROCESS_ENV_PRESERVED"], environment["HIDDENPROCESS_TEST_CHILD"])
	}
}

func requireNativeProbeExitOptions() {
	var options = strings.Fields(os.Getenv("GORACE"))
	if len(options) == 0 || options[len(options)-1] != "atexit_sleep_ms=0" {
		_, _ = fmt.Fprintln(os.Stderr, "native console probe must disable its redundant race exit sleep")
		os.Exit(2)
	}
}
