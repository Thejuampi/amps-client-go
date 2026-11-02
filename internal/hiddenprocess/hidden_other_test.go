//go:build !windows

package hiddenprocess

import (
	"os/exec"
	"syscall"
	"testing"
)

func TestHideConsoleIsNoOp(t *testing.T) {
	var attrs = &syscall.SysProcAttr{}
	var cmd = &exec.Cmd{SysProcAttr: attrs}
	HideConsole(cmd)
	if cmd.SysProcAttr != attrs {
		t.Fatal("non-Windows process attributes were changed")
	}
	var empty = &exec.Cmd{}
	HideConsole(empty)
	if empty.SysProcAttr != nil {
		t.Fatal("non-Windows process attributes were added")
	}
}
