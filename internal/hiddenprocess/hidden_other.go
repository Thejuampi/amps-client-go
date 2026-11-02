//go:build !windows

package hiddenprocess

import "os/exec"

// HideConsole configures a command to run without opening a console window.
func HideConsole(cmd *exec.Cmd) {}
