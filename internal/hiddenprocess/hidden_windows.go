package hiddenprocess

import (
	"os/exec"
	"syscall"
)

const (
	createNoWindow   uint32 = 0x08000000
	createNewConsole uint32 = 0x00000010
	detachedProcess  uint32 = 0x00000008
)

// HideConsole configures a command to run without opening a console window.
func HideConsole(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.HideWindow = true
	cmd.SysProcAttr.CreationFlags = (cmd.SysProcAttr.CreationFlags &^ (createNewConsole | detachedProcess)) | createNoWindow
}
