package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func withToolchainEnv(base []string, toolchain string) []string {
	const key = "GOTOOLCHAIN="

	var env = make([]string, 0, len(base)+1)
	var replaced bool
	for _, entry := range base {
		if strings.HasPrefix(entry, key) {
			if !replaced {
				env = append(env, key+toolchain)
				replaced = true
			}
			continue
		}
		env = append(env, entry)
	}
	if !replaced {
		env = append(env, key+toolchain)
	}
	return env
}

func runWithToolchain(toolchain string, args []string) int {
	if toolchain == "" {
		fmt.Fprintln(os.Stderr, "withtoolchain: -toolchain is required")
		return 2
	}
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "withtoolchain: command is required")
		return 2
	}

	var cmd = exec.Command(args[0], args[1:]...)
	cmd.Env = withToolchainEnv(os.Environ(), toolchain)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode()
		}
		fmt.Fprintf(os.Stderr, "withtoolchain: %v\n", err)
		return 1
	}
	return 0
}

func main() {
	var toolchain = flag.String("toolchain", "", "GOTOOLCHAIN value for the wrapped command")
	flag.Parse()
	os.Exit(runWithToolchain(*toolchain, flag.Args()))
}
