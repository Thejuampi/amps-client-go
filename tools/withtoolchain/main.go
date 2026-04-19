package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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

func validateGoCommandArgs(args []string) ([]string, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("go subcommand is required")
	}

	if strings.EqualFold(filepath.Base(args[0]), "go") {
		args = args[1:]
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("go subcommand is required")
	}
	if strings.ContainsAny(args[0], `/\`) {
		return nil, fmt.Errorf("non-go executable %q is not allowed", args[0])
	}
	switch args[0] {
	case "build", "clean", "env", "fix", "fmt", "generate", "get", "install", "list", "mod", "run", "telemetry", "test", "tool", "version", "vet", "work":
		return args, nil
	default:
		return nil, fmt.Errorf("non-go executable %q is not allowed", args[0])
	}
}

func runWithToolchain(toolchain string, args []string) int {
	if toolchain == "" {
		fmt.Fprintln(os.Stderr, "withtoolchain: -toolchain is required")
		return 2
	}
	var goArgs, err = validateGoCommandArgs(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "withtoolchain: %v\n", err)
		return 2
	}

	var cmd = exec.Command("go", goArgs...) // #nosec G204 -- wrapper is intentionally restricted to the Go toolchain only.
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
