// Package main implements gofer — a cross-platform CLI for interacting with
// AMPS instances. It provides feature parity with 60east's official spark
// utility but compiles to a single native binary with zero external
// dependencies beyond the amps-client-go package.
package main

import (
	"os"

	"github.com/Thejuampi/amps-client-go/internal/gofercli"
)

func main() {
	os.Exit(gofercli.Main(os.Args[1:], os.Stdin, os.Stdout, os.Stderr, os.Environ()))
}
