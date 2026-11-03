package main

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

type benchmarkEnvironment struct {
	GOOS    string `json:"goos"`
	GOARCH  string `json:"goarch"`
	Package string `json:"package"`
	CPU     string `json:"cpu"`
	Procs   int    `json:"procs"`
}

func parseBenchmarkRun(output string, names []string, samples, procs int) (benchmarkRun, error) {
	var run = benchmarkRun{Output: output, Measurements: make(map[string][]benchmarkResult)}
	if samples <= 0 || procs <= 0 {
		return run, fmt.Errorf("sample and worker counts must be positive")
	}
	var expected = make(map[string]bool, len(names))
	for _, name := range names {
		expected[name] = true
	}
	var metadata = map[string]*string{
		"goos:": &run.Environment.GOOS, "goarch:": &run.Environment.GOARCH,
		"pkg:": &run.Environment.Package, "cpu:": &run.Environment.CPU,
	}
	var scanner = bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		var line = strings.TrimSpace(scanner.Text())
		var metadataLine bool
		for prefix, target := range metadata {
			if value, found := strings.CutPrefix(line, prefix); found {
				if *target != "" {
					return run, fmt.Errorf("duplicate benchmark metadata %s", prefix)
				}
				*target = strings.TrimSpace(value)
				metadataLine = true
				break
			}
		}
		if metadataLine || !strings.HasPrefix(line, "Benchmark") {
			continue
		}
		var fields = strings.Fields(line)
		if len(fields) < 8 || len(fields)%2 != 0 {
			return run, fmt.Errorf("incomplete benchmark observation: %q", line)
		}
		var dash = strings.LastIndexByte(fields[0], '-')
		if dash < 0 {
			return run, fmt.Errorf("missing benchmark worker count: %q", fields[0])
		}
		var workers, workerErr = strconv.Atoi(fields[0][dash+1:])
		var name = fields[0][:dash]
		var iterations, iterationErr = strconv.ParseUint(fields[1], 10, 64)
		if workerErr != nil || workers != procs || iterationErr != nil || iterations == 0 || !expected[name] {
			return run, fmt.Errorf("unexpected benchmark identity, workers or iteration count: %q", line)
		}
		var result benchmarkResult
		result.N = iterations
		var seen = make(map[string]bool, 3)
		for index := 2; index < len(fields); index += 2 {
			var unit = fields[index+1]
			if unit != "ns/op" && unit != "B/op" && unit != "allocs/op" {
				continue
			}
			var value, err = strconv.ParseFloat(fields[index], 64)
			if err != nil || !finite(value) || value < 0 || seen[unit] {
				return run, fmt.Errorf("invalid or repeated %s metric: %q", unit, line)
			}
			seen[unit] = true
			switch unit {
			case "ns/op":
				result.NSOp = value
			case "B/op":
				result.BytesOp = value
			case "allocs/op":
				result.AllocsOp = value
			}
		}
		if len(seen) != 3 || !validMetric(result) {
			return run, fmt.Errorf("missing or invalid benchmark metric: %q", line)
		}
		run.Measurements[name] = append(run.Measurements[name], result)
	}
	if err := scanner.Err(); err != nil {
		return run, fmt.Errorf("benchmark output could not be read completely: %w", err)
	}
	for prefix, target := range metadata {
		if *target == "" {
			return run, fmt.Errorf("missing benchmark metadata %s", prefix)
		}
	}
	for _, name := range names {
		if got := len(run.Measurements[name]); got != samples {
			return run, fmt.Errorf("%s: got %d samples, require exactly %d", name, got, samples)
		}
	}
	run.Environment.Procs = procs
	run.Summary = summarizeBenchOutput(run.Measurements)
	return run, nil
}
