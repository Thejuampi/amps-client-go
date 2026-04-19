package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

type coverage struct {
	covered int
	total   int
}

type coverageBlock struct {
	fileName   string
	statements int
	covered    bool
}

var pureFiles = []string{
	"amps/command.go",
	"amps/header.go",
	"amps/message.go",
	"amps/message_stream.go",
	"amps/message_router.go",
	"amps/composite_message_builder.go",
	"amps/composite_message_parser.go",
	"amps/fix_builder.go",
	"amps/fix_shredder.go",
	"amps/nvfix_builder.go",
	"amps/nvfix_shredder.go",
	"amps/errors.go",
	"amps/reconnect_strategy.go",
	"amps/server_chooser.go",
	"amps/store_codec.go",
	"amps/parity_types.go",
	"amps/parity_version.go",
	"amps/version_info.go",
	"amps/internal/bookmark/parser.go",
	"amps/internal/replay/sequencer.go",
	"amps/internal/testutil/fakes.go",
	"amps/cppcompat/types.go",
	"amps/cppcompat/fix.go",
	"amps/cppcompat/recovery.go",
	"amps/cppcompat/stores.go",
}

var pureFileThresholds = map[string]float64{
	"amps/header.go":         99.5,
	"amps/message_stream.go": 99.7,
}

var ioFiles = []string{
	"amps/client.go",
	"amps/client_parity_methods.go",
	"amps/client_cpp_full_methods.go",
	"amps/ha_client.go",
	"amps/subscription_manager.go",
	"amps/bookmark_store.go",
	"amps/publish_store.go",
	"amps/capi/capi.go",
	"amps/internal/wal/wal.go",
}

func parseProfile(path string) (_ map[string]coverage, err error) {
	file, err := os.Open(path) // #nosec G304 -- path is explicitly provided by local CI/operator input
	if err != nil {
		return nil, err
	}
	defer func() {
		closeErr := file.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	result := map[string]coverage{}
	blocks := map[string]coverageBlock{}
	scanner := bufio.NewScanner(file)
	first := true
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if first {
			first = false
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}

		fileRange := fields[0]
		statements, err := strconv.Atoi(fields[1])
		if err != nil {
			return nil, fmt.Errorf("invalid statement count in line %q: %w", line, err)
		}
		hitCount, err := strconv.Atoi(fields[2])
		if err != nil {
			return nil, fmt.Errorf("invalid hit count in line %q: %w", line, err)
		}

		parts := strings.SplitN(fileRange, ":", 2)
		if len(parts) != 2 {
			continue
		}
		fileName := parts[0]
		blockKey := fmt.Sprintf("%s|%s|%d", fileName, parts[1], statements)
		block := blocks[blockKey]
		if block.fileName == "" {
			block.fileName = fileName
			block.statements = statements
		}
		if hitCount > 0 {
			block.covered = true
		}
		blocks[blockKey] = block
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	for _, block := range blocks {
		entry := result[block.fileName]
		entry.total += block.statements
		if block.covered {
			entry.covered += block.statements
		}
		result[block.fileName] = entry
	}

	return result, nil
}

func findCoverage(files map[string]coverage, suffix string) (coverage, bool) {
	for fileName, cov := range files {
		if strings.HasSuffix(fileName, suffix) {
			return cov, true
		}
	}
	return coverage{}, false
}

func pct(c coverage) float64 {
	if c.total == 0 {
		return 0
	}
	return (float64(c.covered) * 100.0) / float64(c.total)
}

func main() {
	profilePath := flag.String("profile", "coverage.out", "path to go coverage profile")
	overallThreshold := flag.Float64("overall", 90.0, "minimum aggregate coverage percentage")
	ioThreshold := flag.Float64("io", 80.0, "minimum io file coverage percentage")
	flag.Parse()

	files, err := parseProfile(*profilePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "coverage gate failed reading profile: %v\n", err)
		os.Exit(1)
	}

	total := coverage{}
	for _, fileCov := range files {
		total.covered += fileCov.covered
		total.total += fileCov.total
	}
	overall := pct(total)

	failures := make([]string, 0)
	if overall+1e-9 < *overallThreshold {
		failures = append(failures, fmt.Sprintf("aggregate coverage %.1f%% is below %.1f%%", overall, *overallThreshold))
	}

	for _, fileName := range pureFiles {
		fileCov, ok := findCoverage(files, fileName)
		if !ok {
			failures = append(failures, fmt.Sprintf("pure file %s is missing from coverage profile", fileName))
			continue
		}
		requiredPct := 100.0
		if threshold, hasThreshold := pureFileThresholds[fileName]; hasThreshold {
			requiredPct = threshold
		}
		filePct := pct(fileCov)
		if filePct+1e-9 < requiredPct {
			failures = append(failures, fmt.Sprintf("pure file %s is %.1f%% (required %.1f%%)", fileName, filePct, requiredPct))
		}
	}

	for _, fileName := range ioFiles {
		fileCov, ok := findCoverage(files, fileName)
		if !ok {
			failures = append(failures, fmt.Sprintf("io file %s is missing from coverage profile", fileName))
			continue
		}
		filePct := pct(fileCov)
		if filePct+1e-9 < *ioThreshold {
			failures = append(failures, fmt.Sprintf("io file %s is %.1f%% (required %.1f%%)", fileName, filePct, *ioThreshold))
		}
	}

	sort.Strings(failures)

	fmt.Printf("aggregate: %.1f%% (%d/%d)\n", overall, total.covered, total.total)
	if len(failures) == 0 {
		fmt.Println("coverage gate: PASS")
		return
	}

	fmt.Println("coverage gate: FAIL")
	for _, failure := range failures {
		fmt.Printf("- %s\n", failure)
	}
	os.Exit(2)
}
