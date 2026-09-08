package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type violation struct {
	Path    string
	Line    int
	Message string
}

var externalBenchmarkClaimPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\b(?:go|this client|client)\s+(?:vs\.?|versus)\s+(?:the\s+)?(?:official\s+)?(?:c|c\+\+)\s*(?:client)?\b`),
	regexp.MustCompile(`(?i)\boutperform(?:s|ed|ing)?\b.{0,80}\bofficial\b`),
	regexp.MustCompile(`(?i)\bfaster than\b.{0,80}\bofficial\b`),
	regexp.MustCompile(`(?i)\bc\s+baselines?\b.{0,80}\bofficial\b`),
	regexp.MustCompile(`(?i)\bcomparison\b.{0,80}\bofficial\b.{0,80}\bbenchmarks?\b`),
	regexp.MustCompile(`(?i)\b(?:go[- ]vs[- ]c|c[- ]vs[- ]go)\b.{0,80}\b(?:benchmark|baseline|comparison|result)s?\b`),
}

var externalResultArtifactPatterns = []string{
	"tools/perf_tail_c_*.json",
	"tools/perf_side_by_side_*.json",
	"tools/perf_side_by_side_*.md",
}

var externalResultIdentityPattern = regexp.MustCompile(`(?i)(?:official[_ -]?(?:c(?:\+\+)?|go)|\bgo\s*(?:vs\.?|versus|-vs-)\s*c(?:\+\+)?\b|\bc(?:\+\+)?\s*(?:vs\.?|versus|-vs-)\s*go\b)`)
var benchmarkResultMetricPattern = regexp.MustCompile(`(?i)(?:p(?:50|95|99)[_ /-]?(?:ns(?:/op|_op)?)?|delta_(?:go_vs_c|c_vs_go)|winner(?:_p95)?|\bns/op\b|\bops/sec\b)`)

var resultTextExtensions = map[string]struct{}{
	".bench": {},
	".csv":   {},
	".json":  {},
	".md":    {},
	".out":   {},
	".tsv":   {},
	".txt":   {},
}

func main() {
	var root = flag.String("root", ".", "repository root to scan")
	flag.Parse()

	var violations, err = scanRepository(*root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "publication check failed: %v\n", err)
		os.Exit(1)
	}
	if len(violations) == 0 {
		fmt.Println("publication check: PASS")
		return
	}

	for _, item := range violations {
		if item.Line > 0 {
			fmt.Fprintf(os.Stderr, "%s:%d: %s\n", item.Path, item.Line, item.Message)
			continue
		}
		fmt.Fprintf(os.Stderr, "%s: %s\n", item.Path, item.Message)
	}
	fmt.Fprintf(os.Stderr, "publication check: FAIL (%d violation(s))\n", len(violations))
	os.Exit(1)
}

func scanRepository(root string) ([]violation, error) {
	var info, err = os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("inspect root: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("root is not a directory: %s", root)
	}

	var violations []violation
	for _, pattern := range externalResultArtifactPatterns {
		var matches, globErr = filepath.Glob(filepath.Join(root, filepath.FromSlash(pattern)))
		if globErr != nil {
			return nil, fmt.Errorf("expand artifact pattern %q: %w", pattern, globErr)
		}
		for _, match := range matches {
			var relativePath, relativeErr = filepath.Rel(root, match)
			if relativeErr != nil {
				return nil, fmt.Errorf("resolve artifact path %q: %w", match, relativeErr)
			}
			violations = append(violations, violation{
				Path:    filepath.ToSlash(relativePath),
				Message: "published external-client benchmark result artifact",
			})
		}
	}

	var walkErr = filepath.WalkDir(root, func(path string, entry fs.DirEntry, entryErr error) error {
		if entryErr != nil {
			return entryErr
		}
		if entry.IsDir() {
			if path == root {
				return nil
			}
			var relativePath, relativeErr = filepath.Rel(root, path)
			if relativeErr != nil {
				return relativeErr
			}
			if relativePath == ".git" || relativePath == ".tmp" {
				return filepath.SkipDir
			}
			return nil
		}

		var extension = strings.ToLower(filepath.Ext(path))
		if extension == ".md" {
			var fileViolations, scanErr = scanPublishedText(root, path)
			if scanErr != nil {
				return scanErr
			}
			violations = append(violations, fileViolations...)
		}
		if _, shouldScan := resultTextExtensions[extension]; shouldScan {
			var fileViolation, scanErr = scanExternalResultText(root, path)
			if scanErr != nil {
				return scanErr
			}
			if fileViolation != nil {
				violations = append(violations, *fileViolation)
			}
		}
		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("scan repository content: %w", walkErr)
	}

	sort.Slice(violations, func(left int, right int) bool {
		if violations[left].Path == violations[right].Path {
			return violations[left].Line < violations[right].Line
		}
		return violations[left].Path < violations[right].Path
	})
	return violations, nil
}

func scanPublishedText(root string, path string) ([]violation, error) {
	// #nosec G304 -- path comes from the repository walk rooted by scanRepository.
	var file, err = os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() {
		_ = file.Close()
	}()

	var relativePath, relativeErr = filepath.Rel(root, path)
	if relativeErr != nil {
		return nil, fmt.Errorf("resolve documentation path %q: %w", path, relativeErr)
	}

	var violations []violation
	var scanner = bufio.NewScanner(file)
	var lineNumber int
	for scanner.Scan() {
		lineNumber++
		var line = scanner.Text()
		for _, pattern := range externalBenchmarkClaimPatterns {
			if pattern.MatchString(line) {
				violations = append(violations, violation{
					Path:    filepath.ToSlash(relativePath),
					Line:    lineNumber,
					Message: "published external-client benchmark comparison",
				})
				break
			}
		}
	}
	if err = scanner.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return violations, nil
}

func scanExternalResultText(root string, path string) (*violation, error) {
	// #nosec G304 -- path comes from the repository walk rooted by scanRepository.
	var file, err = os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() {
		_ = file.Close()
	}()

	var hasExternalIdentity bool
	var hasResultMetric bool
	var scanner = bufio.NewScanner(file)
	for scanner.Scan() {
		var line = scanner.Text()
		hasExternalIdentity = hasExternalIdentity || externalResultIdentityPattern.MatchString(line)
		hasResultMetric = hasResultMetric || benchmarkResultMetricPattern.MatchString(line)
		if hasExternalIdentity && hasResultMetric {
			var relativePath, relativeErr = filepath.Rel(root, path)
			if relativeErr != nil {
				return nil, fmt.Errorf("resolve result path %q: %w", path, relativeErr)
			}
			return &violation{
				Path:    filepath.ToSlash(relativePath),
				Message: "published external-client benchmark result content",
			}, nil
		}
	}
	if err = scanner.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return nil, nil
}
