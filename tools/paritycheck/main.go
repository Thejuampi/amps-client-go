package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type manifestEntry struct {
	CPPSymbol string `json:"cpp_symbol"`
	GoTarget  string `json:"go_target"`
}

type manifestFile struct {
	SourceHeaders []string        `json:"source_headers"`
	Entries       []manifestEntry `json:"entries"`
}

type behaviorGap struct {
	ID           string `json:"id"`
	Domain       string `json:"domain"`
	Description  string `json:"description"`
	PassCriteria string `json:"pass_criteria"`
	Status       string `json:"status"`
}

type behaviorManifest struct {
	Version string        `json:"version"`
	Gaps    []behaviorGap `json:"gaps"`
}

type goTarget struct {
	pkg    string
	kind   string
	symbol string
}

func parseTarget(raw string) (goTarget, error) {
	parts := strings.Split(raw, ":")
	if len(parts) != 3 {
		return goTarget{}, fmt.Errorf("invalid go target: %s", raw)
	}
	return goTarget{pkg: parts[0], kind: parts[1], symbol: parts[2]}, nil
}

func packageDir(pkg string) (string, error) {
	switch pkg {
	case "amps":
		return "amps", nil
	case "amps/capi":
		return filepath.Join("amps", "capi"), nil
	case "amps/cppcompat":
		return filepath.Join("amps", "cppcompat"), nil
	default:
		return "", fmt.Errorf("unknown package: %s", pkg)
	}
}

func readPackageSource(pkg string) (string, error) {
	dir, err := packageDir(pkg)
	if err != nil {
		return "", err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	builder := strings.Builder{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		data, readErr := os.ReadFile(filepath.Join(dir, entry.Name()))
		if readErr != nil {
			return "", readErr
		}
		builder.Write(data)
		builder.WriteString("\n")
	}
	return builder.String(), nil
}

func symbolExists(source string, target goTarget) bool {
	switch target.kind {
	case "type":
		pattern := fmt.Sprintf(`(?m)^type\s+%s\b`, regexp.QuoteMeta(target.symbol))
		return regexp.MustCompile(pattern).FindStringIndex(source) != nil
	case "func":
		pattern := fmt.Sprintf(`(?m)^func\s+%s\s*\(`, regexp.QuoteMeta(target.symbol))
		return regexp.MustCompile(pattern).FindStringIndex(source) != nil
	case "method":
		parts := strings.Split(target.symbol, ".")
		if len(parts) != 2 {
			return false
		}
		receiver := regexp.QuoteMeta(parts[0])
		method := regexp.QuoteMeta(parts[1])
		pattern := fmt.Sprintf(`(?m)^func\s*\(\s*[^)]*\*?%s\s*\)\s*%s\s*\(`, receiver, method)
		return regexp.MustCompile(pattern).FindStringIndex(source) != nil
	default:
		return false
	}
}

func cppToken(symbol string) string {
	if strings.Contains(symbol, "::") {
		parts := strings.Split(symbol, "::")
		return strings.TrimSpace(parts[len(parts)-1])
	}
	return strings.TrimSpace(symbol)
}

func headerContainsSymbol(headerRoot string, headers []string, symbol string) bool {
	token := cppToken(symbol)
	if token == "" {
		return false
	}
	for _, header := range headers {
		path := filepath.Join(headerRoot, header)
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if strings.Contains(string(data), token) {
			return true
		}
	}
	return false
}

func main() {
	manifestPath := flag.String("manifest", filepath.Join("tools", "parity_manifest.json"), "path to parity manifest")
	behaviorManifestPath := flag.String("behavior-manifest", filepath.Join("tools", "parity_behavior_manifest.json"), "path to behavior parity manifest")
	headerRoot := flag.String("headers", filepath.Join("..", "amps-c++-client-5.3.5.1-Windows"), "path to C++ client root")
	flag.Parse()

	data, err := os.ReadFile(*manifestPath)
	if err != nil {
		fmt.Printf("manifest read failed: %v\n", err)
		os.Exit(1)
	}
	manifest := manifestFile{}
	if err = json.Unmarshal(data, &manifest); err != nil {
		fmt.Printf("manifest parse failed: %v\n", err)
		os.Exit(1)
	}

	sources := map[string]string{}
	pkgSet := map[string]struct{}{}
	for _, entry := range manifest.Entries {
		target, parseErr := parseTarget(entry.GoTarget)
		if parseErr != nil {
			fmt.Println(parseErr)
			os.Exit(1)
		}
		pkgSet[target.pkg] = struct{}{}
	}
	for pkg := range pkgSet {
		source, readErr := readPackageSource(pkg)
		if readErr != nil {
			fmt.Printf("package source read failed for %s: %v\n", pkg, readErr)
			os.Exit(1)
		}
		sources[pkg] = source
	}

	missingHeader := []string{}
	missingGo := []string{}
	for _, entry := range manifest.Entries {
		target, _ := parseTarget(entry.GoTarget)
		if !headerContainsSymbol(*headerRoot, manifest.SourceHeaders, entry.CPPSymbol) {
			missingHeader = append(missingHeader, entry.CPPSymbol)
		}
		source := sources[target.pkg]
		if !symbolExists(source, target) {
			missingGo = append(missingGo, fmt.Sprintf("%s -> %s", entry.CPPSymbol, entry.GoTarget))
		}
	}

	fmt.Printf("PARITY_ENTRIES=%d\n", len(manifest.Entries))
	fmt.Printf("MISSING_HEADER_SYMBOLS=%d\n", len(missingHeader))
	for _, item := range missingHeader {
		fmt.Printf("HEADER_MISSING %s\n", item)
	}
	fmt.Printf("MISSING_GO_SYMBOLS=%d\n", len(missingGo))
	for _, item := range missingGo {
		fmt.Printf("GO_MISSING %s\n", item)
	}

	behaviorData, err := os.ReadFile(*behaviorManifestPath)
	if err != nil {
		fmt.Printf("behavior manifest read failed: %v\n", err)
		os.Exit(1)
	}
	behavior := behaviorManifest{}
	if err = json.Unmarshal(behaviorData, &behavior); err != nil {
		fmt.Printf("behavior manifest parse failed: %v\n", err)
		os.Exit(1)
	}
	openGaps := []behaviorGap{}
	for _, gap := range behavior.Gaps {
		status := strings.ToLower(strings.TrimSpace(gap.Status))
		if status != "closed" {
			openGaps = append(openGaps, gap)
		}
	}
	fmt.Printf("BEHAVIOR_ENTRIES=%d\n", len(behavior.Gaps))
	fmt.Printf("OPEN_GAPS=%d\n", len(openGaps))
	for _, gap := range openGaps {
		id := strings.TrimSpace(gap.ID)
		if id == "" {
			id = strings.TrimSpace(gap.Domain)
		}
		if id == "" {
			id = "unknown-gap"
		}
		fmt.Printf("OPEN_GAP %s\n", id)
	}

	if len(missingHeader) > 0 || len(missingGo) > 0 || len(openGaps) > 0 {
		os.Exit(1)
	}
}
