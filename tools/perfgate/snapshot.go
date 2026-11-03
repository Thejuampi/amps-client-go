package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Thejuampi/amps-client-go/internal/hiddenprocess"
)

type snapshotModule struct {
	Main    bool            `json:"Main"`
	Version string          `json:"Version"`
	Replace *snapshotModule `json:"Replace"`
}

type snapshotPackage struct {
	Dir          string          `json:"Dir"`
	ImportPath   string          `json:"ImportPath"`
	Module       *snapshotModule `json:"Module"`
	GoFiles      []string        `json:"GoFiles"`
	CgoFiles     []string        `json:"CgoFiles"`
	CFiles       []string        `json:"CFiles"`
	CXXFiles     []string        `json:"CXXFiles"`
	MFiles       []string        `json:"MFiles"`
	HFiles       []string        `json:"HFiles"`
	FFiles       []string        `json:"FFiles"`
	SFiles       []string        `json:"SFiles"`
	SwigFiles    []string        `json:"SwigFiles"`
	SwigCXXFiles []string        `json:"SwigCXXFiles"`
	SysoFiles    []string        `json:"SysoFiles"`
	EmbedFiles   []string        `json:"EmbedFiles"`
}

func prepareCandidateSnapshot(root, destination, toolchain string) (string, error) {
	var files, err = candidateSnapshotFiles(root, toolchain)
	if err != nil {
		return "", err
	}
	if err = os.Mkdir(destination, 0700); err != nil {
		return "", fmt.Errorf("create new candidate snapshot: %w", err)
	}
	return captureCandidateSnapshot(root, destination, files)
}

func candidateSnapshotDigest(root, toolchain string) (string, error) {
	var files, err = candidateSnapshotFiles(root, toolchain)
	if err != nil {
		return "", err
	}
	return captureCandidateSnapshot(root, "", files)
}

func candidateSnapshotFiles(root, toolchain string) ([]string, error) {
	var absoluteRoot, err = filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	absoluteRoot, err = filepath.EvalSymlinks(absoluteRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve candidate root: %w", err)
	}
	var ctx, cancel = context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var cmd = exec.CommandContext(ctx, "go", "list", "-mod=readonly", "-deps", "-json", "./amps") // #nosec G204 -- fixed Go command; toolchain environment is supplied by the caller.
	hiddenprocess.HideConsole(cmd)
	cmd.Dir = absoluteRoot
	cmd.Env = benchmarkEnv(toolchain)
	var output, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &stderr
	if err = cmd.Run(); err != nil {
		return nil, fmt.Errorf("list candidate dependencies: %w: %s", err, strings.TrimSpace(stderr.String()))
	}

	var names = make(map[string]bool)
	var mainPackages int
	var decoder = json.NewDecoder(&output)
	for {
		var pkg snapshotPackage
		if err = decoder.Decode(&pkg); errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode candidate dependencies: %w", err)
		}
		if pkg.Module == nil {
			continue
		}
		if pkg.Module.Replace != nil && pkg.Module.Replace.Version == "" {
			return nil, fmt.Errorf("local module replacement for %s cannot be snapshotted", pkg.ImportPath)
		}
		if !pkg.Module.Main {
			continue
		}
		mainPackages++
		if err = addSnapshotPackageFiles(absoluteRoot, pkg, names); err != nil {
			return nil, fmt.Errorf("candidate package %s: %w", pkg.ImportPath, err)
		}
	}
	if mainPackages == 0 {
		return nil, fmt.Errorf("candidate dependency list has no main-module packages")
	}
	for _, name := range []string{"go.mod", "go.sum", benchmarkFile} {
		if name == "go.sum" {
			if _, statErr := os.Lstat(filepath.Join(absoluteRoot, name)); errors.Is(statErr, os.ErrNotExist) {
				continue
			} else if statErr != nil {
				return nil, statErr
			}
		}
		var relative string
		relative, err = snapshotRelativeFile(absoluteRoot, absoluteRoot, name)
		if err != nil {
			return nil, fmt.Errorf("candidate input %s: %w", name, err)
		}
		names[relative] = true
	}
	var files = make([]string, 0, len(names))
	for name := range names {
		files = append(files, name)
	}
	sort.Strings(files)
	return files, nil
}

func addSnapshotPackageFiles(root string, pkg snapshotPackage, names map[string]bool) error {
	for _, group := range [][]string{pkg.GoFiles, pkg.CgoFiles, pkg.CFiles, pkg.CXXFiles, pkg.MFiles, pkg.HFiles, pkg.FFiles, pkg.SFiles, pkg.SwigFiles, pkg.SwigCXXFiles, pkg.SysoFiles, pkg.EmbedFiles} {
		for _, name := range group {
			var relative, err = snapshotRelativeFile(root, pkg.Dir, name)
			if err != nil {
				return err
			}
			names[relative] = true
		}
	}
	return nil
}

func snapshotRelativeFile(root, directory, name string) (string, error) {
	if !filepath.IsLocal(name) || !filepath.IsAbs(directory) {
		return "", fmt.Errorf("nonlocal source %q in %q", name, directory)
	}
	var absoluteRoot, err = filepath.Abs(root)
	if err != nil {
		return "", err
	}
	absoluteRoot, err = filepath.EvalSymlinks(absoluteRoot)
	if err != nil {
		return "", err
	}
	var resolvedDirectory string
	resolvedDirectory, err = filepath.EvalSymlinks(directory)
	if err != nil {
		return "", fmt.Errorf("resolve candidate source directory %q: %w", directory, err)
	}
	var relativeDir string
	relativeDir, err = filepath.Rel(absoluteRoot, resolvedDirectory)
	if err != nil || relativeDir != "." && !filepath.IsLocal(relativeDir) {
		return "", fmt.Errorf("source directory %q is outside candidate root", directory)
	}
	var relative = filepath.Join(relativeDir, filepath.FromSlash(name))
	if !filepath.IsLocal(relative) {
		return "", fmt.Errorf("source %q is outside candidate root", name)
	}
	var resolved string
	resolved, err = filepath.EvalSymlinks(filepath.Join(absoluteRoot, relative))
	if err != nil {
		return "", fmt.Errorf("resolve candidate input %s: %w", relative, err)
	}
	var resolvedRelative string
	resolvedRelative, err = filepath.Rel(absoluteRoot, resolved)
	if err != nil || !filepath.IsLocal(resolvedRelative) {
		return "", fmt.Errorf("candidate input %s resolves outside root", relative)
	}
	var info os.FileInfo
	info, err = os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() {
		return "", fmt.Errorf("candidate input %s is not a regular file", relative)
	}
	return filepath.ToSlash(relative), nil
}

func captureCandidateSnapshot(root, destination string, files []string) (string, error) {
	var sourceRoot, err = os.OpenRoot(root)
	if err != nil {
		return "", fmt.Errorf("open candidate root: %w", err)
	}
	defer func() { _ = sourceRoot.Close() }()
	var destinationRoot *os.Root
	if destination != "" {
		destinationRoot, err = os.OpenRoot(destination)
		if err != nil {
			return "", fmt.Errorf("open candidate snapshot: %w", err)
		}
		defer func() { _ = destinationRoot.Close() }()
	}
	var hash = sha256.New()
	_, _ = hash.Write([]byte("amps-candidate-snapshot-v1\x00"))
	for _, name := range files {
		var source *os.File
		source, err = sourceRoot.Open(filepath.FromSlash(name))
		if err != nil {
			return "", fmt.Errorf("read candidate input %s: %w", name, err)
		}
		var info os.FileInfo
		info, err = source.Stat()
		if err != nil {
			_ = source.Close()
			return "", fmt.Errorf("stat candidate input %s: %w", name, err)
		}
		if !info.Mode().IsRegular() {
			_ = source.Close()
			return "", fmt.Errorf("candidate input %s is not a regular file", name)
		}
		var data []byte
		data, err = io.ReadAll(source)
		var closeErr = source.Close()
		if err != nil {
			return "", fmt.Errorf("read candidate input %s: %w", name, err)
		}
		if closeErr != nil {
			return "", fmt.Errorf("close candidate input %s: %w", name, closeErr)
		}
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(name)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(name))
		binary.BigEndian.PutUint64(size[:], uint64(len(data)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write(data)
		if destinationRoot == nil {
			continue
		}
		var target = filepath.FromSlash(name)
		if err = destinationRoot.MkdirAll(filepath.Dir(target), 0700); err != nil {
			return "", err
		}
		var output *os.File
		output, err = destinationRoot.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return "", fmt.Errorf("write snapshot input %s: %w", name, err)
		}
		_, err = output.Write(data)
		closeErr = output.Close()
		if err != nil {
			return "", fmt.Errorf("write snapshot input %s: %w", name, err)
		}
		if closeErr != nil {
			return "", fmt.Errorf("close snapshot input %s: %w", name, closeErr)
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func stripReferenceTests(directory string) error {
	var tempRoot, err = filepath.Abs(os.TempDir())
	if err != nil {
		return err
	}
	tempRoot, err = filepath.EvalSymlinks(tempRoot)
	if err != nil {
		return err
	}
	var reference string
	reference, err = filepath.EvalSymlinks(directory)
	if err != nil {
		return err
	}
	var parent = filepath.Dir(reference)
	var relative string
	relative, err = filepath.Rel(tempRoot, parent)
	if err != nil || !filepath.IsLocal(relative) || filepath.Dir(relative) != "." || filepath.Base(reference) != "reference" || !strings.HasPrefix(filepath.Base(parent), "amps-perfgate-") {
		return fmt.Errorf("refusing to strip tests outside reference scratch %s", reference)
	}
	var harness = filepath.Join(reference, filepath.FromSlash(benchmarkFile))
	return filepath.WalkDir(reference, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || path == harness || !strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		if removeErr := os.Remove(path); removeErr != nil {
			return fmt.Errorf("remove reference test %s: %w", path, removeErr)
		}
		return nil
	})
}
