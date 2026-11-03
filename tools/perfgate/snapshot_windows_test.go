package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"unsafe"
)

func snapshotShortPath(path string) (string, error) {
	var input, err = syscall.UTF16PtrFromString(path)
	if err != nil {
		return "", err
	}
	var proc = syscall.NewLazyDLL("kernel32.dll").NewProc("GetShortPathNameW")
	var required, _, callErr = proc.Call(uintptr(unsafe.Pointer(input)), 0, 0)
	if required == 0 {
		return "", fmt.Errorf("get short path length: %w", callErr)
	}
	var buffer = make([]uint16, int(required))
	var written uintptr
	written, _, callErr = proc.Call(uintptr(unsafe.Pointer(input)), uintptr(unsafe.Pointer(&buffer[0])), uintptr(len(buffer)))
	if written == 0 {
		return "", fmt.Errorf("get short path: %w", callErr)
	}
	if written >= uintptr(len(buffer)) {
		return "", fmt.Errorf("short path exceeded allocated buffer")
	}
	return syscall.UTF16ToString(buffer[:written]), nil
}

func TestSnapshotRelativeFileWindowsShortPath(t *testing.T) {
	var root = filepath.Join(t.TempDir(), "Snapshot directory with a long name")
	var directory = filepath.Join(root, "Package directory with a long name")
	writeSnapshotFixture(t, filepath.Join(directory, "client.go"), "package amps\n")
	var canonicalRoot, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	var canonicalDir string
	canonicalDir, err = filepath.EvalSymlinks(directory)
	if err != nil {
		t.Fatal(err)
	}
	var shortDir string
	shortDir, err = snapshotShortPath(canonicalDir)
	if err != nil {
		t.Fatal(err)
	}
	var filename = "client.go"
	if strings.EqualFold(shortDir, canonicalDir) {
		var git string
		git, err = exec.LookPath("git")
		if err == nil {
			git, err = filepath.Abs(git)
			if err != nil {
				t.Fatal(err)
			}
			git, err = filepath.EvalSymlinks(git)
			if err != nil {
				t.Fatal(err)
			}
			canonicalDir = filepath.Dir(git)
			canonicalRoot = filepath.Dir(canonicalDir)
			filename = filepath.Base(git)
			shortDir, err = snapshotShortPath(canonicalDir)
			if err != nil {
				t.Fatal(err)
			}
		}
		if strings.EqualFold(shortDir, canonicalDir) {
			t.Skip("no 8.3 alias is available for either test directory")
		}
	}
	var canonicalInfo, aliasInfo os.FileInfo
	canonicalInfo, err = os.Stat(canonicalDir)
	if err != nil {
		t.Fatal(err)
	}
	aliasInfo, err = os.Stat(shortDir)
	if err != nil || !os.SameFile(canonicalInfo, aliasInfo) {
		t.Fatalf("8.3 path does not identify the same directory: %q, %v", shortDir, err)
	}
	var got string
	got, err = snapshotRelativeFile(canonicalRoot, shortDir, filename)
	var want = filepath.ToSlash(filepath.Join(filepath.Base(canonicalDir), filename))
	if err != nil || got != want {
		t.Fatalf("8.3 alias %q resolved to %q, error=%v", shortDir, got, err)
	}
	var outsideRoot = t.TempDir()
	if got, err = snapshotRelativeFile(outsideRoot, shortDir, filename); err == nil {
		t.Fatalf("8.3 directory outside candidate root was accepted as %q", got)
	}
}
