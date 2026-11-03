#!/usr/bin/env python3
"""Build and run the official Windows C++ client against an isolated fakeamps."""

import argparse
import hashlib
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parents[2]
VERSION = "5.3.5.14"
LIBRARY_SHA256 = "270ec38a8f1bfd9cad58a19bf9a113deae0aecf74d6d55deb5680c070ac65836"
SCENARIOS = ("live", "bookmark-epoch", "bookmark-now", "bookmark-resume", "sow",
             "queue-ack", "queue-lease", "queue-single-lease", "retained-reconnect", "oof")
NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


class ProbeError(RuntimeError):
    pass


def validate_sdk(sdk):
    header = sdk / "include" / "ampsplusplus.hpp"
    library = sdk / "lib" / "x64" / "release" / "amps.lib"
    version_header = sdk / "include" / "ampsver.h"
    for required in (header, library, version_header):
        if not required.is_file():
            raise ProbeError(f"required SDK file missing: {required}")
    text = version_header.read_text(encoding="utf-8")
    match = re.search(r'^\s*#define\s+AMPS_CLIENT_VERSION\s+"([^"]+)"', text, re.MULTILINE)
    if not match or match.group(1) != VERSION:
        raise ProbeError(f"expected official C++ SDK version {VERSION} in {version_header}")
    digest = hashlib.sha256(library.read_bytes()).hexdigest()
    if digest != LIBRARY_SHA256:
        raise ProbeError(f"SDK library SHA-256 mismatch: expected {LIBRARY_SHA256}, got {digest}")
    return library


def parse_environment(output):
    environment = {}
    for line in output.splitlines():
        if "=" in line and not line.startswith("="):
            key, value = line.split("=", 1)
            environment[key] = value
    return environment


def require_workspace_path(root, path):
    root = root.resolve()
    path = path.resolve()
    if path == root or not path.is_relative_to(root):
        raise ProbeError(f"temporary build directory outside workspace: {path}")
    return path


def run_checked(command, *, timeout, cwd=None, env=None):
    try:
        result = subprocess.run(command, cwd=cwd, env=env, capture_output=True,
                                text=True, timeout=timeout, creationflags=NO_WINDOW)
    except subprocess.TimeoutExpired as error:
        raise ProbeError(f"command timed out after {timeout}s: {command}") from error
    except OSError as error:
        raise ProbeError(f"could not start {command}: {error}") from error
    if result.returncode:
        raise ProbeError(f"command failed (exit {result.returncode}): {command}\n"
                         f"{result.stdout}{result.stderr}")
    return result


def compiler_environment():
    program_files = os.environ.get("ProgramFiles(x86)")
    if not program_files:
        raise ProbeError("ProgramFiles(x86) is required to find Visual Studio Build Tools")
    vswhere = Path(program_files) / "Microsoft Visual Studio" / "Installer" / "vswhere.exe"
    if not vswhere.is_file():
        raise ProbeError(f"Visual Studio locator not found: {vswhere}")
    result = run_checked([str(vswhere), "-latest", "-products", "*", "-requires",
                          "Microsoft.VisualStudio.Component.VC.Tools.x86.x64",
                          "-property", "installationPath"], timeout=20)
    if not result.stdout.strip():
        raise ProbeError("Visual Studio C++ x64 Build Tools are required")
    devcmd = Path(result.stdout.strip()) / "Common7" / "Tools" / "VsDevCmd.bat"
    if not devcmd.is_file():
        raise ProbeError(f"Visual Studio environment script not found: {devcmd}")
    result = run_checked(f'cmd.exe /d /s /c ""{devcmd}" -no_logo -arch=x64 && set"',
                         timeout=30)
    environment = parse_environment(result.stdout)
    paths = next((value for key, value in environment.items() if key.lower() == "path"), "")
    compiler = shutil.which("cl.exe", path=paths)
    if not compiler:
        raise ProbeError("Visual Studio C++ x64 compiler was not found after setup")
    return compiler, environment


def build_probe(sdk, build, library):
    compiler, environment = compiler_environment()
    include = build / "include"
    shutil.copytree(sdk / "include", include / "amps")
    probe = build / "cppfakeamps.exe"
    run_checked([compiler, "/nologo", "/EHsc", "/MD", "/std:c++17",
                 "/I" + str(include), str(ROOT / "tools" / "cppfakeamps" / "probe.cpp"),
                 str(library), "/Fe:" + str(probe), "ws2_32.lib"],
                cwd=build, env=environment, timeout=120)
    return probe


def build_broker(build):
    go = shutil.which("go")
    if not go:
        raise ProbeError("Go is required to build fakeamps")
    broker = build / "fakeamps.exe"
    run_checked([go, "build", "-o", str(broker), "./tools/fakeamps"], cwd=ROOT, timeout=120)
    return broker


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def wait_for_broker(process, log_path, address, timeout=10):
    deadline = time.monotonic() + timeout
    marker = f"listening on {address} "
    while time.monotonic() < deadline:
        log = log_path.read_text(encoding="utf-8", errors="replace")
        if process.poll() is not None:
            raise ProbeError(f"fakeamps exited before readiness:\n{log}")
        if marker in log:
            return
        time.sleep(0.05)
    raise ProbeError(f"fakeamps readiness timed out after {timeout}s:\n{log}")


def stop_broker(process):
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def run_scenarios(broker, probe, build, scenarios):
    address = f"127.0.0.1:{free_port()}"
    log_path = build / "fakeamps.log"
    failures = []
    with log_path.open("w", encoding="utf-8") as log:
        try:
            process = subprocess.Popen([str(broker), "-addr", address, "-lease", "1500ms",
                                        "-queue-lease-interval", "100ms"], stdout=log, stderr=log,
                                       creationflags=NO_WINDOW)
        except OSError as error:
            raise ProbeError(f"could not start fakeamps: {error}") from error
        try:
            wait_for_broker(process, log_path, address)
            for scenario in scenarios:
                try:
                    result = run_checked([str(probe), f"tcp://{address}/amps/json", scenario],
                                         timeout=15)
                    if result.stdout.strip() != f"PASS {scenario}":
                        raise ProbeError(f"{scenario} did not produce its PASS marker: {result.stdout!r}")
                    print(result.stdout, end="", flush=True)
                except ProbeError as error:
                    failures.append(scenario)
                    print(error, file=sys.stderr, flush=True)
        finally:
            stop_broker(process)
    if failures:
        raise ProbeError(f"C++ scenarios failed: {', '.join(failures)}\n"
                         f"fakeamps log:\n{log_path.read_text(encoding='utf-8', errors='replace')}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdk", type=Path, required=True,
                        help="unmodified Windows AMPS C++ SDK 5.3.5.14 directory")
    parser.add_argument("--scenario", action="append", choices=SCENARIOS,
                        help="run a specific scenario; repeat to select multiple")
    args = parser.parse_args(argv)
    try:
        if os.name != "nt":
            raise ProbeError("this runner requires Windows and the Windows C++ SDK")
        if args.scenario and len(set(args.scenario)) != len(args.scenario):
            raise ProbeError("each scenario may be selected only once per run")
        sdk = args.sdk.resolve()
        library = validate_sdk(sdk)
        print(f"AMPS C++ {VERSION}; amps.lib SHA-256 {LIBRARY_SHA256}", flush=True)
        temp_root = require_workspace_path(ROOT, ROOT / ".tmp" / "cppfakeamps")
        temp_root.mkdir(parents=True, exist_ok=True)
        temp_root = require_workspace_path(ROOT, temp_root)
        with tempfile.TemporaryDirectory(prefix="run-", dir=temp_root) as directory:
            build = require_workspace_path(temp_root, Path(directory))
            probe = build_probe(sdk, build, library)
            broker = build_broker(build)
            run_scenarios(broker, probe, build, args.scenario or SCENARIOS)
        return 0
    except (ProbeError, OSError) as error:
        print(f"cppfakeamps: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
