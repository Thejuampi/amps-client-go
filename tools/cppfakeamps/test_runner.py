import importlib.util
import hashlib
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


spec = importlib.util.spec_from_file_location("cppfakeamps_runner", Path(__file__).with_name("run.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class RunnerTest(unittest.TestCase):
    def make_sdk(self, directory):
        sdk = Path(directory)
        (sdk / "include").mkdir()
        (sdk / "include" / "ampsplusplus.hpp").touch()
        (sdk / "include" / "ampsver.h").write_text('#define AMPS_CLIENT_VERSION "5.3.5.14"\n')
        (sdk / "lib" / "x64" / "release").mkdir(parents=True)
        library = sdk / "lib" / "x64" / "release" / "amps.lib"
        library.write_bytes(b"test library")
        return sdk, library

    def test_missing_sdk_fails_with_required_file(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(runner.ProbeError, "include.*ampsplusplus.hpp"):
                runner.validate_sdk(Path(directory))

    def test_missing_library_is_not_silently_skipped(self):
        with tempfile.TemporaryDirectory() as directory:
            sdk = Path(directory)
            (sdk / "include").mkdir()
            (sdk / "include" / "ampsplusplus.hpp").touch()
            with self.assertRaisesRegex(runner.ProbeError, "amps.lib"):
                runner.validate_sdk(sdk)

    def test_wrong_version_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            sdk, _ = self.make_sdk(directory)
            (sdk / "include" / "ampsver.h").write_text('#define AMPS_CLIENT_VERSION "5.3.5.1"\n')
            with self.assertRaisesRegex(runner.ProbeError, "5.3.5.14"):
                runner.validate_sdk(sdk)

    def test_library_fingerprint_is_required(self):
        with tempfile.TemporaryDirectory() as directory:
            sdk, _ = self.make_sdk(directory)
            with self.assertRaisesRegex(runner.ProbeError, "SHA-256"):
                runner.validate_sdk(sdk)

    def test_matching_sdk_passes_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            sdk, library = self.make_sdk(directory)
            digest = hashlib.sha256(library.read_bytes()).hexdigest()
            with mock.patch.object(runner, "LIBRARY_SHA256", digest):
                self.assertEqual(runner.validate_sdk(sdk), library)

    def test_parse_environment_preserves_values_and_ignores_drive_entries(self):
        environment = runner.parse_environment("=C:=C:\\tmp\nPath=C:\\bin\nTOKEN=a=b\n")
        self.assertEqual(environment, {"Path": "C:\\bin", "TOKEN": "a=b"})

    def test_failed_child_reports_exit_code_and_output(self):
        command = [sys.executable, "-c", "import sys; print('out'); print('err', file=sys.stderr); sys.exit(7)"]
        with self.assertRaises(runner.ProbeError) as raised:
            runner.run_checked(command, timeout=5)
        self.assertIn("exit 7", str(raised.exception))
        self.assertIn("out", str(raised.exception))
        self.assertIn("err", str(raised.exception))

    def test_child_timeout_is_a_failure(self):
        command = [sys.executable, "-c", "import time; time.sleep(5)"]
        with self.assertRaisesRegex(runner.ProbeError, "timed out"):
            runner.run_checked(command, timeout=0.2)

    def test_successful_child_returns_output(self):
        command = [sys.executable, "-c", "print('ok')"]
        result = runner.run_checked(command, timeout=5)
        self.assertEqual(result.stdout.strip(), "ok")

    def test_broker_exit_before_readiness_is_a_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "broker.log"
            log.write_text("listen failed", encoding="utf-8")
            process = mock.Mock()
            process.poll.return_value = 1
            with self.assertRaisesRegex(runner.ProbeError, "listen failed"):
                runner.wait_for_broker(process, log, "127.0.0.1:1234", timeout=0.1)

    def test_broker_readiness_requires_own_address(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "broker.log"
            log.write_text("listening on 127.0.0.1:1235 ", encoding="utf-8")
            process = mock.Mock()
            process.poll.return_value = None
            with self.assertRaisesRegex(runner.ProbeError, "readiness timed out"):
                runner.wait_for_broker(process, log, "127.0.0.1:1234", timeout=0.1)

    def test_scenario_failure_runs_remaining_cases_and_stops_broker(self):
        with tempfile.TemporaryDirectory() as directory:
            process = mock.Mock()
            process.poll.return_value = None

            def run_probe(command, **kwargs):
                if command[-1] == "bad":
                    raise runner.ProbeError("native assertion failed")
                return subprocess.CompletedProcess(command, 0, stdout="PASS good\n", stderr="")

            with mock.patch.object(runner, "free_port", return_value=1234), \
                 mock.patch.object(runner, "wait_for_broker"), \
                 mock.patch.object(runner.subprocess, "Popen", return_value=process), \
                 mock.patch.object(runner, "run_checked", side_effect=run_probe) as run:
                with self.assertRaisesRegex(runner.ProbeError, "C\\+\\+ scenarios failed: bad"):
                    runner.run_scenarios(Path("broker"), Path("probe"), Path(directory), ("bad", "good"))
            self.assertEqual(run.call_count, 2)
            process.terminate.assert_called_once()
            process.wait.assert_called_once_with(timeout=5)

    def test_exit_zero_without_matching_pass_marker_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            process = mock.Mock()
            process.poll.return_value = None
            result = subprocess.CompletedProcess([], 0, stdout="", stderr="")
            with mock.patch.object(runner, "free_port", return_value=1234), \
                 mock.patch.object(runner, "wait_for_broker"), \
                 mock.patch.object(runner.subprocess, "Popen", return_value=process), \
                 mock.patch.object(runner, "run_checked", return_value=result):
                with self.assertRaisesRegex(runner.ProbeError, "C\\+\\+ scenarios failed: good"):
                    runner.run_scenarios(Path("broker"), Path("probe"), Path(directory), ("good",))
            process.terminate.assert_called_once()

    def test_duplicate_scenario_is_rejected_before_build(self):
        with mock.patch.object(runner, "validate_sdk") as validate:
            with mock.patch("sys.stderr"):
                result = runner.main(["--sdk", "unused", "--scenario", "live", "--scenario", "live"])
        self.assertEqual(result, 1)
        validate.assert_not_called()

    def test_temporary_root_must_stay_inside_workspace(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "workspace"
            with self.assertRaisesRegex(runner.ProbeError, "outside workspace"):
                runner.require_workspace_path(root, Path(directory) / "outside")

    def test_temporary_root_accepts_workspace_child(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "workspace"
            child = root / ".tmp" / "cppfakeamps"
            self.assertEqual(runner.require_workspace_path(root, child), child.resolve())


if __name__ == "__main__":
    unittest.main()
