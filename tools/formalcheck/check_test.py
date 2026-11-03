import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import os


class FormalCheckTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location('formal_check', Path(__file__).with_name('check.py'))
        self.check = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.check)

    def test_success_requires_explored_states(self):
        output = 'Model checking completed. No error has been found.\n17 states generated, 9 distinct states found, 0 states left on queue.'
        self.assertEqual(9, self.check.verdict(0, output)['distinct_states'])
        with self.assertRaises(ValueError):
            self.check.verdict(0, 'Model checking completed. No error has been found.')

    def test_expected_violation_is_specific(self):
        output = 'Error: Invariant NoLoss is violated.\nState 1: <Initial predicate>\n'
        self.assertEqual('NoLoss', self.check.verdict(12, output, 'NoLoss')['violated'])
        with self.assertRaises(ValueError):
            self.check.verdict(12, output, 'AnotherProperty')
        with self.assertRaises(ValueError):
            self.check.verdict(1, 'Parsing or semantic analysis failed.', 'NoLoss')

    def test_source_drift_is_a_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in self.check.COVERED:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('original source')
            path = root / self.check.COVERED[0]
            record = self.check.source_record(root, self.check.COVERED)
            self.check.require_source_sync(root, record)
            path.write_text('changed source')
            with self.assertRaises(ValueError):
                self.check.require_source_sync(root, record)

    def test_empty_source_record_cannot_claim_sync(self):
        with self.assertRaises(ValueError):
            self.check.require_source_sync(Path.cwd(), {})

    def test_source_record_requires_every_covered_file_and_no_extra(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in self.check.COVERED + ['extra.go']:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('original source')
            record = self.check.source_record(root, self.check.COVERED)
            self.check.require_source_sync(root, record)
            for omitted in self.check.COVERED:
                with self.subTest(omitted=omitted):
                    partial = {name: value for name, value in record.items() if name != omitted}
                    with self.assertRaises(ValueError):
                        self.check.require_source_sync(root, partial)
            with self.subTest(extra=True):
                expanded = self.check.source_record(root, self.check.COVERED + ['extra.go'])
                with self.assertRaises(ValueError):
                    self.check.require_source_sync(root, expanded)

    def test_success_requires_zero_exit_code_and_positive_distinct_states(self):
        completed = 'Model checking completed. No error has been found.\n17 states generated, 9 distinct states found, 0 states left on queue.'
        with self.assertRaises(ValueError):
            self.check.verdict(1, completed)
        with self.assertRaises(ValueError):
            self.check.verdict(0, completed.replace('9 distinct', '0 distinct'))

    def test_expected_counterexample_requires_failure_exit_code(self):
        output = 'Error: Invariant NoLoss is violated.\nState 1: <Initial predicate>\n'
        with self.assertRaises(ValueError):
            self.check.verdict(0, output, 'NoLoss')

    def test_nonempty_queue_is_not_exhaustive_success(self):
        output = 'Model checking completed. No error has been found.\n17 states generated, 9 distinct states found, 3 states left on queue.'
        with self.assertRaises(ValueError):
            self.check.verdict(0, output)

    def test_violation_without_a_trace_is_not_a_counterexample(self):
        with self.assertRaises(ValueError):
            self.check.verdict(12, 'Error: Invariant NoLoss is violated.', 'NoLoss')

    @unittest.skipUnless(os.name == 'nt', 'Windows subprocess startup')
    def test_formal_tools_do_not_create_console_windows(self):
        with patch.object(self.check.subprocess, 'run') as launch:
            self.check.run_tool(['java', '-version'], capture_output=True)
        options = launch.call_args.kwargs
        self.assertTrue(options.get('creationflags', 0) & self.check.subprocess.CREATE_NO_WINDOW)
        self.assertTrue(options['startupinfo'].dwFlags & self.check.subprocess.STARTF_USESHOWWINDOW)
        self.assertEqual(0, options['startupinfo'].wShowWindow)


if __name__ == '__main__':
    unittest.main()
