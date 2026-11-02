import importlib.util
import tempfile
import unittest
from pathlib import Path


def load_checker():
    path = Path(__file__).with_name('cpp_check.py')
    spec = importlib.util.spec_from_file_location('cpp_formal_check', path)
    checker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(checker)
    return checker


class CppFormalCheckTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.check = load_checker()

    def test_cases_include_all_scenarios_and_specific_mutant_failures(self):
        cases = [case for case in self.check.cases() if case[1] == 'CppReplayRouting']
        positives = {constants['Scenario'] for _, _, constants, _, expected in cases if expected is None}
        self.assertEqual({'plain', 'epoch', 'now', 'after'}, positives)
        mutants = {name: expected for name, _, _, _, expected in cases if expected}
        self.assertEqual({
            'CppReplayRouting-missing-sids': 'AllExpectedDelivered',
            'CppReplayRouting-ignore-bookmark': 'AllExpectedDelivered',
            'CppReplayRouting-now-as-epoch': 'NoUnexpectedDelivery',
        }, mutants)

    def test_queue_cases_cover_eligibility_and_connection_identity(self):
        cases = [case for case in self.check.cases() if case[1] == 'CppQueueLease']
        positives = {(constants['Eligibility'], constants['SameSubscriptionID'])
                     for _, _, constants, _, expected in cases if expected is None}
        self.assertEqual({('none', False), ('owner-only', False), ('other-only', True),
                          ('both', False), ('both', True)}, positives)
        mutants = {name: expected for name, _, _, _, expected in cases if expected}
        self.assertEqual({
            'CppQueueLease-no-fallback': 'NoEligibleMessageStrandedAfterDispatch',
            'CppQueueLease-same-id-exclusion': 'PreferOtherWhenEligible',
            'CppQueueLease-drop-pending': 'RetainedAfterDispatch',
            'CppQueueLease-duplicate-record': 'UniqueLease',
        }, mutants)

    def test_source_record_rejects_missing_or_extra_sources_and_drift(self):
        with tempfile.TemporaryDirectory() as directory:
            root, sdk = Path(directory) / 'repo', Path(directory) / 'sdk'
            for base, files in ((root, self.check.REPO_FILES), (sdk, self.check.SDK_FILES)):
                for name in files:
                    path = base / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text('reviewed source', encoding='utf-8')
            record = self.check.source_record(root, sdk)
            self.check.require_source_sync(root, sdk, record)
            for group in ('repo', 'sdk'):
                with self.subTest(group=group, problem='missing'):
                    altered = {key: dict(value) for key, value in record.items()}
                    altered[group].pop(next(iter(altered[group])))
                    with self.assertRaises(ValueError):
                        self.check.require_source_sync(root, sdk, altered)
                with self.subTest(group=group, problem='extra'):
                    altered = {key: dict(value) for key, value in record.items()}
                    altered[group]['extra'] = 'hash'
                    with self.assertRaises(ValueError):
                        self.check.require_source_sync(root, sdk, altered)
                with self.subTest(group=group, problem='drift'):
                    altered = {key: dict(value) for key, value in record.items()}
                    altered[group][next(iter(altered[group]))] = 'different'
                    with self.assertRaises(ValueError):
                        self.check.require_source_sync(root, sdk, altered)

    def test_missing_sdk_explains_how_to_supply_it(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, '--sdk-root'):
                self.check.source_record(Path(directory), Path(directory) / 'missing')

    def test_mutation_target_must_match_exactly_once(self):
        self.assertEqual('a x c', self.check.mutate_source('a b c', 'b', 'x', 'test'))
        for source in ('a c', 'a b b c'):
            with self.subTest(source=source):
                with self.assertRaisesRegex(ValueError, 'test'):
                    self.check.mutate_source(source, 'b', 'x', 'test')

    def test_real_mutation_must_fail_the_intended_assertion(self):
        failure = '--- FAIL: TestFoo (0.00s)\n  wanted exact result\nFAIL\n'
        self.check.require_go_mutation(1, failure, 'TestFoo', 'wanted exact result')
        for code, output in ((0, failure), (1, 'build failed'),
                             (1, '--- FAIL: TestBar\nwanted exact result'),
                             (1, '--- FAIL: TestFoo\nother failure'),
                             (1, failure + 'panic: test timed out')):
            with self.subTest(code=code, output=output):
                with self.assertRaises(ValueError):
                    self.check.require_go_mutation(code, output, 'TestFoo', 'wanted exact result')

    def test_real_baseline_must_actually_run_and_pass(self):
        self.check.require_go_baseline(0, 'ok\tpackage\t0.1s')
        self.check.require_go_baseline(0, 'ok  \tpackage\t0.1s\n')
        for code, output in ((1, 'FAIL'), (0, 'ok [no tests to run]')):
            with self.subTest(code=code, output=output):
                with self.assertRaises(ValueError):
                    self.check.require_go_baseline(code, output)

    def test_output_directory_must_be_empty_and_outside_repo(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'repo'
            root.mkdir()
            with self.assertRaises(ValueError):
                self.check.prepare_output(root / 'inside', root)
            target = Path(directory) / 'external'
            self.assertEqual(target, self.check.prepare_output(target, root))
            (target / 'old.log').write_text('old')
            with self.assertRaises(ValueError):
                self.check.prepare_output(target, root)


if __name__ == '__main__':
    unittest.main()
