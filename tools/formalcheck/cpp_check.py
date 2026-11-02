import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import tempfile


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SDK = ROOT / '.tmp/parity-audit/reference/amps-c++-client-5.3.5.14-Windows'
REPO_FILES = [
    'tools/fakeamps/protocol.go', 'tools/fakeamps/handler.go',
    'tools/fakeamps/journal.go', 'tools/fakeamps/cpp_protocol_regression_test.go',
    'tools/fakeamps/subscription.go', 'tools/fakeamps/cpp_queue_lease_regression_test.go',
    'tools/fakeamps/subscription_test.go',
]
SDK_FILES = [
    'include/ampsplusplus.hpp', 'include/BookmarkStore.hpp',
    'include/MessageRouter.hpp', 'include/ampsver.h', 'src/amps_protocol.c',
    'src/amps_protocol_generated.h', 'lib/x64/release/amps.lib',
]

spec = importlib.util.spec_from_file_location('amps_formal_common', Path(__file__).with_name('check.py'))
common = importlib.util.module_from_spec(spec)
spec.loader.exec_module(common)


def source_record(root, sdk):
    missing = [name for name in SDK_FILES if not (Path(sdk) / name).is_file()]
    if missing:
        raise ValueError('C++ SDK source is missing; supply its root with --sdk-root: ' + ', '.join(missing))
    return {'repo': common.source_record(root, REPO_FILES), 'sdk': common.source_record(sdk, SDK_FILES)}


def require_source_sync(root, sdk, record):
    if set(record) != {'repo', 'sdk'} or set(record['repo']) != set(REPO_FILES) or set(record['sdk']) != set(SDK_FILES):
        raise ValueError('C++ formal verification requires the exact reviewed source set')
    actual = source_record(root, sdk)
    changed = [group + '/' + name for group in actual for name in actual[group]
               if actual[group][name] != record[group][name]]
    if changed:
        raise ValueError('C++ formal verification needs source review: ' + ', '.join(changed))


def cases():
    invariants = ['AllExpectedDelivered', 'NoUnexpectedDelivery']
    defaults = {'BrokenNoSids': False, 'BrokenIgnoreBookmark': False, 'BrokenNowAsEpoch': False}
    result = []
    for scenario in ('plain', 'epoch', 'now', 'after'):
        constants = dict(defaults, Scenario=scenario)
        result.append(('CppReplayRouting-' + scenario, 'CppReplayRouting', constants, invariants, None))
    for name, scenario, setting, expected in (
        ('missing-sids', 'epoch', 'BrokenNoSids', 'AllExpectedDelivered'),
        ('ignore-bookmark', 'epoch', 'BrokenIgnoreBookmark', 'AllExpectedDelivered'),
        ('now-as-epoch', 'now', 'BrokenNowAsEpoch', 'NoUnexpectedDelivery'),
    ):
        constants = dict(defaults, Scenario=scenario)
        constants[setting] = True
        result.append(('CppReplayRouting-' + name, 'CppReplayRouting', constants, invariants, expected))

    invariants = ['NoEligibleMessageStrandedAfterDispatch', 'PreferOtherWhenEligible',
                  'UniqueLease', 'RetainedAfterDispatch']
    defaults = {'BrokenNoFallback': False, 'BrokenSameIDExclusion': False,
                'BrokenDropPending': False, 'BrokenDuplicateRecord': False}
    for eligibility, same_id in (('none', False), ('owner-only', False), ('other-only', True),
                                 ('both', False), ('both', True)):
        constants = dict(defaults, Eligibility=eligibility, SameSubscriptionID=same_id)
        name = 'CppQueueLease-' + eligibility + ('-same-id' if same_id else '')
        result.append((name, 'CppQueueLease', constants, invariants, None))
    for name, eligibility, same_id, setting, expected in (
        ('no-fallback', 'owner-only', False, 'BrokenNoFallback', 'NoEligibleMessageStrandedAfterDispatch'),
        ('same-id-exclusion', 'both', True, 'BrokenSameIDExclusion', 'PreferOtherWhenEligible'),
        ('drop-pending', 'none', False, 'BrokenDropPending', 'RetainedAfterDispatch'),
        ('duplicate-record', 'both', False, 'BrokenDuplicateRecord', 'UniqueLease'),
    ):
        constants = dict(defaults, Eligibility=eligibility, SameSubscriptionID=same_id)
        constants[setting] = True
        result.append(('CppQueueLease-' + name, 'CppQueueLease', constants, invariants, expected))
    return result


def mutate_source(source, before, after, name):
    if source.count(before) != 1:
        raise ValueError('Real mutation target must occur exactly once: ' + name)
    return source.replace(before, after)


def require_go_baseline(code, output):
    if code != 0 or 'no tests to run' in output.lower() or not re.search(r'^ok[ \t]+\S', output, re.MULTILINE):
        raise ValueError('Real Go baseline did not run and pass its regression')


def require_go_mutation(code, output, test, assertion):
    if (code == 0 or '--- FAIL: ' + test not in output or assertion not in output
            or '[build failed]' in output or 'panic: test timed out' in output):
        raise ValueError('Real mutation did not fail the intended Go assertion: ' + test)


def real_mutations(output_root):
    mutations = [
        ('missing-sids', 'tools/fakeamps/protocol.go',
         '\twriteField(buf, "sub_id", subID)\n\twriteField(buf, "sids", subID)\n\twriteField(buf, "bm", bookmark)',
         '\twriteField(buf, "sub_id", subID)\n\twriteField(buf, "bm", bookmark)',
         'TestCPPDeliveryRoutingFieldsAndEscapedHeaders', '^publish$', 'delivery header ='),
        ('ignore-bookmark', 'tools/fakeamps/handler.go',
         'isBookmarkSub := bookmark != "" || containsToken(options, "bookmark")',
         'isBookmarkSub := containsToken(options, "bookmark")',
         'TestCPPBookmarkSubscribeReplayByBookmarkField', '^epoch$', 'replay payloads='),
        ('now-as-epoch', 'tools/fakeamps/handler.go',
         'if bookmark != "0|1|" {', 'if true {',
         'TestCPPBookmarkSubscribeReplayByBookmarkField', '^now$', 'replay payloads='),
        ('no-fallback', 'tools/fakeamps/subscription.go',
         '\t\treturn fallback\n', '\t\treturn nil\n',
         'TestCPPQueueRequeuesExpiredLeaseToOnlySubscriber', '', 'eligible subscriber left message pending'),
        ('same-id-exclusion', 'tools/fakeamps/subscription.go',
         '\t\tif eligible[index].subID == lease.subID && (lease.ownerConn == nil || eligible[index].conn == lease.ownerConn) {',
         '\t\tif eligible[index].subID == lease.subID && eligible[index].conn != lease.ownerConn {\n\t\t\tcontinue\n\t\t}\n'
         '\t\tif eligible[index].subID == lease.subID && (lease.ownerConn == nil || eligible[index].conn == lease.ownerConn) {',
         'TestCPPQueueRedeliveryTargetUsesConnectionIdentity', '^same_ID_on_another_connection$', 'redelivery target ='),
        ('drop-pending', 'tools/fakeamps/subscription.go',
         '\t\tenqueuePendingQueueMessageWithDeliveryState(lease.topic, lease.sowKey, lease.bookmark, lease.payload, lease.timestamp, lease.messageType, true)\n',
         '\t\t// Mutation: drop an expired message with no eligible subscriber.\n',
         'TestRequeueMessageQueuesPendingWhenNoSubscriberAvailable', '', 'expected expired lease to be preserved'),
    ]
    results = []
    for name, file, before, after, test, subtest, assertion in mutations:
        original = ROOT / file
        source = original.read_text(encoding='utf-8')
        altered = mutate_source(source, before, after, name)
        pattern = '^' + test + '$' + ('/' + subtest if subtest else '')
        environment = dict(os.environ, GOTOOLCHAIN='go1.26.10')
        base_command = ['go', 'test', './tools/fakeamps', '-run', pattern, '-count=1', '-timeout=1m']
        baseline = common.run_tool(base_command, cwd=ROOT, capture_output=True, text=True, env=environment, timeout=90)
        baseline_output = baseline.stdout + baseline.stderr
        (output_root / (name + '.baseline.log')).write_text(baseline_output, encoding='utf-8')
        require_go_baseline(baseline.returncode, baseline_output)
        mutant = output_root / (name + '.go.txt')
        mutant.write_text(altered, encoding='utf-8')
        overlay = output_root / (name + '.overlay.json')
        overlay.write_text(json.dumps({'Replace': {str(original): str(mutant)}}), encoding='utf-8')
        command = ['go', 'test', '-overlay', str(overlay), './tools/fakeamps',
                   '-run', pattern, '-count=1', '-timeout=1m']
        process = common.run_tool(command, cwd=ROOT, capture_output=True, text=True, env=environment, timeout=90)
        output = process.stdout + process.stderr
        (output_root / (name + '.mutation.log')).write_text(output, encoding='utf-8')
        require_go_mutation(process.returncode, output, test, assertion)
        results.append({'mutation': name, 'test': pattern, 'assertion': assertion, 'killed': True})
        print('REAL MUTATION KILLED ' + name, flush=True)
    return results


def prepare_output(path, root):
    target, root = Path(path).resolve(), Path(root).resolve()
    if target == root or root in target.parents:
        raise ValueError('Output directory must be outside the repository: ' + str(target))
    target.mkdir(parents=True, exist_ok=True)
    if any(target.iterdir()):
        raise ValueError('Output directory must be empty: ' + str(target))
    return target


def main():
    parser = argparse.ArgumentParser(description='Check bounded C++/fakeamps replay and routing contracts')
    parser.add_argument('--sdk-root', type=Path, default=DEFAULT_SDK)
    parser.add_argument('--jar', help='TLC 1.7.4 jar (also settable with TLA2TOOLS)')
    parser.add_argument('--out', type=Path, help='new empty directory outside the repository')
    parser.add_argument('--models-only', action='store_true', help='skip real Go overlay mutations')
    args = parser.parse_args()

    record = json.loads((ROOT / 'formal/cpp-source-sync.json').read_text(encoding='utf-8'))
    require_source_sync(ROOT, args.sdk_root, record['files'])
    output_root = prepare_output(args.out or tempfile.mkdtemp(prefix='amps-cpp-formal-'), ROOT)
    jar = common.checker_jar(args.jar)
    results = [common.run_case(item, jar, output_root) for item in cases()]
    mutations = [] if args.models_only else real_mutations(output_root)
    require_source_sync(ROOT, args.sdk_root, record['files'])
    summary = {'tlc_version': common.TLC_VERSION, 'tlc_sha256': common.TLC_SHA256,
               'source_sync': record, 'models': results, 'real_mutations': mutations}
    (output_root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n', encoding='utf-8')
    print('CPP FORMAL CHECK PASS: ' + str(output_root), flush=True)


if __name__ == '__main__':
    main()
