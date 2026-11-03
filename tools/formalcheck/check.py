import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import urllib.request


ROOT = Path(__file__).resolve().parents[2]
TLC_VERSION = '1.7.4'
TLC_SHA256 = '936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88'
COVERED = [
    'amps/client.go', 'amps/client_parity_methods.go', 'amps/ha_client.go',
    'amps/publish_store.go', 'amps/internal/wal/wal.go',
    'tools/fakeamps/dedupe.go', 'tools/fakeamps/handler.go', 'tools/fakeamps/journal.go',
]


def run_tool(command, **options):
    if os.name == 'nt':
        startup = subprocess.STARTUPINFO()
        startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startup.wShowWindow = 0
        options.update(startupinfo=startup, creationflags=subprocess.CREATE_NO_WINDOW)
    return subprocess.run(command, **options)


def source_record(root, files):
    return {name: hashlib.sha256((Path(root) / name).read_bytes()).hexdigest() for name in files}


def require_source_sync(root, record):
    if set(record) != set(COVERED):
        raise ValueError('Formal verification requires the exact covered source set')
    actual = source_record(root, record)
    changed = [name for name in record if actual[name] != record[name]]
    if changed:
        raise ValueError('Formal verification needs source review: ' + ', '.join(changed))


def verdict(code, output, expected=None):
    match = re.search(r'Invariant (\w+) is violated', output)
    if expected is not None:
        if code == 0 or not match or match[1] != expected or 'State 1:' not in output:
            raise ValueError('Expected invariant counterexample for ' + expected)
        return {'violated': match[1]}
    states = re.search(r'([\d,]+) states generated, ([\d,]+) distinct states found, ([\d,]+) states left on queue', output)
    if code != 0 or 'Model checking completed. No error has been found.' not in output or not states:
        raise ValueError('TLC did not complete an exhaustive successful check')
    count = int(states[2].replace(',', ''))
    if count == 0 or int(states[3].replace(',', '')) != 0:
        raise ValueError('TLC did not exhaust a nonempty state space')
    return {'generated_states': int(states[1].replace(',', '')), 'distinct_states': count}


def checker_jar(explicit):
    target = Path(explicit or os.environ.get('TLA2TOOLS', Path.home() / '.cache/amps-formal/tlc-1.7.4/tla2tools.jar'))
    if not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        url = 'https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar'
        with urllib.request.urlopen(url, timeout=60) as response:
            data = response.read()
        if hashlib.sha256(data).hexdigest() != TLC_SHA256:
            raise ValueError('Downloaded TLC checksum mismatch')
        target.write_bytes(data)
    if hashlib.sha256(target.read_bytes()).hexdigest() != TLC_SHA256:
        raise ValueError('TLC checksum mismatch: ' + str(target))
    return target.resolve()


def cases():
    result = []

    def add(module, constants, invariants, mutants, suffix=''):
        name = module + suffix
        result.append((name, module, constants, invariants, None))
        for setting, expected in mutants:
            altered = dict(constants)
            altered[setting] = True
            result.append((name + '-' + setting, module, altered, invariants, expected))

    add('PublishRecovery', {
        'MaxMessages': 3, 'MaxGeneration': 2, 'BrokenGate': False,
        'BrokenAck': False, 'BrokenGeneration': False,
    }, ['TypeOK', 'NoLoss', 'PersistedRequested', 'CurrentGenerationOnly'], [
        ('BrokenGate', 'NoLoss'), ('BrokenAck', 'PersistedRequested'),
        ('BrokenGeneration', 'CurrentGenerationOnly'),
    ])
    add('StoreRecovery', {
        'BrokenTailRepair': False, 'BrokenRejection': False, 'BrokenFreeze': False,
    }, ['NoAcceptedLoss', 'RejectedCannotReplay', 'UncertainEvidencePreserved'], [
        ('BrokenTailRepair', 'NoAcceptedLoss'), ('BrokenRejection', 'RejectedCannotReplay'),
        ('BrokenFreeze', 'UncertainEvidencePreserved'),
    ])
    settings = ['BrokenActiveGuard', 'BrokenTerminal', 'BrokenEpoch', 'BrokenPreservePending',
                'BrokenBusyPending', 'BrokenFinishPending', 'BrokenConnectedGuard']
    properties = ['NoCompetingInitialWorker', 'TerminalIsQuiet', 'StaleAttemptIsRejected',
                  'RecoveredSessionIsPreserved', 'NoDroppedReconnect']
    variants = {
        'initial': [('BrokenActiveGuard', 'NoCompetingInitialWorker'), ('BrokenTerminal', 'TerminalIsQuiet')],
        'handoff': [('BrokenPreservePending', 'NoDroppedReconnect'), ('BrokenBusyPending', 'NoDroppedReconnect'),
                    ('BrokenFinishPending', 'NoDroppedReconnect')],
        'listener': [('BrokenConnectedGuard', 'RecoveredSessionIsPreserved')],
        'stale': [('BrokenEpoch', 'StaleAttemptIsRejected')],
    }
    for scenario, mutants in variants.items():
        constants = dict.fromkeys(settings, False)
        constants['Scenario'] = scenario
        add('HAHandoff', constants, properties, mutants, '-' + scenario)
    add('BrokerDedup', {
        'BrokenDeleteRestore': False, 'BrokenPublishRestore': False,
    }, ['DeleteReplayIsIdempotent', 'PublishReplayIsIdempotent', 'NewRowSurvivesOldDelete'], [
        ('BrokenDeleteRestore', 'DeleteReplayIsIdempotent'),
        ('BrokenPublishRestore', 'PublishReplayIsIdempotent'),
    ])
    return result


def config(constants, invariants):
    def value(item):
        if isinstance(item, bool):
            return str(item).upper()
        return json.dumps(item)
    return ('SPECIFICATION Spec\nCONSTANTS\n' +
            ''.join('  ' + key + ' = ' + value(item) + '\n' for key, item in constants.items()) +
            'INVARIANTS\n' + ''.join('  ' + name + '\n' for name in invariants))


def run_case(item, jar, output_root):
    name, module, constants, invariants, expected = item
    folder = output_root / name
    folder.mkdir()
    shutil.copyfile(ROOT / 'formal' / (module + '.tla'), folder / (module + '.tla'))
    (folder / (module + '.cfg')).write_text(config(constants, invariants), encoding='utf-8')
    java = os.environ.get('JAVA', 'java')
    command = [java, '-XX:+UseParallelGC', '-Xmx1g', '-cp', str(jar), 'tlc2.TLC',
               '-workers', '1', '-fp', '0', '-seed', '1', '-lncheck', 'final', '-config', module + '.cfg', module]
    process = run_tool(command, cwd=folder, capture_output=True, text=True, timeout=120)
    output = process.stdout + process.stderr
    (folder / 'tlc.out').write_text(output, encoding='utf-8')
    result = verdict(process.returncode, output, expected)
    result.update({'case': name, 'constants': constants, 'invariants': invariants,
                   'model_sha256': hashlib.sha256((folder / (module + '.tla')).read_bytes()).hexdigest(),
                   'expected_violation': expected, 'exit_code': process.returncode})
    print(('COUNTEREXAMPLE ' if expected else 'PASS ') + name + ' ' + json.dumps(result), flush=True)
    return result


def real_mutations(output_root):
    mutations = [
        ('replay-gate', 'amps/client_parity_methods.go',
         'func (client *Client) replayPublishStore() (bool, error) {\n\tclient.publishLock.Lock()\n\tdefer client.publishLock.Unlock()',
         'func (client *Client) replayPublishStore() (bool, error) {',
         './amps', '^TestDeepAuditLivePublishCannotOvertakeRecovery$'),
        ('legacy-ack', 'amps/client_parity_methods.go',
         'if ack, _ := command.AckType(); ack&AckTypePersisted == 0 {',
         'if ack, _ := command.AckType(); ack&AckTypePersisted != 0 {',
         './amps', '^TestFollowupLegacyPublishBacklogRequestsPersistedAck$'),
        ('wal-tail', 'amps/internal/wal/wal.go',
         'return replay(path, apply, false, true, syncWrite)',
         'return replay(path, apply, false, false, syncWrite)',
         './amps', '^TestDeepAudit(Publish|Bookmark)WALTornTailThenAppendThenRestart$'),
        ('ha-pending', 'amps/ha_client.go', 'if !needsRetry || retry {', 'if true {',
         './amps', '^TestHACancelledOldWorkerHandsOffNewSessionDisconnect$/during-success$'),
        ('delete-restart', 'tools/fakeamps/journal.go',
         'if err := j.restoreDeleteRecord(recBuf); err != nil {\n\t\t\t\treturn err\n\t\t\t}',
         '// mutation: discard delete publisher sequence',
         './tools/fakeamps', '^TestFollowupSequencedDeleteSurvivesDiskRestart$'),
    ]
    results = []
    for name, file, before, after, package, pattern in mutations:
        original = ROOT / file
        source = original.read_text(encoding='utf-8')
        if source.count(before) != 1:
            raise ValueError('Real mutation target changed: ' + name)
        environment = dict(os.environ, GOTOOLCHAIN='go1.26.10')
        baseline = run_tool(['go', 'test', package, '-run', pattern, '-count=1', '-timeout=1m'],
                                  cwd=ROOT, capture_output=True, text=True, env=environment, timeout=90)
        baseline_output = baseline.stdout + baseline.stderr
        (output_root / (name + '.baseline.log')).write_text(baseline_output, encoding='utf-8')
        if baseline.returncode != 0 or 'no tests to run' in baseline_output:
            raise ValueError('Real Go baseline did not pass its regression: ' + name)
        mutant = output_root / (name + '.go.txt')
        mutant.write_text(source.replace(before, after), encoding='utf-8')
        overlay = output_root / (name + '.overlay.json')
        overlay.write_text(json.dumps({'Replace': {str(original): str(mutant)}}), encoding='utf-8')
        command = ['go', 'test', '-overlay', str(overlay), package, '-run', pattern, '-count=1', '-timeout=1m']
        process = run_tool(command, cwd=ROOT, capture_output=True, text=True, env=environment, timeout=90)
        output = process.stdout + process.stderr
        (output_root / (name + '.mutation.log')).write_text(output, encoding='utf-8')
        if process.returncode == 0 or '--- FAIL: Test' not in output or '[build failed]' in output or 'panic: test timed out' in output:
            raise ValueError('Real mutation was not killed by an assertion: ' + name)
        results.append({'mutation': name, 'test': pattern, 'killed': True})
        print('REAL MUTATION KILLED ' + name, flush=True)
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--jar')
    parser.add_argument('--out', type=Path)
    parser.add_argument('--record-sync', action='store_true')
    parser.add_argument('--models-only', action='store_true')
    args = parser.parse_args()
    sync_path = ROOT / 'formal/source-sync.json'
    if args.record_sync:
        revision = run_tool(['git', 'rev-parse', 'HEAD'], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()
        record = {'base_commit': revision, 'uncommitted_sources': True, 'files': source_record(ROOT, COVERED)}
        sync_path.write_text(json.dumps(record, indent=2) + '\n', encoding='utf-8')
    record = json.loads(sync_path.read_text(encoding='utf-8'))
    require_source_sync(ROOT, record['files'])
    output_root = args.out.resolve() if args.out else Path(tempfile.mkdtemp(prefix='amps-formal-'))
    output_root.mkdir(parents=True, exist_ok=True)
    if any(output_root.iterdir()):
        raise ValueError('Output directory must be empty: ' + str(output_root))
    jar = checker_jar(args.jar)
    results = [run_case(item, jar, output_root) for item in cases()]
    mutations = [] if args.models_only else real_mutations(output_root)
    require_source_sync(ROOT, record['files'])
    summary = {'tlc_version': TLC_VERSION, 'tlc_sha256': TLC_SHA256,
               'source_sync': record, 'models': results, 'real_mutations': mutations}
    (output_root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n', encoding='utf-8')
    print('FORMAL CHECK PASS: ' + str(output_root), flush=True)


if __name__ == '__main__':
    main()
