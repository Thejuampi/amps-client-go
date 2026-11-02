---
name: verify-amps-recovery
description: Recheck the bounded formal models of AMPS publish replay, WAL recovery, HA reconnect ownership and fakeamps journal deduplication after changing the covered Go functions or verification files.
---

# Verify AMPS recovery

Read [the scope and source mapping](../../../docs/formal_verification.md).
Read the `formal-check` skill and its Codex adapter from the installed pstack.
Use `maintain-formal-verification-skill` when a covered source hash has changed.

Run from the repository root:

```text
python tools/formalcheck/check.py
```

The runner checks the pinned TLC 1.7.4 checksum, requires a matching source
record, explores seven normal configurations, requires fifteen specific mutant
counterexamples, and checks five corresponding mutations in real Go code using
temporary overlays. The unmodified Go regressions must pass first. It preserves
raw TLC output, generated configs, baseline logs, mutant logs and summary.json
in the printed temporary directory. A parse error, missing trace, nonempty
search queue, zero explored states, source drift or surviving mutation fails.

Read every TLA+ result with the pstack `statechart` skill. Follow its current
instructions and use the repository's formal/statechart-view.mjs when useful.
Keep chart outputs in a new scratch folder outside the repository. Inspect the
raw counterexample and replay its actions against the Go source; a model error
is not a Go finding. Do not weaken a property to obtain a passing result.

Covered source: client.go and client_parity_methods.go (retention/replay),
ha_client.go (attempt and worker handoff), publish_store.go and internal/wal/wal.go
(WAL/checkpoint outcomes), and fakeamps dedupe.go, handler.go and journal.go
(named publisher identity and normal reopen). Exact hashes are in
formal/source-sync.json. The confirmed source commit is
22c28b481fc150cd984a21bac8bc97f67c9d855d. All covered sources were committed and
matched those hashes at synchronization. Verification-only commits may follow
without changing that source commit; the hash guard still applies.

After reviewing every covered source change against the transitions, run
`python tools/formalcheck/check.py --record-sync` to advance the source record.
Do not use that option merely to bypass a drift failure. When the fixes are
committed, review the source again and record that actual sync commit.

The model bounds and exclusions in the report are part of the result. This
checks abstract safety properties, not compiler refinement, all Go inputs,
unbounded executions, real-time deadlines, or end-to-end C++ equivalence.
The normal, race, fuzz, stress, coverage and performance gates remain required.
