---
name: verify-cpp-fakeamps
description: Recheck bounded C++ subscription replay and fakeamps queue redelivery after changing their covered protocol, journal, routing, eligibility, or verification code.
---

# Verify C++ and fakeamps delivery

Read `docs/cpp_fakeamps_formal.md`, the installed pstack `formal-check` skill,
and its Codex adapter. Use `maintain-formal-verification-skill` to review covered
source drift before updating the verification record.

From the repository root run:

```text
python tools/formalcheck/cpp_check.py --sdk-root PATH_TO_SDK
```

The SDK argument may be omitted when the Windows C++ 5.3.5.14 SDK is at the
local default named in the report. Set `TLA2TOOLS` to the pinned TLC 1.7.4 jar
or let the existing checker download it and verify its SHA-256. Python, Java,
and Go 1.26.10 are required. This command is separate from normal Go tests and
does not build the native client. Use the native `tools/cppfakeamps` probe as
separate implementation evidence.

The runner checks the exact repository and SDK source/binary hashes, exhausts
four replay and five queue configurations, requires seven named broken-model
counterexamples, and kills six corresponding real Go source mutations after
unmodified regression tests pass. The two-owner recorded-lease variant is an
abstract structural mutation without a Go overlay. Raw TLC traces, configs,
mutation sources and logs are retained in the printed scratch directory.

Read every TLA+ result using pstack `statechart`, following its current
instructions; use `formal/cpp-statechart-view.mjs` when helpful and place its
outputs in a new scratch directory outside the repository. Replay each
counterexample against the mapped source and regression test. A model failure
is not automatically a product bug. Preserve the failing trace.

Covered Go functions are `parseAMPSHeader`, `buildPublishDelivery`,
`handleConnection`, `messageJournal.replayFrom`, `parseBookmarkSeq`,
`requeueExpiredLeases`, `requeueMessage`, `queueSubEligibleForDelivery`, and
`selectQueueRedeliveryTarget`; covered tests are in the sync record. C++
coverage includes `Client::subscribe`, its publish dispatch,
`MessageRouter::parseRoutes`, bookmark constants, and protocol field names.
Exact files and hashes, including the SDK library, are in
`formal/cpp-source-sync.json`. The confirmed source commit is
`22c28b481fc150cd984a21bac8bc97f67c9d855d`; no covered repository source changes
were pending at synchronization. Later verification-only commits may follow;
the source and SDK hash guards still apply.

If any covered hash changes, inspect each change against the model and tests,
including atomicity and failure paths. Update the model and source record only
after that review; never refresh hashes merely to make the command pass. Record
the new actual base commit and separately identify uncommitted sources, then
run this skill again and demonstrate a real-code break is caught.

The replay model does not cover concurrent snapshot and live publication. The
queue model assumes stable eligibility and serialized dispatch; in particular,
its recorded-lease invariant is not a proof of unique concurrent callbacks.
See the report for all bounds and exclusions. Run ordinary tests and release
gates separately.
