# Changelog

## [Unreleased]

- Correct AMPS WebSocket negotiation and multipart framing in the Go client
  and fakeamps. Deliver complete SOW records before waiting for the rest of a
  batch; preserve terminal write errors without retransmitting partial frames.
- Honor cancellation during WebSocket initialization and make concurrent
  deadlines interrupt active writes over both WS and WSS.
- Replace cross-host absolute performance approval and favorable retries with
  a fixed historical reference, frozen inputs, equal iteration counts in
  alternating pairs, exact allocation counters and retained evidence. Existing
  baseline values, regression budgets and comparison slack remain unchanged.
- Reduce header parsing and message routing overhead while preserving escaped
  headers, per-route duplicate checks, acknowledgements and reentrant global
  handlers. The paired gate checks these paths against the fixed reference.
- Copy active queue entries in two contiguous spans when growing the ring,
  and remove an unused condition variable and its notifications. Preserve FIFO,
  depth rejection, close behavior and waiting consumers.
- Avoid conflation-map locking for streams that have never enabled conflation;
  preserve synchronization after activation, reconfiguration and close.
- Keep GoSec's blocking and SARIF scans out of ignored compiler scratch files,
  so parallel test builds cannot be mistaken for project sources.

## [1.1.0] - 2026-11-02

This minor release corrects the 16 finding groups from the second behavioral review,
strengthens fakeamps interoperability with the official C++ 5.3.5.14 client,
and reduces release-validation time. Existing public client signatures and the
Go module path remain unchanged. The new exported error types and sequence
accessor require a minor version under [SemVer](https://semver.org/spec/v2.0.0.html).

### Compatible public API additions

- `PublishStoreAcceptedError` identifies maintenance failures after acceptance
  and exposes the underlying error through `Unwrap`.
- `PublishStoreUncertainError` exposes the attempted sequence and cause when a
  durable rollback cannot be confirmed.
- `MemoryPublishStore.NextPublishSequence` reports the next automatic sequence
  and whether the sequence space remains available.

### Recovery and client correctness

- Serialize retained publish and delete commands with replay, connection
  generations, and the sequence floor received during logon.
- Repair incomplete WAL tails before appending. Preserve accepted messages
  after maintenance failures, roll back confirmed rejections, and retain
  evidence when a durable outcome is uncertain.
- Coordinate hybrid-store sequence floors and preserve persisted-ack requests
  when replaying both new and legacy retained commands.
- Apply one HA connection budget to transport preflight, logon, and recovery;
  prevent stale callbacks and workers from replacing a recovered session or
  dropping a newer reconnect request.
- Preserve pending queue acknowledgements and retained-command handlers across
  disconnects. Correct failed-handler cancellation and AutoAck ownership.
- Decode escaped JSON headers for getters, routing, and copies; reject invalid
  escapes and emit valid JSON controls and Unicode from message builders.
- Synchronize logon metadata readers and reject obsolete subscription-recovery
  snapshots after unsubscribe.

### Fake broker interoperability

- Recover publisher identity and publish/delete watermarks from disk journals;
  preserve distinct commands that reuse a command ID.
- Emit subscription route lists consumed by the official C++ client for live,
  queue, and out-of-focus deliveries, and escape their topic headers.
- Accept both bookmark field spellings used by clients. Recognize bookmark
  subscriptions without a Go-specific option, honor EPOCH and explicit resume
  positions, and keep NOW subscriptions from replaying historical messages.
- Redeliver an expired queue lease to the original eligible subscriber when no
  alternative can accept it; distinguish subscription ownership by connection.
- Honor explicit OOF subscriptions independently of delta commands, including
  delete and filter-mismatch events.
- Build owned SOW frames with fewer copies and allocations, and escape topics
  consistently in both SOW headers.

New journal readers accept older records. Downgrading fakeamps after writing
new typed metadata is not guaranteed. Its journal and SOW snapshot remain
separate files, without an atomic transaction across a power failure.

### Tests and release validation

- Promote diagnostic regressions into the normal suite, strengthen mutation
  checks, and restore unit tests accidentally excluded by the Integration name
  filter.
- Read the transport under its state lock in the compression regression test,
  fixing the race found by the Linux pre-production stress run.
- Add a strict native C++ interoperability gate to local `make release`, using
  the unmodified pinned SDK library against isolated fakeamps processes.
- Add bounded TLA+/TLC checks for recovery, reconnect ownership, publisher
  deduplication, C++ replay routing, and queue lease dispatch, with broken
  variants and corresponding real-code mutations.
- Run two isolated integration workers while preserving the 20 stress rounds,
  race detection, datasets, timing limits, and performance baseline. Remove
  repeated JSON decoding, redundant frame copies, and duplicate CI scanning.
- Keep Windows test and tooling subprocesses hidden. Remove race-runtime exit
  delays only from isolated native console probes.

See the [second fix record](docs/behavior_parity_second_fixes.md) and
[native C++ interoperability report](docs/cpp_fakeamps_interop.md) for contracts
and scope, and the [release timing evidence](docs/test_suite_timings.md) for
the measured validation costs.

## [1.0.0] - 2026-10-27

This major release consolidates the behavioral parity review against C++
5.3.5.14, the stress fixes, and the patched release toolchain. The module path
remains `github.com/Thejuampi/amps-client-go`.

### Breaking changes and migration

- `cppcompat.CRC.Compute` returns `uint64` and computes the portable C++ 64-bit
  checksum instead of CRC32 IEEE. Update variables, interfaces, and persisted
  checksum fields that expect `uint32`.
- The optional third argument of `Client.Ack` is acknowledgement options, such
  as `cancel`, rather than a subscription id.
- `Command.SetTopN(0)` explicitly requests zero results. Use
  `uint(^uint32(0))` to clear the limit.
- Queue streams defer auto-ack until the consumer advances past the delivered
  message. Bounded streams retain messages and apply backpressure instead of
  evicting unread entries; advance or close streams to release blocked reads.
- `RingSize` limits retained history, while active publishes remain available
  for replay even when they exceed that target.
- New publish stores start with time-based sequences; recovered stores retain
  their recorded sequence history.

See the [migration guide](docs/migration_guide.md) for application changes.
Existing WAL/checkpoint formats remain readable, including legacy
`discard_upto` records. Previously lost entries cannot be reconstructed.

### Client correctness

- Correct all 23 finding groups from the public-function behavior review,
  including acknowledgement encoding, queue batching, auto-ack timing,
  backpressure, and routing by message identifiers.
- Preserve unprocessed bookmarks during out-of-order discard and reconnect;
  suppress discarded duplicates separately for each subscription.
- Retain publishes after failed sends or disconnects, preserve message identity
  after partial writes, and coordinate replay sequences across hybrid stores.
- Avoid duplicate subscription replay within one connection generation and
  propagate recovery and authenticator failures to their callers.
- Apply connection URI options and flush publish batches at size, time, control
  command, and session boundaries.
- Correct header-key matching, empty composite parts, NVFIX sizes, server error
  categories, named-version handling, and C compatibility field mappings.
- Recover when another disconnect arrives as HA recovery completes, and
  suppress obsolete processed-ack warnings after an ack has arrived.
- Include fixes accumulated since 0.8.20 for logon locking, disconnected-error
  classification, heartbeat configuration, empty-read spinning, stream timeout
  and lifecycle races, conflation, and publish/bookmark retention.

### Fake broker and validation

- Improve fakeamps queue acknowledgements, duplicate handling, replay, protocol
  frames, and subscription behavior alongside the client regressions.
- Flush acknowledgements at FIFO boundaries before later queued SOW data,
  preserving earlier data order and normal data-frame coalescing.
- Add deterministic HA and ack-lifecycle reproductions, bounded failure traces,
  and fixes for shuffled-test configuration and completion-event isolation.
- Include all packages and both fakeamps-backed integration suites in
  `stress-check`, with race and shuffle. Keep the default 20 repetitions and
  existing timing limits.
- Promote the 32 original diagnostic cases into the ordinary test suite and
  track the 748-declaration review, 261 symbol mappings, and 35 closed behavior
  contracts. Compatibility limits remain explicit in the
  [audit](docs/behavior_parity_audit.md) and [supported scope](docs/supported_scope.md).

### Security and toolchain

- Pin Go 1.26.10 for release builds, scanners, performance checks, and nested Go
  processes; update CI, nightly, release, and CodeQL toolchain selections.
- Resolve the nine reachable standard-library findings GO-2026-6809,
  GO-2026-6617, GO-2026-6613, GO-2026-6612, GO-2026-6611, GO-2026-6608,
  GO-2026-6607, GO-2026-6604, and GO-2026-6603.
- Preserve the Go 1.25 language floor and test exact Go 1.25.13 compatibility
  without automatic upgrades. Applications consuming this module must select
  their own patched compiler.
- Let the release workflow publish an already prepared version without an
  empty version-bump commit; keep README version updates working with LF and
  CRLF line endings.

### Verification

- The complete strict release pipeline passed with Go 1.26.10: scans, leak
  checks, four fuzzers, 20 shuffled race repetitions of all packages, and
  20 successful repetitions of each integration suite.
- Coverage passed at 91.7% aggregate. Performance passed the original committed
  baseline and limits, with unchanged allocation counts in the toolchain
  comparison. The synthetic benchmark time geomean increased 7.59% from
  Go 1.25.13; Go 1.27.3 measured 14.36% higher and was not selected.
- Validation used Windows/amd64 and ephemeral fakeamps brokers. A live AMPS
  server was not configured; the review does not certify full server parity.
