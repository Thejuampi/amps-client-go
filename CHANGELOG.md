# Changelog

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
