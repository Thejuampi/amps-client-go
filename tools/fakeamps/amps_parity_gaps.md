# fakeamps behavioral conformance gaps

Reviewed 2026-10-24 against the current C++ 5.3.5.14 client source. This broker is
a test harness, not an implementation of every AMPS server feature. Client
source supplies wire contracts but cannot prove every server semantic.

The [behavior audit](../../docs/behavior_parity_audit.md) records evidence,
implemented fixes and the canonical prioritized broker gap list.

Implemented and tested in this revision:

- Exact JSON header keys, unescaping/Unicode, structured unknown fields and
  rejection of malformed headers.
- Unsupported commands fail even when requesting stats.
- C++ queue sow_delete acknowledgements release every batch with a fixed command
  id, preserve other topics and remove acknowledged SOW records; cancel preserves
  and requeues the record.
- Independent JSON fuzz oracle and focused parser and queue-ack benchmarks.
- FIFO acknowledgement flush boundaries, preserving earlier data and coalescing
  later data, with deterministic writer regressions and race/shuffle integration.

Highest-value remaining work:

- Retain publish-sequence deduplication across reconnects under the same publisher;
  test command-id scope and unsequenced publishes independently.
- Model session/logon state and unsupported-filter failures explicitly.
- Establish a deterministic SOW/live-delivery boundary.
- Inject disconnects around apply/ack, delayed independent ack stages and
  fragmented/coalesced frames.
- Bound frame and decompression sizes before allocating.
- Isolate broker instances and inject time for leases, TTL and heartbeats.

Existing implementations already include leases/redelivery, join/view exercises,
delta handling and several advanced filter forms. They require contract-specific
validation; presence does not prove server parity. The previous document listed
several of them as wholly absent and was no longer reliable.
