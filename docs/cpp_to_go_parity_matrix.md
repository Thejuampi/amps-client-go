# C++ to Go Surface Mapping and Behavior Review

The [native C++/fakeamps check](cpp_fakeamps_interop.md) additionally exercises
the official C++ 5.3.5.14 client as a broker test client. It corrects delivery
route lists, bookmark recognition, escaped topics, lease fallback and explicit
OOF behavior. This is separate from public-symbol presence and does not
certify complete server compatibility.

The behavior review uses **C++ 5.3.5.14** (verified again 2026-11-01).
The [audit](behavior_parity_audit.md) records the original implementation differences,
with a [753-declaration inventory](behavior_parity_inventory.md).
The original 23 finding groups have their
[corrections and regression tests](behavior_parity_fixes.md). The
[second audit](behavior_parity_second_audit.md) reproduced **16 finding
groups**, including the previously noted fakeamps reconnect limitation, with
19 probe functions. Those functions now run in the normal suite alongside
additional boundary regressions. The [correction report](behavior_parity_second_fixes.md)
records the fixes, independent review and release gates. The
[gap register](gap_register.md) has **0 open entries** across 53 contracts, including the native C++ interoperability contracts.
Symbol presence is not behavioral equivalence.

The 2026-10-26 [stress follow-up](behavior_parity_fixes.md#seguimiento-del-stress-2026-10-26)
adds deterministic regressions for a disconnect at HA recovery completion and
processed-ack warning lifecycle. The stress gate now includes fakeamps and both
integration suites with race detection and shuffled ordering.

## Symbol Mapping

The API surface mapping is recorded in a machine-readable manifest:

- `tools/parity_manifest.json`

Validation command:

```bash
go run ./tools/paritycheck -manifest tools/parity_manifest.json
```

Current gate result (manifest in this repository revision):

- `PARITY_ENTRIES=261`
- `MISSING_HEADER_SYMBOLS=0`
- `MISSING_GO_SYMBOLS=0`

## Coverage Domains

| Domain | C++/C Source | Go Target |
|---|---|---|
| `Client` parity surface | `ampsplusplus.hpp` | `amps` |
| `HAClient` parity surface | `HAClient.hpp` | `amps` |
| `Command` parity surface | `ampsplusplus.hpp` | `amps` |
| `Message` parity surface | `Message.hpp` | `amps` |
| `MessageStream` parity surface | `ampsplusplus.hpp` | `amps` |
| Utility/model families | utility headers | `amps`, `amps/cppcompat` |
| Store/recovery families | bookmark/publish/recovery headers | `amps/cppcompat` |
| FIX family | `ampsplusplus.hpp` | `amps/cppcompat` |
| C handle API | `amps.h` | `amps/capi` |
| SSL compatibility API | `amps_ssl.h` and `amps.h` SSL entrypoints | `amps/capi` |
| zlib compatibility API | `amps_zlib.h` | `amps/capi` |

## Mapping Rules

- C++ overloads are mapped to explicit Go methods.
- Existing Go API signatures remain backward compatible.
- Compatibility entrypoints are additive.
- Deprecated C++ APIs are excluded from required parity scope.
- C API thread counters map to cumulative receive-lifecycle counters (`create`/`join-like wait`/`detach-like stop`) rather than raw OS thread primitives.
- `Options` helpers from `Message.hpp` map onto `Command` option helpers in Go, including `send_keys`, `fully_durable`, `max_backlog`, and `bookmark_not_found`.

## Related Documentation

WebSocket is a Go transport extension. Its framing is checked against the
official JavaScript client and strict wire tests, rather than inferred from
native C++ TCP framing. See the [WebSocket correction](websocket_protocol_fix.md)
for the protocol, regression coverage and remaining deployment limits.

- [Supported Scope and Constraints](supported_scope.md)
- [Reference: C API Compatibility](capi_reference.md)
- [Reference: C++ Compatibility](cppcompat_reference.md)
- [Testing and Validation](testing_and_validation.md)
