# C++ 5.3.5.1 to Go Parity Matrix

## Source of Truth

Parity is defined by a machine-readable manifest:

- `tools/parity_manifest.json`

Validation command:

```bash
go run ./tools/paritycheck -manifest tools/parity_manifest.json
```

Current gate result (manifest in this repository revision):

- `PARITY_ENTRIES=253`
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

## Related Documentation

- [Supported Scope and Constraints](supported_scope.md)
- [Reference: C API Compatibility](capi_reference.md)
- [Reference: C++ Compatibility](cppcompat_reference.md)
- [Testing and Validation](testing_and_validation.md)
