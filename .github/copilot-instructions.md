# Copilot Instructions — AMPS Go Client

## Big Picture

This is a Go reimplementation of the C++ AMPS client (target: v5.3.5.1), maintained in the workspace alongside the reference C++ client at `amps-c++-client-5.3.5.1-Windows/`. The Go client must maintain **behavioral parity** with the C++ client.

- Module: `github.com/Thejuampi/amps-client-go`
- Single public package: `amps` (all code lives in `amps/`)
- Internal helpers in `amps/internal/` (bookmark parser, WAL, replay sequencer, test utilities)
- Reference parity matrix: [`docs/cpp_to_go_parity_matrix.md`](../docs/cpp_to_go_parity_matrix.md)

## Key Components

| File(s) | Role |
|---|---|
| `client.go` | Core `Client` struct: connection, send/receive loop, ack routing |
| `ha_client.go` | `HAClient`: wraps `Client` with reconnect/resubscribe loop |
| `client_parity_methods.go` | Go implementations of C++ `Client` methods |
| `parity_types.go` | C++ type equivalents (`ConnectionInfo`, `ConnectionState`, listener interfaces) |
| `parity_version.go` | C++ bookmark/version compat aliases (`NOW()`, `BOOKMARK_EPOCH()`, etc.) |
| `command.go` | `Command` builder (sent to server); `Message` is received from server |
| `errors.go` | Error kinds + package-level godoc |
| `reconnect_strategy.go` | `FixedDelayStrategy`, `ExponentialDelayStrategy` |
| `server_chooser.go` | `DefaultServerChooser` — round-robins URIs for `HAClient` |

## Developer Workflows

```bash
make build            # go build ./...
make scan             # blocking code scan suite (static + vuln + secret)
make security-scan    # govulncheck + gitleaks
make secret-scan      # secret scan only
make test             # go test ./... -skip Integration
make test-race        # go test -race ./... -skip Integration
make integration-test # go test ./... -run Integration
make integration-fakeamps # run strict fakeamps-backed integration gates
make vet              # go vet
make fmt              # go fmt
make release          # static-scan + unit + race + build + fakeamps integration + parity + coverage + perf
```

## Coverage Policy (Enforced)

- Coverage gate is required before merge: `make coverage-check`.
- Code scanning gate is required before merge: `make scan`.
- `./amps/...` thresholds are strict: aggregate `>=90%`, pure files `100%`, IO/stateful files `>=80%`.
- Test design target for changed areas is `>=80%`, but gate values above are mandatory.

## Mutation Policy

- Mutation testing is mandatory throughout the full development lifecycle, not just at the end.
- During planning, identify the branches, guards, invariants, failure paths, and boundaries that a mutation could break.
- During RED, write tests that would fail if conditions are inverted, branches are deleted, counters are off by one, or error paths are skipped.
- During implementation, keep code small enough that each critical branch has a clear killing test.
- During review, explicitly ask which test fails if a branch is removed or a condition is inverted.
- Before completion, do manual mutation thinking and, for non-trivial changes, spot-check a few realistic mutations.
- Do not confuse high line coverage with strong tests; mutation resistance is required evidence of test quality.

## Release Gate Policy

- Release verification is `make release`.
- Release includes the blocking code scan suite (`make scan`), not just local linting.
- Release may not skip or relax fakeamps-backed integration.
- Release may not skip or relax parity validation.
- Strict release environments must provide the sibling C++ reference tree at `../amps-c++-client-5.3.5.1-Windows`.
- Strict release must run both:
  - `go test -count=1 ./amps -run Integration` against ephemeral fakeamps endpoints
  - `go test -count=1 ./tools/fakeamps -run Integration`
- SemVer selection for release:
  - patch for fixes, hardening, tests, and release-process-only changes
  - minor for backwards-compatible feature additions
  - major for breaking API or behavior changes

## Coding Conventions

- **Local variable declarations** — prefer `var` for clarity, but `:=` short declarations are allowed where concise and improve readability.
- **No FQN** — always use named imports; never `amps.NewClient` inside the package.
- **1 assert per test** — use soft assertions only when 2+ checks are strictly necessary.
- **Tests are whitebox** — all test files use `package amps` (not `package amps_test`).
- **KISS / DRY** — if a pattern appears twice, extract it.
- **Comments** — only when they explain intent the code doesn't convey.

## Error Handling

Always use `NewError(ErrorKind, message)`:

```go
return NewError(CommandError, "nil HAClient")
return NewError(ConnectionError, "no URI available")
```

Available error kinds: `CommandError`, `ConnectionError` (and others in `errors.go`).

## Parity Pattern

When adding a C++ API equivalent:

1. Add the Go type/interface to `parity_types.go` if it's a new concept.
2. Add the implementation method to `client_parity_methods.go`.
3. Add function-adapter types for interfaces (e.g., `ConnectionStateListenerFunc`).
4. Update `docs/cpp_to_go_parity_matrix.md`.

Getter/setter naming mirrors C++: `SetFoo(v)` / `Foo()`.

## Testing Patterns

- `test_helpers_test.go` provides `testConn` (fake `net.Conn` with `enqueueRead`) for unit tests without a real server.
- `internal/testutil.Counter` provides deterministic ID sequences.
- Integration tests require a live AMPS server and are tagged with `Integration` in their test name — run with `make integration-test`.
- For changed logic, tests should be strong enough to fail for deleted guards, inverted booleans, missing side effects, missing error propagation, and off-by-one behavior.

## HAClient Reconnect Flow

`HAClient` installs an internal disconnect handler on the underlying `Client`. On disconnect it spawns a goroutine that calls `ConnectAndLogon()` using the `ServerChooser` and `ReconnectDelayStrategy`. The `stopped` and `reconnecting` guards prevent concurrent reconnect loops.
