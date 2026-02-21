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
make test             # go test ./...
make test-race        # go test -race ./...
make integration-test # go test ./... -run Integration
make vet              # go vet
make fmt              # go fmt
make release          # vet + test + build (release gate)
```

## Coding Conventions

- **`var` for all local variables** — never `:=` short declarations (per project preference).
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

## HAClient Reconnect Flow

`HAClient` installs an internal disconnect handler on the underlying `Client`. On disconnect it spawns a goroutine that calls `ConnectAndLogon()` using the `ServerChooser` and `ReconnectDelayStrategy`. The `stopped` and `reconnecting` guards prevent concurrent reconnect loops.
