# AGENTS Guide for `amps-client-go`

This file is for autonomous coding agents working in this repository.
Follow it strictly.

## Rule Sources and Precedence
1. Direct user request in the current task.
2. This `AGENTS.md`.
3. `.github/copilot-instructions.md`.
4. Existing repository conventions in code/tests.

Cursor-specific rules:
- No `.cursor/rules/` directory exists.
- No `.cursorrules` file exists.

Copilot-specific rules:
- `.github/copilot-instructions.md` exists and is mandatory.
- This guide merges its important points.

## Project Context
- Module: `github.com/Thejuampi/amps-client-go`
- Go version: `1.25`
- Public package: `amps`
- Internal helpers: `amps/internal/...`
- Fake broker: `tools/fakeamps`
- Parity checker: `tools/paritycheck`
- Coverage gate: `tools/coveragegate`
- Perf tooling: `tools/perfgate`, `tools/perfreport`

## Non-Negotiable Engineering Principles
- TDD is mandatory for every request.
- Always do `RED -> GREEN -> REFACTOR`.
- Always follow `KISS`, `DRY`, `SOLID`, and `YAGNI`.
- Every behavior change requires tests, even tiny ones.
- Example: timeout `1 -> 2` must have tests for both `1` and `2`.

## Build, Lint, and Test Commands
Preferred Make targets:
```bash
make build
make test
make test-race
make integration-test
make fmt
make vet
make parity-check
make coverage-check
make perf-check
make release
```

Equivalent direct commands:
```bash
go build ./...
go test ./...
go test -race ./...
go test ./... -run Integration
go fmt ./...
go vet ./...
go run ./tools/paritycheck -manifest tools/parity_manifest.json -behavior-manifest tools/parity_behavior_manifest.json
go test -count=1 ./amps/... -coverprofile=coverage.out
go run ./tools/coveragegate -profile coverage.out
go run ./tools/perfgate -baseline tools/perf_baseline.json
```

### Focused test commands (important)
Single test in one package:
```bash
go test ./amps -run '^TestClientConnect$' -count=1
```

Single fake broker test:
```bash
go test ./tools/fakeamps -run '^TestHandleConnectionCommandFlow$' -count=1
```

Single subtest:
```bash
go test ./amps -run '^TestClientConnect$/ReconnectPath$' -count=1
```
Verbose single test:
```bash
go test ./amps -run '^TestName$' -count=1 -v
```

## Coverage and Mutation Expectations
- Test design target for changed feature areas: `>= 80%` coverage.
- Coverage gate is mandatory and must pass before merge (`make coverage-check`).
- Enforced repository gate for `./amps/...`:
  - aggregate `>= 90%`
  - pure files `100%`
  - IO/stateful files `>= 80%`

Quick coverage checks:
```bash
go test ./tools/fakeamps -coverprofile tools/fakeamps.cover.out
go tool cover -func tools/fakeamps.cover.out
go test -count=1 ./amps/... -coverprofile=coverage.out
go run ./tools/coveragegate -profile coverage.out
```

Mutation requirements:
- Mutation testing is mandatory in practice.
- If tests still pass after random logic changes, tests are not strong enough.
- For every critical branch, add assertions that fail when logic is inverted/removed.

## Required TDD Workflow
1. RED: write/adjust failing tests first.
2. GREEN: implement minimal code to pass.
3. REFACTOR: improve design while keeping tests green.
4. Re-run focused tests, then package tests, then `go test ./...`.
5. Re-check coverage for changed packages.
Never skip RED.

## Code Style and Conventions
### Formatting and structure
- `gofmt` is source of truth.
- Keep functions small and intention-revealing.
- Avoid over-engineering (YAGNI).

### Imports
- Use imports, never fully qualified names inside same package.
- Keep import groups in standard Go order.
- Avoid alias imports unless required for collisions.

### Variables and types
- Use `var` for local variable declarations.
- Prefer concrete types internally; interfaces at boundaries.
- Keep exported API shapes aligned with C++ parity intent.

### Naming
- Follow Go naming (`CamelCase`, exported names capitalized).
- Keep parity naming style where relevant: `SetFoo(...)` and `Foo()`.
- Use consistent domain terms (command id, sub id, bookmark, publish store).

### Error handling
- In `amps` package code, use `NewError(...)` with correct kind.
- Return early on errors and include actionable context.
- Do not swallow errors silently.

### Comments
- Add comments only for non-obvious intent/protocol semantics.
- Do not restate obvious code.

## Testing Conventions
- Prefer white-box tests in same package (`package amps` for amps tests).
- Keep tests deterministic and fast.
- One assert per test is preferred; split tests if assertions are independent.
- For matrix inputs, use table-driven tests.
- Cover happy path, edge cases, and failures.

## Parity-Driven Development Rules
- This repo targets behavioral parity with C++ AMPS client semantics.
- For parity features, update implementation + tests + parity docs/manifests:
  - `docs/cpp_to_go_parity_matrix.md`
  - `tools/parity_manifest.json`
  - `tools/parity_behavior_manifest.json`

## Agent Completion Checklist
- RED test added and verified failing.
- GREEN implementation added and tests passing.
- Focused tests run.
- `go test ./...` run.
- Coverage gate verified (`make coverage-check` or equivalent).
- Mutation resistance evaluated and strengthened.

When in doubt: choose simpler design, stronger tests, and explicit behavior.
