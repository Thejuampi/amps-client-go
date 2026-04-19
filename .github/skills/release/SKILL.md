---
name: release
description: "Release amps-client-go. Use when preparing a releasable main branch, triaging release or CI failures, running the strict local release, or validating the hosted release workflow."
---

# Release

`/release` means: release.

Default flow:

1. Get `main` releasable
2. Run the blocking gates locally
3. Push fixes and inspect GitHub Actions if anything failed
4. Run the strict local or hosted release path
5. Verify the published version and release artifacts

Use:

- scan only: `make scan`
- strict local gates: `make release`
- hosted-equivalent gates: `make release-hosted`
- strict local release: `.\release.local.ps1`
- hosted release: GitHub `Release` workflow on `main`

Requirements:

- branch `main`
- clean tracked tree
- `..\amps-c++-client-5.3.5.1-Windows` exists
- `git`, `go`, `make`, and `gh` work

Local release rules:

- `make scan` is mandatory before release
- `make release` is the strict local release gate
- `make release-hosted` matches the hosted workflow and includes `scan`, `perf-check`, tests, build, integration, parity-if-available, and coverage
- hosted release no longer skips the perf gate

GitHub Actions triage:

- list recent runs: `gh run list --repo Thejuampi/amps-client-go --limit 20`
- inspect a failed run: `gh run view <run-id> --repo Thejuampi/amps-client-go --log-failed`
- if a PR has old bot comments but the latest `CI` and `CodeQL` runs are green, treat those comments as stale unless a current local repro still exists

Version choice:

- patch: fixes, hardening, tests, tooling, release-process-only changes
- minor: backward-compatible features or public API additions
- major: breaking API or behavior changes

Done means:

- latest `main` push has green `CI` and `CodeQL`
- tag exists remotely
- GitHub release exists
- `go list -m -versions github.com/Thejuampi/amps-client-go` shows the version
