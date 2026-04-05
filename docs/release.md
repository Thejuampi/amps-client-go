# Release Process

Strict local releases in this repository are driven by `make release` from an environment that has both:

- the sibling C++ parity reference tree at `../amps-c++-client-5.3.5.1-Windows`
- the ability to build and run `tools/fakeamps`

No release path may skip or relax those prerequisites.

The canonical gate is:

```bash
make release
```

That target runs:

- static analysis
- unit tests
- race tests
- build
- fakeamps-backed integration for `./amps`
- `tools/fakeamps` integration tests
- parity check
- coverage gate
- performance gate

Only after `make release` passes may a version bump, push, tag, and GitHub Release happen.

## SemVer Choice

Pick the release version from the merged changes:

- patch `X.Y.Z+1`: bug fixes, hardening, tests, tooling, release-process-only changes
- minor `X.Y+1.0`: backwards-compatible features or public API additions
- major `X+1.0.0`: breaking API or behavior changes

For the current change set, the correct bump is a patch release.

## Local Strict Release

`release.local.ps1` is the strict local release path. It:

1. validates branch and clean-tree state
2. runs `.\tools\static-scan-linux.ps1` before editing version files so Windows release prep also catches `//go:build !windows` static-analysis failures
3. updates `VERSION`, `README.md`, and `amps/client.go`
4. runs `make release`
5. commits `release: vX.Y.Z`
6. creates and pushes annotated tag `vX.Y.Z`
7. publishes the GitHub Release with generated notes via `gh release create`
8. verifies the remote tag, GitHub release, and Go module version visibility before declaring success

The release perf gate is confirmatory rather than one-shot: `tools/perfgate` first runs the full benchmark set normally, then reruns only initially failing microbenchmarks with a longer benchtime before declaring a real regression. That keeps release validation strict while reducing false failures from shared-host benchmark noise.

Run it from `main` on a machine that has the sibling C++ reference tree.

Convenience targets:

```bash
make release
make release-dry-run RELEASE_VERSION=0.8.10
make release-local RELEASE_VERSION=0.8.10 RELEASE_FLAGS="-Yes"
```

- `make release` is verification only.
- `make release-dry-run` runs the scripted release flow and then restores the version files instead of committing or publishing.
- `make release-local` runs the local release script, optionally non-interactively when `RELEASE_VERSION` and `RELEASE_FLAGS="-Yes"` are provided.

## GitHub Actions Release Workflow

The `Release` workflow now runs the same `release.local.ps1` path in hosted mode on `ubuntu-latest`.

Hosted mode uses `make release-hosted`, so the workflow keeps the same scripted version/tag/publish logic while using the hosted gate target where parity is conditional and the microbenchmark perf gate is intentionally skipped.

That split is deliberate: the perf baseline is calibrated for the strict local release environment, not for shared GitHub-hosted Linux runners. Hosted workflow releases stay deterministic by enforcing the functional and coverage gates there while keeping perf as a strict local pre-release requirement.

## One-time Setup

1. GitHub Actions must be enabled.
2. Default branch must be `main`.
3. For local strict release, install `git`, `go`, `make`, and `gh`.
4. Configure a repository Actions secret named `RELEASE_PUSH_TOKEN`.
  It must contain a token for a user or bot that can push to `main`, create tags, and create GitHub releases.
5. If `main` is protected, that token must be allowed to bypass branch protection.

## Every Release

Local strict path:

1. Ensure `../amps-c++-client-5.3.5.1-Windows` exists next to this repository.
2. On Windows, run `.\tools\static-scan-linux.ps1` before commit when touching files with non-Windows build tags, or let `.\release.local.ps1` run that preflight for you during release prep.
3. Optionally run `make release` manually if you want a verification-only pass before changing any release files.
4. Prefer `make release-dry-run RELEASE_VERSION=X.Y.Z` as the rehearsal path.
5. Run `make release-local RELEASE_VERSION=X.Y.Z RELEASE_FLAGS="-Yes"` for the non-interactive scripted publish, or `.\release.local.ps1` if you want prompts.

Prepared-runner GitHub Actions path:

1. Open `Actions` in GitHub.
2. Choose the `Release` workflow.
3. Click `Run workflow` on branch `main`.
4. Enter the version in `X.Y.Z` format.
5. Leave `dry_run` unchecked for a real release.
6. Click `Run workflow`.

That is the full release path. The workflow uses `release.local.ps1 -Hosted`, so dry runs and real releases follow the same scripted release logic as local release.

## Safe Test

Use the same workflow with `dry_run=true` and a version that does not exist yet.

That runs the full validation path and version-file rewrite in the runner, but it does not:

- commit to `main`
- create a tag
- push anything
- publish a GitHub Release

## What The Workflow Checks

- The workflow is running from the latest `origin/main` commit.
- The version matches `X.Y.Z`.
- Tag `vX.Y.Z` does not already exist.
- The hosted workflow runner has `git`, `go`, `make`, `gh`, and `pwsh`.
- The same scripted release path as local release succeeds in hosted mode, with the hosted-safe gate set (`make release-hosted`).

## What Gets Published

- Commit on `main`: `release: vX.Y.Z`
- Annotated tag: `vX.Y.Z`
- GitHub Release with generated notes
- Best-effort warm-up requests for Go proxy and `pkg.go.dev`

## Troubleshooting

- Workflow says it is not running from latest `main`:
  - Re-run it from the newest `main` commit.
- Workflow says tag already exists:
  - Use a new version.
- Push to `main` fails from the workflow:
  - Configure `RELEASE_PUSH_TOKEN` with a token that can bypass branch protection, push tags, and create releases.
- Validation fails:
  - Fix the failing code or tests, then rerun the workflow.
- Linux CI static analysis fails on files that were not checked locally on Windows:
  - Run `.\tools\static-scan-linux.ps1`.
  - Fix the reported `//go:build !windows` or other Linux-target issues before commit or release prep.
- Workflow says required tools are missing:
  - Fix the runner image or workflow environment so `git`, `go`, `make`, `gh`, and `pwsh` are available.

## Verification

```powershell
go list -m -versions github.com/Thejuampi/amps-client-go
```

Published release pages:

- `https://github.com/Thejuampi/amps-client-go/releases`
- `https://pkg.go.dev/github.com/Thejuampi/amps-client-go/amps`
