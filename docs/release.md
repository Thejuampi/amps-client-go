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

The release perf gate is confirmatory rather than one-shot: `tools/perfgate` first runs the full benchmark set normally, then reruns only initially failing microbenchmarks with a longer benchtime before declaring a real regression. That keeps release validation strict while reducing false failures from shared-host benchmark noise.

Run it from `main` on a machine that has the sibling C++ reference tree.

## GitHub Actions Release Workflow

The `Release` workflow runs `make release-hosted`.

That keeps the same release gates as local release, except parity is conditional:

- if `../amps-c++-client-5.3.5.1-Windows` exists on the runner, parity runs
- if it does not exist, parity is skipped and the rest of the hosted release gates still run

Use local `make release` when you need the full strict parity-validated release path.

## One-time Setup

1. GitHub Actions must be enabled.
2. Default branch must be `main`.
3. For local strict release, install `git`, `go`, `make`, and `gh`.
4. Prefer configuring a repository Actions secret named `RELEASE_PUSH_TOKEN`.
  It should contain a token for a user or bot that can push to `main` and create tags.
5. If `main` is protected, that token must be allowed to bypass branch protection.
  If you do not configure `RELEASE_PUSH_TOKEN`, the workflow falls back to `GITHUB_TOKEN`, which only works when GitHub Actions itself is allowed to push release commits to `main`.

## Every Release

Local strict path:

1. Ensure `../amps-c++-client-5.3.5.1-Windows` exists next to this repository.
2. On Windows, run `.\tools\static-scan-linux.ps1` before commit when touching files with non-Windows build tags, or let `.\release.local.ps1` run that preflight for you during release prep.
3. Optionally run `make release` manually if you want a dry run before changing any release files.
4. Run `.\release.local.ps1` and provide the chosen `X.Y.Z` version.

Prepared-runner GitHub Actions path:

1. Open `Actions` in GitHub.
2. Choose the `Release` workflow.
3. Click `Run workflow` on branch `main`.
4. Enter the version in `X.Y.Z` format.
5. Leave `dry_run` unchecked for a real release.
6. Click `Run workflow`.

That is the full release path.

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
- Static analysis passes.
- Unit tests pass.
- Race tests pass.
- Build passes.
- fakeamps-backed `./amps` integration tests pass.
- `tools/fakeamps` integration tests pass.
- Coverage gate passes.
- Parity check passes when the sibling C++ reference tree is present.
- Perf gate passes.

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
  - Configure `RELEASE_PUSH_TOKEN` with a token that can bypass branch protection and push tags.
  - Or update branch protection so GitHub Actions and the workflow `GITHUB_TOKEN` can push release commits.
- Validation fails:
  - Fix the failing code or tests, then rerun the workflow.
- Linux CI static analysis fails on files that were not checked locally on Windows:
  - Run `.\tools\static-scan-linux.ps1`.
  - Fix the reported `//go:build !windows` or other Linux-target issues before commit or release prep.

- Workflow ran without parity because the C++ reference tree is missing:
  - Use a runner that provides `../amps-c++-client-5.3.5.1-Windows` if you want parity in GitHub Actions.
  - Or perform the strict local release path with `release.local.ps1`.

## Verification

```powershell
go list -m -versions github.com/Thejuampi/amps-client-go
```

Published release pages:

- `https://github.com/Thejuampi/amps-client-go/releases`
- `https://pkg.go.dev/github.com/Thejuampi/amps-client-go/amps`
