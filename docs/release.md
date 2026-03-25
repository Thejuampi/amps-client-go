# Release Process

Strict releases in this repository are driven by `make release` from an environment that has both:

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
2. updates `VERSION`, `README.md`, and `amps/client.go`
3. runs `make release`
4. commits `release: vX.Y.Z`
5. creates and pushes annotated tag `vX.Y.Z`
6. publishes the GitHub Release with generated notes via `gh release create`

Run it from `main` on a machine that has the sibling C++ reference tree.

## GitHub Actions Release Workflow

The `Release` workflow also uses the strict release gates, but it requires a prepared runner with the sibling C++ reference tree available at `../amps-c++-client-5.3.5.1-Windows`.

If that tree is missing, the workflow must fail rather than skip parity.

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
2. Run `make release` and confirm every gate passes.
3. Run `release.local.ps1` and provide the chosen `X.Y.Z` version.

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
- Parity check passes with the sibling C++ reference tree present.
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

- Workflow fails because the C++ reference tree is missing:
  - Use a prepared runner that provides `../amps-c++-client-5.3.5.1-Windows`.
  - Or perform the strict release locally with `release.local.ps1`.

## Verification

```powershell
go list -m -versions github.com/Thejuampi/amps-client-go
```

Published release pages:

- `https://github.com/Thejuampi/amps-client-go/releases`
- `https://pkg.go.dev/github.com/Thejuampi/amps-client-go/amps`
