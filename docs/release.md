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
- native official C++ 5.3.5.14 interoperability against fakeamps
- parity check
- coverage gate
- performance gate

Only after `make release` passes may a version bump, push, tag, and GitHub Release happen.

## SemVer Choice

Pick the release version from the merged changes:

- patch `X.Y.Z+1`: bug fixes, hardening, tests, tooling, release-process-only changes
- minor `X.Y+1.0`: backwards-compatible features or public API additions
- major `X+1.0.0`: breaking API or behavior changes

The 1.0.0 release is a major bump: `cppcompat.CRC.Compute` changes its return
type from `uint32` to `uint64`, and corrected acknowledgement, stream, and query
semantics can require application changes. See the
[changelog](../CHANGELOG.md) and [migration guide](migration_guide.md).

Prepare `VERSION`, the README version, `amps.ClientVersion`, and the newest
dated changelog entry together. Tests require those markers to agree. The
module path stays `github.com/Thejuampi/amps-client-go` for v1.

The 1.1.0 release is a minor: it adds the public error types
`PublishStoreAcceptedError` and `PublishStoreUncertainError`, and
`MemoryPublishStore.NextPublishSequence`, while preserving existing client
signatures. Those compatible public additions require a minor version under
[SemVer](https://semver.org/spec/v2.0.0.html). Its local strict gate also runs
`make cpp-integration`, which requires Windows x64, Python, Visual Studio C++
Build Tools, and the external unmodified Windows C++ 5.3.5.14 SDK. Supply its
location with `CPP_SDK_ROOT`; the default is the local parity-audit cache.
Missing prerequisites fail this gate. See the
[C++ interoperability report](cpp_fakeamps_interop.md) for coverage and limits.

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

The `Release` workflow runs `make release-hosted`.

That keeps the same release gates as local release, except parity is conditional:

- if `../amps-c++-client-5.3.5.1-Windows` exists on the runner, parity runs
- if it does not exist, parity is skipped and the rest of the hosted release gates still run

Use local `make release` when you need the full strict parity-validated release path.

The hosted target does not include the Windows native C++ probe. A hosted
release alone therefore does not establish the C++ interoperability result
recorded for a locally validated release.

The workflow accepts a version already prepared and committed on `main`. It
runs all gates and skips the version-bump commit when the markers already
match, then creates the requested tag and release. The changelog must contain
the corresponding version before running the workflow. A dry run still skips
committing, tagging, pushing, and publishing.

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
