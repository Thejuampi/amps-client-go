# Release Process (GitHub Actions, Tag Driven)

This project releases from git tags named `vX.Y.Z` on branch `main`.

No PGP keys are required for this flow.

## One-time Setup

1. Commit and push release automation files:
   - `.github/workflows/ci.yml`
   - `.github/workflows/release.yml`
   - `.gitignore`
   - `docs/release.md`
2. Ensure GitHub Actions is enabled for the repository.
3. Confirm default branch is `main`.

## Every Release (Easy Path)

1. Open PowerShell in the repository root.
2. Run:

```powershell
pwsh -File .\release.local.ps1
```

3. Follow prompts in order:
   - Enter version in `X.Y.Z` format.
   - Confirm release when asked.
4. The script performs:
   - Preflight checks (tools, branch, clean tree, sync with `origin/main`, tags)
   - Version updates in `VERSION`, `README.md`, and `amps/client.go`
   - Release gates (`vet`, `test`, `build`, parity, coverage gate)
   - Commit, annotated tag, and push

## What Happens After Push

1. Pushing tag `vX.Y.Z` triggers `.github/workflows/release.yml`.
2. Workflow verifies `VERSION` matches tag.
3. Workflow reruns release gates.
4. Workflow creates a GitHub Release with generated notes.
5. Workflow sends best-effort requests to:
   - `https://proxy.golang.org/github.com/Thejuampi/amps-client-go/@v/vX.Y.Z.info`
   - `https://pkg.go.dev/github.com/Thejuampi/amps-client-go/amps@vX.Y.Z`

## Troubleshooting

- Dirty working tree:
  - Commit, stash, or discard local changes, then rerun script.
- Not on `main`:
  - Switch with `git checkout main`.
- Local branch not in sync with `origin/main`:
  - Pull fast-forward only:
    - `git pull --ff-only origin main`
  - Or push pending commits before releasing.
- Tag already exists:
  - Use a new version and tag.
- Coverage/parity gate failure:
  - Fix code/tests first, then rerun `release.local.ps1`.
- Release workflow failed:
  - Open GitHub Actions run logs and fix the failing step.

## Verification Commands

```powershell
go list -m -versions github.com/Thejuampi/amps-client-go
```

Open in browser after release:

- `https://github.com/Thejuampi/amps-client-go/releases`
- `https://pkg.go.dev/github.com/Thejuampi/amps-client-go/amps`
