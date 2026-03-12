# Release Process (GitHub Actions, One Click)

This repository releases from a single manual GitHub Actions workflow run on `main`.

The workflow does all of the release work:

1. Updates `VERSION`, `README.md`, and `amps/client.go`
2. Runs release gates
3. Commits the version bump
4. Creates and pushes annotated tag `vX.Y.Z`
5. Publishes the GitHub Release

No local script, local tag creation, or local push is required for the normal path.

## One-time Setup

1. GitHub Actions must be enabled.
2. Default branch must be `main`.
3. The workflow token must be allowed to push the release commit to `main`.
   If `main` is protected, add GitHub Actions to the bypass list or otherwise allow the workflow's `GITHUB_TOKEN` to push release commits.

## Every Release

1. Open `Actions` in GitHub.
2. Choose the `Release` workflow.
3. Click `Run workflow` on branch `main`.
4. Enter the version in `X.Y.Z` format.
5. Click `Run workflow`.

That is the full release path.

## What The Workflow Checks

- The workflow is running from the latest `origin/main` commit.
- The version matches `X.Y.Z`.
- Tag `vX.Y.Z` does not already exist.
- Static analysis passes.
- Unit tests pass.
- Race tests pass.
- Build passes.
- Coverage gate passes.
- Parity check passes when the C++ reference tree is present.

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
  - Update branch protection so GitHub Actions can push release commits.
- Validation fails:
  - Fix the failing code or tests, then rerun the workflow.

## Legacy Fallback

`release.local.ps1` is still available as a manual fallback, but it is no longer the primary release path.

## Verification

```powershell
go list -m -versions github.com/Thejuampi/amps-client-go
```

Published release pages:

- `https://github.com/Thejuampi/amps-client-go/releases`
- `https://pkg.go.dev/github.com/Thejuampi/amps-client-go/amps`
