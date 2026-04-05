---
name: release
description: "Release amps-client-go. Default: get main ready, push it, run the strict scripted release, and verify the published version."
---

# Release

`/release` means: release.

Default flow:

1. Get `main` releasable
2. Commit pending release changes if needed
3. Push `main`
4. Run the strict scripted release
5. Verify the published version

Use:

- real release: `make release-local RELEASE_VERSION=X.Y.Z RELEASE_FLAGS="-Yes"`
- dry run only: `make release-dry-run RELEASE_VERSION=X.Y.Z`
- gates only: `make release`
- hosted release: GitHub `Release` workflow on `amps-release`

Requirements:

- branch `main`
- clean tracked tree
- `..\amps-c++-client-5.3.5.1-Windows` exists
- `git`, `go`, `make`, and `gh` work

Done means:

- tag exists remotely
- GitHub release exists
- `go list -m -versions github.com/Thejuampi/amps-client-go` shows the version
