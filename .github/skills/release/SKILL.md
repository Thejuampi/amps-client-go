---
name: release
description: Keep amps-client-go main release-ready. Use when preparing main for release, running release gates, rehearsing a release, publishing a version, or recovering from a failed release attempt.
---

# Release

Use this skill for versioned releases in `amps-client-go`.

## Default action

Unless the user explicitly asks for a different release task, the default action is:

1. get `main` into releasable condition
2. make sure all intended release changes are committed
3. make sure `main` is pushed and aligned with `origin/main`
4. run the full release gates
5. cut and publish the release through the scripted release path
6. verify the released tag, GitHub release, and Go module visibility

That means the normal flow is:

- all required changes committed
- pushed to `origin/main`
- release gates green
- no unresolved release blockers
- release published after the branch is proven ready

The repository has a strict release policy:

- do **not** skip `make release` locally
- do **not** skip fakeamps-backed integration
- do **not** skip parity when the local strict path is used
- do **not** edit release version files manually unless the user explicitly asks

## Primary sources

Read these first when working on a release:

1. `docs/release.md`
2. `README.md` release automation section
3. `Makefile`
4. `release.local.ps1`

## Decide the correct path

Choose the smallest safe path that satisfies the task:

1. **Verify gates only**
   - Use `make release`
   - This validates the release pipeline only
   - It does **not** bump versions, create tags, or publish

2. **Rehearse a release without publishing**
   - Use `make release-dry-run RELEASE_VERSION=X.Y.Z`
   - This runs the scripted release flow, then restores the version files automatically

3. **Publish a local strict release**
   - Use `make release-local RELEASE_VERSION=X.Y.Z RELEASE_FLAGS="-Yes"`
   - This runs the scripted release flow, commits the release, creates the tag, pushes, publishes the GitHub release, and verifies the remote publish state

4. **Use the hosted GitHub workflow**
   - Use the `Release` workflow when the user explicitly wants the GitHub Actions path
   - Remember that hosted release uses `make release-hosted`, so parity only runs if the sibling C++ reference tree is available on the runner

## Preconditions

Before any real release:

1. Ensure the branch is `main`
2. Ensure the tracked working tree is clean
3. Ensure the sibling C++ reference tree exists at `..\amps-c++-client-5.3.5.1-Windows`
4. Ensure `git`, `go`, `make`, and `gh` are available
5. Ensure credentials can push to `main`, push tags, and create GitHub releases
6. Choose the SemVer bump using the repository policy in `docs/release.md`

## Command guide

### Verification only

```bash
make release
```

### Dry-run rehearsal

```bash
make release-dry-run RELEASE_VERSION=0.8.10
```

### Real local release

```bash
make release-local RELEASE_VERSION=0.8.10 RELEASE_FLAGS="-Yes"
```

### Post-publish verification

```bash
git ls-remote --tags origin v0.8.10
gh release view v0.8.10 --json tagName,name,isDraft,isPrerelease,url,publishedAt
go list -m -versions github.com/Thejuampi/amps-client-go
```

## Guardrails

- Prefer `make release-dry-run` before a real publish when validating a new release flow or after modifying release tooling
- Prefer `make release-local` over manually editing `VERSION`, `README.md`, and `amps/client.go`
- Treat `make release` as **verification only**
- If the user asks whether a release is truly done, verify:
  - the release commit/tag exists
  - the GitHub release exists
  - the Go module version is visible

## Failure handling

If a dry run or local release fails:

1. Read the failing output first
2. Fix the root cause instead of bypassing the gate
3. Re-run the dry-run path before a real publish
4. If `release.local.ps1` failed before creating the release commit, it should restore the version files automatically
5. If the failure happened after push or publish steps, explicitly inspect git state and GitHub release state before retrying

## Completion criteria

A release-management task is complete only when the requested path finishes and the relevant state is confirmed:

- **verification task**: `make release` or `make release-dry-run` succeeded
- **publish task**: release commit exists, tag exists remotely, GitHub release exists, and the Go module version is visible
