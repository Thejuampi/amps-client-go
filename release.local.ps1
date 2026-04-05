[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Fail {
	param([string]$Message)
	Write-Host ""
	Write-Host "ERROR: $Message" -ForegroundColor Red
	exit 1
}

function Step {
	param([string]$Message)
	Write-Host ""
	Write-Host "==> $Message" -ForegroundColor Cyan
}

function Require-Tool {
	param([string]$Name)
	if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
		Fail "Required tool '$Name' not found in PATH."
	}
}

function Run-External {
	param(
		[string]$Command,
		[string[]]$Arguments
	)

	Write-Host ("+ {0} {1}" -f $Command, ($Arguments -join " "))
	& $Command @Arguments
	if ($LASTEXITCODE -ne 0) {
		Fail "Command failed: $Command $($Arguments -join ' ')"
	}
}

function Confirm-Yes {
	param([string]$Prompt)
	$answer = (Read-Host "$Prompt [y/N]").Trim().ToLowerInvariant()
	return $answer -eq "y" -or $answer -eq "yes"
}

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $repoRoot
$linuxStaticScanScript = Join-Path $repoRoot "tools\static-scan-linux.ps1"

Step "Checking required tools"
Require-Tool "git"
Require-Tool "go"
Require-Tool "make"
Require-Tool "gh"

Step "Checking git repository state"
$insideRepo = (& git rev-parse --is-inside-work-tree).Trim()
if ($LASTEXITCODE -ne 0 -or $insideRepo -ne "true") {
	Fail "This script must run inside a git repository."
}

$branch = (& git rev-parse --abbrev-ref HEAD).Trim()
if ($LASTEXITCODE -ne 0) {
	Fail "Could not determine current branch."
}
if ($branch -ne "main") {
	Fail "Current branch is '$branch'. Switch to 'main' first."
}

$status = (& git status --porcelain).Trim()
if ($LASTEXITCODE -ne 0) {
	Fail "Could not read git status."
}
if ($status.Length -gt 0) {
	Fail "Working tree is not clean. Commit/stash/discard changes before releasing."
}

Step "Fetching origin and tags"
Run-External "git" @("fetch", "origin", "--tags", "--prune")

$leftRight = (& git rev-list --left-right --count origin/main...HEAD).Trim()
if ($LASTEXITCODE -ne 0) {
	Fail "Could not compare HEAD with origin/main."
}
$counts = $leftRight -split "\s+"
if ($counts.Count -lt 2) {
	Fail "Unexpected git rev-list output: '$leftRight'"
}
[int]$behind = $counts[0]
[int]$ahead = $counts[1]

if ($behind -gt 0) {
	Fail "Local main is behind origin/main by $behind commit(s). Run 'git pull --ff-only origin main' first."
}
if ($ahead -gt 0) {
	Fail "Local main is ahead of origin/main by $ahead commit(s). Push or rebase before releasing."
}

if (-not (Test-Path $linuxStaticScanScript)) {
	Fail "Required script '$linuxStaticScanScript' not found."
}

Step "Running Linux-target static-analysis preflight"
try {
	& $linuxStaticScanScript
}
catch {
	Fail ("Linux-target static-analysis preflight failed. Fix the reported issues before preparing a release. " + $_.Exception.Message)
}

$currentVersion = (Get-Content VERSION -Raw).Trim()
Write-Host "Current VERSION: $currentVersion"

$version = (Read-Host "Enter release version (X.Y.Z)").Trim()
if ($version -notmatch '^\d+\.\d+\.\d+$') {
	Fail "Invalid version '$version'. Use X.Y.Z format."
}
$tag = "v$version"

$existingTag = (& git tag --list $tag).Trim()
if ($LASTEXITCODE -ne 0) {
	Fail "Could not query existing tags."
}
if ($existingTag -eq $tag) {
	Fail "Tag '$tag' already exists."
}

if (-not (Confirm-Yes "Prepare release $tag from branch main?")) {
	Fail "Release cancelled."
}

Step "Updating version files"
[System.IO.File]::WriteAllText((Join-Path $repoRoot "VERSION"), "$version`n")

$readmePath = Join-Path $repoRoot "README.md"
$readmeText = [System.IO.File]::ReadAllText($readmePath)
$readmePattern = '(?m)^Version:\s*`[^`]+`$'
$readmeMatches = [regex]::Matches($readmeText, $readmePattern).Count
if ($readmeMatches -ne 1) {
	Fail "Expected exactly one README version line. Found $readmeMatches."
}
$readmeUpdated = [regex]::Replace($readmeText, $readmePattern, "Version: ``$version``")
[System.IO.File]::WriteAllText($readmePath, $readmeUpdated)

$clientPath = Join-Path $repoRoot "amps/client.go"
$clientText = [System.IO.File]::ReadAllText($clientPath)
$clientPattern = '(?m)^(\s*ClientVersion\s*=\s*)"[^"]+"'
$clientMatches = [regex]::Matches($clientText, $clientPattern).Count
if ($clientMatches -ne 1) {
	Fail "Expected exactly one ClientVersion constant. Found $clientMatches."
}
$clientUpdated = [regex]::Replace($clientText, $clientPattern, ('${1}"' + $version + '"'))
[System.IO.File]::WriteAllText($clientPath, $clientUpdated)

Step "Running release gates"
Run-External "make" @("release")

Step "Showing release file diff"
Run-External "git" @("--no-pager", "diff", "--", "VERSION", "README.md", "amps/client.go")

if (-not (Confirm-Yes "Commit, tag, and push release $tag now?")) {
	Fail "Release cancelled after checks. Version file edits remain in your working tree."
}

Step "Committing release changes"
Run-External "git" @("add", "VERSION", "README.md", "amps/client.go")
Run-External "git" @("-c", "commit.gpgSign=false", "commit", "-m", "release: $tag")

Step "Creating annotated tag (unsigned)"
Run-External "git" @("-c", "tag.gpgSign=false", "tag", "-a", $tag, "-m", "Release $tag")

Step "Pushing branch and tag"
Run-External "git" @("push", "origin", "main")
Run-External "git" @("push", "origin", $tag)

Step "Publishing GitHub release"
Run-External "gh" @("release", "create", $tag, "--verify-tag", "--generate-notes")

Step "Release request submitted"
Write-Host "GitHub release page:"
Write-Host "  https://github.com/Thejuampi/amps-client-go/releases/tag/$tag"
Write-Host "Go proxy warm-up URL:"
Write-Host "  https://proxy.golang.org/github.com/Thejuampi/amps-client-go/@v/$tag.info"
Write-Host "pkg.go.dev URL:"
Write-Host "  https://pkg.go.dev/github.com/Thejuampi/amps-client-go/amps@$tag"
Write-Host "Version list command:"
Write-Host "  go list -m -versions github.com/Thejuampi/amps-client-go"
