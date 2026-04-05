[CmdletBinding()]
param(
	[string]$Version,
	[switch]$Yes,
	[switch]$DryRun,
	[int]$ModuleVerifyAttempts = 12,
	[int]$ModuleVerifyDelaySeconds = 10
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Fail {
	param([string]$Message)
	throw [System.InvalidOperationException]::new($Message)
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
	if ($Yes) {
		Write-Host "$Prompt [auto-yes]" -ForegroundColor DarkGray
		return $true
	}
	$answer = (Read-Host "$Prompt [y/N]").Trim().ToLowerInvariant()
	return $answer -eq "y" -or $answer -eq "yes"
}

function Restore-VersionFiles {
	param(
		[string]$VersionPath,
		[string]$VersionText,
		[string]$ReadmePath,
		[string]$ReadmeText,
		[string]$ClientPath,
		[string]$ClientText
	)

	[System.IO.File]::WriteAllText($VersionPath, $VersionText)
	[System.IO.File]::WriteAllText($ReadmePath, $ReadmeText)
	[System.IO.File]::WriteAllText($ClientPath, $ClientText)
}

function Verify-PublishedRelease {
	param(
		[string]$Tag,
		[string]$Version
	)

	Step "Verifying published release"
	Run-External "git" @("ls-remote", "--tags", "origin", $Tag)
	Run-External "gh" @("release", "view", $Tag, "--json", "tagName,name,isDraft,isPrerelease,url,publishedAt")

	$moduleVisible = $false
	$versionPattern = '(^|\s){0}(\s|$)' -f [regex]::Escape($Version)
	for ($attempt = 1; $attempt -le $ModuleVerifyAttempts; $attempt++) {
		Write-Host ("Checking Go module registry visibility ({0}/{1})..." -f $attempt, $ModuleVerifyAttempts)
		$versionsOutput = & go list -m -versions github.com/Thejuampi/amps-client-go
		$versionsText = if ($null -eq $versionsOutput) { "" } else { ($versionsOutput | Out-String).Trim() }
		if ($LASTEXITCODE -eq 0 -and $versionsText -match $versionPattern) {
			Write-Host $versionsText
			$moduleVisible = $true
			break
		}
		if ($attempt -lt $ModuleVerifyAttempts) {
			Start-Sleep -Seconds $ModuleVerifyDelaySeconds
		}
	}

	if (-not $moduleVisible) {
		Fail "Published tag '$Tag' exists, but the Go module registry did not show version '$Version' after $ModuleVerifyAttempts attempt(s)."
	}
}

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$versionPath = Join-Path $repoRoot "VERSION"
$readmePath = Join-Path $repoRoot "README.md"
$clientPath = Join-Path $repoRoot "amps/client.go"
$linuxStaticScanScript = Join-Path $repoRoot "tools\static-scan-linux.ps1"
$originalVersionText = ""
$originalReadmeText = ""
$originalClientText = ""
$versionFilesUpdated = $false
$releaseCommitted = $false

try {
	Set-Location $repoRoot

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

	$statusOutput = & git status --porcelain --untracked-files=no
	if ($LASTEXITCODE -ne 0) {
		Fail "Could not read git status."
	}
	$status = if ($null -eq $statusOutput) { "" } else { ($statusOutput | Out-String).Trim() }
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
	& $linuxStaticScanScript
	if ($LASTEXITCODE -ne 0) {
		Fail "Linux-target static-analysis preflight failed. Fix the reported issues before preparing a release."
	}

	$currentVersion = (Get-Content $versionPath -Raw).Trim()
	Write-Host "Current VERSION: $currentVersion"

	$releaseVersion = if ([string]::IsNullOrWhiteSpace($Version)) {
		(Read-Host "Enter release version (X.Y.Z)").Trim()
	}
	else {
		$Version.Trim()
	}
	if ($releaseVersion -notmatch '^\d+\.\d+\.\d+$') {
		Fail "Invalid version '$releaseVersion'. Use X.Y.Z format."
	}
	$tag = "v$releaseVersion"

	$existingTagOutput = & git tag --list $tag
	if ($LASTEXITCODE -ne 0) {
		Fail "Could not query existing tags."
	}
	$existingTag = if ($null -eq $existingTagOutput) { "" } else { ($existingTagOutput | Out-String).Trim() }
	if ($existingTag -eq $tag) {
		Fail "Tag '$tag' already exists."
	}

	if (-not (Confirm-Yes "Prepare release $tag from branch main?")) {
		Fail "Release cancelled."
	}

	$originalVersionText = [System.IO.File]::ReadAllText($versionPath)
	$originalReadmeText = [System.IO.File]::ReadAllText($readmePath)
	$originalClientText = [System.IO.File]::ReadAllText($clientPath)

	Step "Updating version files"
	[System.IO.File]::WriteAllText($versionPath, "$releaseVersion`n")

	$readmePattern = '(?m)^Version:\s+.*$'
	$readmeMatches = [regex]::Matches($originalReadmeText, $readmePattern).Count
	if ($readmeMatches -ne 1) {
		Fail "Expected exactly one README version line. Found $readmeMatches."
	}
	$readmeUpdated = [regex]::Replace($originalReadmeText, $readmePattern, "Version: ``$releaseVersion``")
	[System.IO.File]::WriteAllText($readmePath, $readmeUpdated)

	$clientPattern = '(?m)^(\s*ClientVersion\s*=\s*)"[^"]+"'
	$clientMatches = [regex]::Matches($originalClientText, $clientPattern).Count
	if ($clientMatches -ne 1) {
		Fail "Expected exactly one ClientVersion constant. Found $clientMatches."
	}
	$clientUpdated = [regex]::Replace($originalClientText, $clientPattern, ('${1}"' + $releaseVersion + '"'))
	[System.IO.File]::WriteAllText($clientPath, $clientUpdated)
	$versionFilesUpdated = $true

	Step "Running release gates"
	Run-External "make" @("release")

	Step "Showing release file diff"
	Run-External "git" @("--no-pager", "diff", "--", "VERSION", "README.md", "amps/client.go")

	if ($DryRun) {
		Step "Dry run complete"
		Write-Host "Release validation passed for $tag. Version file edits will now be restored because -DryRun was requested."
		return
	}

	if (-not (Confirm-Yes "Commit, tag, and push release $tag now?")) {
		Fail "Release cancelled after checks. Version file edits will be restored automatically."
	}

	Step "Committing release changes"
	Run-External "git" @("add", "VERSION", "README.md", "amps/client.go")
	Run-External "git" @("-c", "commit.gpgSign=false", "commit", "-m", "release: $tag", "-m", "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>")
	$releaseCommitted = $true

	Step "Creating annotated tag (unsigned)"
	Run-External "git" @("-c", "tag.gpgSign=false", "tag", "-a", $tag, "-m", "Release $tag")

	Step "Pushing branch and tag"
	Run-External "git" @("push", "origin", "main")
	Run-External "git" @("push", "origin", $tag)

	Step "Publishing GitHub release"
	Run-External "gh" @("release", "create", $tag, "--verify-tag", "--generate-notes")

	Verify-PublishedRelease -Tag $tag -Version $releaseVersion

	Step "Release request submitted"
	Write-Host "GitHub release page:"
	Write-Host "  https://github.com/Thejuampi/amps-client-go/releases/tag/$tag"
	Write-Host "Go proxy warm-up URL:"
	Write-Host "  https://proxy.golang.org/github.com/Thejuampi/amps-client-go/@v/$tag.info"
	Write-Host "pkg.go.dev URL:"
	Write-Host "  https://pkg.go.dev/github.com/Thejuampi/amps-client-go/amps@$tag"
	Write-Host "Version list command:"
	Write-Host "  go list -m -versions github.com/Thejuampi/amps-client-go"
}
catch {
	Write-Host ""
	Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
	exit 1
}
finally {
	if ($versionFilesUpdated -and -not $releaseCommitted) {
		Restore-VersionFiles -VersionPath $versionPath -VersionText $originalVersionText -ReadmePath $readmePath -ReadmeText $originalReadmeText -ClientPath $clientPath -ClientText $originalClientText
	}
}
