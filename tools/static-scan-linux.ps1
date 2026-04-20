[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Fail {
	param([string]$Message)
	throw $Message
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

function Get-MakeVariable {
	param(
		[string]$Path,
		[string]$Name
	)

	$content = [System.IO.File]::ReadAllText($Path)
	$pattern = '(?m)^' + [regex]::Escape($Name) + '\s*\?=\s*(.+)$'
	$match = [regex]::Match($content, $pattern)
	if (-not $match.Success) {
		Fail "Could not find '$Name' in $Path."
	}
	return $match.Groups[1].Value.Trim()
}

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptRoot
Set-Location $repoRoot

Require-Tool "go"

$makefilePath = Join-Path $repoRoot "Makefile"
if (-not (Test-Path $makefilePath)) {
	Fail "Makefile not found at $makefilePath."
}

$staticcheckVersion = Get-MakeVariable -Path $makefilePath -Name "STATICCHECK_VERSION"
$ineffassignVersion = Get-MakeVariable -Path $makefilePath -Name "INEFFASSIGN_VERSION"
$errcheckVersion = Get-MakeVariable -Path $makefilePath -Name "ERRCHECK_VERSION"
$golangciLintVersion = Get-MakeVariable -Path $makefilePath -Name "GOLANGCI_LINT_VERSION"
$toolExtension = ""
if ($env:OS -eq "Windows_NT") {
	$toolExtension = ".exe"
}

$toolBin = Join-Path ([System.IO.Path]::GetTempPath()) "amps-static-scan-linux-tools"
$staticcheckPath = Join-Path $toolBin ("staticcheck" + $toolExtension)
$ineffassignPath = Join-Path $toolBin ("ineffassign" + $toolExtension)
$errcheckPath = Join-Path $toolBin ("errcheck" + $toolExtension)
$golangciLintPath = Join-Path $toolBin ("golangci-lint" + $toolExtension)

$oldGOBIN = $env:GOBIN
$oldGOOS = $env:GOOS
$oldGOARCH = $env:GOARCH
$oldCGOEnabled = $env:CGO_ENABLED

try {
	Remove-Item -Recurse -Force $toolBin -ErrorAction SilentlyContinue
	New-Item -ItemType Directory -Force -Path $toolBin | Out-Null

	Step "Installing analyzer binaries for the host platform"
	$env:GOBIN = $toolBin
	Run-External "go" @("install", "honnef.co/go/tools/cmd/staticcheck@$staticcheckVersion")
	Run-External "go" @("install", "github.com/gordonklaus/ineffassign@$ineffassignVersion")
	Run-External "go" @("install", "github.com/kisielk/errcheck@$errcheckVersion")
	Run-External "go" @("install", "github.com/golangci/golangci-lint/cmd/golangci-lint@$golangciLintVersion")

	Step "Running Linux-target static analysis"
	$env:GOOS = "linux"
	$env:GOARCH = "amd64"
	$env:CGO_ENABLED = "0"
	Run-External "go" @("vet", "./...")
	Run-External $staticcheckPath @("-checks=SA*", "./...")
	Run-External $ineffassignPath @("./...")
	Run-External $errcheckPath @("-ignoretests", "./...")
	Run-External "go" @("run", "./tools/patterncheck", "./...")
	Run-External $golangciLintPath @("run", "--config", ".golangci.yml", "./...")

	Step "Linux-target static analysis passed"
}
finally {
	if ($null -ne $oldGOBIN) {
		$env:GOBIN = $oldGOBIN
	} else {
		Remove-Item Env:GOBIN -ErrorAction SilentlyContinue
	}
	if ($null -ne $oldGOOS) {
		$env:GOOS = $oldGOOS
	} else {
		Remove-Item Env:GOOS -ErrorAction SilentlyContinue
	}
	if ($null -ne $oldGOARCH) {
		$env:GOARCH = $oldGOARCH
	} else {
		Remove-Item Env:GOARCH -ErrorAction SilentlyContinue
	}
	if ($null -ne $oldCGOEnabled) {
		$env:CGO_ENABLED = $oldCGOEnabled
	} else {
		Remove-Item Env:CGO_ENABLED -ErrorAction SilentlyContinue
	}
	Remove-Item -Recurse -Force $toolBin -ErrorAction SilentlyContinue
}
