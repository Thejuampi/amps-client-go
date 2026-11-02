param(
    [string]$Run = '^TestDeepAudit',
    [ValidateRange(1, 1000)][int]$Count = 1,
    [string]$Cpu = '1,2,8'
)

$ErrorActionPreference = 'Stop'
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$previousToolchain = $env:GOTOOLCHAIN
Push-Location $repoRoot
try {
    $env:GOTOOLCHAIN = 'go1.26.10'
    & go test -race ./amps ./amps/cppcompat ./tools/fakeamps -run $Run -count $Count -cpu $Cpu -timeout 3m -v
    $testExitCode = $LASTEXITCODE
} finally {
    $env:GOTOOLCHAIN = $previousToolchain
    Pop-Location
}
exit $testExitCode
