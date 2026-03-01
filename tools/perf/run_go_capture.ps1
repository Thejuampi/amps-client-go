param(
  [string]$Profile = "single_tcp",
  [string]$Package = "./amps",
  [string]$Bench = ".",
  [string]$Benchtime = "1s",
  [int]$Samples = 20,
  [string]$Out = "tools/perf_tail_current.json",
  [string]$ExtraBench = "",
  [string]$ExtraBenchtime = "1x",
  [int]$RetryAttempts = 3,
  [string]$RetryDelay = "2s",
  [string]$Timeout = "5m",
  [string]$ProgressInterval = "20s",
  [switch]$StartFakeamps,
  [string]$FakeampsAddr = "127.0.0.1:19000",
  [string]$FakeampsStdoutLog = "tools/fakeamps_benchmark.stdout.log",
  [string]$FakeampsStderrLog = "tools/fakeamps_benchmark.stderr.log"
)

$ErrorActionPreference = "Stop"

$fakeampsProcess = $null

try {
  if ($StartFakeamps) {
    $fakeampsArgs = @("run", "./tools/fakeamps", "-addr", $FakeampsAddr, "-benchmark-stability")
    $fakeampsProcess = Start-Process -FilePath "go" -ArgumentList $fakeampsArgs -PassThru -RedirectStandardOutput $FakeampsStdoutLog -RedirectStandardError $FakeampsStderrLog
    Start-Sleep -Milliseconds 750
  }

  $env:PERF_FAKEAMPS_URI = "tcp://$FakeampsAddr/amps/json"

  go run ./tools/perfreport capture-go `
    -package $Package `
    -bench $Bench `
    -benchtime $Benchtime `
    -samples $Samples `
    -profile $Profile `
    -out $Out `
    -timeout $Timeout `
    -progress-interval $ProgressInterval `
    -retry-attempts $RetryAttempts `
    -retry-delay $RetryDelay `
    -extra-bench $ExtraBench `
    -extra-benchtime $ExtraBenchtime
}
finally {
  if ($null -ne $fakeampsProcess -and -not $fakeampsProcess.HasExited) {
    Stop-Process -Id $fakeampsProcess.Id -Force
  }
}
