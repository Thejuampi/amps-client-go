param(
  [string]$Profile = "single_tcp",
  [string]$Package = "./amps",
  [string]$Bench = ".",
  [string]$Benchtime = "1s",
  [int]$Samples = 20,
  [string]$Out = "tools/perf_tail_current.json",
  [string]$ExtraBench = "",
  [string]$ExtraBenchtime = "1x",
  [string]$ExtraBench2 = "",
  [string]$ExtraBenchtime2 = "1x",
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
    $addrParts = $FakeampsAddr.Split(':')
    $fakeampsPort = [int]$addrParts[$addrParts.Length - 1]
    $listeners = Get-NetTCPConnection -LocalPort $fakeampsPort -State Listen -ErrorAction SilentlyContinue
    foreach ($listener in $listeners) {
      try {
        Stop-Process -Id $listener.OwningProcess -Force -ErrorAction Stop
      }
      catch {
      }
    }
    Start-Sleep -Milliseconds 250

    $fakeampsArgs = @("run", "./tools/fakeamps", "-addr", $FakeampsAddr, "-benchmark-stability")
    $fakeampsProcess = Start-Process -FilePath "go" -ArgumentList $fakeampsArgs -PassThru -RedirectStandardOutput $FakeampsStdoutLog -RedirectStandardError $FakeampsStderrLog
    Start-Sleep -Milliseconds 750
    if ($fakeampsProcess.HasExited) {
      throw "fakeamps failed to start; check $FakeampsStderrLog"
    }
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
    -extra-benchtime $ExtraBenchtime `
    -extra-bench-2 $ExtraBench2 `
    -extra-benchtime-2 $ExtraBenchtime2
}
finally {
  if ($null -ne $fakeampsProcess -and -not $fakeampsProcess.HasExited) {
    Stop-Process -Id $fakeampsProcess.Id -Force
  }
}
