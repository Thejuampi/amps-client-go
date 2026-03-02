param(
  [string]$Profile = "single_tcp",
  [string]$Exe = "./official_c_parity_benchmark.exe",
  [string]$ExtraExe = "",
  [string]$RequireBenchmarks = "",
  [int]$Samples = 20,
  [string]$Out = "tools/perf_tail_c_current.json",
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

  $captureArgs = @(
    "run", "./tools/perfreport", "capture-c",
    "-exe", $Exe,
    "-extra-exe", $ExtraExe,
    "-samples", $Samples,
    "-profile", $Profile,
    "-out", $Out,
    "-timeout", $Timeout,
    "-progress-interval", $ProgressInterval
  )

  if (-not [string]::IsNullOrWhiteSpace($RequireBenchmarks)) {
    $captureArgs += @("-require-benchmarks", $RequireBenchmarks)
  }

  go @captureArgs
}
finally {
  if ($null -ne $fakeampsProcess -and -not $fakeampsProcess.HasExited) {
    Stop-Process -Id $fakeampsProcess.Id -Force
  }
}
