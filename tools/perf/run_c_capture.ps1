param(
  [string]$Profile = "single_tcp",
  [string]$Exe = "./official_c_parity_benchmark.exe",
  [string]$ExtraExe = "",
  [int]$Samples = 20,
  [string]$Out = "tools/perf_tail_c_current.json",
  [string]$Timeout = "5m",
  [string]$ProgressInterval = "20s"
)

$ErrorActionPreference = "Stop"

go run ./tools/perfreport capture-c `
  -exe $Exe `
  -extra-exe $ExtraExe `
  -samples $Samples `
  -profile $Profile `
  -out $Out `
  -timeout $Timeout `
  -progress-interval $ProgressInterval
