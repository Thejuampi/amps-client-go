param(
  [string]$Profile = "single_tcp",
  [string]$Package = "./amps",
  [string]$Bench = ".",
  [string]$Benchtime = "1s",
  [int]$Samples = 20,
  [string]$Out = "tools/perf_tail_current.json",
  [string]$ExtraBench = "",
  [string]$ExtraBenchtime = "1x",
  [string]$Timeout = "5m",
  [string]$ProgressInterval = "20s"
)

$ErrorActionPreference = "Stop"

go run ./tools/perfreport capture-go `
  -package $Package `
  -bench $Bench `
  -benchtime $Benchtime `
  -samples $Samples `
  -profile $Profile `
  -out $Out `
  -timeout $Timeout `
  -progress-interval $ProgressInterval `
  -extra-bench $ExtraBench `
  -extra-benchtime $ExtraBenchtime
