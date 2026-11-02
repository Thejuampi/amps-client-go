param([string]$ReferenceRoot = '')

$ErrorActionPreference = 'Stop'
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
if ($ReferenceRoot -eq '') {
    $ReferenceRoot = Join-Path $repoRoot '.tmp/parity-audit/reference/amps-c++-client-5.3.5.14-Windows'
}
$sourcePath = Join-Path $ReferenceRoot 'src/amps_protocol.c'
$source = [IO.File]::ReadAllText($sourcePath)
$start = $source.IndexOf('static size_t get_string(')
if ($start -lt 0) { throw 'SDK parser function not found' }
$end = $source.IndexOf('static size_t get_number(', $start)
if ($start -lt 0 -or $end -le $start) { throw 'SDK parser function not found' }
$functionSource = $source.Substring($start, $end-$start)
$prefix = @'
#include <stdio.h>
#include <string.h>
#include <assert.h>
static const char quote = '"';
static const char backslash = '\\';
'@
$driver = @'
static int check(const char* label, const char* encoded, const char* expected) {
  char data[256];
  strcpy(data, encoded);
  size_t decodedLength = 0;
  get_string(data, data + strlen(data), &decodedLength);
  int correct = decodedLength == strlen(expected) && memcmp(data, expected, decodedLength) == 0;
  printf("%s=%s decoded_hex=", label, correct ? "PASS" : "FAIL");
  for (size_t i=0; i<decodedLength; ++i) printf("%02x", (unsigned char)data[i]);
  puts("");
  return !correct;
}
int main(void) {
  int failed = 0;
  failed += check("backslash", "orders\\\\archive\"", "orders\\archive");
  failed += check("quote", "sub\\\"1\"", "sub\"1");
  failed += check("newline", "line\\nnext\"", "line\nnext");
  return failed;
}
'@
$oraclePath = Join-Path $repoRoot '.tmp/deep-audit/cpp-string-oracle.c'
[IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($oraclePath)) | Out-Null
$oracleBinary = Join-Path $repoRoot '.tmp/deep-audit/cpp-string-oracle.exe'
[IO.File]::WriteAllText($oraclePath, $prefix + "`n" + $functionSource + "`n" + $driver)
& gcc -std=c11 -Wall -Wextra -o $oracleBinary $oraclePath
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& $oracleBinary
exit $LASTEXITCODE
