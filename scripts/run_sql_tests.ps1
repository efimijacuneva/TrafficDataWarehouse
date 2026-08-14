<#
.SYNOPSIS
    Run the warehouse (Tier 3) test scripts against a loaded TrafficDW.

.DESCRIPTION
    Executes tests/sql/*.sql inside the SQL Server container. Each script prints
    one row per assertion with a PASS/FAIL verdict and THROWs if anything failed,
    so `sqlcmd -b` gives a non-zero exit and this runner reports it.

    Order matters:
      1. warehouse invariants  - read-only, safe any time
      2. idempotency           - re-runs the latest load date
      3. SCD2 scenario         - CHANGES THE SOURCE (raises a speed limit) and
                                 loads the next day, so it runs last

    -InvariantsOnly skips the two that mutate state.

.EXAMPLE
    .\scripts\run_sql_tests.ps1
    .\scripts\run_sql_tests.ps1 -InvariantsOnly
#>
[CmdletBinding()]
param(
    [string]$SqlPassword = $(if ($env:SQL_PASSWORD) { $env:SQL_PASSWORD } else { "ChangeMe_Demo1!" }),
    [switch]$InvariantsOnly
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot

# PowerShell 5.1 wraps a native command's stderr in ErrorRecords as soon as it is
# redirected; with $ErrorActionPreference='Stop' a harmless warning (docker info
# prints "WARNING: No blkio throttle..." on a healthy engine) then becomes a
# TERMINATING error even though the command exited 0. Judge success by EXIT CODE.
function Invoke-NativeQuiet {
    param([Parameter(Mandatory)][scriptblock]$Command)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $out = & $Command 2>&1
        return [pscustomobject]@{ ExitCode = $LASTEXITCODE; Output = $out }
    }
    finally { $ErrorActionPreference = $prev }
}

$tests = @(
    @{ Name = "Warehouse invariants"; File = "test_warehouse_invariants.sql"; Mutates = $false }
    @{ Name = "Idempotency";          File = "test_idempotency.sql";          Mutates = $true  }
    @{ Name = "SCD2 scenario";        File = "test_scd2_scenario.sql";        Mutates = $true  }
)

# preflight: the container must be up, or every test would "fail" misleadingly
if ((Invoke-NativeQuiet { docker inspect trafficdw-mssql }).ExitCode -ne 0) {
    Write-Host "BLOCKED - the trafficdw-mssql container is not running." -ForegroundColor Yellow
    Write-Host "  start it with: docker compose -f orchestration/docker-compose.yml up -d mssql"
    exit 2
}

$results = @()
foreach ($t in $tests) {
    if ($InvariantsOnly -and $t.Mutates) {
        Write-Host "SKIP  $($t.Name) (-InvariantsOnly)" -ForegroundColor DarkGray
        $results += [pscustomobject]@{ Test = $t.Name; Result = "SKIP" }
        continue
    }

    Write-Host ""
    Write-Host ("=" * 70) -ForegroundColor DarkCyan
    Write-Host "  $($t.Name)" -ForegroundColor Cyan
    Write-Host ("=" * 70) -ForegroundColor DarkCyan

    # tests/ is inside the repo, which is NOT mounted into the mssql container
    # (only ../sql is), so the script has to get in some other way.
    #
    # It is COPIED IN, not piped to stdin. Piping (`Get-Content -Raw | docker
    # exec -i ...`) encodes stdin with $OutputEncoding, whose default differs
    # between PowerShell hosts and profiles; where it carries a UTF-8 preamble
    # the file arrives with a byte-order-mark and sqlcmd fails on the very first
    # token with "Incorrect syntax near '<U+FEFF>'". docker cp moves the bytes
    # verbatim, so the result no longer depends on the caller's session.
    $path = Join-Path $RepoRoot "tests\sql\$($t.File)"
    if (-not (Test-Path $path)) { throw "missing test script: $path" }

    $inContainer = "/tmp/$($t.File)"
    & docker cp $path "trafficdw-mssql:$inContainer" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "docker cp failed for $($t.File)" }

    & docker exec trafficdw-mssql `
        /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P $SqlPassword -C -b -I -d TrafficDW -i $inContainer

    $verdict = if ($LASTEXITCODE -eq 0) { "PASS" } else { "FAIL" }
    $colour  = if ($LASTEXITCODE -eq 0) { "Green" } else { "Red" }
    Write-Host ""
    Write-Host "  -> $($t.Name): $verdict" -ForegroundColor $colour
    $results += [pscustomobject]@{ Test = $t.Name; Result = $verdict }
}

Write-Host ""
Write-Host ("=" * 70) -ForegroundColor DarkCyan
Write-Host "  SQL TEST SUMMARY" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor DarkCyan
$results | Format-Table Test, Result -AutoSize | Out-Host

$failed = @($results | Where-Object Result -eq "FAIL").Count
if ($failed -gt 0) {
    Write-Host "$failed test script(s) FAILED." -ForegroundColor Red
    exit 1
}
Write-Host "All executed SQL test scripts passed." -ForegroundColor Green