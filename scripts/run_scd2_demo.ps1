<#
.SYNOPSIS
    Demonstrate SCD Type 2 versioning end to end - the exam demo.

.DESCRIPTION
    Shows the dimension BEFORE, changes a speed limit in the OLTP source, runs
    the warehouse pipeline for the next day, then shows the version history and
    every assertion of the Type 2 contract.

    This exists because the shipped 7-day dataset never changes a dimension
    attribute, so SCD2 versioning, the Type 0 carry-forward, the Type 6
    synchronisation and the point-in-time fact join never execute during a
    normal run. Nothing proved they worked.

    What a professor sees:

        SEG-001  v1  limit 50  effective D1  expired D2-1  current NO
        SEG-001  v2  limit 60  effective D2  expired 9999  current YES

        ... plus 14 assertions, all PASS

    Repeatable: each run raises the limit by 10 and adds one more version.

.EXAMPLE
    .\scripts\run_scd2_demo.ps1
    .\scripts\run_scd2_demo.ps1 -Segment SEG-005
#>
[CmdletBinding()]
param(
    [string]$SqlPassword = $(if ($env:SQL_PASSWORD) { $env:SQL_PASSWORD } else { "ChangeMe_Demo1!" }),
    [string]$Segment = "SEG-001"
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


if ((Invoke-NativeQuiet { docker inspect trafficdw-mssql }).ExitCode -ne 0) {
    Write-Host "BLOCKED - the trafficdw-mssql container is not running." -ForegroundColor Yellow
    Write-Host "  start it with: docker compose -f orchestration/docker-compose.yml up -d mssql"
    exit 2
}


Write-Host ""
Write-Host ("=" * 70) -ForegroundColor DarkCyan
Write-Host "  SCD TYPE 2 DEMONSTRATION - business key $Segment" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor DarkCyan
Write-Host ""
Write-Host "What this proves:" -ForegroundColor DarkGray
Write-Host "  * a source change creates a NEW dimension row, it does not overwrite" -ForegroundColor DarkGray
Write-Host "  * the old row is expired, keeping history queryable" -ForegroundColor DarkGray
Write-Host "  * facts loaded BEFORE the change still resolve to the OLD version" -ForegroundColor DarkGray
Write-Host "  * Type 0 (original limit) and Type 6 (current limit) behave differently" -ForegroundColor DarkGray
Write-Host ""

$script = Join-Path $RepoRoot "tests\sql\test_scd2_scenario.sql"
if (-not (Test-Path $script)) { throw "missing: $script" }

# The script is COPIED into the container rather than piped to stdin: piping
# encodes with $OutputEncoding, which in some PowerShell sessions prefixes a
# UTF-8 byte-order-mark and makes sqlcmd fail on the first token with
# "Incorrect syntax near '<U+FEFF>'". docker cp moves the bytes verbatim.
$toSend = $script
if ($Segment -ne "SEG-001") {
    # the scenario targets SEG-001 by default; write a patched copy for another key
    $sql = (Get-Content $script -Raw) -replace
           "DECLARE @Segment      VARCHAR\(30\) = 'SEG-001';",
           "DECLARE @Segment      VARCHAR(30) = '$Segment';"
    $toSend = Join-Path $env:TEMP "scd2_$Segment.sql"
    # UTF8Encoding($false) = no BOM, for the same reason as above
    [System.IO.File]::WriteAllText($toSend, $sql, (New-Object System.Text.UTF8Encoding($false)))
}

& docker cp $toSend "trafficdw-mssql:/tmp/scd2_scenario.sql" | Out-Null
if ($LASTEXITCODE -ne 0) { throw "docker cp failed" }

& docker exec trafficdw-mssql `
    /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P $SqlPassword -C -b -I -d TrafficDW -i /tmp/scd2_scenario.sql

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "SCD2 DEMONSTRATION FAILED - see the assertion table above." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Follow-up queries worth showing live:" -ForegroundColor Green
Write-Host "  -- full version history"
Write-Host "  SELECT * FROM dim.DimRoadSegment WHERE SegmentCode = '$Segment' ORDER BY VersionNumber;"
Write-Host "  -- the same segment in the Power BI semantic layer"
Write-Host "  SELECT * FROM mart.vPbiRoadSegment WHERE SegmentCode = '$Segment';"
Write-Host "  -- point-in-time: which version do older facts point at?"
Write-Host "  SELECT d.VersionNumber, d.SpeedLimitKmh, COUNT(*) AS Facts"
Write-Host "  FROM fact.FactTrafficEvent f JOIN dim.DimRoadSegment d"
Write-Host "    ON d.RoadSegmentKey = f.RoadSegmentKey"
Write-Host "  WHERE d.SegmentCode = '$Segment' GROUP BY d.VersionNumber, d.SpeedLimitKmh;"