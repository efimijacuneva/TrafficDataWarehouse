<#
.SYNOPSIS
    End-to-end verification of the Smart Traffic Data Warehouse.

.DESCRIPTION
    Checks every layer of the project and prints a PASS / FAIL / SKIP per area.

    RULE: something is marked PASS only if it was ACTUALLY VERIFIED. Anything
    that could not be checked because a dependency is missing is reported SKIP
    with the reason - never PASS. A green board therefore means something.

.EXAMPLE
    .\scripts\verify_project.ps1
    .\scripts\verify_project.ps1 -SkipTests      # faster: no pytest run
#>
[CmdletBinding()]
param(
    [string]$SqlPassword = $(if ($env:SQL_PASSWORD) { $env:SQL_PASSWORD } else { "ChangeMe_Demo1!" }),
    [switch]$SkipTests
)

$ErrorActionPreference = "Continue"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$Results  = New-Object System.Collections.ArrayList

function Add-Result([string]$Area, [string]$Status, [string]$Detail) {
    [void]$Results.Add([pscustomobject]@{ Area = $Area; Status = $Status; Detail = $Detail })
    $colour = switch ($Status) { "PASS" { "Green" } "FAIL" { "Red" } "SKIP" { "DarkYellow" } default { "Gray" } }
    Write-Host ("  {0,-22} {1,-6} {2}" -f $Area, $Status, $Detail) -ForegroundColor $colour
}

# Runs a native command, captures its output, and reports the EXIT CODE rather
# than letting stderr decide. Also gives every call site a single place to read
# both, which the two-tier test step below needs.
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

function Test-Sql([string]$Query, [string]$Database = "TrafficDW") {
    # returns the scalar result, or $null when SQL Server is unreachable
    $out = & docker exec trafficdw-mssql /opt/mssql-tools18/bin/sqlcmd `
        -S localhost -U sa -P $SqlPassword -C -b -h -1 -W -d $Database -Q $Query 2>$null
    if ($LASTEXITCODE -ne 0) { return $null }
    return ($out | Where-Object { $_ -match '\S' } | Select-Object -First 1).Trim()
}

Write-Host ""
Write-Host ("=" * 60) -ForegroundColor DarkCyan
Write-Host "  SMART TRAFFIC PROJECT VERIFICATION" -ForegroundColor Cyan
Write-Host ("=" * 60) -ForegroundColor DarkCyan
Write-Host ""

# ------------------------------------------------------- 1. repository -----
Write-Host "Repository" -ForegroundColor White
$required = @(
    "sql\oltp\02_tables.sql", "sql\warehouse\02_dimensions.sql", "sql\warehouse\03_facts.sql",
    "sql\etl\04_load_facts.sql", "sql\etl\05_quality_checks.sql",
    "spark\jobs\01_ingest_raw.py", "spark\jobs\05_load_warehouse.py", "spark\utils\quality.py",
    "data_generator\generate_data.py", "orchestration\docker-compose.yml",
    "tests\test_generator.py", "tests\sql\test_scd2_scenario.sql", "dashboard\app.py"
)
$missing = $required | Where-Object { -not (Test-Path (Join-Path $RepoRoot $_)) }
if ($missing) { Add-Result "Required files" "FAIL" "missing: $($missing -join ', ')" }
else          { Add-Result "Required files" "PASS" "$($required.Count) key files present" }

# --------------------------------------------------- 2. host toolchain -----
Write-Host ""
Write-Host "Host environment" -ForegroundColor White
$py = (Get-Command python -ErrorAction SilentlyContinue)
if ($py) {
    $v = (& python --version 2>&1)
    Add-Result "Python" "PASS" $v
} else { Add-Result "Python" "FAIL" "python not on PATH" }

$dockerOk = $false
if (Get-Command docker -ErrorAction SilentlyContinue) {
    & docker info *> $null
    if ($LASTEXITCODE -eq 0) { $dockerOk = $true; Add-Result "Docker engine" "PASS" "running" }
    else { Add-Result "Docker engine" "FAIL" "installed but the engine is not running" }
} else { Add-Result "Docker engine" "FAIL" "docker not on PATH" }

$jdbc = Get-ChildItem (Join-Path $RepoRoot "orchestration\jars") -Filter "mssql-jdbc-*.jar" -ErrorAction SilentlyContinue
if ($jdbc) { Add-Result "JDBC driver" "PASS" $jdbc[0].Name }
else       { Add-Result "JDBC driver" "SKIP" "not downloaded yet - run_end_to_end.ps1 fetches it" }

# ------------------------------------------------------- 3. containers -----
Write-Host ""
Write-Host "Containers" -ForegroundColor White
foreach ($c in @("trafficdw-mssql", "trafficdw-spark")) {
    if (-not $dockerOk) { Add-Result $c "SKIP" "docker engine unavailable"; continue }
    $state = (& docker inspect --format '{{.State.Status}}' $c 2>$null)
    if ($LASTEXITCODE -ne 0)     { Add-Result $c "SKIP" "not created - start the stack" }
    elseif ($state -eq "running"){ Add-Result $c "PASS" "running" }
    else                         { Add-Result $c "FAIL" "state: $state" }
}

# ---------------------------------------------------------- 4. lake -------
Write-Host ""
Write-Host "Data lake" -ForegroundColor White
$raw = Get-ChildItem (Join-Path $RepoRoot "data\raw") -Filter "sensor_readings_*.csv" -ErrorAction SilentlyContinue
if ($raw) { Add-Result "Raw feed" "PASS" "$($raw.Count) day(s) generated" }
else      { Add-Result "Raw feed" "SKIP" "no raw data - run the generator" }

foreach ($zone in @(
    @{ N = "Bronze";     P = "data\bronze\sensor_readings" },
    @{ N = "Silver";     P = "data\silver\traffic_events" },
    @{ N = "Quarantine"; P = "data\silver\_quarantine\traffic_events" },
    @{ N = "Gold";       P = "data\gold\hourly_traffic" })) {
    $dir = Join-Path $RepoRoot $zone.P
    if (Test-Path $dir) {
        $parts = @(Get-ChildItem $dir -Directory -ErrorAction SilentlyContinue)
        $files = @(Get-ChildItem $dir -Recurse -Filter "*.parquet" -ErrorAction SilentlyContinue)
        if ($files.Count -gt 0) { Add-Result $zone.N "PASS" "$($parts.Count) partition(s), $($files.Count) parquet file(s)" }
        else                    { Add-Result $zone.N "FAIL" "directory exists but holds no parquet" }
    } else { Add-Result $zone.N "SKIP" "not produced yet - run the Spark jobs" }
}

# bronze must be partitioned by ingest_date, not by the event-derived date:
# that is the fix for the silent-data-loss defect
$bronze = Join-Path $RepoRoot "data\bronze\sensor_readings"
if (Test-Path $bronze) {
    $parts = @(Get-ChildItem $bronze -Directory | Select-Object -ExpandProperty Name)
    $byEvent  = @($parts | Where-Object { $_ -like "event_date=*" }).Count
    $byIngest = @($parts | Where-Object { $_ -like "ingest_date=*" }).Count
    if ($byIngest -gt 0 -and $byEvent -eq 0) { Add-Result "Bronze partitioning" "PASS" "ingest_date ($byIngest) - no-silent-loss layout" }
    elseif ($byEvent -gt 0)                  { Add-Result "Bronze partitioning" "FAIL" "still event_date ($byEvent) - re-run job 01, then scripts/clean_lake.ps1" }
    else                                     { Add-Result "Bronze partitioning" "SKIP" "no partitions found" }
} else { Add-Result "Bronze partitioning" "SKIP" "bronze not produced yet" }

# the reconciliation record job 02 writes: rows_in = good + quarantined
$recon = Join-Path $RepoRoot "data\silver\reconciliation"
if (Test-Path $recon) { Add-Result "Lake reconciliation" "PASS" "silver/reconciliation written by job 02" }
else                  { Add-Result "Lake reconciliation" "SKIP" "not produced yet - run job 02" }

# ------------------------------------------------------ 5. SQL Server ------
Write-Host ""
Write-Host "SQL Server" -ForegroundColor White
$version = if ($dockerOk) { Test-Sql "SET NOCOUNT ON; SELECT 1" "master" } else { $null }
if (-not $version) {
    foreach ($a in @("Connectivity","Databases","Schemas","Dimensions","Facts","Unknown members",
                     "SCD2 integrity","Quality checks","Quality gate","Analytics SQL")) {
        Add-Result $a "SKIP" "SQL Server unreachable"
    }
} else {
    Add-Result "Connectivity" "PASS" "reachable in trafficdw-mssql"

    $dbs = Test-Sql "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name IN ('TrafficOLTP','TrafficDW')" "master"
    if ($dbs -eq "2") { Add-Result "Databases" "PASS" "TrafficOLTP + TrafficDW" }
    else              { Add-Result "Databases" "FAIL" "expected 2, found $dbs - deploy the schema" }

    $sch = Test-Sql "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.schemas WHERE name IN ('dim','fact','stg','etl','mart')"
    if ($sch -eq "5") { Add-Result "Schemas" "PASS" "dim/fact/stg/etl/mart" }
    else              { Add-Result "Schemas" "FAIL" "expected 5, found $sch" }

    $dims = Test-Sql "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.tables WHERE schema_id = SCHEMA_ID('dim')"
    if ([int]$dims -ge 10) { Add-Result "Dimensions" "PASS" "$dims dimension tables" }
    else                   { Add-Result "Dimensions" "FAIL" "expected 10, found $dims" }

    $facts = Test-Sql "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.tables WHERE schema_id = SCHEMA_ID('fact')"
    if ($facts -eq "3") { Add-Result "Facts" "PASS" "3 fact tables (transaction / periodic / accumulating)" }
    else                { Add-Result "Facts" "FAIL" "expected 3, found $facts" }

    $procs = Test-Sql "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.procedures WHERE schema_id = SCHEMA_ID('etl')"
    if ([int]$procs -ge 18) { Add-Result "ETL procedures" "PASS" "$procs procedures in etl" }
    else                    { Add-Result "ETL procedures" "FAIL" "expected >= 18, found $procs" }

    $unk = Test-Sql @"
SET NOCOUNT ON;
SELECT (CASE WHEN EXISTS(SELECT 1 FROM dim.DimDate WHERE DateKey=-1) THEN 1 ELSE 0 END)
     + (CASE WHEN EXISTS(SELECT 1 FROM dim.DimTime WHERE TimeKey=-1) THEN 1 ELSE 0 END)
     + (CASE WHEN EXISTS(SELECT 1 FROM dim.DimRoadSegment WHERE RoadSegmentKey=-1) THEN 1 ELSE 0 END)
     + (CASE WHEN EXISTS(SELECT 1 FROM dim.DimSensor WHERE SensorKey=-1) THEN 1 ELSE 0 END)
     + (CASE WHEN EXISTS(SELECT 1 FROM dim.DimTrafficCamera WHERE CameraKey=-1) THEN 1 ELSE 0 END)
     + (CASE WHEN EXISTS(SELECT 1 FROM dim.DimVehicleType WHERE VehicleTypeKey=-1) THEN 1 ELSE 0 END)
     + (CASE WHEN EXISTS(SELECT 1 FROM dim.DimWeatherCondition WHERE WeatherKey=-1) THEN 1 ELSE 0 END)
     + (CASE WHEN EXISTS(SELECT 1 FROM dim.DimTrafficLight WHERE TrafficLightKey=-1) THEN 1 ELSE 0 END)
     + (CASE WHEN EXISTS(SELECT 1 FROM dim.DimIncidentType WHERE IncidentTypeKey=-1) THEN 1 ELSE 0 END)
     + (CASE WHEN EXISTS(SELECT 1 FROM dim.DimEmergencyUnit WHERE EmergencyUnitKey=-1) THEN 1 ELSE 0 END)
"@
    if ($unk -eq "10") { Add-Result "Unknown members" "PASS" "all 10 dimensions carry key -1" }
    else               { Add-Result "Unknown members" "FAIL" "only $unk/10 dimensions have the -1 member" }

    $rows = Test-Sql "SET NOCOUNT ON; SELECT COUNT_BIG(*) FROM fact.FactTrafficEvent"
    if ([long]$rows -gt 0) { Add-Result "Fact data" "PASS" "$rows rows in FactTrafficEvent" }
    else                   { Add-Result "Fact data" "SKIP" "warehouse empty - run the pipeline" }

    $scd = Test-Sql @"
SET NOCOUNT ON;
SELECT (SELECT COUNT(*) FROM (SELECT SegmentCode FROM dim.DimRoadSegment WHERE IsCurrent=1 AND RoadSegmentKey<>-1
                              GROUP BY SegmentCode HAVING COUNT(*)>1) a)
     + (SELECT COUNT(*) FROM dim.DimRoadSegment WHERE RoadSegmentKey<>-1 AND EffectiveDate>ExpirationDate)
     + (SELECT COUNT(*) FROM dim.DimSensor WHERE SensorKey<>-1 AND EffectiveDate>ExpirationDate)
"@
    if ($scd -eq "0") { Add-Result "SCD2 integrity" "PASS" "one current row per key, no inverted intervals" }
    else              { Add-Result "SCD2 integrity" "FAIL" "$scd violation(s) - see tests/sql/test_warehouse_invariants.sql" }

    $checks = Test-Sql "SET NOCOUNT ON; SELECT COUNT(*) FROM etl.QualityCheckCatalog WHERE IsEnabled=1"
    if ([int]$checks -ge 40) { Add-Result "Quality checks" "PASS" "$checks rules in the catalog" }
    elseif ($checks)         { Add-Result "Quality checks" "FAIL" "expected >= 40, found $checks" }
    else                     { Add-Result "Quality checks" "SKIP" "catalog not deployed" }

    $failed = Test-Sql @"
SET NOCOUNT ON;
WITH latest AS (SELECT CheckName, Severity, Status,
                       ROW_NUMBER() OVER (PARTITION BY CheckName ORDER BY CheckedAt DESC, QualityCheckLogID DESC) rn
                FROM etl.QualityCheckLog)
SELECT COUNT(*) FROM latest WHERE rn=1 AND Severity='Error' AND Status='Fail'
"@
    if ($null -eq $failed -or $failed -eq "") { Add-Result "Quality gate" "SKIP" "no checks have run yet" }
    elseif ($failed -eq "0")                  { Add-Result "Quality gate" "PASS" "no Error-severity failures" }
    else                                      { Add-Result "Quality gate" "FAIL" "$failed Error check(s) failing - EXEC etl.usp_GetQualityReport" }

    # ------------------------------------------- 5b. advanced SQL deliverables --
    # THE BLIND SPOT THIS CLOSES: nothing else in this suite ever EXECUTES
    # sql/analytics/*.sql. The board therefore read READY while
    # 03_recursive_cte.sql aborted on every single run - a missing semicolon
    # before a CTE's WITH - because the failure lived in a graded deliverable
    # that no automated step touched. A demo script that has never been run is
    # not evidence of anything.
    #
    # Two ways to fail, because "it did not error" is a weak bar for a query
    # whose whole job is to return an answer:
    #   * non-zero exit  -> the script is broken
    #   * zero rows      -> it runs but demonstrates nothing. That was real too:
    #                       Q2 asked for routes to an intersection 11 hops away
    #                       with the search capped at 5, so it could only ever
    #                       return an empty set.
    # Zero-row results are reported as FAIL rather than a warning: an empty
    # showcase query in front of an examiner is a defect, not a curiosity.
    $analyticsDir = Join-Path $RepoRoot "sql\analytics"
    if (-not (Test-Path $analyticsDir)) {
        Add-Result "Analytics SQL" "SKIP" "sql/analytics not present"
    } else {
        $scripts = Get-ChildItem $analyticsDir -Filter "*.sql" | Sort-Object Name
        if (-not $scripts) {
            Add-Result "Analytics SQL" "SKIP" "no .sql files in sql/analytics"
        } else {
            $broken = @(); $barren = @()
            foreach ($sc in $scripts) {
                # docker cp, never `-i` with piped stdin: PowerShell encodes the
                # stream with $OutputEncoding, which varies by session and can
                # prepend a BOM that sqlcmd reports as "Incorrect syntax near '?'".
                $cp = Invoke-NativeQuiet { docker cp $sc.FullName trafficdw-mssql:/tmp/_verify_analytics.sql }
                if ($cp.ExitCode -ne 0) { $broken += "$($sc.BaseName) (copy failed)"; continue }

                $r = Invoke-NativeQuiet {
                    docker exec trafficdw-mssql /opt/mssql-tools18/bin/sqlcmd `
                        -S localhost -U sa -P $SqlPassword -C -b -I -d TrafficDW -i /tmp/_verify_analytics.sql
                }
                if ($r.ExitCode -ne 0) {
                    $msg = ($r.Output | Select-String -Pattern "^Msg \d+" | Select-Object -First 1)
                    $broken += "$($sc.BaseName)$(if ($msg) { " - $($msg.Line.Trim())" })"
                }
                elseif ($r.Output | Select-String -Pattern "^\(0 rows affected\)" -Quiet) {
                    $barren += $sc.BaseName
                }
            }

            if ($broken) {
                Add-Result "Analytics SQL" "FAIL" "$($broken.Count)/$($scripts.Count) failed: $($broken -join '; ')"
            } elseif ($barren) {
                Add-Result "Analytics SQL" "FAIL" "$($barren -join ', ') ran but returned an empty result set"
            } else {
                Add-Result "Analytics SQL" "PASS" "$($scripts.Count) script(s) executed, all returned rows"
            }
        }
    }
}

# ---------------------------------------------------------- 6. tests ------
# TWO TIERS, RUN WHERE EACH ACTUALLY WORKS.
#
# The host tier excludes the `spark` marker on purpose. PySpark on Windows needs
# the JVM to accept a callback socket from the Python worker; when a firewall or
# loopback policy blocks it, every Spark test burns ~17s before dying with
#     java.net.SocketTimeoutException: Accept timed out
#         at org.apache.spark.api.python.PythonWorkerFactory.createSimpleWorker
# With 43 such tests and no bound, this step looked like a hang. Installing a JDK
# on the host makes the tests RUN rather than skip, which is exactly when the
# problem appears - so the fix is to run them where Spark is known good: the
# container, which is the project's primary execution path anyway.
Write-Host ""
Write-Host "Tests" -ForegroundColor White
if ($SkipTests) {
    Add-Result "pytest (host)" "SKIP" "-SkipTests specified"
    Add-Result "pytest (spark)" "SKIP" "-SkipTests specified"
}
elseif (-not $py) {
    Add-Result "pytest (host)" "SKIP" "python unavailable"
    Add-Result "pytest (spark)" "SKIP" "python unavailable"
}
else {
    # --- tier 1: source-data invariants, no JVM needed -----------------------
    $r = Invoke-NativeQuiet {
        python -m pytest (Join-Path $RepoRoot "tests") -q -m "not spark" -p no:cacheprovider
    }
    $summary = ($r.Output | Where-Object { $_ -match "passed|failed|error" } | Select-Object -Last 1)
    $summary = ($summary -replace '\s+', ' ').Trim()
    if ($r.ExitCode -eq 0)      { Add-Result "pytest (host)" "PASS" $summary }
    elseif ($r.ExitCode -eq 5)  { Add-Result "pytest (host)" "SKIP" "no non-spark tests collected" }
    else                        { Add-Result "pytest (host)" "FAIL" $summary }

    # --- tier 2: Spark tests, inside the container --------------------------
    if (-not $dockerOk) {
        Add-Result "pytest (spark)" "SKIP" "docker engine unavailable"
    } else {
        $up = Invoke-NativeQuiet { docker inspect -f '{{.State.Running}}' trafficdw-spark }
        if ($up.ExitCode -ne 0 -or ("$($up.Output)" -notmatch 'true')) {
            Add-Result "pytest (spark)" "SKIP" "trafficdw-spark not running - start the stack"
        } else {
            # pyspark lives under /opt/spark/python; only spark-submit puts it on
            # sys.path, so plain python3 needs it spelled out
            $pyPath = "/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"
            $has = Invoke-NativeQuiet { docker exec trafficdw-spark python3 -m pytest --version }
            if ($has.ExitCode -ne 0) {
                Add-Result "pytest (spark)" "SKIP" "pytest not installed in the container - docker exec trafficdw-spark pip install pytest"
            } else {
                $r2 = Invoke-NativeQuiet {
                    docker exec -w /opt/project -e PYTHONPATH=$pyPath trafficdw-spark `
                        python3 -m pytest tests -q --no-header -p no:cacheprovider
                }
                $s2 = ($r2.Output | Where-Object { $_ -match "passed|failed|error" } | Select-Object -Last 1)
                $s2 = ($s2 -replace '\s+', ' ').Trim()
                if ($r2.ExitCode -eq 0) { Add-Result "pytest (spark)" "PASS" "in container: $s2" }
                else                    { Add-Result "pytest (spark)" "FAIL" "in container: $s2" }
            }
        }
    }
}

# ------------------------------------------------------ 7. dashboard ------
Write-Host ""
Write-Host "Dashboard" -ForegroundColor White
if (-not $py) { Add-Result "Streamlit app" "SKIP" "python unavailable" }
else {
    & python -c "import streamlit, plotly" 2>$null
    if ($LASTEXITCODE -eq 0) { Add-Result "Streamlit app" "PASS" "dependencies installed - streamlit run dashboard/app.py" }
    else { Add-Result "Streamlit app" "SKIP" "pip install -r requirements-dev.txt to enable" }
}

# ------------------------------------------------------------ summary -----
Write-Host ""
Write-Host ("=" * 60) -ForegroundColor DarkCyan
$pass = @($Results | Where-Object Status -eq "PASS").Count
$fail = @($Results | Where-Object Status -eq "FAIL").Count
$skip = @($Results | Where-Object Status -eq "SKIP").Count

Write-Host ("  RESULT: {0} PASS   {1} FAIL   {2} SKIP" -f $pass, $fail, $skip) -ForegroundColor White
Write-Host ""
if ($fail -gt 0) {
    Write-Host "  FINAL STATUS: NOT READY - $fail check(s) failed" -ForegroundColor Red
} elseif ($skip -gt 0) {
    Write-Host "  FINAL STATUS: PARTIALLY VERIFIED - $skip check(s) could not run" -ForegroundColor Yellow
    Write-Host "  (nothing failed; the skipped items need infrastructure or a pipeline run)" -ForegroundColor DarkGray
} else {
    Write-Host "  FINAL STATUS: READY" -ForegroundColor Green
}
Write-Host ("=" * 60) -ForegroundColor DarkCyan
Write-Host ""

if ($fail -gt 0) { exit 1 }