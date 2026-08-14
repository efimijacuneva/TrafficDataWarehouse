<#
.SYNOPSIS
    One-command end-to-end run of the Smart City Traffic Data Warehouse
    (docs/13_execution_runbook.md).

.DESCRIPTION
    Drives the full documented workflow on a local machine:
      1. generate synthetic source data (CSV / JSON, seeded + manifest)
      2. create the OLTP + warehouse databases and load OLTP sample data
      3. per load date: Spark bronze -> silver -> gold, JDBC load to staging,
         T-SQL warehouse pipeline (dims -> facts -> quality checks)
      4. Spark KPI datasets over the full gold layer
      5. data-quality gate (etl.usp_AssertQuality) per batch
      6. production-style execution summary (etl.usp_ExecutionSummary)

    Every step is idempotent per date (safe to re-run); a failing step aborts
    the run with a non-zero exit code, like a production scheduler would.

.EXAMPLE
    .\scripts\run_end_to_end.ps1
    .\scripts\run_end_to_end.ps1 -Days 7 -Sensors 200 -StartDate 2026-06-01 -Seed 42
    .\scripts\run_end_to_end.ps1 -SkipSetup -Days 1 -StartDate 2026-06-08   # add one day

.NOTES
    THE single primary execution path. Everything except the data generator runs
    inside Docker, so the only host prerequisites are:

        Docker Desktop (running)  +  Python 3.10+

    No host Java, Spark or SQL Server installation is needed - they live in the
    containers started by orchestration/docker-compose.yml.

    Data outside 2026 needs new partition boundaries (etl.usp_AddMonthlyPartition).
#>
[CmdletBinding()]
param(
    [int]$Days = 7,
    [int]$Sensors = 200,
    [string]$StartDate = "2026-06-01",
    [int]$Seed = 42,
    [int]$EventsPerHour = 4000,
    [string]$SqlPassword = "ChangeMe_Demo1!",   # matches MSSQL_SA_PASSWORD in compose
    [switch]$SkipSetup,                    # databases/schema already deployed
    [switch]$SkipGenerate,                 # raw files already generated
    [switch]$SkipInfra,                    # containers already up
    [string]$JdbcVersion = "12.6.1.jre11"
)

$ErrorActionPreference = "Stop"
$RepoRoot   = Split-Path -Parent $PSScriptRoot
$ComposeDir = Join-Path $RepoRoot "orchestration"
$JarsDir    = Join-Path $ComposeDir "jars"
$JdbcJar    = "mssql-jdbc-$JdbcVersion.jar"
$JdbcUrl    = "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/$JdbcVersion/$JdbcJar"
$PhaseLog = New-Object System.Collections.ArrayList
$RunStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

# ---------------------------------------------------------------- helpers ---
# Windows PowerShell 5.1 wraps a native command's stderr in ErrorRecords AS SOON
# AS IT IS REDIRECTED (*> $null, 2>&1, 2>$null). With $ErrorActionPreference =
# 'Stop' that turns any harmless warning into a TERMINATING error even when the
# command exited 0 - e.g. `docker info` printing
#     WARNING: No blkio throttle.read_bps_device support
# used to abort the whole preflight on a perfectly healthy Docker install.
# Run redirected native calls with the preference relaxed and judge success by
# the EXIT CODE, which is the only reliable signal.
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

function Write-Banner([string]$Text) {
    Write-Host ""
    Write-Host ("=" * 78) -ForegroundColor DarkCyan
    Write-Host ("  " + $Text) -ForegroundColor Cyan
    Write-Host ("=" * 78) -ForegroundColor DarkCyan
}

function Invoke-Phase([string]$Name, [scriptblock]$Body) {
    Write-Banner $Name
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        & $Body
        $sw.Stop()
        [void]$PhaseLog.Add([pscustomobject]@{ Phase = $Name; Status = "OK"; Seconds = [math]::Round($sw.Elapsed.TotalSeconds, 1) })
    }
    catch {
        $sw.Stop()
        [void]$PhaseLog.Add([pscustomobject]@{ Phase = $Name; Status = "FAILED"; Seconds = [math]::Round($sw.Elapsed.TotalSeconds, 1) })
        Write-Host ""
        Write-Host "PHASE FAILED: $Name" -ForegroundColor Red
        Write-Host $_.Exception.Message -ForegroundColor Red
        Show-PhaseSummary
        exit 1
    }
}

# SQL runs INSIDE the mssql container (sqlcmd ships in the image), so the host
# needs no sqlcmd install and there is no host->container auth/TLS to configure.
#   -b: exit non-zero on SQL errors (THROW/RAISERROR) -> the phase fails
#   -I: QUOTED_IDENTIFIER ON (required by the filtered SCD2 indexes)
#   -C: trust the container's self-signed certificate
function Invoke-SqlQuery([string]$Query, [string]$Database = "TrafficDW") {
    & docker exec trafficdw-mssql /opt/mssql-tools18/bin/sqlcmd `
        -S localhost -U sa -P $SqlPassword -C -b -I -d $Database -Q $Query
    if ($LASTEXITCODE -ne 0) { throw "sqlcmd failed (exit $LASTEXITCODE): $Query" }
}

function Invoke-SqlFile([string]$RelativePath) {
    # ../sql is mounted read-only at /opt/sql inside the mssql container
    $containerPath = "/opt/sql/" + (($RelativePath -replace '\\', '/') -replace '^sql/', '')
    Write-Host ("  sqlcmd -i " + $RelativePath)
    & docker exec trafficdw-mssql /opt/mssql-tools18/bin/sqlcmd `
        -S localhost -U sa -P $SqlPassword -C -b -I -d master -i $containerPath
    if ($LASTEXITCODE -ne 0) { throw "sqlcmd failed for $RelativePath (exit $LASTEXITCODE)" }
}

function Invoke-SparkJob([string]$JobRelativePath, [string[]]$JobArgs, [switch]$WithJdbc) {
    $containerJob = $JobRelativePath -replace '\\', '/'
    $submitArgs = @(
        "exec",
        "-e", "PYSPARK_PYTHON=/usr/bin/python3",
        "-e", "PYSPARK_DRIVER_PYTHON=/usr/bin/python3",
        "-w", "/opt/project",
        "trafficdw-spark",
        "/opt/spark/bin/spark-submit"
    )
    if ($WithJdbc) {
        # --jars with the PRE-DOWNLOADED driver, not --packages: no internet
        # dependency mid-pipeline and no Ivy resolution on every submit. The
        # previous version declared a $JdbcPackage parameter and then never
        # passed it, so job 05 always died with ClassNotFoundException.
        $submitArgs += @("--jars", "/opt/jars/$JdbcJar")
    }
    $submitArgs += "/opt/project/$containerJob"
    $submitArgs += $JobArgs

    Write-Host ("  docker " + ($submitArgs -join " "))
    & docker @submitArgs
    if ($LASTEXITCODE -ne 0) { throw "spark-submit failed for $JobRelativePath (exit $LASTEXITCODE)" }
}

function Show-PhaseSummary {
    Write-Banner "RUN TIMING"
    $PhaseLog | Format-Table Phase, Status, Seconds -AutoSize | Out-Host
    Write-Host ("Total wall time: {0:n1} min" -f $RunStopwatch.Elapsed.TotalMinutes)
}

# -------------------------------------------------------------- preflight ---
# Checks the tools this script ACTUALLY uses. The previous version required
# spark-submit on the host PATH and then never called it (every job goes through
# `docker exec`), so it passed preflight on machines where it could not work and
# failed it on machines where it could.
Invoke-Phase "Preflight - host tools" {
    foreach ($tool in @("docker", "python")) {
        if ($null -eq (Get-Command $tool -ErrorAction SilentlyContinue)) {
            throw "Required tool not found on PATH: $tool (see docs/13_execution_runbook.md, Prerequisites)"
        }
        Write-Host ("  found: " + (Get-Command $tool).Source)
    }
    if ((Invoke-NativeQuiet { docker info }).ExitCode -ne 0) {
        throw "Docker is installed but its engine is not running. Start Docker Desktop and re-run."
    }
    Write-Host "  docker engine: running"
    Write-Host "  (Java, Spark and SQL Server run in containers - no host install required)"
}

# ------------------------------------------------------ JDBC driver, once ---
Invoke-Phase "Provision MS SQL JDBC driver ($JdbcJar)" {
    if (-not (Test-Path $JarsDir)) { New-Item -ItemType Directory -Force $JarsDir | Out-Null }
    $target = Join-Path $JarsDir $JdbcJar
    if (Test-Path $target) {
        Write-Host ("  already present: {0} ({1:n0} bytes)" -f $target, (Get-Item $target).Length)
    } else {
        Write-Host "  downloading $JdbcUrl"
        try {
            Invoke-WebRequest -Uri $JdbcUrl -OutFile $target -UseBasicParsing
        } catch {
            throw ("Could not download the JDBC driver. Download $JdbcJar manually from " +
                   "Maven Central, place it in $JarsDir, then re-run. Cause: " + $_.Exception.Message)
        }
        Write-Host ("  saved: {0} ({1:n0} bytes)" -f $target, (Get-Item $target).Length)
    }
}

# ------------------------------------------------------- start the stack ----
if (-not $SkipInfra) {
    Invoke-Phase "Start Docker stack (SQL Server + Spark)" {
        Push-Location $ComposeDir
        try {
            & docker compose up -d mssql spark
            if ($LASTEXITCODE -ne 0) { throw "docker compose up failed (exit $LASTEXITCODE)" }
        } finally { Pop-Location }

        Write-Host "  waiting for the SQL Server healthcheck..." -NoNewline
        $state = ""
        $deadline = (Get-Date).AddMinutes(5)
        while ((Get-Date) -lt $deadline) {
            $state = (Invoke-NativeQuiet {
                docker inspect --format '{{.State.Health.Status}}' trafficdw-mssql
            }).Output | Select-Object -First 1
            if ($state -eq "healthy") { Write-Host " healthy"; break }
            Write-Host "." -NoNewline
            Start-Sleep -Seconds 5
        }
        if ($state -ne "healthy") {
            throw "SQL Server did not become healthy within 5 minutes. Inspect: docker logs trafficdw-mssql"
        }
    }
}

Invoke-Phase "Verify SQL Server reachable" {
    Invoke-SqlQuery "SELECT @@SERVERNAME AS ServerName, @@VERSION AS Version;" "master"
}

# ------------------------------------------------------- 1. generate data ---
if (-not $SkipGenerate) {
    Invoke-Phase "1. Generate synthetic source data ($Days days, $Sensors sensors, seed $Seed)" {
        & python (Join-Path $RepoRoot "data_generator\generate_data.py") `
            --days $Days --sensors $Sensors --start $StartDate `
            --events-per-hour $EventsPerHour --seed $Seed
        if ($LASTEXITCODE -ne 0) { throw "data generator failed (exit $LASTEXITCODE)" }
    }
}

# ------------------------------------- 2. databases, schema, OLTP samples ---
if (-not $SkipSetup) {
    Invoke-Phase "2. Create databases + schema, load OLTP sample data" {
        Invoke-SqlQuery @"
IF DB_ID('TrafficOLTP') IS NOT NULL
BEGIN
    ALTER DATABASE [TrafficOLTP] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [TrafficOLTP];
END
IF DB_ID('TrafficDW') IS NOT NULL
BEGIN
    ALTER DATABASE [TrafficDW] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [TrafficDW];
END
"@ "master"

        $setupFiles = @(
            "sql\oltp\01_create_database.sql",
            "sql\oltp\02_tables.sql",
            "sql\oltp\03_sample_data.sql",
            "sql\warehouse\01_create_warehouse.sql",
            "sql\warehouse\02_dimensions.sql",
            "sql\warehouse\03_facts.sql",
            "sql\warehouse\04_seed_date_time.sql",
            "sql\warehouse\05_indexes_partitioning.sql",
            "sql\etl\00_security.sql",
            "sql\etl\01_staging.sql",
            "sql\etl\02_etl_framework.sql",
            "sql\etl\03_load_dimensions.sql",
            "sql\etl\04_load_facts.sql",
            "sql\etl\05_quality_checks.sql",
            "sql\etl\06_execution_summary.sql",
            "sql\warehouse\06_mart_views.sql",
            "sql\warehouse\07_powerbi_views.sql"
        )
        foreach ($f in $setupFiles) { Invoke-SqlFile $f }
    }
}

# ------------------------- 3. per-date: Spark medallion + warehouse load ----
$startDt = [datetime]::ParseExact($StartDate, "yyyy-MM-dd", $null)
for ($offset = 0; $offset -lt $Days; $offset++) {
    $loadDate = $startDt.AddDays($offset).ToString("yyyy-MM-dd")

    Invoke-Phase "3. Load date $loadDate  (Spark bronze->silver->gold, stage, warehouse)" {
        Invoke-SparkJob "spark\jobs\01_ingest_raw.py"          @("--date", $loadDate)
        Invoke-SparkJob "spark\jobs\02_clean_validate.py"      @("--date", $loadDate)
        Invoke-SparkJob "spark\jobs\03_transform_aggregate.py" @("--date", $loadDate)
        Invoke-SparkJob "spark\jobs\05_load_warehouse.py"      @("--date", $loadDate) -WithJdbc

        Invoke-SqlQuery "EXEC etl.usp_RunNightlyPipeline @LoadDate = '$loadDate';"

        # production gate: fail this date's run if any Error-severity check failed
        Invoke-SqlQuery ("DECLARE @b INT = (SELECT MAX(ETLBatchID) FROM etl.BatchLog); " +
                         "EXEC etl.usp_AssertQuality @b;")
    }
}

# --------------------------------------------------- 4. Spark KPI datasets --
Invoke-Phase "4. Spark KPI datasets (gold/kpi_*, quality_daily)" {
    Invoke-SparkJob "spark\jobs\04_generate_kpis.py" @()
}

# ------------------------------------------- 5. quality report + summary ----
$endDate = $startDt.AddDays($Days - 1).ToString("yyyy-MM-dd")

Invoke-Phase "5. Data-quality report (last batch)" {
    Invoke-SqlQuery "EXEC etl.usp_GetQualityReport;"
}

Invoke-Phase "6. Execution summary $StartDate .. $endDate" {
    Invoke-SqlQuery "EXEC etl.usp_ExecutionSummary '$StartDate', '$endDate';"
}

# ------------------------------------------------ generated-data manifest ---
$manifestPath = Join-Path $RepoRoot "data\raw\_manifest.json"
if (Test-Path $manifestPath) {
    Write-Banner "GENERATED SOURCE DATA (data/raw/_manifest.json)"
    $manifest = Get-Content $manifestPath -Raw | ConvertFrom-Json
    $totalCsv = ($manifest | Measure-Object -Property sensor_csv_rows -Sum).Sum
    $totalCam = ($manifest | Measure-Object -Property camera_events -Sum).Sum
    Write-Host ("  Days generated       : {0}" -f @($manifest).Count)
    Write-Host ("  Sensors              : {0}" -f $Sensors)
    Write-Host ("  Sensor CSV rows      : {0:n0}" -f $totalCsv)
    Write-Host ("  Camera JSON events   : {0:n0}" -f $totalCam)
    Write-Host ("  Raw records total    : {0:n0}" -f ($totalCsv + $totalCam))
}

Show-PhaseSummary
Write-Host ""
Write-Host "Warehouse is loaded. Next steps:" -ForegroundColor Green
Write-Host "  - verify everything :  .\scriptserify_project.ps1"
Write-Host "  - launch dashboard  :  streamlit run dashboard/app.py"
Write-Host "  - demonstrate SCD2  :  .\scripts
un_scd2_demo.ps1"
Write-Host "  - Power BI          :  import the mart.vPbi* views (docs/12_powerbi_dashboards.md)"
Write-Host "  - full report anytime: EXEC etl.usp_ExecutionSummary;"
Write-Host ""
Write-Host "The stack is still running. Stop it with:" -ForegroundColor DarkGray
Write-Host "  docker compose -f orchestration/docker-compose.yml down" -ForegroundColor DarkGray
