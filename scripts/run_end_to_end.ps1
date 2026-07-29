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
    Prerequisites (see docs/13): SQL Server 2019+ reachable, sqlcmd, Python 3.10+
    with pyspark installed, spark-submit on PATH, JDK for Spark.
    Data outside 2026 needs new partition boundaries (etl.usp_AddMonthlyPartition).
#>
[CmdletBinding()]
param(
    [int]$Days = 7,
    [int]$Sensors = 200,
    [string]$StartDate = "2026-06-01",
    [int]$Seed = 42,
    [int]$EventsPerHour = 4000,
    [string]$SqlServer = "localhost",
    [string]$SqlUser = "",                 # empty = Windows integrated auth
    [string]$SqlPassword = "",
    [switch]$SkipSetup,                    # databases/schema already deployed
    [switch]$SkipGenerate,                 # raw files already generated
    [string]$JdbcPackage = "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$PhaseLog = New-Object System.Collections.ArrayList
$RunStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

# ---------------------------------------------------------------- helpers ---
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

function Get-SqlcmdArgs([string]$Database) {
    # -b: exit non-zero on SQL errors (THROW/RAISERROR) -> phase fails
    # -I: QUOTED_IDENTIFIER ON (required by the filtered SCD2 indexes)
    $sqlArgs = @("-S", $SqlServer, "-d", $Database, "-b", "-I")
    if ($SqlUser -ne "") { $sqlArgs += @("-U", $SqlUser, "-P", $SqlPassword) }
    else                 { $sqlArgs += "-E" }
    return $sqlArgs
}

function Invoke-SqlFile([string]$RelativePath) {
    $path = Join-Path $RepoRoot $RelativePath
    Write-Host ("  sqlcmd -i " + $RelativePath)
    & sqlcmd @(Get-SqlcmdArgs "master") -i $path
    if ($LASTEXITCODE -ne 0) { throw "sqlcmd failed for $RelativePath (exit $LASTEXITCODE)" }
}

function Invoke-SqlQuery([string]$Query, [string]$Database = "TrafficDW") {
    & sqlcmd @(Get-SqlcmdArgs $Database) -Q $Query
    if ($LASTEXITCODE -ne 0) { throw "sqlcmd failed (exit $LASTEXITCODE): $Query" }
}

function Invoke-SparkJob([string]$JobRelativePath, [string[]]$JobArgs, [bool]$WithJdbc = $false) {
    $job = Join-Path $RepoRoot $JobRelativePath
    $submitArgs = @()
    if ($WithJdbc) { $submitArgs += @("--packages", $JdbcPackage) }
    $submitArgs += $job
    $submitArgs += $JobArgs
    Write-Host ("  spark-submit " + $JobRelativePath + " " + ($JobArgs -join " "))
    & spark-submit @submitArgs
    if ($LASTEXITCODE -ne 0) { throw "spark-submit failed for $JobRelativePath (exit $LASTEXITCODE)" }
}

function Show-PhaseSummary {
    Write-Banner "RUN TIMING"
    $PhaseLog | Format-Table Phase, Status, Seconds -AutoSize | Out-Host
    Write-Host ("Total wall time: {0:n1} min" -f $RunStopwatch.Elapsed.TotalMinutes)
}

# -------------------------------------------------------------- preflight ---
Invoke-Phase "Preflight checks" {
    foreach ($tool in @("python", "sqlcmd", "spark-submit")) {
        if ($null -eq (Get-Command $tool -ErrorAction SilentlyContinue)) {
            throw "Required tool not found on PATH: $tool (see docs/13_execution_runbook.md, Prerequisites)"
        }
        Write-Host ("  found: " + (Get-Command $tool).Source)
    }
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
        Invoke-SparkJob "spark\jobs\05_load_warehouse.py"      @("--date", $loadDate) $true

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
Write-Host "  - point Power BI at the mart.vPbi* views (docs/12_powerbi_dashboards.md)"
Write-Host "  - explore sql/analytics/ and the mart report views"
Write-Host "  - full report anytime:  EXEC etl.usp_ExecutionSummary;"
