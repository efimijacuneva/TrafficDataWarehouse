<#
.SYNOPSIS
    Prune lake partitions that Spark's dynamic partition overwrite cannot remove.

.DESCRIPTION
    Spark's partitionOverwriteMode=dynamic replaces the partitions a job WRITES
    and never deletes ones it does not touch. That is exactly what we want for
    idempotent day-level reruns, but it leaves two kinds of debris behind:

    1. STALE DATES - re-generating the source with a different date range (or a
       different --seed) leaves the old date partitions forever, and a later
       full-layer read (job 04 scans all of gold) silently includes them.

    2. STALE PARTITION SCHEME - if the partition COLUMN itself changes, the old
       directories are not merely stale, they are structurally incompatible.
       Spark refuses to read the dataset at all:

           Conflicting directory structures detected:
             .../sensor_readings/event_date=2026-06-05
             .../sensor_readings/ingest_date=2026-06-01

       This is what happens when bronze moves from event_date (derived from the
       row's own timestamp, which a corrupt row can lie about) to ingest_date
       (the date of the file it arrived in). The new layout is correct; the old
       directories simply have to go.

    The current scheme is inferred from the MOST RECENTLY WRITTEN partition, so
    the script needs no hard-coded knowledge of which column each zone uses.

    Delta Lake's replaceWhere would make all of this unnecessary (docs/11).

.EXAMPLE
    .\scripts\clean_lake.ps1 -WhatIf     # show what would be deleted
    .\scripts\clean_lake.ps1             # delete it
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param()

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$RawDir   = Join-Path $RepoRoot "data\raw"
$DataDir  = Join-Path $RepoRoot "data"

if (-not (Test-Path $RawDir)) { throw "data/raw not found - generate source data first." }

# the dates the current raw feed actually covers, taken from the filenames
$validDates = @(
    Get-ChildItem $RawDir -Filter "sensor_readings_*.csv" | ForEach-Object {
        if ($_.BaseName -match '_(\d{4})(\d{2})(\d{2})$') { "$($Matches[1])-$($Matches[2])-$($Matches[3])" }
    }
) | Sort-Object -Unique

if (-not $validDates) { throw "No sensor_readings_YYYYMMDD.csv files in data/raw - nothing to compare against." }

Write-Host ""
Write-Host "Current raw feed covers $($validDates.Count) date(s): $($validDates[0]) .. $($validDates[-1])" -ForegroundColor Cyan
Write-Host ""

# A "dataset" is any directory that directly contains partition directories,
# e.g. data/bronze/sensor_readings or data/silver/_quarantine/traffic_events.
$datasets = Get-ChildItem $DataDir -Recurse -Directory -ErrorAction SilentlyContinue |
    Where-Object {
        @(Get-ChildItem $_.FullName -Directory -ErrorAction SilentlyContinue |
          Where-Object { $_.Name -match '^[A-Za-z_]+=' }).Count -gt 0
    }

$removedScheme = 0
$removedDate   = 0
$kept          = 0

foreach ($ds in $datasets) {
    $parts = @(Get-ChildItem $ds.FullName -Directory | Where-Object { $_.Name -match '^([A-Za-z_]+)=(.+)$' })
    if (-not $parts) { continue }

    $rel = $ds.FullName.Substring($RepoRoot.Length + 1)

    # --- 1. partition-scheme conflict --------------------------------------
    $columns = $parts | ForEach-Object { ($_.Name -split '=', 2)[0] } | Sort-Object -Unique
    if ($columns.Count -gt 1) {
        # the scheme of the most recently written partition is the current one
        $current = ($parts | Sort-Object LastWriteTime -Descending | Select-Object -First 1).Name -replace '=.*$', ''
        Write-Host "  $rel" -ForegroundColor White
        Write-Host "    scheme conflict: $($columns -join ', ')  ->  keeping '$current'" -ForegroundColor Yellow

        foreach ($p in $parts) {
            $col = ($p.Name -split '=', 2)[0]
            if ($col -ne $current) {
                if ($PSCmdlet.ShouldProcess("$rel\$($p.Name)", "Remove obsolete partition scheme")) {
                    Remove-Item $p.FullName -Recurse -Force
                }
                $removedScheme++
            }
        }
        # re-read what survived, for the date pass below
        $parts = @(Get-ChildItem $ds.FullName -Directory | Where-Object { $_.Name -match '^([A-Za-z_]+)=(.+)$' })
    }

    # --- 2. stale dates -----------------------------------------------------
    foreach ($p in $parts) {
        $value = ($p.Name -split '=', 2)[1]
        # only date-valued partitions are compared against the feed
        if ($value -notmatch '^\d{4}-\d{2}-\d{2}$') { $kept++; continue }
        if ($validDates -contains $value) { $kept++; continue }

        if ($PSCmdlet.ShouldProcess("$rel\$($p.Name)", "Remove stale date partition")) {
            Remove-Item $p.FullName -Recurse -Force
        }
        Write-Host "    stale date: $($p.Name)" -ForegroundColor DarkYellow
        $removedDate++
    }
}

$verb = if ($WhatIfPreference) { "would be removed" } else { "removed" }
Write-Host ""
Write-Host "Kept $kept partition(s) matching the current feed and scheme." -ForegroundColor Green
Write-Host "$removedScheme obsolete-scheme and $removedDate stale-date partition(s) $verb." -ForegroundColor Green
if (($removedScheme + $removedDate) -gt 0 -and -not $WhatIfPreference) {
    Write-Host ""
    Write-Host "Re-run the pipeline so every layer is rebuilt consistently:" -ForegroundColor DarkGray
    Write-Host "  .\scripts\run_end_to_end.ps1 -SkipInfra -SkipGenerate" -ForegroundColor DarkGray
}
