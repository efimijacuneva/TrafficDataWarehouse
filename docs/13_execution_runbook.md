# Part 17 — End-to-End Execution Runbook

> Driver script: [scripts/run_end_to_end.ps1](../scripts/run_end_to_end.ps1) ·
> Summary report: [sql/etl/06_execution_summary.sql](../sql/etl/06_execution_summary.sql)
> This document lets another developer reproduce the whole project from a clean
> machine and finish with a production-style run report.

## 1. Prerequisites

| Requirement | Notes |
|---|---|
| SQL Server 2019+ | local instance, or `docker compose up -d mssql` from [orchestration/](../orchestration/) |
| `sqlcmd` | ships with SSMS / mssql-tools; script uses `-b -I` (fail on error, quoted identifiers) |
| Python 3.10+ | `pip install -r requirements.txt` (pyspark 3.5.1) |
| JDK 11/17 + `spark-submit` on PATH | pyspark's bundled `spark-submit` works: `pip show pyspark` → `Scripts/` |
| MS SQL JDBC driver | pulled automatically via `spark-submit --packages` (needs internet on first run) |
| Windows only: `winutils.exe` / `HADOOP_HOME` | standard Spark-on-Windows requirement for local file writes |

> **Partition coverage:** `pfMonthlyDateKey` covers 2026 (monthly) — generating
> data outside 2026 requires `EXEC etl.usp_AddMonthlyPartition @BoundaryDateKey = <yyyymm01>` first
> ([05_indexes_partitioning.sql](../sql/warehouse/05_indexes_partitioning.sql)).

## 2. One-command run

```powershell
# from the repo root — 7 seeded days, 200 sensors, full setup + load + report
.\scripts\run_end_to_end.ps1

# custom run
.\scripts\run_end_to_end.ps1 -Days 7 -Sensors 200 -StartDate 2026-06-01 -Seed 42 `
                             -SqlServer localhost

# append one more day to an existing warehouse (no re-setup)
.\scripts\run_end_to_end.ps1 -SkipSetup -Days 1 -StartDate 2026-06-08
```

The script is **idempotent per date** (every Spark job overwrites its date
partition; the transaction and snapshot facts delete-and-reload their date,
the incident fact and dimensions MERGE by key), aborts on the
first failing step with a non-zero exit code, and asserts the data-quality gate
after every load date — exactly what a production scheduler would do.

## 3. Execution order (manual / any platform)

The same sequence the script automates; run these by hand on Linux/macOS or
inside the docker containers. **Order matters and is a hard rule** — schema
before loads, dimensions before facts (doc 06).

### Phase A — generate source data (once per dataset)

```bash
python data_generator/generate_data.py --days 7 --sensors 200 --start 2026-06-01 --seed 42
# writes data/raw/*.csv|json + _manifest.json (row counts for reconciliation)
```

### Phase B — create databases and deploy code (once)

```bash
# source system
sqlcmd -S localhost -b -I -i sql/oltp/01_create_database.sql
sqlcmd -S localhost -b -I -i sql/oltp/02_tables.sql
sqlcmd -S localhost -b -I -i sql/oltp/03_sample_data.sql          # ← loads OLTP
# warehouse schema
sqlcmd -S localhost -b -I -i sql/warehouse/01_create_warehouse.sql
sqlcmd -S localhost -b -I -i sql/warehouse/02_dimensions.sql
sqlcmd -S localhost -b -I -i sql/warehouse/03_facts.sql
sqlcmd -S localhost -b -I -i sql/warehouse/04_seed_date_time.sql
sqlcmd -S localhost -b -I -i sql/warehouse/05_indexes_partitioning.sql
# ETL framework + loads + quality + summary
sqlcmd -S localhost -b -I -i sql/etl/00_security.sql              # ← etl_spark login (Spark JDBC, job 05)
sqlcmd -S localhost -b -I -i sql/etl/01_staging.sql
sqlcmd -S localhost -b -I -i sql/etl/02_etl_framework.sql
sqlcmd -S localhost -b -I -i sql/etl/03_load_dimensions.sql
sqlcmd -S localhost -b -I -i sql/etl/04_load_facts.sql
sqlcmd -S localhost -b -I -i sql/etl/05_quality_checks.sql        # ← quality framework
sqlcmd -S localhost -b -I -i sql/etl/06_execution_summary.sql     # ← run report
# presentation layer
sqlcmd -S localhost -b -I -i sql/warehouse/06_mart_views.sql
sqlcmd -S localhost -b -I -i sql/warehouse/07_powerbi_views.sql   # ← Power BI layer
```

### Phase C — per load date (repeat for each day)

```bash
D=2026-06-01
spark-submit spark/jobs/01_ingest_raw.py          --date $D        # raw → bronze
spark-submit spark/jobs/02_clean_validate.py      --date $D        # bronze → silver (+ quarantine)
spark-submit spark/jobs/03_transform_aggregate.py --date $D        # silver → gold
spark-submit --packages com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11 \
             spark/jobs/05_load_warehouse.py      --date $D        # gold → stg.* (JDBC)

sqlcmd -S localhost -d TrafficDW -b -I \
  -Q "EXEC etl.usp_RunNightlyPipeline @LoadDate = '$D';"           # extract → dims (SCD) → facts → quality
sqlcmd -S localhost -d TrafficDW -b -I \
  -Q "DECLARE @b INT=(SELECT MAX(ETLBatchID) FROM etl.BatchLog); EXEC etl.usp_AssertQuality @b;"   # gate
```

### Phase D — once after all dates

```bash
spark-submit spark/jobs/04_generate_kpis.py                        # gold KPI datasets
sqlcmd -S localhost -d TrafficDW -b -I -Q "EXEC etl.usp_GetQualityReport;"
sqlcmd -S localhost -d TrafficDW -b -I -Q "EXEC etl.usp_ExecutionSummary '2026-06-01','2026-06-07';"
```

### Phase E — reporting layer

Open Power BI Desktop → connect to `TrafficDW` → import `mart.vPbi*` →
build per [docs/12_powerbi_dashboards.md](12_powerbi_dashboards.md).
SQL-only consumers use the `mart.v*` report views and `sql/analytics/`.

**Alternative:** the Airflow DAG ([orchestration/dags/traffic_dw_dag.py](../orchestration/dags/traffic_dw_dag.py))
runs Phases C–D on a nightly schedule with retries, SLAs, backfills, and the
quality gate as a first-class task.

## 4. The execution summary

`EXEC etl.usp_ExecutionSummary @LoadDateFrom, @LoadDateTo` renders the run
report from the framework's own audit trail (BatchLog/RowLog/QualityCheckLog)
plus warehouse counts. The one-command script prints it automatically, together
with the generated-data numbers from `data/raw/_manifest.json`.

**Shape of the report** (result set 1; values below are *illustrative* — a
seeded run reproduces its own exact numbers, which is the point of `--seed`):

```
Metric                                            Value
------------------------------------------------  -----------------------
Load window                                       2026-06-01 .. 2026-06-07
Batches executed                                  7
Batches succeeded / failed                        7 / 0
Total execution time                              14 min 32 s
Traffic events loaded (transaction fact)          481,204
Hourly snapshot rows (periodic fact)              20,160
Incidents tracked (accumulating fact)             38
Active sensors (current dimension rows)           200
Active road segments (current dimension rows)     120
Rows rejected (quarantined, replayable)           7,318
Inferred members created (late-arriving dims)     0
SCD2 versions created (RoadSegment + Sensor)      0
Quality checks run (passed / failed)              238 (238 / 0)
```

Result set 2 lists per-batch detail (status, duration, rows in/out/rejected);
result set 3 breaks rejected rows down by reason (`SPEED_RANGE`,
`MISSING_KEY`, `DUPLICATE`, …) — the generator injects ~1.5% defects
deliberately, so **a healthy run has a non-zero reject count**; zero rejects
would actually indicate the silver gate is not working.

## 5. Verifying a run (spot checks)

```sql
-- batch + step audit trail
SELECT TOP (20) * FROM etl.BatchLog ORDER BY ETLBatchID DESC;
SELECT * FROM etl.RowLog WHERE ETLBatchID = <batch>;

-- quality gate detail for a batch
EXEC etl.usp_GetQualityReport <batch>;

-- the three fact grains answer real questions immediately
SELECT * FROM mart.vTopBusiestRoads;
SELECT * FROM mart.vRushHourProfile WHERE IsWeekend = 0 ORDER BY HourOfDay;
SELECT * FROM mart.vPbiKpiDaily ORDER BY DateKey;
```

Idempotency proof (doc 10's integration test, by hand): re-run Phase C for the
same date twice → fact counts identical, `DUP_FACT_TRAFFICEVENT` still passes.

## 6. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `spark-submit` not found | pyspark installed but Scripts dir not on PATH → `pip show -f pyspark`, add `Scripts/` |
| Spark write fails on Windows (`HADOOP_HOME`/winutils) | install winutils matching Hadoop 3.3, set `HADOOP_HOME` |
| Job 05: `ClassNotFoundException: SQLServerDriver` | run with `--packages` (Phase C) or put the JDBC jar on the classpath |
| Job 05: SSL/trust error connecting | dev certs: JDBC URL already sets `trustServerCertificate=true`; check server/port |
| `CREATE INDEX ... QUOTED_IDENTIFIER` error | run sqlcmd with `-I` (the script always does) |
| Fact insert FK violation on DateKey | load date outside DimDate seed or partition range → re-seed dates / add partition |
| `usp_AssertQuality` THROWs 50040 | the gate is doing its job: `EXEC etl.usp_GetQualityReport;` lists which checks failed and how many rows |
| Airflow task `sql_quality_checks` red | same as above — the DAG gate asserts the batch's quality log |
