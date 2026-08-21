# Part 17 — End-to-End Execution Runbook

> Driver script: [scripts/run_end_to_end.ps1](../scripts/run_end_to_end.ps1) ·
> Summary report: [sql/etl/06_execution_summary.sql](../sql/etl/06_execution_summary.sql)
> This document lets another developer reproduce the whole project from a clean
> machine and finish with a production-style run report.

## 1. Prerequisites

**There is ONE primary execution path, and it is containerised.** Spark, Java and
SQL Server all run in Docker; only the data generator runs on the host. That is
why the list below is two lines long.

| Requirement | Notes |
|---|---|
| **Docker Desktop**, running | Provides SQL Server 2022 and Spark 3.5.1 (`orchestration/docker-compose.yml`). `scripts/run_end_to_end.ps1` starts the stack itself and waits on the SQL Server healthcheck. |
| **Python 3.10+** | Only for `data_generator/generate_data.py`. `pip install -r requirements.txt` if you also want to run the Spark tests on the host. |
| *(internet, first run only)* | The script downloads the MS SQL JDBC driver once into `orchestration/jars/` and then passes it to Spark with `--jars`. No host Java, no host `spark-submit`, no host `sqlcmd`. |

> **Why not host-installed Spark?** It is possible (`pip install pyspark`, a JDK,
> and on Windows `winutils.exe` + `HADOOP_HOME`), but it is a second execution
> model that will drift from the first. Everything documented here — the script,
> the Airflow DAG, the SQL test runners — goes through the containers.

> **Partition coverage:** `pfMonthlyDateKey` covers 2026 (monthly) — generating
> data outside 2026 requires `EXEC etl.usp_AddMonthlyPartition @BoundaryDateKey = <yyyymm01>` first
> ([05_indexes_partitioning.sql](../sql/warehouse/05_indexes_partitioning.sql)).

## 2. One-command run

```powershell
# from the repo root — starts Docker, deploys schema, generates data,
# runs 7 seeded days end to end, gates quality, prints the run report
.\scripts\run_end_to_end.ps1

# custom run
.\scripts\run_end_to_end.ps1 -Days 7 -Sensors 200 -StartDate 2026-06-01 -Seed 42

# append one more day to an existing warehouse (no re-setup)
.\scripts\run_end_to_end.ps1 -SkipSetup -Days 1 -StartDate 2026-06-08

# containers already up, schema already deployed
.\scripts\run_end_to_end.ps1 -SkipInfra -SkipSetup -SkipGenerate
```

Then verify and demonstrate:

```powershell
.\scripts\verify_project.ps1     # end-to-end health check, PASS/FAIL per area
.\scripts\run_sql_tests.ps1      # warehouse invariants, idempotency, SCD2
.\scripts\run_scd2_demo.ps1      # the SCD Type 2 demonstration
streamlit run dashboard/app.py   # the visual pipeline dashboard
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
# Everything runs INSIDE the containers. ../sql is mounted read-only at /opt/sql
# in the mssql container, so deployment needs no host sqlcmd.
SQLC() { docker exec trafficdw-mssql /opt/mssql-tools18/bin/sqlcmd \
           -S localhost -U sa -P 'ChangeMe_Demo1!' -C -b -I -d master -i "$1"; }

# source system
SQLC /opt/sql/oltp/01_create_database.sql
SQLC /opt/sql/oltp/02_tables.sql
SQLC /opt/sql/oltp/03_sample_data.sql               # <- loads OLTP
# warehouse schema
SQLC /opt/sql/warehouse/01_create_warehouse.sql
SQLC /opt/sql/warehouse/02_dimensions.sql
SQLC /opt/sql/warehouse/03_facts.sql
SQLC /opt/sql/warehouse/04_seed_date_time.sql
SQLC /opt/sql/warehouse/05_indexes_partitioning.sql
# ETL framework + loads + quality + summary
SQLC /opt/sql/etl/00_security.sql                   # <- etl_spark login (Spark JDBC, job 05)
SQLC /opt/sql/etl/01_staging.sql
SQLC /opt/sql/etl/02_etl_framework.sql
SQLC /opt/sql/etl/03_load_dimensions.sql
SQLC /opt/sql/etl/04_load_facts.sql
SQLC /opt/sql/etl/05_quality_checks.sql             # <- quality framework (41 checks)
SQLC /opt/sql/etl/06_execution_summary.sql          # <- run report
# presentation layer
SQLC /opt/sql/warehouse/06_mart_views.sql
SQLC /opt/sql/warehouse/07_powerbi_views.sql        # <- Power BI layer
```

### Phase C — per load date (repeat for each day)

```bash
D=2026-06-01
SPARK="docker exec -w /opt/project trafficdw-spark /opt/spark/bin/spark-submit"
SQL() { docker exec trafficdw-mssql /opt/mssql-tools18/bin/sqlcmd \
          -S localhost -U sa -P 'ChangeMe_Demo1!' -C -b -I -d TrafficDW -Q "$1"; }

$SPARK /opt/project/spark/jobs/01_ingest_raw.py          --date $D   # raw    -> bronze
$SPARK /opt/project/spark/jobs/02_clean_validate.py      --date $D   # bronze -> silver + quarantine
$SPARK /opt/project/spark/jobs/03_transform_aggregate.py --date $D   # silver -> gold
# job 05 needs the JDBC driver the runner downloaded into orchestration/jars
$SPARK --jars /opt/jars/mssql-jdbc-12.6.1.jre11.jar \
       /opt/project/spark/jobs/05_load_warehouse.py      --date $D   # gold   -> stg.* (JDBC)

SQL "EXEC etl.usp_RunNightlyPipeline @LoadDate = '$D';"              # extract -> dims -> facts -> quality
SQL "DECLARE @b INT=(SELECT MAX(ETLBatchID) FROM etl.BatchLog); EXEC etl.usp_AssertQuality @b;"  # gate
```

### Phase D — once after all dates

```bash
$SPARK /opt/project/spark/jobs/04_generate_kpis.py                 # gold KPI datasets
$SQL "EXEC etl.usp_GetQualityReport;"
$SQL "EXEC etl.usp_ExecutionSummary '2026-06-01','2026-06-07';"
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

**Shape of the report** (result set 1). The right-hand column below describes
what each metric MEANS and, where it is arithmetically determined, how it is
derived — it is deliberately **not** a transcript of a run. Paste your own
output here after executing the pipeline; the `--seed` makes it reproducible.

```
Metric                                            Value
------------------------------------------------  -----------------------
Load window                                       <from>.. <to>
Batches executed                                  = number of load dates
Batches succeeded / failed                        all succeeded, or the run aborted
Total execution time                              wall clock, from etl.BatchLog
Traffic events loaded (transaction fact)          raw rows - quarantined - rejected
Hourly snapshot rows (periodic fact)              current segments x 24 x load dates
Incidents tracked (accumulating fact)             incidents DETECTED in the window
Active sensors (current dimension rows)           = sensors in TrafficOLTP
Active road segments (current dimension rows)     = segments in TrafficOLTP
Rows rejected (quarantined, replayable)           ~1.5% defect rate, minus the
                                                  sentinel defect (routed to the
                                                  unknown member, not rejected)
Inferred members created (late-arriving dims)     0 for a clean run: every business
                                                  key in the feed exists in the OLTP
SCD2 versions created (RoadSegment + Sensor)      0 until an attribute changes -
                                                  run scripts/run_scd2_demo.ps1
Quality checks run (passed / failed)              40 per batch
```

> **Sanity-check your own output.** `Traffic events loaded` can never exceed the
> raw record count in `data/raw/_manifest.json`; `Hourly snapshot rows` must equal
> segments x 24 x dates exactly; `Quality checks run` must be 40 x batches. If any
> of those disagree, something is wrong — that arithmetic is exactly what
> `tests/test_pipeline_reconciliation.py` automates.

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

-- no silent data loss: staged = loaded + rejected, per batch
EXEC etl.usp_ValidateBatch <batch>;

-- the SCD2 version history (after scripts/run_scd2_demo.ps1)
SELECT RoadSegmentKey, VersionNumber, SpeedLimitKmh, OriginalSpeedLimitKmh,
       CurrentSpeedLimitKmh, EffectiveDate, ExpirationDate, IsCurrent
FROM dim.DimRoadSegment WHERE SegmentCode = 'SEG-001' ORDER BY VersionNumber;

-- the three fact grains answer real questions immediately
SELECT * FROM mart.vTopBusiestRoads;
SELECT * FROM mart.vRushHourProfile WHERE IsWeekend = 0 ORDER BY HourOfDay;
SELECT * FROM mart.vPbiKpiDaily ORDER BY DateKey;
```

Idempotency proof — now automated rather than done by hand:

```powershell
.\scriptsun_sql_tests.ps1      # includes tests/sql/test_idempotency.sql
```

It re-runs the latest load date and asserts unchanged fact counts, no duplicate
`EventID`, no duplicate segment-hour, no spurious SCD2 versions and no
double-counted rejects.

## 6. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `docker` not found / engine not running | Start Docker Desktop. The runner preflights this and aborts with a clear message. |
| `trafficdw-spark`/`trafficdw-mssql` not found | The stack is down: `docker compose -f orchestration/docker-compose.yml up -d mssql spark` |
| SQL Server never becomes healthy | It needs ~30-60s on first start and at least 2GB RAM allocated to Docker. `docker logs trafficdw-mssql` |
| Job 05: `ClassNotFoundException: SQLServerDriver` | The driver is missing from `orchestration/jars/`. Re-run `scripts/run_end_to_end.ps1` (it downloads it), or fetch `mssql-jdbc-12.6.1.jre11.jar` from Maven Central into that folder manually. |
| Spark write fails on Windows (`HADOOP_HOME`/winutils) | Only affects a HOST-installed Spark. The containerised path is unaffected — this is one reason it is the primary path. |
| Job 05: SSL/trust error connecting | dev certs: JDBC URL already sets `trustServerCertificate=true`; check server/port |
| `CREATE INDEX ... QUOTED_IDENTIFIER` error | run sqlcmd with `-I` (the script always does) |
| Fact insert FK violation on DateKey | load date outside DimDate seed or partition range → re-seed dates / add partition |
| `usp_AssertQuality` THROWs 50040 | the gate is doing its job: `EXEC etl.usp_GetQualityReport;` lists which checks failed and how many rows |
| Airflow task `sql_quality_checks` red | same as above — the DAG gate asserts the batch's quality log |
