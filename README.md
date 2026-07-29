# Smart City Traffic Analytics Data Warehouse

**Design and Implementation of a Scalable Traffic Analytics Data Warehouse Using SQL Server and Apache Spark**

A production-style Data Engineering project that turns high-volume operational traffic data
(sensors, cameras, traffic lights, weather stations, incident systems) into an analytical
platform for historical analysis, BI reporting and decision making.

---

## Architecture at a Glance

```
 ┌────────────────┐   CSV / JSON / logs   ┌───────────────────────────────┐
 │  SOURCE LAYER  │ ────────────────────► │        SPARK LAKE (Parquet)   │
 │  OLTP DB       │                       │  bronze  →  silver  →  gold   │
 │  Sensor feeds  │                       │  raw       clean      KPIs    │
 │  Camera JSON   │                       └──────────────┬────────────────┘
 │  Weather API   │                                      │ JDBC / bulk load
 └───────┬────────┘                                      ▼
         │  incremental extract (watermarks)   ┌────────────────────────┐
         └──────────────────────────────────►  │  SQL SERVER WAREHOUSE  │
                                               │  stg → dim/fact → mart │
                                               │  Star Schema (Kimball) │
                                               └───────────┬────────────┘
                                                           │
                                            Power BI / SSRS / Analytical SQL
```

Two complementary processing paths, one conformed warehouse:

| Path | Engine | Data | Why |
|------|--------|------|-----|
| **Big-data path** | Apache Spark (PySpark) | Raw sensor CSV, camera JSON (millions of rows/day) | Distributed cleansing, dedup, validation, aggregation; columnar Parquet storage |
| **Relational path** | T-SQL ETL | OLTP reference data (roads, sensors, incidents, fleet) | Set-based MERGE loads, SCD management, referential integrity |

The warehouse itself follows the **Kimball methodology**: bus matrix → conformed dimensions →
star schemas with three fact-table types (transaction, periodic snapshot, accumulating snapshot).

---

## Repository Structure

```
SmartTrafficDataWarehouse/
├── README.md
├── docs/                          # Full project documentation
│   ├── 01_business_analysis.md    # Problem, goals, users, KPIs, dashboards
│   ├── 02_source_system.md        # OLTP design + ER diagram
│   ├── 03_bus_matrix.md           # Kimball bus matrix + grain decisions
│   ├── 04_dimensional_model.md    # Star schema, dims, facts, design rationale
│   ├── 05_scd_strategy.md         # SCD Types 0–6 with implementations
│   ├── 06_etl_design.md           # ETL architecture, incremental loads, errors
│   ├── 07_spark_pipeline.md       # Medallion pipeline, Spark vs SQL rationale
│   ├── 08_columnar_storage.md     # Parquet, compression, pushdown, pruning
│   ├── 09_performance.md          # Indexing, partitioning, Spark tuning
│   ├── 10_testing_quality.md      # Data quality framework and tests
│   ├── 11_future_improvements.md  # Roadmap (Airflow, Kafka, Delta Lake…)
│   ├── 12_powerbi_dashboards.md   # Power BI semantic model + 5 dashboards
│   ├── 13_execution_runbook.md    # End-to-end execution guide + run report
│   └── 14_quality_framework.md    # Post-load data-quality framework (34 checks)
├── sql/
│   ├── oltp/                      # Source system: DDL + sample data
│   ├── warehouse/                 # DW: schemas, dims, facts, indexes, mart + Power BI views
│   ├── etl/                       # Security, staging, logging, SCD/fact loads, quality checks, run summary
│   └── analytics/                 # Window functions, CUBE/ROLLUP, CTEs, Top-N
├── spark/
│   ├── config/                    # Spark session factory, JDBC settings
│   ├── jobs/                      # 01_ingest → 05_load_warehouse pipeline
│   └── utils/                     # Shared validation / quality helpers
├── scripts/                       # run_end_to_end.ps1 — one-command full pipeline
├── data_generator/                # Realistic synthetic source data (CSV/JSON)
├── data/                          # Lake zones: raw / bronze / silver / gold
├── orchestration/                 # Airflow DAG + docker-compose (bonus)
├── reports/                       # Business report catalogue + views
└── presentation/                  # Slide deck (Markdown + Mermaid diagrams)
```

---

## Technology Stack

- **SQL Server 2019+** — warehouse engine (clustered columnstore, partitioning, MERGE, window functions, GROUPING SETS/ROLLUP/CUBE, recursive CTEs)
- **Apache Spark 3.5 (PySpark)** — distributed ingest, cleansing, aggregation (DataFrame API **and** Spark SQL)
- **Parquet** — columnar lake storage with snappy compression, partitioned by event date
- **Python 3.10+** — data generator and Spark jobs
- **Power BI** — reporting layer over dedicated semantic views (`mart.vPbi*`, docs 12)
- **Docker / Airflow** — optional orchestration layer (see `orchestration/`, docs 11)

## Quick Start

**One command (Windows):** `.\scripts\run_end_to_end.ps1` — generates data, deploys
both databases, runs Spark bronze→silver→gold, loads the warehouse per date,
asserts the data-quality gate and prints a production-style execution summary.
Manual/Linux steps and troubleshooting: [docs/13_execution_runbook.md](docs/13_execution_runbook.md).

```bash
# 1. Generate synthetic source data (CSV sensor feeds, JSON camera/incident events)
python data_generator/generate_data.py --days 7 --sensors 200 --seed 42

# 2. Create source + warehouse databases (run in order, sqlcmd or SSMS)
sqlcmd -S localhost -b -I -i sql/oltp/01_create_database.sql
sqlcmd -S localhost -b -I -i sql/oltp/02_tables.sql
sqlcmd -S localhost -b -I -i sql/oltp/03_sample_data.sql
sqlcmd -S localhost -b -I -i sql/warehouse/01_create_warehouse.sql
sqlcmd -S localhost -b -I -i sql/warehouse/02_dimensions.sql
sqlcmd -S localhost -b -I -i sql/warehouse/03_facts.sql
sqlcmd -S localhost -b -I -i sql/warehouse/04_seed_date_time.sql
sqlcmd -S localhost -b -I -i sql/warehouse/05_indexes_partitioning.sql
sqlcmd -S localhost -b -I -i sql/etl/00_security.sql              # etl_spark login for Spark JDBC (job 05)
sqlcmd -S localhost -b -I -i sql/etl/01_staging.sql
sqlcmd -S localhost -b -I -i sql/etl/02_etl_framework.sql
sqlcmd -S localhost -b -I -i sql/etl/03_load_dimensions.sql
sqlcmd -S localhost -b -I -i sql/etl/04_load_facts.sql
sqlcmd -S localhost -b -I -i sql/etl/05_quality_checks.sql       # data-quality framework (docs 14)
sqlcmd -S localhost -b -I -i sql/etl/06_execution_summary.sql    # run-report procedure (docs 13)
sqlcmd -S localhost -b -I -i sql/warehouse/06_mart_views.sql
sqlcmd -S localhost -b -I -i sql/warehouse/07_powerbi_views.sql  # Power BI semantic layer (docs 12)

# 3. Run the Spark medallion pipeline (per load date)
spark-submit spark/jobs/01_ingest_raw.py          --date 2026-06-01
spark-submit spark/jobs/02_clean_validate.py      --date 2026-06-01
spark-submit spark/jobs/03_transform_aggregate.py --date 2026-06-01
spark-submit spark/jobs/04_generate_kpis.py
spark-submit --packages com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11 \
             spark/jobs/05_load_warehouse.py      --date 2026-06-01

# 4. Load the warehouse, assert quality, print the run report
sqlcmd -S localhost -d TrafficDW -b -I -Q "EXEC etl.usp_RunNightlyPipeline @LoadDate = '2026-06-01';"
sqlcmd -S localhost -d TrafficDW -b -I -Q "DECLARE @b INT=(SELECT MAX(ETLBatchID) FROM etl.BatchLog); EXEC etl.usp_AssertQuality @b;"
sqlcmd -S localhost -d TrafficDW -b -I -Q "EXEC etl.usp_ExecutionSummary;"

# 5. Explore analytics / build the dashboards
sqlcmd -S localhost -i sql/analytics/01_window_functions.sql
# Power BI: import mart.vPbi* views and follow docs/12_powerbi_dashboards.md
```

## Documentation Map

Start with [docs/01_business_analysis.md](docs/01_business_analysis.md) and read in order —
each document builds on the previous one, mirroring how the system was designed:
business requirements → source system → bus matrix → dimensional model → SCD → ETL → Spark → performance.

Then the delivery layer: [docs/12](docs/12_powerbi_dashboards.md) (Power BI dashboards),
[docs/13](docs/13_execution_runbook.md) (end-to-end execution + run report),
[docs/14](docs/14_quality_framework.md) (automated post-load data-quality gate).
