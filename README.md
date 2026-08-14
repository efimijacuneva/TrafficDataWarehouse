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
│   ├── 14_quality_framework.md    # Post-load data-quality framework (40 checks)
│   └── 15_defense_guide.md        # Likely exam questions with concise answers
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
├── tests/                         # pytest (Spark + source data) + T-SQL warehouse tests
├── dashboard/                     # Streamlit pipeline / analytics / quality / SCD demo
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
- **Docker** — the primary runtime: SQL Server 2022 + Spark 3.5.1 (`orchestration/docker-compose.yml`)
- **Airflow** — optional orchestration demo reusing the same containers (docs 11)
- **Streamlit + Plotly** — the local visual dashboard (`dashboard/`)
- **pytest** — Spark rule tests and lake reconciliation (`tests/`)

## Quick Start

**Prerequisites: Docker Desktop (running) and Python 3.10+.** Nothing else —
Spark, Java and SQL Server all run in containers, and the MS SQL JDBC driver is
downloaded automatically on the first run.

```powershell
# ONE command: starts Docker, deploys both databases, generates 7 seeded days,
# runs Spark bronze->silver->gold, loads the warehouse per date, asserts the
# data-quality gate, and prints a production-style execution summary.
.\scripts\run_end_to_end.ps1
```

Then verify, test and demonstrate:

```powershell
.\scripts\verify_project.ps1     # health check of every layer, PASS/FAIL per area
pytest tests -v -rs              # Spark + source-data tests (skips state what is missing)
.\scripts\run_sql_tests.ps1      # warehouse invariants, idempotency, SCD2 scenario
.\scripts\run_scd2_demo.ps1      # SCD Type 2 versioning, live
streamlit run dashboard/app.py   # the visual pipeline dashboard
```

Manual step-by-step execution (any platform), the full command list and
troubleshooting: **[docs/13_execution_runbook.md](docs/13_execution_runbook.md)**.

### What a run proves

| Guarantee | How it is demonstrated |
|---|---|
| **No row is ever silently lost** | Bronze is partitioned by `ingest_date` (the file's own date), never by a value inside the row, so a corrupt or future-dated timestamp cannot hide a row from the validation gate. Job 02 writes `silver/reconciliation` asserting `rows_in = rows_good + rows_quarantined`, and `tests/test_pipeline_reconciliation.py` checks it against the raw manifest. |
| **Reruns are safe** | Every Spark job overwrites only its own date partition; both dated facts delete-and-reload by `DateKey`; the accumulating fact and all dimensions MERGE on a key. `tests/sql/test_idempotency.sql` asserts it. |
| **History is preserved** | `scripts/run_scd2_demo.ps1` changes a speed limit in the source and shows the old version expiring, the new one becoming current, and *older facts still resolving to the old version*. |
| **Bad data is caught, not deleted** | ~1.5% deliberate defects are injected; each lands in quarantine with a reason. 40 catalogued quality checks then gate the load. |

## Documentation Map

Start with [docs/01_business_analysis.md](docs/01_business_analysis.md) and read in order —
each document builds on the previous one, mirroring how the system was designed:
business requirements → source system → bus matrix → dimensional model → SCD → ETL → Spark → performance.

Then the delivery layer: [docs/12](docs/12_powerbi_dashboards.md) (Power BI dashboards),
[docs/13](docs/13_execution_runbook.md) (end-to-end execution + run report),
[docs/14](docs/14_quality_framework.md) (automated post-load data-quality gate).
