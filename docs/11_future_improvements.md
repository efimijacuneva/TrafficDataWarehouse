# Part 15 (Bonus) — Modern Extensions & Roadmap

Evaluated against one question: *does it genuinely improve this platform, or is it résumé
decoration?* Verdicts below; two are included in the repo, the rest are documented decisions.

## Included in this project

### Apache Airflow — INCLUDED ([orchestration/dags/traffic_dw_dag.py](../orchestration/dags/traffic_dw_dag.py))
The pipeline has real dependency structure (generator → Spark 01→05 → dim loads → fact
loads → quality checks) plus retries, SLAs and backfill semantics ("re-run 2026-06-14").
Cron/SQL Agent can sequence steps but cannot express *data-interval-aware backfills* or
per-task retry policy. Airflow is the industry-standard answer; the DAG maps 1:1 to the
documented ETL flow. **Why not heavier (Dagster/ADF):** Airflow is free, local, and the
concepts transfer.

### Docker — INCLUDED ([orchestration/docker-compose.yml](../orchestration/docker-compose.yml))
Reviewers/teammates must reproduce the environment (SQL Server + Spark + Airflow) on any
machine. `docker compose up` beats a 3-page install guide. Zero architectural risk.

## Recommended next, deliberately not implemented

### Delta Lake — STRONG NEXT STEP
Plain Parquet's weaknesses are exactly Delta's features: no ACID on concurrent writes, no
MERGE (our idempotency relies on partition overwrite), no time travel, no schema
enforcement on write. Adopting Delta on silver/gold would enable streaming upserts and
`MERGE`-based late-data handling with ~10 lines of config change (`format("delta")`).
Not included only to keep the core project on vanilla Parquet as the brief's learning goal.

### Apache Kafka — WHEN LATENCY REQUIREMENTS ARRIVE
Today the business contract is hourly/nightly analytics; files satisfy it. The moment the
TMC wants live dashboards (<1 min), sensors should publish to Kafka topics and Spark
Structured Streaming should populate silver continuously. The medallion design was chosen
so this swap changes only the ingestion edge — bronze becomes a topic sink, jobs 02–05 survive.

### Power BI — YES, AS THE BI LAYER
The `mart` views are designed as its semantic source (star-schema-friendly, measure columns
pre-named). Import mode over gold Parquet or DirectQuery over columnstore both work.
Not committed to the repo because .pbix binaries don't diff; the dashboard spec is in doc 01.

### Apache Iceberg — NOT NOW
Same problem class as Delta; choosing both is redundant. Iceberg wins in multi-engine
(Trino/Flink) shops; our stack is Spark-centric → Delta is the better fit if/when needed.

### Azure Data Factory — NOT NOW
Managed ELT/orchestration duplicates Airflow's role and breaks local reproducibility.
Right answer *if* the platform moves to Azure wholesale (then: ADF or Fabric pipelines +
Synapse/Fabric warehouse); premature here.

## Other roadmap items
- **ML congestion prediction** — gold layer already emits training-ready features
  (segment×hour history, weather, holidays); next step is a simple gradient-boosted
  baseline predicting next-hour volume.
- **Great Expectations / dbt tests** — externalize the quality rules of doc 10.
- **Row-level security** in SQL Server for district-scoped analyst access.
- **Real-time sensor-health alerting** off the reject-rate gold dataset.
