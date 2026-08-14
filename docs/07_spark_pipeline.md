# Part 8 — Apache Spark Pipeline

> Code: [spark/](../spark/). Jobs run in order 01→05; each is independently re-runnable per date.

## Why Spark at all (and not SQL Server alone)?

| Concern | SQL Server alone | With Spark |
|---|---|---|
| 40–80M raw CSV/JSON rows/day | BULK INSERT into rowstore, then set-based cleansing — single-node CPU/IO bound; parsing JSON at this volume in T-SQL is painful | Parallel parse/clean across cores/nodes; linear scale-out by adding executors |
| Semi-structured input (nested camera JSON) | `OPENJSON` per row, brittle schemas | Native schema-on-read, nested field access, permissive/quarantine modes |
| Reprocessing history (schema fix, new KPI) | Re-run ETL against archived files through the DB engine | Recompute silver/gold from immutable bronze Parquet without touching the DB |
| Data-science feature access | DB contention, extract requests | Direct columnar access to gold Parquet |
| Cost of storing raw detail | Premium DB storage + licensing | Cheap object/file storage |

**Division of labour (the architectural decision):** Spark owns *volume* (raw files →
clean, aggregated, columnar); SQL Server owns *conformance and serving* (SCDs, star schema,
BI concurrency, security). The warehouse never ingests a raw file; Spark never manages
slowly changing history. Each engine does only what it is best at.

## Medallion Architecture

```
 data/raw/                 data/bronze/                data/silver/                 data/gold/
 sensor_*.csv   ──01──►    parquet, as-landed  ──02──► validated, deduped,  ──03──► hourly aggregates,
 camera_*.json             + audit columns             typed, quarantined           KPIs, top-N marts
 weather_*.json            (immutable archive)         partitioned by date          ──04──► kpi datasets
                                                                                    ──05──► JDBC → SQL stg
```

| Zone | Contract | Written by |
|---|---|---|
| **bronze** | Exactly what arrived + `_ingest_ts`, `_source_file`, `_batch_id`. Never mutated → replayable system of record | `01_ingest_raw.py` |
| **silver** | Typed, validated, deduplicated, conformed column names; bad rows in `silver/_quarantine` with reasons | `02_clean_validate.py` |
| **gold** | Business-grain aggregates (segment×hour), KPI tables — the shape the DW and data scientists consume | `03_transform_aggregate.py`, `04_generate_kpis.py` |

## Job Responsibilities

1. **`01_ingest_raw.py`** — reads raw CSV (explicit schema, `mode=PERMISSIVE` +
   `_corrupt_record` capture) and multiline JSON; stamps audit columns; writes bronze
   Parquet partitioned by `event_date`.
2. **`02_clean_validate.py`** — the data-quality gate: null/range/domain rules from
   `utils/quality.py`, deduplication with `row_number()` over the business event key,
   timestamp normalization, quarantine writing. Emits a per-rule quality metrics dataset.
3. **`03_transform_aggregate.py`** — enriches detections with the hour's city-wide weather
   aggregate (worst condition reported across stations, averaged temperature — joined via
   broadcast), derives congestion measures, aggregates to segment×hour grain.
   Demonstrates **both APIs deliberately**: DataFrame API for the pipeline plumbing,
   **Spark SQL** for the business aggregations (readable by analysts).
4. **`04_generate_kpis.py`** — computes the KPI datasets (daily volume/speed per segment
   with `MAX_BY` peak hour, `RANK` city-wide and `LAG` day-over-day delta; weather speed
   penalty vs a DRY baseline; P85 rush hours; a pivoted daily quality dataset) using window
   functions over gold. The congestion index is **not** computed here — the speed limit is a
   dimension attribute that never reaches gold, so it is derived in the warehouse load.
5. **`05_load_warehouse.py`** — writes gold datasets to SQL Server staging via JDBC
   (`truncate=true` + `batchsize=10000`, idempotent per load date), where the T-SQL procs
   take over. Pure handover: no joins, no aggregation, no broadcast.

## Spark SQL and DataFrame API — both, on purpose

- **DataFrame API** → typed, composable, unit-testable transformations
  (`.withColumn`, `.dropDuplicates`, window specs) — engineering code.
- **Spark SQL** → the aggregation/KPI logic that mirrors warehouse queries — analyst-readable
  and portable to the DW. Registered temp views make the same DataFrames queryable in SQL.

Both compile to the same Catalyst logical plan — the choice is about *audience*, not performance.

## Key techniques used in the code

- Explicit `StructType` schema for the high-volume CSV feed (no inference pass, stable
  types, `_corrupt_record` capture); the small JSON feeds are read schema-on-read.
- `broadcast()` hint for the hourly weather aggregate in job 03 — <=24 rows against
  hundreds of thousands of detections, so the small side ships to every executor and the
  large side is never shuffled. It is the only broadcast join in the pipeline.
- Repartition-by-partition-column before write → one tidy file per date partition instead
  of hundreds of small files.
- `spark.sql.shuffle.partitions` tuned per job size; AQE enabled (doc 09).
- Idempotency: every job overwrites its **date partition** (`partitionOverwriteMode=dynamic`),
  so re-running a day is safe.
