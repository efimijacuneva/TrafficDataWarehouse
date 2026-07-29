# Part 12 — Performance & Optimization

> DDL: [sql/warehouse/05_indexes_partitioning.sql](../sql/warehouse/05_indexes_partitioning.sql)

## SQL Server side

### Indexing strategy (rule: index for the workload, not per column)

| Object | Index | Why |
|---|---|---|
| `fact.FactTrafficEvent` | **Clustered columnstore (CCI)**, partition-aligned | Pure scan/aggregate workload over billions of rows: ~10× compression, batch-mode execution, segment elimination on DateKey. No PK-style clustered B-tree — point lookups don't happen here |
| `fact.FactHourlyTraffic` | Clustered B-tree `(DateKey, HourOfDay, RoadSegmentKey)` + **nonclustered columnstore** | Reloaded per date + refreshed intraday (CCI dislikes deletes/updates) → rowstore clustered for the write path, NCCI for analytical scans — a classic HTAP split |
| `fact.FactIncidentLifecycle` | Clustered B-tree on `IncidentKey`, NC indexes on milestone date keys | Small, update-in-place table; B-tree is correct, columnstore would fragment |
| Dimensions | Clustered PK on surrogate key + **unique NC on (BK, IsCurrent) filtered `WHERE IsCurrent=1`** + NC on BK+validity dates | SK joins from facts; fast current-row SCD lookups; point-in-time lookups |
| Staging | Heaps, no indexes | Bulk-load speed; indexes would only slow truncate-and-load |

**Clustered vs nonclustered recap:** clustered = the physical order of the table (one per
table); nonclustered = separate B-tree pointing back at it. Columnstore = neither — column
segments with metadata, optimized for scans of millions of rows, not seeks of one.

### Partitioning
`FactTrafficEvent` is partitioned by **month on DateKey** (`RANGE RIGHT` on yyyymm01 boundaries):
- **Partition elimination** — a month-filtered query touches 1/60 of the data.
- **Sliding window maintenance** — old months `SWITCH`ed out to archive in metadata-only time;
  no billion-row DELETE ever runs.
- Partition-aligned CCI enables per-partition rebuilds.

### Statistics
Auto-create/auto-update on; after each nightly load the ETL runs `UPDATE STATISTICS` on
touched facts (incremental stats per partition) — ascending-date facts are the textbook
case of stale-stats misestimates on "yesterday" predicates.

### Other
- `mart` views pre-join facts to dims → consistent semantics + plan reuse.
- Foreign keys on facts kept **trusted but NOT enforced at load** in bulk paths?  No —
  we keep them enforced; volume that would make them prohibitive lives in Spark. Decision
  documented: integrity > marginal load speed at our DW volumes.

## Spark side

| Technique | Where used | Effect |
|---|---|---|
| **Broadcast join** | Segment/station lookups in jobs 03/05 (`broadcast(dim_df)`) | Eliminates shuffle of the 80M-row side entirely; small table shipped to every executor |
| **Shuffle tuning** | `spark.sql.shuffle.partitions` sized ≈ input-partitions; **AQE on** (`adaptive.enabled`, `coalescePartitions`, `skewJoin`) | No 200-empty-task default; skewed segment hot-spots split automatically |
| **Caching** | `df.cache()` on the cleaned detections DF in job 03 — reused by the hourly aggregation and the event-grain gold write | Avoids re-reading/re-validating silver per consumer; unpersisted after use |
| **Partition-aware writes** | `repartition("event_date")` + `partitionOverwriteMode=dynamic` | Few large files, idempotent day-level reruns |
| **Explicit schemas** | all readers | Skips inference pass; stable types |
| **Parquet pushdown/pruning** | all readers | See doc 08 |
| **Kryo serialization** | `config/spark_config.py` | Cheaper serialization in shuffles/caches |

## Measured expectations (design targets)
- Hourly snapshot query (30 days, one road): CCI segment elimination + month partitions →
  < 1 s on laptop-scale data.
- Spark end-to-end for one day of 5M generated events: < 5 min locally (`local[*]`).
- Nightly T-SQL load: dominated by FactTrafficEvent insert; CCI bulk insert path
  (batches ≥ 102,400 rows) keeps it minimally logged.
