# Part 9 — Columnar Storage (Parquet)

All silver/gold datasets are stored as **Parquet, snappy-compressed, partitioned by `event_date`**.

## Why columnar

Row storage (CSV, JSON, rowstore tables) lays data out record-by-record; analytical queries
("average speed by hour") need 2–3 columns of 15 but must read every byte of every row.
Columnar formats store each column contiguously, so:

1. **Column projection** — a query touching `speed_kmh, event_hour` reads only those column
   chunks. On our 15-column detection dataset that's ~85% I/O eliminated before any filtering.
2. **Compression** — same-typed, similar-valued data compresses far better:
   dictionary encoding (2,000 distinct sensor IDs across millions of rows), run-length
   encoding (sorted date/hour columns), bit-packing (small-int vehicle classes), then a
   general codec (snappy = fast, default; zstd = smaller, chosen for bronze archive).
   Observed on generated data: raw CSV → Parquet/snappy ≈ 8–12×.
3. **Predicate pushdown** — Parquet stores min/max statistics per row group (~128MB) and
   per page. `WHERE speed_kmh > 120` skips whole row groups whose max ≤ 120 *without
   decoding them*. Spark pushes such filters into the reader automatically.
4. **Partition pruning** — directory-level elimination: with
   `.../silver/traffic_events/event_date=2026-06-15/`, a query filtering on that date lists
   one directory instead of scanning a month. Pushdown works *inside* files; pruning works
   *before opening files at all*.
5. **Schema evolution & self-description** — schema travels in the file footer; columns can
   be added over time (`mergeSchema`).

## Format comparison

| | CSV | JSON | ORC | **Parquet** |
|---|---|---|---|---|
| Layout | row text | row text, nested | columnar | columnar |
| Schema | none (re-inferred, fragile) | implicit per record | in file | in file |
| Types | everything is a string | limited (no date/decimal) | rich | rich + logical types |
| Compression | whole-file only (gzip = unsplittable) | same, worse (repeated keys) | excellent | excellent |
| Pushdown/statistics | none | none | yes (+bloom filters) | yes |
| Splittable for parallelism | only uncompressed | poorly | yes | yes |

**vs CSV:** CSV has no types (dates and NULLs are conventions), no statistics, no
projection — every query is a full parse of every byte. Fine as an *interchange* format at
the raw edge; wrong as a *storage* format.

**vs JSON:** JSON repeats every key on every record (massive size overhead), parses even
slower, and multiline JSON is effectively unsplittable. Kept only at the landing zone
because devices emit it.

**vs ORC:** ORC is a genuine peer (arguably better SQL-engine compression, ACID hooks in
Hive). Parquet wins here on **ecosystem breadth** — first-class in Spark, Pandas/Arrow,
Power BI, DuckDB, Delta Lake/Iceberg (both build on Parquet), and every cloud warehouse.
For a Spark-centric, multi-consumer platform, Parquet is the default choice; ORC would not
be wrong, just less universal.

## Project conventions

- Partition column: `event_date` (daily) — matches every query's dominant filter and the
  reprocessing unit. **Not** sensor_id (2,000 partitions × small files = the classic
  small-file anti-pattern).
- Target file size 128–512MB via `repartition("event_date")` before write.
- snappy for silver/gold (read-heavy, CPU-cheap), zstd for bronze (write-once archive).
- Sort within partitions by `road_segment_id, event_ts` → better RLE + tighter min/max
  stats → sharper pushdown.
