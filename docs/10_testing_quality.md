# Part 13 (Testing section) — Testing & Data Quality

## Data Quality Framework

Quality rules are defined once ([spark/utils/quality.py](../spark/utils/quality.py)) and
enforced at the silver gate; SQL-side checks re-verify at the warehouse boundary.

| Dimension of quality | Rule examples | Enforcement |
|---|---|---|
| Completeness | sensor_id, event_ts, segment_id NOT NULL | Spark silver gate → quarantine |
| Validity | 0 < speed ≤ 250; occupancy ∈ [0,100]; ts within batch window, not future | Spark + staging CHECKs |
| Uniqueness | (sensor_id, event_ts, event_id) unique | `row_number()` dedup, kept-latest |
| Consistency | segment's road exists; direction ∈ {NB,SB,EB,WB} | FK candidate checks → inferred/unknown member |
| Timeliness | file arrival vs. expected schedule | batch log + orchestration SLA |
| Reconciliation | extracted = loaded + rejected, per batch | `etl.usp_ValidateBatch` |

Quarantined rows carry `reject_reason` and are re-playable; the reject **rate per rule per
day** is itself a gold dataset (sensor-health KPI feeds off it).

## Test Catalogue

### 1. Unit tests (transform logic)
- Spark: pytest + local SparkSession fixtures; golden-input DataFrames asserting each
  cleansing rule (e.g. speed=999 → quarantined with reason `SPEED_RANGE`).
- SQL: SCD procs tested with a scripted scenario table — insert BK v1 → change attribute →
  run proc → assert exactly one expired + one current row, version increments, hash updates.

### 2. Integration tests (pipeline)
- Generate a deterministic day (`generate_data.py --seed 42`), run jobs 01–05 + SQL loads,
  assert row counts at every zone match the manifest the generator writes.
- Idempotency test: run the same day twice → identical fact counts (no duplicates).

### 3. Warehouse invariants (run post-load, `sql/etl/05_quality_checks.sql` patterns)
- **No orphan keys:** every fact FK resolves (LEFT JOIN dim WHERE dim.SK IS NULL → 0 rows).
- **SCD2 sanity:** per BK exactly one `IsCurrent=1`; validity intervals non-overlapping,
  gap-free; `ExpirationDate > EffectiveDate`.
- **Accumulating monotonicity:** Detected ≤ Dispatched ≤ Arrived ≤ Cleared ≤ Closed
  wherever populated; lag measures non-negative.
- **Snapshot density:** rows per day = active segments × 24.
- **Measure ranges:** CongestionIndex ∈ [0,1]; AvgSpeed ≤ 1.5 × speed limit.

### 4. Regression / performance
- Nightly load duration and per-query timings logged to `etl.BatchLog`; alert on >2× baseline.
