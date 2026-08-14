# Part 13 (Testing section) — Testing & Data Quality

## Data Quality Framework

Quality rules are defined once ([spark/utils/quality.py](../spark/utils/quality.py)) and
enforced at the silver gate; SQL-side checks re-verify at the warehouse boundary.

| Dimension of quality | Rule examples | Enforcement |
|---|---|---|
| Completeness | sensor_id, event_ts, segment_id NOT NULL | Spark silver gate → quarantine |
| Validity | 0 < speed ≤ 250; occupancy ∈ [0,100]; ts within batch window, not future | Spark + staging CHECKs |
| Uniqueness | `event_id` unique per load date | `row_number()` dedup ordered by `event_ts DESC, _source_file, detector_code` (deterministic) |
| Consistency | segment's road exists; direction ∈ {NB,SB,EB,WB} | FK candidate checks → inferred/unknown member |
| Timeliness | file arrival vs. expected schedule | batch log + orchestration SLA |
| Reconciliation | extracted = loaded + rejected, per batch | `etl.usp_ValidateBatch` |

Quarantined rows carry `reject_reason` and are re-playable; the reject **rate per rule per
day** is itself a gold dataset (sensor-health KPI feeds off it).

## Test Catalogue

> **Status per section:** §1 and §2 are now **shipped as code** in [tests/](../tests/) —
> pytest for the Spark rules and the lake reconciliation, T-SQL scripts for the warehouse.
> §3 (warehouse invariants) is **implemented and automated**: it runs after every load and
> gates the orchestrator. §4 (regression/performance) remains **design only** — durations
> are logged to `etl.BatchLog`, the alerting hook is a roadmap item (doc 11).
>
> Tests that cannot run on a given machine are **skipped with a reason**, never silently
> passed: `pytest -rs` prints `NOT VERIFIED - no JVM on PATH` rather than a green tick.

### 1. Unit tests (transform logic) — **implemented**
- Spark: [tests/test_spark_rules.py](../tests/test_spark_rules.py) — pytest + a local
  SparkSession fixture, hand-built DataFrames per rule (speed=999 → `SPEED_RANGE`,
  future timestamp → `FUTURE_TIMESTAMP`, rule precedence, deterministic dedup, the
  sensor/camera split, and the banding boundaries against `etl.fn_WeatherBands`).
- SQL: [tests/sql/test_scd2_scenario.sql](../tests/sql/test_scd2_scenario.sql) — changes a
  speed limit in the OLTP source, re-runs the pipeline, and asserts the full Type 2
  contract in 14 checks including point-in-time fact resolution.
- Source data: [tests/test_generator.py](../tests/test_generator.py) — proves the generated
  baseline is valid by construction (milestone monotonicity) *and* that every deliberate
  defect class is present, so the gate is exercised rather than decorative.

### 2. Integration tests (pipeline) — **implemented**
- [tests/test_pipeline_reconciliation.py](../tests/test_pipeline_reconciliation.py) is the
  no-silent-loss proof: `rows_in == rows_good + rows_quarantined` from
  `silver/reconciliation`, bronze recounted against silver + quarantine, future-dated rows
  asserted **present in quarantine**, and `_manifest.json` finally read back and reconciled
  against bronze. This is the test that would have caught the earlier data-loss defect.
- [tests/sql/test_idempotency.sql](../tests/sql/test_idempotency.sql): re-runs a load date
  and asserts unchanged fact counts, no duplicate `EventID`, no duplicate segment-hour, no
  spurious SCD2 versions and no double-counted rejects.

### 3. Warehouse invariants (implemented in [sql/etl/05_quality_checks.sql](../sql/etl/05_quality_checks.sql) — executed automatically after every load and gated by the orchestrator; full rule catalogue in [doc 14](14_quality_framework.md))
- **No orphan keys:** every fact FK resolves (LEFT JOIN dim WHERE dim.SK IS NULL → 0 rows).
- **SCD2 sanity:** per BK exactly one `IsCurrent=1` (and never zero); validity intervals
  non-overlapping — so point-in-time fact lookups are always unambiguous.
- **Accumulating monotonicity:** Detected ≤ Dispatched ≤ Arrived ≤ Cleared ≤ Closed
  wherever populated; lag measures non-negative.
- **Snapshot density:** rows per day = active segments × 24.
- **Measure ranges:** CongestionIndex ∈ [0,1]; AvgSpeed ≤ 1.5 × speed limit.

### 4. Regression / performance — design
- Nightly load duration and per-query timings logged to `etl.BatchLog`; alert on >2× baseline
  (durations are logged today; the alerting hook is a roadmap item, doc 11).
