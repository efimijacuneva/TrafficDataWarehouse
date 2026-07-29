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

> **Status per section:** §3 (warehouse invariants) is fully **implemented and automated** —
> it runs after every load and gates the orchestrator. §2's flow is **executed end-to-end**
> by [scripts/run_end_to_end.ps1](../scripts/run_end_to_end.ps1) (seeded generator, manifest,
> quality gate per date); the idempotency check is run manually per
> [docs/13 §5](13_execution_runbook.md). §1 and §4 are the **test design** — specified here,
> not yet shipped as code in this repository.

### 1. Unit tests (transform logic) — design
- Spark: pytest + local SparkSession fixtures; golden-input DataFrames asserting each
  cleansing rule (e.g. speed=999 → quarantined with reason `SPEED_RANGE`).
- SQL: SCD procs exercised with a scripted scenario table — insert BK v1 → change attribute →
  run proc → assert exactly one expired + one current row, version increments, hash updates.

### 2. Integration tests (pipeline) — automated by the end-to-end script
- Generate a deterministic day (`generate_data.py --seed 42`), run jobs 01–05 + SQL loads,
  assert the data-quality gate per load date; the generator's `_manifest.json` provides the
  row counts for zone-by-zone reconciliation.
- Idempotency test: run the same day twice → identical fact counts (no duplicates).

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
