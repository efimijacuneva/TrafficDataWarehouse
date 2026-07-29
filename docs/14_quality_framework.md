# Part 18 — Production Data Quality Framework

> Implementation: [sql/etl/05_quality_checks.sql](../sql/etl/05_quality_checks.sql).
> This is the runtime half of the quality story: [doc 10](10_testing_quality.md)
> defines the *testing strategy* (unit/integration invariants); this framework
> executes those invariants **automatically after every load** and turns them
> into a pass/fail gate a scheduler can act on.

## 1. Design

**Catalog-driven.** Every validation is a *row* in `etl.QualityCheckCatalog`
(name, category, target table, severity, rationale, scalar `CountQuery`
returning the number of failed rows). The runner `etl.usp_RunQualityChecks`
executes all enabled checks with `sp_executesql`, times each one, and writes
results to `etl.QualityCheckLog`. Adding a validation is an `INSERT`, not a
new procedure — one definition of "valid", the same philosophy as
`spark/utils/quality.py` on the lake side.

```
 etl.QualityCheckCatalog          etl.usp_RunQualityChecks           etl.QualityCheckLog
 (rules as data:                  (executes, times, logs;    ──►    (one row per check per run:
  34 checks, 6 categories)  ──►    never mutates data)               Pass/Fail, rows, ms)
                                                                          │
                          etl.usp_GetQualityReport  ◄── report ──────────┤
                          etl.usp_AssertQuality     ◄── gate (THROW) ────┘
```

**Severity model.** `Error` checks THROW through `usp_AssertQuality` and fail
the orchestrator task; `Warning` checks (plausibility signals like avg speed >
1.5× limit) are logged and reported but never block a load — a real pipeline
must distinguish "wrong" from "worth a look".

**Scoping.** High-volume checks against the columnstore fact are scoped to the
loaded date (`@DateKey`) — *validate what you just loaded*; small-table and
dimension checks run globally. Checks may use `@ETLBatchID`, `@LoadDate`,
`@DateKey`; batch-scoped checks return 0 on ad-hoc (batch-less) runs.

**Failure of a check ≠ crash of the run.** A check that itself errors is
caught, logged with `FailedRows = -1` and recorded in `etl.ErrorLog`; the
remaining checks still run (a broken monitor must never hide other findings).

## 2. Execution points

| Point | Call | Behavior |
|---|---|---|
| Nightly pipeline (`etl.usp_RunNightlyPipeline`) | `usp_RunQualityChecks @FailOnError = 0` | runs automatically after every load; log-only, so `BatchLog.Status` keeps meaning "the *load* succeeded" |
| Airflow DAG task `sql_quality_checks` | `usp_AssertQuality @b` | THROWs on any failed Error check → `sqlcmd -b` exits non-zero → the task (and run) fails. Warnings pass. |
| One-command script ([scripts/run_end_to_end.ps1](../scripts/run_end_to_end.ps1)) | assert after each date + report at the end | same gate semantics as the DAG |
| Ad hoc | `EXEC etl.usp_RunQualityChecks @LoadDate = '2026-06-15';` | full sweep any time, e.g. after a manual fix |

## 3. The validation report

`EXEC etl.usp_GetQualityReport [@ETLBatchID]` (default: latest batch) returns:

**Result set 1 — run summary:** batch, checks run, passed, failed errors,
failed warnings, total duration, `GateResult` (PASS/FAIL).

**Result set 2 — detail, failures first:**

| ValidationName | Category | Severity | PassFail | AffectedTable | FailedRowCount | ExecutionTimeMs | CheckedAt |
|---|---|---|---|---|---|---|---|
| SNAPSHOT_DENSITY_FHT | Completeness | Warning | Fail | fact.FactHourlyTraffic | 24 | 12 | … |
| ORPHAN_FTE_SEGMENT | Integrity | Error | Pass | fact.FactTrafficEvent | 0 | 148 | … |
| … | | | | | | | |

Only the **latest result per check** counts — fixing data and re-running the
checks clears the gate without deleting history. The full history stays in
`etl.QualityCheckLog` and feeds the Power BI quality page
(`mart.vPbiQualityChecks`, [doc 12 §3.4](12_powerbi_dashboards.md)).

## 4. Rule catalogue — every check and why it exists

The rationale also lives *in the catalog itself* (`Rationale` column), so
`SELECT * FROM etl.QualityCheckCatalog` is always current documentation.

### Integrity (12 checks) — every fact row must resolve

| Check | Target | Sev. | Why it exists |
|---|---|---|---|
| `ORPHAN_FTE_DATE / _TIME / _SEGMENT / _SENSOR / _VEHICLETYPE / _WEATHER` | FactTrafficEvent | Error | FKs are enforced, but bulk paths can disable them and partition `SWITCH` can bypass them. TimeKey is *computed* (hour×60+min) — arithmetic bugs make invalid keys; Sensor/Segment come from **point-in-time SCD2 lookups** where a wrong validity join yields keys of nonexistent versions; Weather keys come through the banding function, so a Spark↔SQL banding drift surfaces here first. Independent proof, per relationship, scoped to the loaded date. |
| `ORPHAN_FHT_DATE / _SEGMENT / _WEATHER` | FactHourlyTraffic | Error | The snapshot is replaced per load date; a bad key written for another date sits outside every reload window and would persist silently. |
| `ORPHAN_FIL_DIMS`, `ORPHAN_FIL_MILESTONE_DATES` | FactIncidentLifecycle | Error | The accumulating fact is UPDATEd in place — one bad milestone update corrupts the whole incident row. All five role-playing date keys must be real days or the reserved −1 ("not yet"). |
| `FK_CONSTRAINTS_TRUSTED` | sys.foreign_keys | Error | Re-enabling constraints `WITH NOCHECK` leaves them **untrusted**: the optimizer ignores them and violations can hide inside. Referential integrity must be provably intact, not just declared. |

### Uniqueness (2 checks) — no double counting

| Check | Target | Sev. | Why it exists |
|---|---|---|---|
| `DUP_BK_TYPE1_DIMS` | 5 Type-1 dims | Error | A duplicated business key doubles every fact joined through it. Unique indexes enforce; the check proves they weren't dropped in a tuning experiment. |
| `DUP_FACT_TRAFFICEVENT` | FactTrafficEvent | Error | **The most important check in the catalog:** the clustered-columnstore fact has *no unique constraint by design* (doc 09), so this is the only duplicate guard. A duplicate EventID means batch idempotency (delete-then-insert) regressed. |

### SCD (6 checks) — history must be unambiguous

| Check | Target | Sev. | Why it exists |
|---|---|---|---|
| `DUP_CURRENT_DIMROADSEGMENT / _DIMSENSOR` | SCD2 dims | Error | SCD2 invariant: exactly one `IsCurrent = 1` per BK — two current rows double-count all "as-is" reporting. The filtered unique index enforces it; the check proves it. |
| `SCD2_OVERLAP_DIMROADSEGMENT / _DIMSENSOR` | SCD2 dims | Error | Overlapping validity intervals make point-in-time fact lookups ambiguous: one event date matches two versions → the fact load joins twice → duplicated facts. |
| `SCD2_NOCURRENT_DIMROADSEGMENT / _DIMSENSOR` | SCD2 dims | Error | A BK with only expired versions fell out of as-is reporting — the classic "MERGE expired the old row, then failed before inserting the new one" partial failure. |

### Validity (7 checks) — measures must be physically possible

| Check | Target | Sev. | Why it exists |
|---|---|---|---|
| `NEGATIVE_COUNTS_FHT` | FactHourlyTraffic | Error | Counts are physical quantities; negative counts (or heavy > total) mean the aggregation or reload arithmetic broke. |
| `SPEED_RANGE_FTE` | FactTrafficEvent | Error | Speeds outside (0, 250] should have been quarantined at the *silver gate* AND at *staging validation*; one reaching the fact means **both** gates were bypassed — defense in depth, verified. |
| `SPEED_VS_LIMIT_FHT` | FactHourlyTraffic | **Warning** | Hourly avg > 1.5× the limit is implausible but not impossible (sensor calibration, unit error). Investigate, don't block — the doc 10 invariant with production-grade severity. |
| `CONGESTION_RANGE_FHT` | FactHourlyTraffic | Error | CongestionIndex is defined on [0,1]; out-of-range values poison every averaged KPI downstream. |
| `FUTURE_DATE_FTE` | FactTrafficEvent | Error | Future-dated facts inflate "today" dashboards with data that hasn't happened; catches rows that escaped both timestamp rules upstream. |
| `MILESTONE_ORDER_FIL` | FactIncidentLifecycle | Error | Accumulating-snapshot invariant: Detected ≤ Dispatched ≤ Arrived ≤ Cleared ≤ Closed wherever populated; violations produce nonsense SLA percentiles. |
| `NEGATIVE_LAGS_FIL` | FactIncidentLifecycle | Error | Stored lag measures must agree with the milestone keys they were derived from. |

### Completeness (4 checks) — presence, not just correctness

| Check | Target | Sev. | Why it exists |
|---|---|---|---|
| `UNKNOWN_MEMBER_MISSING` | all 10 dims | Error | The −1 member is load-bearing: fact loads fall back to it via `ISNULL(key, −1)`. If it's missing, the *next* batch dies mid-load with FK errors — this check fails first, with a better message. |
| `MANDATORY_DIMS_POPULATED` | 6 core dims | Error | Against an empty dimension every fact row maps to Unknown and the warehouse "works" while being analytically useless — the silent worst case. |
| `SNAPSHOT_DENSITY_FHT` | FactHourlyTraffic | **Warning** | Periodic-snapshot contract: current segments × 24 rows per loaded day, zero-traffic hours included. Warning (not Error) because it presumes the pipeline ran for `@LoadDate` and because later changes to the segment set (new/retired business keys) legitimately shift the current-segment count. |
| `UNKNOWN_RATE_FTE` | FactTrafficEvent | **Warning** | Unknown-keyed rows are legal by design but analytically blind; a spike signals upstream master-data gaps. Monitoring signal, not a gate. |

### Reconciliation (3 checks) — no row unaccounted for

| Check | Target | Sev. | Why it exists |
|---|---|---|---|
| `RECON_ROWLOG_BATCH` | etl.RowLog | Error | The framework contract (doc 06): extracted = inserted + updated + rejected per step. A mismatch is silent data loss or double-counting *inside* the batch. |
| `RECON_STG_FACT_EVENTS` | stg → fact | Error | Independent cross-check of the RowLog *from the actual tables*: staged rows for the date = fact rows + quarantined rows for the batch. Auto-skips (returns 0) once staging has been truncated by a later run. |
| `RECON_STG_FACT_HOURLY` | stg → fact | Error | The density spine may only *add* zero rows, never drop staged aggregates; a staged segment-hour missing from the fact means the reload quietly skipped data. |

## 5. Deliberate non-checks

- **FactHourlyTraffic natural-key duplicates** — physically impossible: the
  clustered PK on (DateKey, HourOfDay, RoadSegmentKey) enforces it; a catalog
  check would only re-test SQL Server itself. Same for `UQ_FIL_IncidentNumber`.
- **Row-level value re-validation of quarantined data** — rejects are already
  isolated in `stg.RejectTrafficEvent` with reasons; re-checking them would
  make the gate fail on data that was correctly kept *out* of the warehouse.

## 6. Extending the framework

```sql
INSERT INTO etl.QualityCheckCatalog
      (CheckName, Category, TargetTable, Severity, Rationale, CountQuery, SortOrder)
VALUES ('MY_NEW_CHECK', 'Validity', 'fact.FactHourlyTraffic', 'Warning',
        N'Why this matters…',
        N'SELECT COUNT(*) FROM fact.FactHourlyTraffic WHERE <failure condition>', 90);
```

Rules: the query must return a single scalar (failed-row count), may reference
`@ETLBatchID` / `@LoadDate` / `@DateKey`, must be read-only. Disable a noisy
check with `UPDATE … SET IsEnabled = 0` (redeploying
`05_quality_checks.sql` resets the catalog — it is code, not data).

## 7. Relationship to the rest of the quality story

| Layer | Where | What it catches |
|---|---|---|
| Silver gate (Spark) | `spark/utils/quality.py`, job 02 | bad *incoming* rows → quarantine, before the warehouse |
| Staging validation | `etl.usp_LoadFactTrafficEvent` | bad rows at the warehouse boundary → `stg.RejectTrafficEvent` |
| **This framework** | post-load, per batch | violations of *warehouse invariants* — the state the loads were supposed to produce |
| Test strategy | [doc 10](10_testing_quality.md) | unit/integration testing of the transform logic itself |
| Visibility | `mart.vPbiQualityChecks` → [doc 12 §3.4](12_powerbi_dashboards.md) | pass/fail history on the Sensor Performance dashboard |
