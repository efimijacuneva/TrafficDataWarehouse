# Part 19 — Defense Guide

Likely examiner questions with concise answers, each pointing at the file that
proves it. Answers describe **what the code actually does**, including where it
falls short — a weakness you name yourself costs far less than one the examiner
finds.

---

## Architecture

**Why both Spark and SQL Server? Isn't one enough?**
They solve different problems. Spark owns *volume*: parsing and cleaning
semi-structured files across cores, with immutable bronze so silver can be
recomputed after a rule change without touching the database. SQL Server owns
*conformance and serving*: surrogate keys, SCD history, referential integrity,
transactions, BI concurrency. Those are stateful transactional operations that
must live in exactly one engine or the two implementations drift. The boundary
is enforced, not just drawn: `etl_spark` has permissions on the `stg` schema and
nothing else, so Spark physically cannot write a dimension.
→ `sql/etl/00_security.sql`, `spark/jobs/05_load_warehouse.py`

**Why Kimball and not Inmon?**
The consumers are business users and BI tools and the scope is one department.
Kimball gives readable wide dimensions, one join per dimension, and an
incremental delivery path through the bus matrix — the hourly-traffic star could
ship before the incident star and still integrate, because both use the same
conformed `DimDate` and `DimRoadSegment`. Inmon's normalized enterprise
warehouse plus downstream marts is right for integrating many source systems
across an enterprise; here it adds a layer and delays value.

**Why Bronze/Silver/Gold instead of loading raw straight into the warehouse?**
Each boundary buys something. Bronze is the replayable record — fix a rule,
recompute silver, never re-request the source files. Silver is the trust
boundary, and the quarantine makes the cost of that trust visible. Gold is where
aggregation happens once, correctly, instead of in every report.

**Why not load facts directly from Spark?**
Surrogate-key assignment and SCD history need a single writer and real
transactions. A JDBC write cannot participate in the delete-then-insert
atomicity the fact loads depend on, and doing point-in-time SCD2 lookups from
parallel executors would need distributed coordination for no benefit.

---

## Dimensional modeling

**What is grain, and what is yours?**
Grain is what one row means — declared *before* choosing dimensions or facts.
Three grains here: one vehicle detection; one road segment per clock hour
(including empty hours); one incident for its whole life.

**Explain the three fact types.**
A **transaction** fact records an event that happened — insert-only, no row if
nothing happened. A **periodic snapshot** records state over a fixed repeating
interval — predictable row count, and a row exists *even when nothing happened*,
which is what makes absence measurable. An **accumulating snapshot** records one
instance of a process with a beginning and end — inserted at the start, then
revisited and updated as milestones occur, with many date keys and stored lags.
One sentence: transaction answers "what happened", periodic answers "what was
the state", accumulating answers "how long does it take and where does it stall".

**Why a snapshot when you already have the transaction fact?**
Query economics (dashboards ask hourly; scanning every event row for that is
wasteful), **density** (a transaction fact cannot record absence — "which
segments were empty at 03:00" is unanswerable from it), and semi-additive
measures being pre-weighted once rather than mis-averaged by report writers.

**Why surrogate keys?**
Source keys get reused and recycled; SCD2 needs several rows per business key,
which is impossible if the key *is* the identifier; 4-byte integer joins beat
varchar joins at fact scale; and reserved members (−1, inferred rows) need keys
no source can collide with.

**What is a degenerate dimension?**
A dimension key on the fact with no dimension table, because it has no
attributes worth storing: `EventID` and `IncidentNumber` here. A `DimEvent`
would have one column and as many rows as the fact — a join for nothing.

**What is a role-playing dimension?**
One physical dimension serving several logical roles. `FactIncidentLifecycle`
has five `DimDate` and five `DimTime` keys, exposed as named views
(`dim.DimDate_Detected` …) so BI tools present five independent date filters.
Views rather than copies, because copies would need five loads and could drift.

**Why is `DimRoadSegment` denormalized? Isn't redundancy bad?**
Redundancy causes update anomalies under *concurrent writes*. A dimension is
loaded by one controlled ETL process, so that risk does not exist. What it buys
is real: the source needs six joins to get from a measurement to a district
name; the warehouse needs one.

**Your camera dimension — why is it modelled that way?**
A detection comes from either a loop/radar sensor or an ANPR camera, so
`FactTrafficEvent` carries **both** `SensorKey` and `CameraKey`; exactly one
resolves and the other takes the unknown member, with `DetectorType` recording
which. *(Earlier the camera code was aliased into `sensor_serial`, which made
every camera a permanently-inferred `DimSensor` row while `DimTrafficCamera` was
referenced by no fact at all. `DETECTOR_ROUTING_FTE` now enforces the rule.)*

---

## SCD

**What happens when a road's speed limit changes?**
The SCD2 loader detects it by hash-diff, expires the current row
(`ExpirationDate = load date − 1`, `IsCurrent = 0`) and inserts a new version
with an incremented `VersionNumber` and a new surrogate key. Facts loaded before
the change keep pointing at the old version, because fact loads join
**point-in-time** (`@LoadDate BETWEEN EffectiveDate AND ExpirationDate`) rather
than on `IsCurrent = 1`. Demonstrate it live with `scripts/run_scd2_demo.ps1`.

**Walk me through the SCD2 implementation.**
Hash the Type-2 attribute set with `HASHBYTES('SHA2_256', CONCAT_WS('|',…))`;
complete any inferred skeletons first, Type-1 style; correct same-day
re-versions in place; then a `MERGE` matched on business key + `IsCurrent = 1`
expires changed rows and inserts brand-new keys, with `OUTPUT $action` captured
into a table variable; a follow-up `INSERT` driven by that variable creates the
new version. **The second statement is necessary because T-SQL's MERGE permits
only one INSERT clause** — you cannot expire and insert in one statement.

**Which SCD types did you actually implement?**
0, 1, 2, 3 and 6 are implemented. **4 and 5 are documented, not built** — they
would apply if sensor status flipped hourly, which it does not in this model.
Type 6 is `CurrentSpeedLimitKmh` on `DimRoadSegment`: synchronised across all
versions while `SpeedLimitKmh` stays frozen per version, so one query can
compare against both the limit at the time and today's limit.

**What is a late-arriving dimension?**
A fact mentioning a business key the dimension has not seen yet.
`usp_CreateInferredMembers` inserts a skeleton with the **real** key
(`IsInferred = 1`) so the fact is attributed immediately; when master data
arrives the SCD2 procedure overwrites that row **Type 1, in place**. The
subtlety: completion must be Type 1, not Type 2 — the skeleton was never a real
historical state, so versioning it would fabricate history.

**Unknown member vs inferred member?**
Unknown (−1) is for "there is **no** business key" — nothing to infer. Inferred
is for "there **is** a key but the dimension does not know it yet". Confusing
them either loses recoverable attribution or invents a dimension row.

---

## ETL

**What is a watermark and why does yours advance last?**
A stored marker of how far the last successful extract reached. The upper bound
is captured **once before reading** — otherwise a row modified during the
extract could be read twice or skipped forever — and the extract *returns* it
rather than persisting it. `usp_AdvanceWatermarks` writes it as the pipeline's
**final** step, so any failure leaves watermarks untouched and the rerun
re-extracts the same delta. *Honest limitation:* a `DATETIME2` watermark can
miss rows whose transaction commits after the bound was captured; `rowversion` +
`MIN_ACTIVE_ROWVERSION()` is the standard hardening and is not implemented.

**What is idempotency, and is your pipeline idempotent?**
Running the same operation twice gives the same result as once. Spark writes
overwrite only the touched date partition; both dated facts delete-and-reload by
`DateKey` inside one transaction; the accumulating fact and all dimensions MERGE
on a key; inferred and unknown members are `NOT EXISTS`-guarded; rejects are
deleted by date so reruns do not double-count. Asserted by
`tests/sql/test_idempotency.sql`.

**What happens if the ETL fails halfway?**
Every procedure is in TRY/CATCH; the catch logs to `etl.ErrorLog`, marks the
batch Failed and re-throws, so `sqlcmd -b` exits non-zero and the orchestrator
task fails. Multi-statement procedures use `XACT_ABORT ON` with explicit
transactions, so a fact load's delete and insert are atomic. And watermarks
never advanced, so the rerun re-reads the same delta.

**How do you know no row is silently lost?**
Bronze is partitioned by `ingest_date` — the date of the *file* — never by a
value inside the row. That matters: a row with a future or NULL timestamp used
to land in a partition the per-date read never opened, so it reached neither
silver nor quarantine and no count could see it. Job 02 now writes
`silver/reconciliation` proving `rows_in = rows_good + rows_quarantined`, and
`tests/test_pipeline_reconciliation.py` checks that against the raw manifest.

---

## SQL & Spark

**Why `ROW_NUMBER` instead of `RANK`?**
`ROW_NUMBER` is always unique within a partition, so `WHERE rn <= 3` returns
exactly three rows; `RANK` shares numbers on ties and could return four. Use
`ROW_NUMBER` for a guaranteed count (per-group Top-N, deduplication), `RANK`
where position out of a total is the business meaning, `DENSE_RANK` where you
want distinct *levels* without gaps.

**Why a recursive CTE?**
The road network is a directed graph and "how far can a jam propagate in N hops"
is transitive closure — ordinary joins would need one join per hop, hard-coded.
The anchor selects segments leaving the blocked intersection; the recursive
member follows `EndIntersectionID → StartIntersectionID`; a cycle guard stops
revisits; `OPTION (MAXRECURSION)` overrides the default limit of 100.

**ROLLUP vs CUBE vs GROUPING SETS?**
`ROLLUP(A,B)` gives a hierarchical drill-up path — `(A,B)`, `(A)`, `()` — so
column order matters. `CUBE(A,B)` gives every combination (2ⁿ sets) for
independent dimensions. `GROUPING SETS` lists precisely the levels you want.
`GROUPING_ID` labels which set a row came from — without it you cannot tell
"NULL because subtotal" from "NULL because the data is NULL".

**Why columnstore?**
The event fact is a pure scan/aggregate workload, so a clustered columnstore
gives ~10× compression, batch-mode execution and segment elimination. The hourly
fact is deleted and reloaded per date, and columnstore dislikes deletes, so it
gets a clustered B-tree for the write path plus a nonclustered columnstore for
scans — an HTAP split.

**Why Parquet?**
Columnar, so a query touching 2 of 15 columns reads only those chunks;
self-describing with real types; per-row-group min/max statistics enabling
predicate pushdown; and directory-level partition pruning on `event_date`.
Pruning happens *before* files are opened, pushdown *inside* the files opened.
zstd for bronze (write-once archive), snappy for silver/gold (read-heavy).

**Where is Spark actually doing meaningful work?**
Explicit `StructType` + `PERMISSIVE` corrupt-record capture on the high-volume
CSV; a `broadcast()` join of the hourly weather aggregate (≤24 rows) against
hundreds of thousands of detections, so the large side is never shuffled;
`row_number()` deduplication over a deterministic window; Spark SQL for the
segment×hour aggregation and the KPI window functions; `cache()` where a
DataFrame feeds two consumers; AQE for post-shuffle coalescing and skew joins.
*Honest note:* jobs run `local[*]` — correct at this data volume, and scaling out
is a `--master` flag away.

---

## The awkward ones — answer these before they are asked

**Did you test any of this?**
Yes, and I can show which parts are *not* covered. `tests/` has three tiers:
source-data invariants (pure Python, runs anywhere), Spark rule tests and lake
reconciliation (need a JVM), and T-SQL warehouse tests (need SQL Server).
Anything that cannot run is **skipped with a reason**, never silently passed.
Regression/performance testing remains design only.

**Did you ever weaken a check to make the pipeline pass?**
It happened and it was reversed. `MILESTONE_ORDER_FIL` was downgraded from Error
to Warning because the sample generator computed `RoadClearedAt` and `ClosedAt`
from independent offsets, so **60 of 180 incidents closed before the road was
cleared**. The correct fix was the data, not the rule: each milestone is now
derived from its predecessor, the invariant holds by construction, and the check
is back at Error severity gating the pipeline.

**If I ran this on a clean machine right now, would it work?**
Yes, with Docker Desktop and Python. `.\scripts\run_end_to_end.ps1` starts the
containers, downloads the JDBC driver, deploys both databases, generates the
data and runs every load date through the quality gate. Everything runs in
containers precisely so there is one execution path that cannot drift from the
documentation.

**What would you do next with more time?**
Delta Lake on silver/gold — it removes the stale-partition problem and gives
`MERGE` for late-arriving data; `rowversion` watermarking to close the
commit-ordering gap; materialized aggregate fact tables instead of views; and
row-level security for district-scoped analysts.