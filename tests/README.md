# Test suite

Three tiers, ordered by what infrastructure they need. Each tier states its
prerequisite honestly — a test that cannot run is **skipped with a reason**,
never silently passed.

| Tier | Files | Needs | Run with |
|---|---|---|---|
| **1. Source-data invariants** | `test_generator.py` | Python only | `pytest tests -m "not spark and not sqlserver"` |
| **2. Spark transformation logic** | `test_spark_rules.py`, `test_pipeline_reconciliation.py` | Java 11/17 + pyspark | `pytest tests -m spark` |
| **3. Warehouse behaviour** | `sql/*.sql` | SQL Server + a completed load | `scripts/run_sql_tests.ps1` |

```bash
pip install -r requirements-dev.txt
pytest tests -v                       # everything runnable here
pytest tests -v -m "not spark"        # skip anything needing a JVM
```

## What each tier proves

### Tier 1 — source-data invariants (`test_generator.py`)

Runs against `data/raw/` with no engine at all. Proves the generator produces
data that is *valid by construction* while still injecting the defects the
pipeline is supposed to catch:

- incident milestones are monotonic (`Detected ≤ Dispatched ≤ Arrived ≤ Cleared ≤ Closed`)
  — this is the invariant that `MILESTONE_ORDER_FIL` gates on, and the reason
  that check could be restored to `Error` severity;
- every deliberate defect class is actually present, so the quality gate is
  exercised rather than decorative;
- the manifest agrees with the files on disk (the reconciliation baseline);
- `--seed` really is deterministic.

### Tier 2 — Spark transformation logic

`test_spark_rules.py` drives every validation rule with hand-built DataFrames,
including the rule that used to be unreachable:

- `FUTURE_TIMESTAMP` fires on a future-dated row **and that row survives into
  quarantine** — the regression test for the silent-data-loss bug;
- rule precedence (a row failing two rules reports the more specific one);
- deterministic deduplication (the same input yields the same survivor twice);
- the sensor/camera detector split.

`test_pipeline_reconciliation.py` is the end-to-end no-silent-loss proof: it
reads `data/silver/reconciliation` and asserts `rows_in == rows_good +
rows_quarantined`, then cross-checks silver + quarantine against the raw
manifest. **This is the test that would have caught the 689 lost rows.**

### Tier 3 — warehouse behaviour (`tests/sql/`)

| File | Proves |
|---|---|
| `test_scd2_scenario.sql` | The full Type 2 contract on a real speed-limit change: two versions, exactly one current, version incremented, correct validity interval, Type 0 preserved, Type 6 synchronised, and historical facts still pointing at version 1. |
| `test_idempotency.sql` | Re-running a load date changes no fact count and creates no duplicate `EventID`. |
| `test_warehouse_invariants.sql` | Unknown members present, no SCD2 overlaps, no orphan keys, snapshot density, reconciliation. |

Run them after a completed pipeline load:

```powershell
.\scripts\run_sql_tests.ps1
```

Each script prints one row per assertion with a `PASS`/`FAIL` verdict and exits
non-zero if anything failed, so it works in CI as well as by hand.