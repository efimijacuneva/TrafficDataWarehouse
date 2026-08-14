"""Smart Traffic Data Warehouse — local pipeline dashboard.

WHY STREAMLIT: the goal is to make the DATA ENGINEERING visible, not to build a
web application. Streamlit is a single `pip install`, runs from one command, and
reads the project's real Parquet and SQL output directly with pandas — no API
layer, no build step, no server to deploy. Power BI covers the business-user
reporting story (docs/12); this covers the pipeline story, which Power BI cannot
show: zone-by-zone row accounting, quarantine reasons, SCD version history and
the three fact grains side by side.

    pip install -r requirements-dev.txt
    streamlit run dashboard/app.py

Everything shown is read from the real lake and warehouse. If a layer has not
been produced yet the page says so — it never falls back to demo data.
"""
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
import data_access as da  # noqa: E402
from data_access import weighted_by  # noqa: E402

st.set_page_config(page_title="Smart Traffic DW", page_icon="🛣️", layout="wide")

PALETTE = {
    "raw": "#8C8C8C", "bronze": "#A97142", "silver": "#7D8B99",
    "gold": "#C9A227", "sql": "#1C5E88", "ok": "#1F6B4C",
    "warn": "#B4820A", "bad": "#A8201A",
}


# --------------------------------------------------------------- helpers ---
def show_status(status: str, what: str) -> bool:
    """Render an honest message for a missing/failed source. True = usable."""
    if status == "ok":
        return True
    if status == "missing":
        st.info(f"**{what}** has not been produced yet. "
                f"Run `.\\scripts\\run_end_to_end.ps1` to populate it.")
    elif status.startswith("error:"):
        st.error(f"**{what}** could not be read — {status[6:]}")
    return False


def metric_row(items):
    cols = st.columns(len(items))
    for col, (label, value, help_text) in zip(cols, items):
        col.metric(label, value, help=help_text)


def bar(df, x, y, title, color=None, orientation="v"):
    import plotly.express as px

    fig = px.bar(df, x=x, y=y, title=title, orientation=orientation,
                 color=color, color_discrete_sequence=[PALETTE["sql"]])
    fig.update_layout(margin=dict(l=10, r=10, t=48, b=10), height=380,
                      showlegend=color is not None)
    return fig


# ============================================================ PAGE 1 =======
def page_pipeline():
    st.header("Pipeline overview")
    st.caption("Raw → Spark → Bronze → Silver → Gold → SQL Server → Analytics")

    zones = da.lake_zone_summary()
    st.subheader("Lake zones")

    cols = st.columns(5)
    for col, (_, row) in zip(cols, zones.iterrows()):
        state = "✅" if row["Exists"] and row["Files"] else "⚪"
        col.metric(
            f"{state} {row['Zone']}",
            f"{row['SizeMB']:,.1f} MB",
            f"{int(row['Partitions'])} partitions · {int(row['Files'])} files",
        )

    st.divider()

    # ---- the no-silent-loss accounting -------------------------------------
    st.subheader("Row accounting — the no-silent-loss guarantee")
    st.caption(
        "Every row read from bronze leaves the validation gate either clean or "
        "quarantined. Job 02 writes this arithmetic to `silver/reconciliation` "
        "so it can be asserted, not assumed."
    )
    recon, status = da.reconciliation()
    if show_status(status, "silver/reconciliation"):
        recon = recon.sort_values("ingest_date")
        total_in = int(recon["rows_in"].sum())
        total_good = int(recon["rows_good"].sum())
        total_bad = int(recon["rows_quarantined"].sum())
        balanced = bool((recon["rows_in"] ==
                         recon["rows_good"] + recon["rows_quarantined"]).all())

        metric_row([
            ("Rows in", f"{total_in:,}", "read from bronze for these dates"),
            ("Accepted → silver", f"{total_good:,}", "passed every validation rule"),
            ("Quarantined", f"{total_bad:,}",
             "failed a rule; kept with a reason, never deleted"),
            ("Balanced", "YES ✅" if balanced else "NO ❌",
             "rows_in == rows_good + rows_quarantined"),
        ])
        if not balanced:
            st.error("Reconciliation does NOT balance — rows are being lost. "
                     "Run `pytest tests -m spark` for the detail.")
        st.dataframe(recon[["ingest_date", "rows_in", "rows_good",
                            "rows_quarantined", "balanced"]],
                     use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Warehouse batches")
    ok, msg = da.warehouse_available()
    if not ok:
        st.warning(f"Warehouse not connected — {msg}")
        st.caption("Lake pages above still work; warehouse pages need SQL Server.")
        return
    batches, s = da.query(da.SQL_BATCHES)
    if show_status(s, "etl.BatchLog"):
        st.dataframe(batches, use_container_width=True, hide_index=True)
        succeeded = int((batches["Status"] == "Succeeded").sum())
        metric_row([
            ("Batches", f"{len(batches)}", "one per load date"),
            ("Succeeded", f"{succeeded}", None),
            ("Rows loaded", f"{int(batches['RowsInserted'].sum()):,}", None),
            ("Rows rejected", f"{int(batches['RowsRejected'].sum()):,}",
             "quarantined in stg.RejectTrafficEvent"),
        ])


# ============================================================ PAGE 2 =======
def page_analytics():
    st.header("Traffic analytics")
    ok, msg = da.warehouse_available()

    if ok:
        st.caption("Read from the warehouse mart views (`mart.v*`).")
        kpi, s = da.query(da.SQL_KPI_DAILY)
        if show_status(s, "mart.vPbiKpiDaily"):
            latest = kpi.iloc[-1]
            metric_row([
                ("Total vehicles", f"{int(kpi['TotalVehicles'].sum()):,}", None),
                ("Network avg speed", f"{latest['NetworkAvgSpeedKmh']:.1f} km/h",
                 "volume-weighted, latest day"),
                ("Congestion index", f"{latest['NetworkCongestionIndex']:.3f}",
                 "1 − avg speed / limit; target < 0.35"),
                ("Incidents", f"{int(kpi['Incidents'].sum()):,}", None),
            ])
            import plotly.express as px
            fig = px.line(kpi, x="DateKey", y=["TotalVehicles", "HeavyVehicles"],
                          title="Daily volume", markers=True)
            fig.update_layout(height=360, margin=dict(l=10, r=10, t=48, b=10))
            st.plotly_chart(fig, use_container_width=True)

        c1, c2 = st.columns(2)
        with c1:
            roads, s = da.query(da.SQL_TOP_ROADS)
            if show_status(s, "mart.vTopBusiestRoads"):
                st.plotly_chart(bar(roads.head(10), "TotalVehicles", "RoadName",
                                    "Busiest roads", orientation="h"),
                                use_container_width=True)
            rush, s = da.query(da.SQL_RUSH_HOUR)
            if show_status(s, "mart.vRushHourProfile"):
                st.plotly_chart(bar(rush, "HourOfDay", "TotalVehicles",
                                    "Volume by hour of day"),
                                use_container_width=True)
        with c2:
            weather, s = da.query(da.SQL_BY_WEATHER)
            if show_status(s, "mart.vTrafficByWeather"):
                # mart.vTrafficByWeather is at condition x precip band x severity
                # grain, so "Rain" is three rows. Plotting ConditionName on x
                # without collapsing them first makes Plotly STACK the three
                # (barmode defaults to 'relative'), rendering Rain at
                # 36.7 + 41.2 + 33.0 = 110.9 km/h - a sum of averages, and taller
                # than Dry. Collapse to the chart's grain first, VOLUME-WEIGHTED,
                # which is the same rule sql/warehouse/06_mart_views.sql states
                # for every pre-averaged measure on the hourly snapshot.
                by_condition = weighted_by(weather, "ConditionName",
                                           "AvgSpeedKmh", "TotalVehicles")
                st.plotly_chart(bar(by_condition, "ConditionName", "AvgSpeedKmh",
                                    "Average speed by weather (volume-weighted)"),
                                use_container_width=True)
                with st.expander("Detail by precipitation band"):
                    st.caption(
                        "The underlying view keeps the finer grain. Speeds here "
                        "are per band and must not be summed or plainly averaged "
                        "across bands - weight them by TotalVehicles."
                    )
                    st.dataframe(weather, use_container_width=True, hide_index=True)
            weekday, s = da.query(da.SQL_BY_WEEKDAY)
            if show_status(s, "mart.vTrafficByWeekday"):
                st.plotly_chart(bar(weekday, "DayName", "TotalVehicles",
                                    "Volume by weekday"),
                                use_container_width=True)

        detector, s = da.query(da.SQL_DETECTOR_MIX)
        if show_status(s, "detector mix"):
            st.subheader("Detector technology mix")
            st.caption(
                "Loop/radar sensors and ANPR cameras are separate asset "
                "dimensions on the same fact — `SensorKey` or `CameraKey` "
                "resolves, and the other holds the unknown member (−1)."
            )
            st.dataframe(detector, use_container_width=True, hide_index=True)
        return

    # ---- warehouse down: fall back to the GOLD LAYER, clearly labelled -----
    st.warning(f"Warehouse not connected — {msg}")
    st.caption("Falling back to the **gold Parquet layer**, which holds the same "
               "aggregates before they are loaded into SQL Server.")
    hourly, s = da.hourly_traffic()
    if not show_status(s, "gold/hourly_traffic"):
        return

    metric_row([
        ("Segment-hours", f"{len(hourly):,}", "rows in gold/hourly_traffic"),
        ("Total vehicles", f"{int(hourly['vehicle_count'].sum()):,}", None),
        ("Avg speed", f"{hourly['avg_speed_kmh'].mean():.1f} km/h", None),
        ("Segments", f"{hourly['segment_code'].nunique():,}", None),
    ])
    by_hour = hourly.groupby("hour_of_day", as_index=False)["vehicle_count"].sum()
    st.plotly_chart(bar(by_hour, "hour_of_day", "vehicle_count",
                        "Volume by hour of day (gold layer)"),
                    use_container_width=True)
    busiest = (hourly.groupby("segment_code", as_index=False)["vehicle_count"]
               .sum().nlargest(10, "vehicle_count"))
    st.plotly_chart(bar(busiest, "vehicle_count", "segment_code",
                        "Busiest segments (gold layer)", orientation="h"),
                    use_container_width=True)


# ============================================================ PAGE 3 =======
def page_quality():
    st.header("Data quality")
    st.caption("Bad data is quarantined with a reason — never deleted.")

    quar, s = da.quarantine()
    if show_status(s, "silver/_quarantine"):
        by_reason = (quar.groupby("reject_reason", as_index=False)
                     .size().rename(columns={"size": "Rows"})
                     .sort_values("Rows", ascending=False))
        metric_row([
            ("Quarantined rows", f"{len(quar):,}", "kept and replayable"),
            ("Distinct reasons", f"{len(by_reason)}", None),
            ("Dates covered", f"{quar['ingest_date'].nunique()}", None),
        ])
        st.plotly_chart(bar(by_reason, "reject_reason", "Rows",
                            "Quarantined rows by rule"), use_container_width=True)

        # The rule table is COMPUTED against the quarantine, not hard-coded.
        # It previously claimed "every one is exercised by a deliberate defect",
        # which is false on this dataset: 3 of the 8 rules never fire, and a
        # reader can disprove the claim in one query. Showing which rules are
        # armed-but-unexercised, and why, is both honest and a better answer -
        # a rule that never fires is not dead code, it is a guard against a
        # defect this particular generator does not produce.
        st.markdown("**What each rule catches, and whether it fired on this data**")
        catalogue = [
            ("MISSING_KEY", "No event id, detector or timestamp",
             "generator injects blank sensor_serial"),
            ("SPEED_RANGE", "Speed outside (0, 250] km/h",
             "generator injects 999 km/h"),
            ("OCCUPANCY_RANGE", "Occupancy outside [0, 100] %",
             "generator injects 150%"),
            ("FUTURE_TIMESTAMP", "Event dated in the future (clock skew)",
             "generator injects +365 days"),
            ("DUPLICATE", "Repeated event_id; one survivor kept deterministically",
             "generator writes the row twice"),
            ("DATE_MISMATCH", "Event date does not match the file it arrived in",
             "no such defect injected - a future date is caught by the more "
             "specific FUTURE_TIMESTAMP rule first"),
            ("BAD_DIRECTION", "Direction outside {NB, SB, EB, WB}",
             "no such defect injected - the generator only emits valid directions"),
            ("CORRUPT_RECORD", "CSV line that would not parse at all",
             "no such defect injected - every generated line is well-formed"),
        ]
        counts = by_reason.set_index("reject_reason")["Rows"].to_dict()
        rules = pd.DataFrame([
            {"Rule": r,
             "Catches": what,
             "Fired": f"{counts.get(r, 0):,}" if counts.get(r) else "—",
             "Status": "exercised" if counts.get(r) else "armed, not triggered",
             "Why": why}
            for r, what, why in catalogue
        ])
        st.dataframe(rules, use_container_width=True, hide_index=True)
        armed = int((rules.Status == "armed, not triggered").sum())
        st.caption(
            f"{len(rules) - armed} of {len(rules)} rules fired on this dataset. "
            f"The other {armed} are armed but not triggered — the generator does "
            "not produce those defects. They still run on every row, so a real "
            "feed carrying them would be caught. `tests/test_spark_rules.py` "
            "proves each one independently with hand-built input."
        )

        with st.expander("Sample quarantined rows (real data)"):
            cols = [c for c in ["event_id", "event_ts", "detector_type",
                                "detector_code", "segment_code", "speed_kmh",
                                "occupancy_pct", "reject_reason"] if c in quar.columns]
            st.dataframe(quar[cols].head(50), use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Post-load quality gate")
    ok, msg = da.warehouse_available()
    if not ok:
        st.warning(f"Warehouse not connected — {msg}")
        return
    checks, s = da.query(da.SQL_QUALITY_LATEST)
    if show_status(s, "etl.QualityCheckLog"):
        errors = int(((checks["Status"] == "Fail") &
                      (checks["Severity"] == "Error")).sum())
        warns = int(((checks["Status"] == "Fail") &
                     (checks["Severity"] == "Warning")).sum())
        metric_row([
            ("Checks run", f"{len(checks)}", "catalogue in etl.QualityCheckCatalog"),
            ("Passed", f"{int((checks['Status'] == 'Pass').sum())}", None),
            ("Failed (Error)", f"{errors}", "these block the pipeline"),
            ("Gate", "PASS ✅" if errors == 0 else "FAIL ❌",
             "usp_AssertQuality THROWs on any Error failure"),
        ])
        if warns:
            st.info(f"{warns} Warning-severity check(s) failing — reported, never blocking.")
        st.dataframe(checks, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Warehouse-boundary rejects — the second gate")
    # An EMPTY table here is the expected, healthy outcome, so it must not be
    # reported through show_status(), which would say "has not been produced yet -
    # run the pipeline" and imply something is broken. Validation is layered:
    # Spark quarantines bad rows at the SILVER gate, so only clean rows ever
    # reach gold and therefore stg.TrafficEvent. The staging validation in
    # etl.usp_LoadFactTrafficEvent re-checks the same rules at the warehouse
    # boundary and finds nothing left to reject. Zero rows means the first gate
    # did its job - defense in depth, with the outer layer idle.
    rejects, s = da.query(da.SQL_REJECTS_BY_REASON)
    if s == "ok" and not rejects.empty:
        total = int(rejects["RejectedRows"].sum())
        st.warning(
            f"{total:,} row(s) were rejected at the WAREHOUSE boundary. That means "
            "bad data got past the Spark silver gate and was only caught by the "
            "second layer — worth investigating upstream."
        )
        st.plotly_chart(bar(rejects, "RejectReason", "RejectedRows",
                            "Rejected at staging validation"),
                        use_container_width=True)
    elif s.startswith("error:"):
        st.error(f"stg.RejectTrafficEvent could not be read — {s[6:]}")
    else:
        st.success(
            "**Empty — and that is the expected result.** Every bad row was already "
            "quarantined by the Spark silver gate above, so nothing survived to be "
            "rejected again at the warehouse boundary."
        )
        st.caption(
            "Two independent gates enforce the same rules: `spark/utils/quality.py` "
            "on the way into silver, and the validation inside "
            "`etl.usp_LoadFactTrafficEvent` on the way into the fact table. The "
            "outer gate staying idle is the point — if it ever fires, the inner one "
            "regressed."
        )


# ============================================================ PAGE 4 =======
def page_scd():
    st.header("SCD Type 2 — dimension history")
    st.caption("A changed attribute creates a NEW row; the old one is expired, "
               "not overwritten — so historical facts stay correct.")

    ok, msg = da.warehouse_available()
    if not ok:
        st.warning(f"Warehouse not connected — {msg}")
        st.info("This page reads `dim.DimRoadSegment` directly.")
        return

    versioned, s = da.query(da.SQL_SCD_VERSIONED)
    if not show_status(s, "dim.DimRoadSegment"):
        return

    multi = versioned[versioned["Versions"] > 1]
    if multi.empty:
        st.warning(
            "**No segment has more than one version yet.** The 7-day demo "
            "dataset never changes a dimension attribute, so the SCD2 code path "
            "has not executed."
        )
        st.markdown(
            "Create real history — this is the exam demonstration:\n"
            "```powershell\n.\\scripts\\run_scd2_demo.ps1\n```\n"
            "It raises a speed limit in the OLTP source, re-runs the pipeline "
            "for the next day, and asserts the full Type 2 contract."
        )
    else:
        st.success(f"{len(multi)} segment(s) have version history.")

    default = multi["SegmentCode"].iloc[0] if not multi.empty else versioned["SegmentCode"].iloc[0]
    options = list(versioned["SegmentCode"])
    segment = st.selectbox("Business key", options, index=options.index(default))

    hist, s = da.query(da.SQL_SCD_HISTORY.replace("?", f"'{segment}'"))
    if not show_status(s, f"history for {segment}"):
        return

    for _, r in hist.iterrows():
        current = bool(r["IsCurrent"])
        with st.container(border=True):
            c1, c2, c3 = st.columns([1, 2, 2])
            c1.markdown(f"### v{int(r['VersionNumber'])}")
            c1.markdown("🟢 **CURRENT**" if current else "⚪ expired")
            c2.markdown(
                f"**Speed limit (Type 2, as-was):** {int(r['SpeedLimitKmh'])} km/h  \n"
                f"**Original (Type 0):** {int(r['OriginalSpeedLimitKmh'])} km/h  \n"
                f"**Current (Type 6):** {int(r['CurrentSpeedLimitKmh'])} km/h"
            )
            c3.markdown(
                f"**Surrogate key:** {int(r['RoadSegmentKey'])}  \n"
                f"**Effective:** {r['EffectiveDate']}  \n"
                f"**Expires:** {r['ExpirationDate']}"
            )

    st.divider()
    st.markdown("""
**Reading this as a Kimball model**

| Column | SCD type | Behaviour |
|---|---|---|
| `SpeedLimitKmh` | **Type 2** | Frozen per version — a 2024 fact still sees the 2024 limit |
| `OriginalSpeedLimitKmh` | **Type 0** | Never changes, on any version |
| `CurrentSpeedLimitKmh` | **Type 6** | Overwritten on *every* version, so one query can compare "vs the limit then" and "vs the limit now" |
| `EffectiveDate` / `ExpirationDate` | housekeeping | The validity interval facts join on point-in-time |
| `IsCurrent` | housekeeping | Exactly one per business key — enforced by a filtered unique index |
""")
    st.dataframe(hist, use_container_width=True, hide_index=True)


# ============================================================ PAGE 5 =======
def page_facts():
    st.header("Fact table explorer")
    st.caption("Three business processes, three grains, three Kimball fact types.")

    ok, msg = da.warehouse_available()
    if not ok:
        st.warning(f"Warehouse not connected — {msg}")
        return

    grains, s = da.query(da.SQL_FACT_GRAINS)
    if show_status(s, "fact tables"):
        st.dataframe(grains, use_container_width=True, hide_index=True)

    tabs = st.tabs(["Transaction", "Periodic snapshot", "Accumulating snapshot"])

    with tabs[0]:
        st.subheader("fact.FactTrafficEvent")
        st.markdown("""
**Grain:** one vehicle detected by one sensor **or** camera, at one instant.

**Why a transaction fact:** it *is* the atomic business event. Rows are inserted
and never updated; if nothing happened there is no row. Everything coarser is
derivable from it — the reverse is impossible.

**Insert / update:** insert-only. Reprocessing a date deletes that `DateKey` and
reloads it, which is what makes reruns safe.
""")
        df, s = da.query("SELECT TOP (20) * FROM fact.FactTrafficEvent ORDER BY DateKey DESC, TimeKey")
        if show_status(s, "FactTrafficEvent"):
            st.dataframe(df, use_container_width=True, hide_index=True)

    with tabs[1]:
        st.subheader("fact.FactHourlyTraffic")
        st.markdown("""
**Grain:** one road segment per clock hour — **including hours with zero traffic**.

**Why a periodic snapshot:** dashboards ask hourly questions, and scanning
millions of event rows to answer them is wasteful. Crucially a snapshot records
*absence*: "which segments were empty at 03:00" is unanswerable from a
transaction fact, because no row exists. The density is guaranteed by
cross-joining the current segments with a 24-hour spine.

**Insert / update:** delete-and-reload per load date.
""")
        df, s = da.query("SELECT TOP (20) * FROM fact.FactHourlyTraffic ORDER BY DateKey DESC, HourOfDay")
        if show_status(s, "FactHourlyTraffic"):
            st.dataframe(df, use_container_width=True, hide_index=True)

    with tabs[2]:
        st.subheader("fact.FactIncidentLifecycle")
        st.markdown("""
**Grain:** one incident, for its whole life.

**Why an accumulating snapshot:** the process is a finite pipeline —
Detected → Dispatched → Arrived → Cleared → Closed — with five role-playing
date/time key pairs. Storing the lags physically turns every SLA question into a
plain aggregate; the alternative (a row per status change) forces a self-join
for each one.

**Insert / update:** inserted at detection with unreached milestones set to the
unknown member (−1), then **UPDATEd in place** as each milestone arrives. This is
the only fact table that is updated.
""")
        df, s = da.query("""SELECT TOP (20) IncidentNumber, CurrentStatus, Severity,
                                   DetectedDateKey, DispatchedDateKey, ArrivedDateKey,
                                   ClearedDateKey, ClosedDateKey,
                                   MinutesToDispatch, MinutesToArrive, MinutesToClear,
                                   TotalDurationMinutes
                            FROM fact.FactIncidentLifecycle ORDER BY IncidentKey DESC""")
        if show_status(s, "FactIncidentLifecycle"):
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.caption("A milestone key of −1 means 'not reached yet' — the unknown "
                       "member, so the join stays an inner join and 'not yet' is explicit.")


# ================================================================ main =====
PAGES = {
    "1 · Pipeline overview": page_pipeline,
    "2 · Traffic analytics": page_analytics,
    "3 · Data quality": page_quality,
    "4 · SCD history": page_scd,
    "5 · Fact explorer": page_facts,
}

st.sidebar.title("🛣️ Smart Traffic DW")
st.sidebar.caption("Local dashboard over the real pipeline output")
choice = st.sidebar.radio("Page", list(PAGES))
st.sidebar.divider()

ok, msg = da.warehouse_available()
st.sidebar.markdown(f"**Warehouse:** {'🟢 connected' if ok else '🔴 not connected'}")
if not ok:
    st.sidebar.caption(msg)
zones = da.lake_zone_summary()
produced = int(zones["Exists"].sum())
st.sidebar.markdown(f"**Lake zones:** {produced}/5 produced")
st.sidebar.divider()
st.sidebar.caption(
    "Everything here is read from real project output. Nothing is simulated — "
    "a layer that has not been produced says so."
)

PAGES[choice]()