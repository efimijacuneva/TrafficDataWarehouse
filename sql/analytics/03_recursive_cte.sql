/* ============================================================================
   Part 10 — Advanced SQL: RECURSIVE CTEs
   The road network is a graph (segments connect intersections) — recursion is
   the natural tool for path/propagation questions the star schema alone
   cannot answer.
   (A third recursive CTE lives in sql/warehouse/04_seed_date_time.sql where
    it generates the 1,440-row DimTime.)
   ========================================================================== */
USE TrafficOLTP;   -- graph traversal runs on the normalized network model
GO

/* ============================================================================
   Q1. "If intersection X is blocked, which downstream corridor is reachable
        within N hops — i.e. how far can a jam propagate along connected
        segments?"
       Anchor: segments leaving the blocked intersection.
       Recursion: follow EndIntersection → StartIntersection connectivity.
   ========================================================================== */
DECLARE @BlockedIntersection INT = 5, @MaxHops INT = 4;

WITH reachable AS (
    /* anchor: segments flowing OUT of the blocked intersection */
    SELECT s.RoadSegmentID, s.SegmentCode, s.StartIntersectionID, s.EndIntersectionID,
           1 AS Hop,
           CAST(s.SegmentCode AS NVARCHAR(4000)) AS PathTaken
    FROM oltp.RoadSegment s
    WHERE s.StartIntersectionID = @BlockedIntersection

    UNION ALL

    /* recursive step: any segment that starts where a reached segment ends */
    SELECT nxt.RoadSegmentID, nxt.SegmentCode, nxt.StartIntersectionID, nxt.EndIntersectionID,
           r.Hop + 1,
           CAST(r.PathTaken + N' → ' + nxt.SegmentCode AS NVARCHAR(4000))
    FROM reachable r
    JOIN oltp.RoadSegment nxt
      ON nxt.StartIntersectionID = r.EndIntersectionID
    WHERE r.Hop < @MaxHops
      /* cycle guard: don't revisit a segment already on this path */
      AND r.PathTaken NOT LIKE N'%' + nxt.SegmentCode + N'%'
)
SELECT Hop, SegmentCode, PathTaken
FROM reachable
ORDER BY Hop, SegmentCode
OPTION (MAXRECURSION 100);
GO

/* ============================================================================
   Q2. "Emergency routing support: enumerate every acyclic path (≤ 5 segments)
        between two intersections, with total length — shortest first."

   Choosing the target: segments chain along each road (segment i runs from
   intersection ((i-1) % 59)+1 to (i % 59)+1), so the network is a chain and
   intersection N sits N-1 hops from intersection 1. With the depth capped at
   5 segments, anything past intersection 6 is unreachable BY CONSTRUCTION and
   the query returns an empty set - which looks like a broken recursion rather
   than a correct "no route". Intersection 6 is the furthest reachable target,
   and it yields 72 distinct acyclic paths for TOP (10) to rank.
   ========================================================================== */
DECLARE @FromIntersection INT = 1, @ToIntersection INT = 6;

WITH paths AS (
    SELECT s.RoadSegmentID, s.EndIntersectionID,
           CAST(s.SegmentCode AS NVARCHAR(4000)) AS Route,
           s.LengthM AS TotalLengthM,
           1 AS Segments
    FROM oltp.RoadSegment s
    WHERE s.StartIntersectionID = @FromIntersection

    UNION ALL

    SELECT nxt.RoadSegmentID, nxt.EndIntersectionID,
           CAST(p.Route + N' → ' + nxt.SegmentCode AS NVARCHAR(4000)),
           p.TotalLengthM + nxt.LengthM,
           p.Segments + 1
    FROM paths p
    JOIN oltp.RoadSegment nxt ON nxt.StartIntersectionID = p.EndIntersectionID
    WHERE p.Segments < 5
      AND p.EndIntersectionID <> @ToIntersection            -- stop extending finished paths
      AND p.Route NOT LIKE N'%' + nxt.SegmentCode + N'%'    -- acyclic
)
SELECT TOP (10) Route, TotalLengthM, Segments
FROM paths
WHERE EndIntersectionID = @ToIntersection
ORDER BY TotalLengthM
OPTION (MAXRECURSION 500);
GO

/* ============================================================================
   Q3. "Gap detection in the warehouse date spine" — recursive date generator
        joined against loaded fact dates; any date the recursion produces that
        has no fact rows is a missed load. (Runs on TrafficDW.)
   ========================================================================== */
USE TrafficDW;
GO
/* Bound the spine by what the pipeline actually claims to have loaded, rather
   than a hard-coded month: a fixed 30-day window against a 7-day load reports
   23 "missing" dates that were never supposed to exist, which reads like 23
   pipeline failures.

   Bounding by SUCCEEDED batches alone is still not enough, because not every
   batch is supposed to produce facts. The SCD2 scenario test
   (tests/sql/test_scd2_scenario.sql) changes a speed limit and runs a batch on
   the day AFTER the last load to force a Type 2 version. That batch succeeds
   and there is no staged traffic for the date, so the phantom-day guard in
   04_load_facts.sql skips the snapshot - correctly - and logs its Fact.* steps
   with 0 rows inserted. Counting that date as a gap turns a passing test into
   what looks like a failed load.

   Note the signal is NOT "did the batch run a Fact.* step" - batch 9 runs all
   three. It is "did a Fact.* step actually insert anything". Between the first
   and last batch that really loaded facts, a date with no fact rows is a
   genuine missed load; dimension-only maintenance outside that window is
   correctly ignored. */
DECLARE @From DATE = (SELECT MIN(b.LoadDate) FROM etl.BatchLog b
                      WHERE b.Status = 'Succeeded'
                        AND EXISTS (SELECT 1 FROM etl.RowLog r
                                    WHERE r.ETLBatchID = b.ETLBatchID
                                      AND r.StepName LIKE 'Fact.%'
                                      AND r.RowsInserted > 0));
DECLARE @To   DATE = (SELECT MAX(b.LoadDate) FROM etl.BatchLog b
                      WHERE b.Status = 'Succeeded'
                        AND EXISTS (SELECT 1 FROM etl.RowLog r
                                    WHERE r.ETLBatchID = b.ETLBatchID
                                      AND r.StepName LIKE 'Fact.%'
                                      AND r.RowsInserted > 0));
IF @From IS NULL BEGIN PRINT 'No fact-loading batches yet - nothing to gap-check.'; RETURN; END;
-- ^ the semicolon is REQUIRED: T-SQL only accepts WITH as the start of a CTE if
--   the preceding statement is terminated, and without it the whole batch fails
--   with "Incorrect syntax near the keyword 'with'".

/* The result ALWAYS carries at least one row: either the gaps, or an explicit
   "none" verdict. An empty grid is ambiguous to whoever reads the report - it
   cannot be told apart from a query that failed to run, was pointed at the
   wrong database, or was never executed at all - and a health check whose
   healthy state is indistinguishable from silence is not much of a check.
   Stating "no gaps between X and Y" also lets scripts/verify_project.ps1 treat
   an empty result set as a genuine defect everywhere, with no exceptions to
   carve out. */
WITH date_spine AS (
    SELECT @From AS D
    UNION ALL
    SELECT DATEADD(DAY, 1, D) FROM date_spine WHERE D < @To
),
gaps AS (
    SELECT ds.D
    FROM date_spine ds
    LEFT JOIN (SELECT DISTINCT DateKey FROM fact.FactHourlyTraffic) f
           ON f.DateKey = CONVERT(INT, FORMAT(ds.D, 'yyyyMMdd'))
    WHERE f.DateKey IS NULL
)
SELECT CONVERT(VARCHAR(10), g.D, 23)  AS LoadDate,
       'GAP - no facts for this date' AS Verdict
FROM gaps g
UNION ALL
SELECT CONCAT(CONVERT(VARCHAR(10), @From, 23), ' .. ', CONVERT(VARCHAR(10), @To, 23)),
       'OK - every date in the loaded range has facts'
WHERE NOT EXISTS (SELECT 1 FROM gaps)
OPTION (MAXRECURSION 366);
GO
