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
   ========================================================================== */
DECLARE @FromIntersection INT = 1, @ToIntersection INT = 12;

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
DECLARE @From DATE = '2026-06-01', @To DATE = '2026-06-30';

WITH date_spine AS (
    SELECT @From AS D
    UNION ALL
    SELECT DATEADD(DAY, 1, D) FROM date_spine WHERE D < @To
)
SELECT ds.D                            AS MissingLoadDate
FROM date_spine ds
LEFT JOIN (SELECT DISTINCT DateKey FROM fact.FactHourlyTraffic) f
       ON f.DateKey = CONVERT(INT, FORMAT(ds.D, 'yyyyMMdd'))
WHERE f.DateKey IS NULL
OPTION (MAXRECURSION 366);
GO
