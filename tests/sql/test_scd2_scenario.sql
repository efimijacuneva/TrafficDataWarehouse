/* ============================================================================
   SCD TYPE 2 SCENARIO TEST — the demonstration for the oral defense.

   WHY THIS EXISTS
   ---------------
   The shipped 7-day dataset never changes a dimension attribute, so the most
   important logic in the whole warehouse — Type 2 versioning, the Type 0
   carry-forward, the Type 6 synchronisation and the point-in-time fact join —
   never executed. Nothing proved it worked. This script forces a real change
   and asserts the entire contract.

   THE SCENARIO
   ------------
       Day 1 (already loaded)   SEG-001 speed limit = L        version 1, current
       -- speed limit raised by 10 km/h in the OLTP source --
       Day 2 (this script)      SEG-001 speed limit = L + 10   version 2, current
                                                    version 1   expired

   WHAT IT ASSERTS
       1. exactly two versions exist for the business key
       2. exactly one is current
       3. VersionNumber incremented 1 -> 2
       4. the old row expired the day BEFORE the new one became effective
          (no gap, no overlap - point-in-time lookups stay unambiguous)
       5. surrogate keys differ (facts can point at either version)
       6. Type 0: OriginalSpeedLimitKmh identical on both versions
       7. Type 6: CurrentSpeedLimitKmh synchronised to the NEW limit on BOTH
       8. Type 2: SpeedLimitKmh frozen per version (old row keeps the old limit)
       9. point-in-time correctness: day-1 facts still resolve to version 1
      10. the SCD2 quality checks still pass

   USAGE
       sqlcmd -S localhost -U sa -P '...' -C -b -I -d TrafficDW \
              -i tests/sql/test_scd2_scenario.sql
   or  .\scripts\run_scd2_demo.ps1

   The script is REPEATABLE: it raises the limit by 10 each run, so running it
   twice produces versions 2 and 3 and every assertion still holds.
   ========================================================================== */
SET NOCOUNT ON;
USE TrafficDW;
GO

DECLARE @Segment      VARCHAR(30) = 'SEG-001';
DECLARE @Day1         DATE;
DECLARE @Day2         DATE;
DECLARE @Failures     INT = 0;

/* the last date actually loaded becomes "day 1"; the change lands the day after */
SELECT @Day1 = MAX(LoadDate) FROM etl.BatchLog WHERE Status = 'Succeeded';
IF @Day1 IS NULL
BEGIN
    RAISERROR('No successful batch found. Run the pipeline before this test.', 16, 1);
    RETURN;
END
SET @Day2 = DATEADD(DAY, 1, @Day1);

PRINT '=================================================================';
PRINT ' SCD2 SCENARIO TEST';
PRINT '   business key : ' + @Segment;
PRINT '   day 1 (loaded): ' + CONVERT(CHAR(10), @Day1, 120);
PRINT '   day 2 (change): ' + CONVERT(CHAR(10), @Day2, 120);
PRINT '=================================================================';

/* ---------------------------------------------------------------- BEFORE -- */
DECLARE @KeyBefore INT, @LimitBefore SMALLINT, @VersionBefore INT, @OriginalBefore SMALLINT;
SELECT @KeyBefore = RoadSegmentKey, @LimitBefore = SpeedLimitKmh,
       @VersionBefore = VersionNumber, @OriginalBefore = OriginalSpeedLimitKmh
FROM dim.DimRoadSegment
WHERE SegmentCode = @Segment AND IsCurrent = 1;

IF @KeyBefore IS NULL
BEGIN
    RAISERROR('Segment %s has no current dimension row. Load the warehouse first.', 16, 1, @Segment);
    RETURN;
END

PRINT '';
PRINT 'BEFORE  key=' + CAST(@KeyBefore AS VARCHAR(12))
    + '  version=' + CAST(@VersionBefore AS VARCHAR(12))
    + '  SpeedLimitKmh=' + CAST(@LimitBefore AS VARCHAR(12))
    + '  OriginalSpeedLimitKmh=' + CAST(@OriginalBefore AS VARCHAR(12));

/* ------------------------------------------------- 1. change the SOURCE --- */
/* The warehouse must react to a change in the OLTP system of record, not to a
   hand-edited dimension row — that is what makes this an end-to-end test.    */
DECLARE @NewLimit SMALLINT = @LimitBefore + 10;
UPDATE TrafficOLTP.oltp.RoadSegment
SET    SpeedLimitKmh = @NewLimit,
       ModifiedAt    = SYSUTCDATETIME()     -- moves the high-watermark
WHERE  SegmentCode = @Segment;

PRINT 'SOURCE  speed limit ' + CAST(@LimitBefore AS VARCHAR(12))
    + ' -> ' + CAST(@NewLimit AS VARCHAR(12)) + ' km/h in TrafficOLTP';

/* --------------------------------------- 2. run the pipeline for day 2 ---- */
EXEC etl.usp_RunNightlyPipeline @LoadDate = @Day2;
PRINT 'PIPELINE ran for ' + CONVERT(CHAR(10), @Day2, 120);
PRINT '';

/* ------------------------------------------------------------- 3. assert -- */
DECLARE @results TABLE (
    Seq      TINYINT,
    Assertion VARCHAR(80),
    Expected  VARCHAR(60),
    Actual    VARCHAR(60),
    Verdict   CHAR(4)
);

DECLARE @versions INT       = (SELECT COUNT(*)  FROM dim.DimRoadSegment WHERE SegmentCode = @Segment);
DECLARE @currents INT       = (SELECT COUNT(*)  FROM dim.DimRoadSegment WHERE SegmentCode = @Segment AND IsCurrent = 1);
DECLARE @maxVersion INT     = (SELECT MAX(VersionNumber) FROM dim.DimRoadSegment WHERE SegmentCode = @Segment);
DECLARE @distinctOriginal INT = (SELECT COUNT(DISTINCT OriginalSpeedLimitKmh) FROM dim.DimRoadSegment WHERE SegmentCode = @Segment);
DECLARE @distinctCurrentCol INT = (SELECT COUNT(DISTINCT CurrentSpeedLimitKmh) FROM dim.DimRoadSegment WHERE SegmentCode = @Segment);

DECLARE @KeyAfter INT, @LimitAfter SMALLINT, @EffAfter DATE, @Type6After SMALLINT;
SELECT @KeyAfter = RoadSegmentKey, @LimitAfter = SpeedLimitKmh,
       @EffAfter = EffectiveDate, @Type6After = CurrentSpeedLimitKmh
FROM dim.DimRoadSegment WHERE SegmentCode = @Segment AND IsCurrent = 1;

DECLARE @ExpOld DATE, @LimitOld SMALLINT;
SELECT @ExpOld = ExpirationDate, @LimitOld = SpeedLimitKmh
FROM dim.DimRoadSegment
WHERE SegmentCode = @Segment AND RoadSegmentKey = @KeyBefore;

INSERT INTO @results VALUES
 (1, 'A new version row was created',
     '>= 2', CAST(@versions AS VARCHAR(12)),
     CASE WHEN @versions >= 2 THEN 'PASS' ELSE 'FAIL' END),
 (2, 'Exactly one version is current (SCD2 invariant)',
     '1', CAST(@currents AS VARCHAR(12)),
     CASE WHEN @currents = 1 THEN 'PASS' ELSE 'FAIL' END),
 (3, 'VersionNumber incremented',
     CAST(@VersionBefore + 1 AS VARCHAR(12)), CAST(@maxVersion AS VARCHAR(12)),
     CASE WHEN @maxVersion = @VersionBefore + 1 THEN 'PASS' ELSE 'FAIL' END),
 (4, 'New version carries the NEW speed limit (Type 2)',
     CAST(@NewLimit AS VARCHAR(12)), CAST(@LimitAfter AS VARCHAR(12)),
     CASE WHEN @LimitAfter = @NewLimit THEN 'PASS' ELSE 'FAIL' END),
 (5, 'Old version still carries the OLD limit (history preserved)',
     CAST(@LimitBefore AS VARCHAR(12)), CAST(@LimitOld AS VARCHAR(12)),
     CASE WHEN @LimitOld = @LimitBefore THEN 'PASS' ELSE 'FAIL' END),
 (6, 'Old row expired the day before the new one (no gap/overlap)',
     CONVERT(CHAR(10), DATEADD(DAY,-1,@Day2), 120), CONVERT(CHAR(10), @ExpOld, 120),
     CASE WHEN @ExpOld = DATEADD(DAY,-1,@Day2) THEN 'PASS' ELSE 'FAIL' END),
 (7, 'New row effective on the load date',
     CONVERT(CHAR(10), @Day2, 120), CONVERT(CHAR(10), @EffAfter, 120),
     CASE WHEN @EffAfter = @Day2 THEN 'PASS' ELSE 'FAIL' END),
 (8, 'Surrogate key differs (facts can reference either version)',
     'different', CASE WHEN @KeyAfter <> @KeyBefore THEN 'different' ELSE 'same' END,
     CASE WHEN @KeyAfter <> @KeyBefore THEN 'PASS' ELSE 'FAIL' END),
 (9, 'TYPE 0: OriginalSpeedLimitKmh never changes across versions',
     '1 distinct', CAST(@distinctOriginal AS VARCHAR(12)) + ' distinct',
     CASE WHEN @distinctOriginal = 1 THEN 'PASS' ELSE 'FAIL' END),
 (10,'TYPE 6: CurrentSpeedLimitKmh synchronised on ALL versions',
     '1 distinct', CAST(@distinctCurrentCol AS VARCHAR(12)) + ' distinct',
     CASE WHEN @distinctCurrentCol = 1 THEN 'PASS' ELSE 'FAIL' END),
 (11,'TYPE 6: that synchronised value is the NEW limit',
     CAST(@NewLimit AS VARCHAR(12)), CAST(@Type6After AS VARCHAR(12)),
     CASE WHEN @Type6After = @NewLimit THEN 'PASS' ELSE 'FAIL' END);

/* --- 12/13. POINT-IN-TIME: historical facts keep their as-was version -----
   Anchor on the latest date that ACTUALLY HAS TRAFFIC FACTS for this segment,
   not on MAX(LoadDate). Those differ as soon as this script has run once: it
   loads the day AFTER the last data day, so MAX(LoadDate) becomes a date with
   no detections at all and "do its facts point at v1?" is vacuously 0 rows -
   a FAIL that says nothing about the warehouse.

   The real property is version-agnostic and holds however many times this has
   run: every fact must reference the segment version whose validity interval
   COVERS THE FACT'S OWN DATE. */
DECLARE @FactDateKey INT = (
    SELECT MAX(f.DateKey) FROM fact.FactTrafficEvent f
    JOIN dim.DimRoadSegment d ON d.RoadSegmentKey = f.RoadSegmentKey
    WHERE d.SegmentCode = @Segment);

DECLARE @ExpectedKey INT = (
    SELECT TOP (1) RoadSegmentKey FROM dim.DimRoadSegment
    WHERE SegmentCode = @Segment
      AND CONVERT(INT, FORMAT(EffectiveDate,  'yyyyMMdd')) <= @FactDateKey
      AND CONVERT(INT, FORMAT(ExpirationDate, 'yyyyMMdd')) >= @FactDateKey);

DECLARE @factsOnCorrectVersion INT = (
    SELECT COUNT(*) FROM fact.FactTrafficEvent
    WHERE DateKey = @FactDateKey AND RoadSegmentKey = @ExpectedKey);

DECLARE @factsOnWrongVersion INT = (
    SELECT COUNT(*) FROM fact.FactTrafficEvent f
    JOIN dim.DimRoadSegment d ON d.RoadSegmentKey = f.RoadSegmentKey
    WHERE d.SegmentCode = @Segment
      AND f.DateKey = @FactDateKey
      AND f.RoadSegmentKey <> @ExpectedKey);

INSERT INTO @results VALUES
 (12, 'POINT-IN-TIME: facts resolve to the version valid on THEIR date',
      '> 0 rows on SK ' + ISNULL(CAST(@ExpectedKey AS VARCHAR(12)), '?'),
      CAST(@factsOnCorrectVersion AS VARCHAR(12)) + ' rows (date ' + CAST(@FactDateKey AS VARCHAR(12)) + ')',
      CASE WHEN @factsOnCorrectVersion > 0 THEN 'PASS' ELSE 'FAIL' END),
 (13, 'POINT-IN-TIME: no fact points at a version that was not yet valid',
      '0 rows', CAST(@factsOnWrongVersion AS VARCHAR(12)) + ' rows',
      CASE WHEN @factsOnWrongVersion = 0 THEN 'PASS' ELSE 'FAIL' END),
 (15, 'The NEW version carries no historical facts yet (it is current-only)',
      '0 rows',
      CAST((SELECT COUNT(*) FROM fact.FactTrafficEvent
            WHERE RoadSegmentKey = @KeyAfter AND DateKey < CONVERT(INT, FORMAT(@Day2,'yyyyMMdd'))) AS VARCHAR(12)) + ' rows',
      CASE WHEN (SELECT COUNT(*) FROM fact.FactTrafficEvent
                 WHERE RoadSegmentKey = @KeyAfter AND DateKey < CONVERT(INT, FORMAT(@Day2,'yyyyMMdd'))) = 0
           THEN 'PASS' ELSE 'FAIL' END);

/* --- 14. the SCD2 invariants the quality gate enforces -------------------- */
DECLARE @overlaps INT = (
    SELECT COUNT(*) FROM dim.DimRoadSegment a
    JOIN dim.DimRoadSegment b ON b.SegmentCode = a.SegmentCode AND b.RoadSegmentKey > a.RoadSegmentKey
    WHERE a.RoadSegmentKey <> -1
      AND a.EffectiveDate <= b.ExpirationDate AND b.EffectiveDate <= a.ExpirationDate);

INSERT INTO @results VALUES
 (14, 'No overlapping validity intervals anywhere in the dimension',
      '0', CAST(@overlaps AS VARCHAR(12)),
      CASE WHEN @overlaps = 0 THEN 'PASS' ELSE 'FAIL' END);

/* ------------------------------------------------------------- 4. report -- */
PRINT 'VERSION HISTORY AFTER THE CHANGE';
PRINT '---------------------------------------------------------------------';
SELECT RoadSegmentKey        AS [SK],
       VersionNumber         AS [Ver],
       SpeedLimitKmh         AS [Limit (Type 2, as-was)],
       OriginalSpeedLimitKmh AS [Original (Type 0)],
       CurrentSpeedLimitKmh  AS [Current (Type 6)],
       EffectiveDate,
       ExpirationDate,
       IsCurrent
FROM dim.DimRoadSegment
WHERE SegmentCode = @Segment
ORDER BY VersionNumber;

PRINT '';
PRINT 'ASSERTIONS';
PRINT '---------------------------------------------------------------------';
SELECT Seq, Assertion, Expected, Actual, Verdict FROM @results ORDER BY Seq;

SELECT @Failures = COUNT(*) FROM @results WHERE Verdict = 'FAIL';
DECLARE @Total INT = (SELECT COUNT(*) FROM @results);   -- PRINT cannot take a subquery

PRINT '';
IF @Failures = 0
BEGIN
    PRINT '=================================================================';
    PRINT ' SCD2 SCENARIO: ALL ' + CAST(@Total AS VARCHAR(12)) + ' ASSERTIONS PASSED';
    PRINT '=================================================================';
END
ELSE
BEGIN
    DECLARE @msg NVARCHAR(200) = CONCAT('SCD2 SCENARIO FAILED: ', @Failures, ' assertion(s) did not hold.');
    THROW 50100, @msg, 1;   -- non-zero exit under sqlcmd -b
END
GO