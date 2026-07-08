/* ============================================================================
   TrafficDW — indexing & partition maintenance (see docs/09_performance.md)
   ========================================================================== */
USE TrafficDW;
GO

/* ============================================================================
   fact.FactTrafficEvent — CLUSTERED COLUMNSTORE, partition-aligned
   Workload: pure analytical scans/aggregations over hundreds of millions of
   rows → CCI gives ~10x compression, batch mode, segment elimination.
   NOTE: creating a CCI on a partitioned heap keeps partition alignment.
   ========================================================================== */
IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = 'CCI_FactTrafficEvent'
                 AND object_id = OBJECT_ID('fact.FactTrafficEvent'))
BEGIN
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_FactTrafficEvent
        ON fact.FactTrafficEvent
        ON psMonthlyDateKey (DateKey);
END
GO

/* ============================================================================
   fact.FactHourlyTraffic — HTAP split:
   clustered B-tree (created with the PK) serves the hourly UPSERT path;
   a NONCLUSTERED COLUMNSTORE serves analytical scans.
   ========================================================================== */
IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = 'NCCI_FactHourlyTraffic'
                 AND object_id = OBJECT_ID('fact.FactHourlyTraffic'))
BEGIN
    CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_FactHourlyTraffic
        ON fact.FactHourlyTraffic
           (DateKey, HourOfDay, RoadSegmentKey, WeatherKey,
            VehicleCount, HeavyVehicleCount, IncidentCount,
            AvgSpeedKmh, P85SpeedKmh, AvgOccupancyPct, CongestionIndex);
END
GO

/* covering NC index for the segment-centric dashboard path (seek + range) */
IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = 'IX_FHT_Segment_Date'
                 AND object_id = OBJECT_ID('fact.FactHourlyTraffic'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_FHT_Segment_Date
        ON fact.FactHourlyTraffic (RoadSegmentKey, DateKey)
        INCLUDE (VehicleCount, AvgSpeedKmh, CongestionIndex);
END
GO

/* ============================================================================
   fact.FactIncidentLifecycle — small, updated in place → B-trees only.
   Milestone date keys indexed for funnel/SLA date-range queries.
   ========================================================================== */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FIL_DetectedDate'
               AND object_id = OBJECT_ID('fact.FactIncidentLifecycle'))
    CREATE NONCLUSTERED INDEX IX_FIL_DetectedDate
        ON fact.FactIncidentLifecycle (DetectedDateKey)
        INCLUDE (IncidentTypeKey, Severity, MinutesToArrive, MinutesToClear, CurrentStatus);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FIL_Status'
               AND object_id = OBJECT_ID('fact.FactIncidentLifecycle'))
    CREATE NONCLUSTERED INDEX IX_FIL_Status
        ON fact.FactIncidentLifecycle (CurrentStatus)
        INCLUDE (DetectedDateKey, RoadSegmentKey);
GO

/* ============================================================================
   Partition maintenance — sliding window helpers
   ========================================================================== */
/* Add next month's boundary (run monthly, ahead of time). SPLIT must happen
   while the rightmost partition is still empty to stay metadata-only. */
CREATE OR ALTER PROCEDURE etl.usp_AddMonthlyPartition
    @BoundaryDateKey INT   -- e.g. 20270201
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT EXISTS (SELECT 1
                   FROM sys.partition_range_values rv
                   JOIN sys.partition_functions f ON f.function_id = rv.function_id
                   WHERE f.name = 'pfMonthlyDateKey'
                     AND CONVERT(INT, rv.value) = @BoundaryDateKey)
    BEGIN
        ALTER PARTITION SCHEME psMonthlyDateKey NEXT USED [PRIMARY];
        ALTER PARTITION FUNCTION pfMonthlyDateKey() SPLIT RANGE (@BoundaryDateKey);
    END
END
GO

/* Statistics refresh after nightly load (ascending-key facts get stale fast) */
CREATE OR ALTER PROCEDURE etl.usp_UpdateFactStatistics
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE STATISTICS fact.FactTrafficEvent    WITH RESAMPLE;
    UPDATE STATISTICS fact.FactHourlyTraffic   WITH RESAMPLE;
    UPDATE STATISTICS fact.FactIncidentLifecycle WITH RESAMPLE;
END
GO

PRINT 'Indexes, partitioning helpers and statistics procs created.';
GO
