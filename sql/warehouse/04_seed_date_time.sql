/* ============================================================================
   TrafficDW — seed DimDate (2020-01-01 .. 2029-12-31) and DimTime (minute grain)
   DimDate uses a set-based tally; DimTime additionally demonstrates a
   RECURSIVE CTE as an alternative generation technique (Part 10 requirement).
   Both are SCD Type 0: seeded once, never updated.
   ========================================================================== */
USE TrafficDW;
SET NOCOUNT ON;
GO

/* -------------------------------------------------------------- DimDate --- */
DECLARE @start DATE = '2020-01-01', @end DATE = '2029-12-31';

;WITH tally AS (
    SELECT TOP (DATEDIFF(DAY, @start, @end) + 1)
           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
    FROM sys.all_objects a CROSS JOIN sys.all_objects b
),
d AS (SELECT DATEADD(DAY, n, @start) AS FullDate FROM tally)
INSERT INTO dim.DimDate (DateKey, FullDate, [Year], [Quarter], [MonthNumber], MonthName,
                         YearMonth, WeekOfYear, DayOfMonth, DayOfWeek, DayName,
                         IsWeekend, IsHoliday, HolidayName, Season)
SELECT CONVERT(INT, FORMAT(FullDate, 'yyyyMMdd')),
       FullDate,
       YEAR(FullDate),
       DATEPART(QUARTER, FullDate),
       MONTH(FullDate),
       DATENAME(MONTH, FullDate),
       FORMAT(FullDate, 'yyyy-MM'),
       DATEPART(ISO_WEEK, FullDate),
       DAY(FullDate),
       ((DATEPART(WEEKDAY, FullDate) + @@DATEFIRST - 2) % 7) + 1,   -- ISO: 1 = Monday
       DATENAME(WEEKDAY, FullDate),
       CASE WHEN ((DATEPART(WEEKDAY, FullDate) + @@DATEFIRST - 2) % 7) + 1 IN (6,7) THEN 1 ELSE 0 END,
       0, NULL,
       CASE WHEN MONTH(FullDate) IN (12,1,2)  THEN 'Winter'
            WHEN MONTH(FullDate) IN (3,4,5)   THEN 'Spring'
            WHEN MONTH(FullDate) IN (6,7,8)   THEN 'Summer'
            ELSE 'Autumn' END
FROM d
WHERE NOT EXISTS (SELECT 1 FROM dim.DimDate x
                  WHERE x.DateKey = CONVERT(INT, FORMAT(d.FullDate, 'yyyyMMdd')));
GO

/* national holidays (North Macedonia, sample set — extend per deployment year) */
UPDATE dim.DimDate SET IsHoliday = 1, HolidayName = h.HolidayName
FROM dim.DimDate d
JOIN (VALUES
    ('0101', N'New Year''s Day'),
    ('0524', N'St. Cyril and Methodius'),
    ('0802', N'Republic Day (Ilinden)'),
    ('0908', N'Independence Day'),
    ('1011', N'Uprising Day'),
    ('1223', N'Referendum Day')
) h(MMdd, HolidayName)
  ON FORMAT(d.FullDate, 'MMdd') = h.MMdd
WHERE d.DateKey <> -1;
GO

/* ------------------------------------------------- DimTime (recursive CTE) - */
;WITH minutes AS (
    SELECT 0 AS m
    UNION ALL
    SELECT m + 1 FROM minutes WHERE m < 1439
)
INSERT INTO dim.DimTime (TimeKey, TimeBK, [Hour], [Minute], HourBand, DayPart, IsRushHour)
SELECT m,
       CONCAT(RIGHT('0' + CAST(m / 60 AS VARCHAR(2)), 2), ':', RIGHT('0' + CAST(m % 60 AS VARCHAR(2)), 2)),
       m / 60,
       m % 60,
       CONCAT(RIGHT('0' + CAST(m / 60 AS VARCHAR(2)), 2), ':00 - ',
              RIGHT('0' + CAST((m / 60 + 1) % 24 AS VARCHAR(2)), 2), ':00'),
       CASE WHEN m < 360  THEN 'Night'          -- 00:00-06:00
            WHEN m < 600  THEN 'Morning'        -- 06:00-10:00
            WHEN m < 840  THEN 'Midday'         -- 10:00-14:00
            WHEN m < 1080 THEN 'Afternoon'      -- 14:00-18:00
            WHEN m < 1320 THEN 'Evening'        -- 18:00-22:00
            ELSE 'Night' END,
       CASE WHEN (m BETWEEN 420 AND 539) OR (m BETWEEN 960 AND 1109) THEN 1 ELSE 0 END  -- 07:00-09:00, 16:00-18:30
FROM minutes
WHERE NOT EXISTS (SELECT 1 FROM dim.DimTime t WHERE t.TimeKey = minutes.m)
OPTION (MAXRECURSION 1440);
GO

PRINT 'DimDate and DimTime seeded.';
GO
