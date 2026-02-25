IF OBJECT_ID('dbo.DateTable', 'U') IS NOT NULL
    DROP TABLE dbo.DateTable;
GO

CREATE TABLE dbo.DateTable (
    DateKey        INT         NOT NULL PRIMARY KEY, -- YYYYMMDD format
    DateValue      DATE        NOT NULL,
    DayNumber      TINYINT     NOT NULL, -- 1-31
    MonthNumber    TINYINT     NOT NULL, -- 1-12
    MonthName      VARCHAR(20) NOT NULL,
    QuarterNumber  TINYINT     NOT NULL, -- 1-4
    YearNumber     SMALLINT    NOT NULL,
    DayOfWeek      TINYINT     NOT NULL, -- 1=Monday, 7=Sunday
    DayName        VARCHAR(20) NOT NULL,
    IsWeekend      BIT         NOT NULL
);
GO

-- Populate the table
DECLARE @StartDate DATE = '2018-01-01';
DECLARE @EndDate   DATE = '2023-03-01';

;WITH DateSequence AS (
    SELECT @StartDate AS DateValue
    UNION ALL
    SELECT DATEADD(DAY, 1, DateValue)
    FROM DateSequence
    WHERE DateValue < @EndDate
)
INSERT INTO dbo.DateTable
SELECT 
    CONVERT(INT, FORMAT(DateValue, 'yyyyMMdd')) AS DateKey,
    DateValue,
    DAY(DateValue) AS DayNumber,
    MONTH(DateValue) AS MonthNumber,
    DATENAME(MONTH, DateValue) AS MonthName,
    DATEPART(QUARTER, DateValue) AS QuarterNumber,
    YEAR(DateValue) AS YearNumber,
    DATEPART(WEEKDAY, DateValue) AS DayOfWeek,
    DATENAME(WEEKDAY, DateValue) AS DayName,
    CASE WHEN DATEPART(WEEKDAY, DateValue) IN (1, 7) THEN 1 ELSE 0 END AS IsWeekend
FROM DateSequence
OPTION (MAXRECURSION 0); -- Allow recursion beyond 100
GO

