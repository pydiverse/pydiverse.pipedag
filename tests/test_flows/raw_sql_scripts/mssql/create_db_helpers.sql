-- This is intentionally crazy TSQL code similar to code "found in the wild"

/*
  Section: Procedures
*/
IF OBJECT_ID(N'{{out_schema}}.CREATEALLDATES', N'P') IS NOT NULL DROP PROCEDURE {{out_schema}}.CREATEALLDATES;
GO
CREATE PROCEDURE {{out_schema}}.CREATEALLDATES
    (
        @StartDate AS DATE, @EndDate AS DATE
    ) AS
DECLARE @Current AS DATE = DATEADD(DD, 0, @StartDate); DROP TABLE IF EXISTS ##alldates CREATE TABLE ##alldates (
    dt DATE PRIMARY KEY
) WHILE @Current <= @EndDate BEGIN
    INSERT INTO ##alldates
    VALUES (@Current);
    SET @Current = DATEADD(DD, 1, @Current) -- add 1 to current day
END
GO


/*
  Section: Functions
*/
IF OBJECT_ID(N'{{out_schema}}.get_db_sampling_factor', N'FN') IS NOT NULL DROP FUNCTION {{out_schema}}.get_db_sampling_factor;
GO
CREATE FUNCTION {{out_schema}}.get_db_sampling_factor () RETURNS INT AS
BEGIN
    DECLARE @sampling_rate INT;
    SELECT @sampling_rate = ISNULL(TRY_CAST(RIGHT(DB_NAME(), LEN(DB_NAME()) - CHARINDEX('_m', DB_NAME()) - 1) AS INT),
                                   1 -- fallback: take full sample
        );
    RETURN @sampling_rate
END;