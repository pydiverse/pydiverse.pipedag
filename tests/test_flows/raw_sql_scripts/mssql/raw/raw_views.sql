-- This is intentionally crazy TSQL code similar to code "found in the wild"
{{helper_schema}}.CREATEALLDATES '2022-01-01', '2023-01-01'
SELECT * INTO {{out_schema}}.dummy_dates FROM ##alldates
GO
SELECT 1000000 as entity_nr, cast('1000-01-01' as DATE) as start_date, cast('9999-01-01' as DATE) as end_date INTO {{out_schema}}.schema00_raw01_table
GO
SELECT '1' as mod_type, cast('1000-01-01' as DATE) as start_date, cast('9999-01-01' as DATE) as end_date INTO {{out_schema}}.filter_table
GO

/*
    SECTION: SAMPLING
*/
GO
DECLARE @START BIGINT = 0 + (SELECT CAST(MIN(entity_nr) AS BIGINT) FROM {{out_schema}}.schema00_raw01_table);
DECLARE @END BIGINT = (SELECT CAST(MAX(entity_nr) AS BIGINT) FROM {{out_schema}}.schema00_raw01_table);
DECLARE @STEP INT = {{helper_schema}}.get_db_sampling_factor();
DROP TABLE IF EXISTS {{out_schema}}.sample_entities;
WITH L0 AS (SELECT c FROM (SELECT 1 UNION ALL SELECT 1) AS D(c)), -- 2^1
  L1   AS (SELECT 1 AS c FROM L0 AS A CROSS JOIN L0 AS B),       -- 2^2
  L2   AS (SELECT 1 AS c FROM L1 AS A CROSS JOIN L1 AS B),       -- 2^4
  L3   AS (SELECT 1 AS c FROM L2 AS A CROSS JOIN L2 AS B),       -- 2^8
  L4   AS (SELECT 1 AS c FROM L3 AS A CROSS JOIN L3 AS B),       -- 2^16
  L5   AS (SELECT 1 AS c FROM L4 AS A CROSS JOIN L4 AS B),       -- 2^32
  Nums AS (SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS k FROM L5)
SELECT k * @STEP + @START AS nr
INTO {{out_schema}}.sample_entities
FROM nums
WHERE k <= (@END - @START) / @STEP
CREATE UNIQUE CLUSTERED INDEX nr_index ON {{out_schema}}.sample_entities (nr) WITH ( FILLFACTOR = 100, DATA_COMPRESSION = ROW );


/*
    SECTION: Raw-Tables
*/
GO
PRINT (CAST(GETDATE() AS VARCHAR) + ': {{out_schema}}.raw01')
DROP VIEW IF EXISTS {{out_schema}}.raw01
GO
CREATE VIEW {{out_schema}}.raw01
AS
SELECT entity_nr                 entity
     , start_date                     start_date
     , end_date                  end_date
FROM {{out_schema}}.schema00_raw01_table WITH (NOLOCK)
INNER JOIN sample_entities WITH (NOLOCK)
ON entity_nr = sample_entities.nr


/*
    SECTION: Reference tables
*/

GO
PRINT(CAST(GETDATE() AS VARCHAR) + ': {{out_schema}}.fm_mod_type')
DROP VIEW IF EXISTS {{out_schema}}.fm_mod_type
GO
CREATE VIEW {{out_schema}}.fm_mod_type
AS
SELECT mod_type     x_inv_type
     , start_date               start_date
     , end_date              end_date
FROM {{out_schema}}.filter_table WITH(NOLOCK)
GO
