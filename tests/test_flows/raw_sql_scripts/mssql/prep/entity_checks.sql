-- This is intentionally crazy TSQL code similar to code "found in the wild"
DROP TABLE IF EXISTS {{out_schema}}.table01
PRINT(CAST(GETDATE() AS VARCHAR) + ': {{out_schema}}.table01 - Create table')
GO
CREATE TABLE {{out_schema}}.table01 (
    entity       VARCHAR(17)     NOT NULL
  , reason      VARCHAR(50)     NOT NULL
  PRIMARY KEY (entity, reason)
)


PRINT(CAST(GETDATE() AS VARCHAR) + ': {{out_schema}}.table01 - Missing')
GO
INSERT INTO {{out_schema}}.table01 WITH (TABLOCKX)
SELECT DISTINCT raw01.entity        entity
             , 'Missing in raw01' reason
FROM {{in_schema}}.raw01 WITH (NOLOCK)
LEFT JOIN (
    SELECT DISTINCT entity
    FROM {{in_schema}}.raw01 WITH (NOLOCK)
) raw01x
ON raw01.entity = raw01x.entity
WHERE raw01.end_date = '9999-01-01'
  AND raw01x.entity IS NULL


PRINT(CAST(GETDATE() AS VARCHAR) + ': {{out_schema}}.table01 - more missing in raw01')
GO
INSERT INTO {{out_schema}}.table01 WITH(TABLOCKX)
SELECT
       raw01.entity               entity
     , 'missing'    reason
FROM {{in_schema}}.raw01 raw01 WITH(NOLOCK)
GROUP BY raw01.entity
HAVING MAX(raw01.end_date) < '9999-01-01'


PRINT(CAST(GETDATE() AS VARCHAR) + ': {{out_schema}}.table01 - Inconsistency correction')
GO
WITH entity_ids AS (
    SELECT DISTINCT raw01.entity                   entity
    FROM {{in_schema}}.raw01 raw01 WITH (NOLOCK)
    INNER JOIN ( -- filter
        SELECT entity
        FROM {{in_schema}}.raw01 WITH (NOLOCK)
        WHERE end_date = '9999-01-01'
    ) raw01_final
    ON raw01.entity = raw01_final.entity
    WHERE 1=1
)
INSERT INTO {{out_schema}}.table01 WITH(TABLOCKX)
SELECT x.entity
     , 'Inconsistency correction'   reason
FROM entity_ids x
INNER JOIN entity_ids y
ON x.entity = y.entity
WHERE x.entity <> y.entity
GROUP BY x.entity
