-- This is intentionally crazy TSQL code similar to code "found in the wild"

USE master
GO

/*
    SECTION: raw01A
*/
PRINT(CAST(GETDATE() AS VARCHAR) + ': {{out_schema}}.raw01A')
GO
CREATE TABLE {{out_schema}}.raw01A(
    entity                         VARCHAR(17) NOT NULL
  , start_date                           DATE        NOT NULL
  , end_date                           DATE        NOT NULL
  , PRIMARY KEY(entity, start_date)
)
INSERT INTO {{out_schema}}.raw01A WITH(TABLOCKX)
SELECT apgs.entity                                             entity
     , apgs.start_date                                               start_date
     , apgs.end_date                                               end_date
FROM (
  SELECT entity
       , start_date
       , end_date
  FROM {{in_schema}}.raw01 apgs WITH(NOLOCK)
) apgs
INNER JOIN (
  SELECT DISTINCT entity
  FROM {{in_schema}}.raw01 WITH(NOLOCK)
) base
  ON apgs.entity = base.entity
CREATE INDEX raw_start_date ON {{out_schema}}.raw01A (start_date DESC)
CREATE INDEX raw_start_date_end_date ON {{out_schema}}.raw01A (end_date, start_date DESC)
