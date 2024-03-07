-- This is intentionally crazy TSQL code similar to code "found in the wild"

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
GO
SELECT 'äöüßéç' as string_col INTO {{out_schema}}.special_chars
GO
CREATE TABLE {{out_schema}}.special_chars2 (
  id TINYINT NOT NULL PRIMARY KEY,
  string_col VARCHAR(60) NOT NULL
)
INSERT INTO {{out_schema}}.special_chars2 (id, string_col) VALUES
(1, 'äöüßéç')
GO
-- check that both strings match and have length 7 with NOT NULL constraint
CREATE TABLE {{out_schema}}.special_chars_join (
  string_col VARCHAR(60) NOT NULL,
  string_col2 VARCHAR(60) NOT NULL,
  string_col3 VARCHAR(60) NOT NULL
)
INSERT INTO {{out_schema}}.special_chars_join
SELECT a.string_col, b.string_col, c.string_col
FROM {{out_schema}}.special_chars a
FULL OUTER JOIN {{out_schema}}.special_chars2 b ON a.string_col = b.string_col
FULL OUTER JOIN {{out_schema}}.special_chars2 c ON a.string_col = c.string_col
    and len(a.string_col) = 6 and len(c.string_col) = 6