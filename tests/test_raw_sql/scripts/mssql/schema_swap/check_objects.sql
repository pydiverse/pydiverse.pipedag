-- Test that table exists
SELECT 1 FROM {{in_schema}}.t;
GO

-- Test that view exists
SELECT 1 FROM {{in_schema}}.v;
GO

-- Test that procedure exists
{{in_schema}}.p 1;
GO

-- Test that function exists
SELECT ({{in_schema}}.f (1, 2));
GO

