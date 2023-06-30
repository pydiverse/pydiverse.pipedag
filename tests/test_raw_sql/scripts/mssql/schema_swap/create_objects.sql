-- Create a table
SELECT 1 as x, 2 as y INTO {{out_schema}}.t;
GO

-- Create a view
CREATE VIEW {{out_schema}}.v AS SELECT * FROM t;
GO

-- Create a procedure
CREATE PROC {{out_schema}}.p(@id INT) AS
BEGIN
    SELECT *
    FROM t
    WHERE x = @id
END;
GO

-- Create a function
CREATE FUNCTION {{out_schema}}.f(@x INT, @y INT)
RETURNS INT
AS
BEGIN
    RETURN (@x + @y)
END;
GO