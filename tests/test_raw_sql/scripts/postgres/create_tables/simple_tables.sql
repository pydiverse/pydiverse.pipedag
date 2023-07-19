CREATE TABLE {{out_schema}}.table_1 AS SELECT 1 as x, 1 as y;
INSERT INTO {{out_schema}}.table_1 VALUES (1, 2);
INSERT INTO {{out_schema}}.table_1 VALUES (1, 3);

CREATE TABLE {{out_schema}}.table_2 AS SELECT 1 as x, 1 as y;
INSERT INTO {{out_schema}}.table_2 VALUES (2, 2);
INSERT INTO {{out_schema}}.table_2 VALUES (3, 3);
