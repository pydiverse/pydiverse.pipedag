# Best practices: moving from Raw SQL over handwritten SELECT statements to programmatic SQL

Pydiverse.pipedag should enable adoption for whatever data pipeline code already exists and then allow for gradual 
improvements. Many data pipelines start with considerable code bases of raw SQL scripts. The example 
[raw_sql](/examples/raw_sql) shows how to wrap raw SQL files with pipedag tasks. Such a raw SQL script may look as follows:

```sql  
DROP TABLE IF EXISTS {{out_schema}}.table01
GO
CREATE TABLE {{out_schema}}.table01 (
    entity       VARCHAR(17)     NOT NULL
  , reason      VARCHAR(50)     NOT NULL
  PRIMARY KEY (entity, reason)
)
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
```

The actual important user specification part of this SQL script, however, is the `SELECT` statement. As a next step, it is
suggested to extract all `SELECT` statements and to let pipedag do the materialization in form of a `CREATE TABLE AS SELECT`
statement. This will also lead to more portable code since the actual materialization is done quite differently for different
database engines and a lot of options may exist how to do this differently (i.e. compression, etc.):

```python
import sqlalchemy as sa
import pydiverse.pipedag as dag
from pydiverse.pipedag import materialize

def ref(table: sa.Alias):
    return f"[{table.original.schema}].[{table.original.name}]"

@materialize(lazy=True, input_type=sa.Table)
def table01(raw01: sa.Alias):
    sql = f"""
        SELECT DISTINCT raw01.entity entity, 'Missing in raw01' reason
        FROM {ref(raw01)} as raw01 WITH (NOLOCK)
        LEFT JOIN (
            SELECT DISTINCT entity
            FROM {ref(raw01)} as raw01 WITH (NOLOCK)
        ) raw01x
        ON raw01.entity = raw01x.entity
        WHERE raw01.end_date = '9999-01-01'
          AND raw01x.entity IS NULL
    """    
    return dag.Table(sa.text(sql), name="table01", primary_key=["entity", "reason"])
```
Attention: sa.Alias only exists for SQLAlchemy >= 2.0. Use sa.Table or sa.sql.expression.Alias for older versions.

In the [raw sql example](/examples/raw_sql), this task can be embedded even in the middle of raw SQL scripts:

```python
    with Flow() as flow:
        with Stage("helper") as out_stage:
            helper = tsql("create_db_helpers.sql", parent_dir, out_stage=out_stage)
        with Stage("raw") as out_stage:
            _dir = parent_dir / "raw"
            raw = tsql("raw_views.sql", _dir, out_stage=out_stage, helper_sql=helper)
        with Stage("prep") as prep_stage:
            _dir = parent_dir / "prep"
            # >> Start of tasks with individual select statements (this comment is just a comment):
            _table01 = table01(raw["raw01"])
            # << End of tasks with individual select statements:
            prep = tsql(
                "more_tables.sql", _dir, in_sql=raw, out_stage=prep_stage, depend=_table01
            )
            _ = prep
```

In the end, the goal is to see the complete dependency tree on table level in codes. Pipedag can handle lists and 
dictionaries. So it is no problem for a task to return more than one output table.

Once all `SELECT` statements are extracted, the next step is to convert them to programmatic SQL. This has the advantage
that general software engineering principles can be used to share code and to test parts of the code. Pipedag supports
multiple ways of describing SQL statements in code (See [Backends](/table_backends)). The most stable way of writing SQL
statements in python code, however, is SQLAlchemy. The task above would look like this in SQLAlchemy:

```python
import sqlalchemy as sa
import pydiverse.pipedag as dag
from pydiverse.pipedag import materialize

@materialize(lazy=True, input_type=sa.Table)
def table01(raw01: sa.Alias):
    raw01x = sa.select([raw01.c.entity]).distinct().alias("raw01x")
    sql = (
        sa.select([raw01.c.entity, sa.literal("Missing in raw01").label("reason")])
        .select_from(raw01.outer_join(raw01x, raw01.c.entity == raw01x.c.entity))
        .where((raw01.c.end_date == "9999-01-01") & (raw01x.c.entity.is_(None)))
    )
    return dag.Table(sql, name="table01", primary_key=["entity", "reason"])
```