# Raw SQL example pipeline

Even though it is not the recommended way to run larger Raw SQL scripts within pipedag, the aim of 
pydiverse.pipedag is to enable adoption with the least amount possible given existing code. And lots of data pipelines
have extensive code bases of raw SQL scripts.

Pipedag supports automatic cache invalidation for raw SQL scripts and schema swapping. So these are benefits you get 
from pipedag adoption out of the box. It will detect tables created by a raw SQL script, and it will offer to reference
such tables with `raw_sql["table_name"]` get-item syntax both at flow declaration time (will be lazily evaluated by 
consumer task) and at runtime.

The following code can be found in this [zip file](raw_sql.zip). It keeps the raw SQL scripts in files and creates
a generic pipedag task called `tsql` which replaces schema references in the raw SQL text. The prefixes `out_`, `in_`,
and `helper_` are arbitrary and can be modified and enriched as you see fit. The flow builds a dependency chain between
`tsql` calls to ensure that SQL files are executed in correct order. It doesn't manage dependencies on table level though.
See [best practices around SQL](best_practices_sql) for how to move from raw SQL scripts to better dependency management
and thus more fine-grained cache invalidation.

```python
from __future__ import annotations

from pathlib import Path

import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, materialize
from pydiverse.pipedag.context import ConfigContext, StageLockContext
from pydiverse.pipedag.container import RawSql
from tests.fixtures.instances import with_instances

"""
Attention:
Wrapping Raw SQL statements should always be just the first step of pipedag adoption.
Ideally, the next step is to extract individual transformations (SELECT statements) so
they can be gradually converted from text SQL to programmatically created SQL (python).
"""


@materialize(input_type=sa.Table, lazy=True)
def tsql(
        name: str,
        script_directory: Path,
        *,
        out_stage: Stage | None = None,
        in_sql=None,
        helper_sql=None,
        depend=None,
):
    _ = depend  # only relevant for adding additional task dependency
    script_path = script_directory / name
    sql = Path(script_path).read_text(encoding="utf-8")
    sql = raw_sql_bind_schema(sql, "out_", out_stage, transaction=True)
    sql = raw_sql_bind_schema(sql, "in_", in_sql)
    sql = raw_sql_bind_schema(sql, "helper_", helper_sql)
    return RawSql(sql, Path(script_path).name)


def raw_sql_bind_schema(
        sql, prefix: str, stage: Stage | RawSql | None, *, transaction=False
):
    if isinstance(stage, RawSql):
        stage = stage.stage
    config = ConfigContext.get()
    store = config.store.table_store
    if stage is not None:
        stage_name = stage.transaction_name if transaction else stage.name
        schema = store.get_schema(stage_name).get()
        sql = sql.replace("{{%sschema}}" % prefix, schema)
    return sql


@with_instances("mssql")
def test_raw_sql():
    instance_name = ConfigContext.get().instance_name
    parent_dir = Path(__file__).parent / "raw_sql_scripts" / instance_name

    with Flow() as flow:
        with Stage("helper") as out_stage:
            helper = tsql("create_db_helpers.sql", parent_dir, out_stage=out_stage)
        with Stage("raw") as out_stage:
            _dir = parent_dir / "raw"
            raw = tsql("raw_views.sql", _dir, out_stage=out_stage, helper_sql=helper)
        with Stage("prep") as prep_stage:
            _dir = parent_dir / "prep"
            prep = tsql(
                "entity_checks.sql", _dir, in_sql=raw, out_stage=prep_stage, depend=raw
            )
            prep = tsql(
                "more_tables.sql", _dir, in_sql=raw, out_stage=prep_stage, depend=prep
            )
            _ = prep

    # on a fresh database, this will create indexes with Raw-SQL
    _run_and_check(flow, prep_stage)
    # make sure cached execution creates the same indexes
    _run_and_check(flow, prep_stage)


def _run_and_check(flow, prep_stage):
    config_ctx = ConfigContext.get()

    with StageLockContext():
        flow_result = flow.run()
        assert flow_result.successful

        schema = config_ctx.store.table_store.get_schema(prep_stage.name).get()
        inspector = sa.inspect(config_ctx.store.table_store.engine)

        # these constraints might be a bit too harsh in case this test is extended
        # to more databases
        assert set(inspector.get_table_names(schema=schema)) == {
            "raw01A",
            "table01",
            "special_chars",
            "special_chars2",
            "special_chars_join",
        }

        pk = inspector.get_pk_constraint("raw01A", schema=schema)
        assert pk["constrained_columns"] == ["entity", "start_date"]
        assert pk["name"].startswith("PK__raw01A__")

        pk = inspector.get_pk_constraint("table01", schema=schema)
        assert pk["constrained_columns"] == ["entity", "reason"]
        assert pk["name"].startswith("PK__table01__")
        assert len(inspector.get_indexes("table01", schema=schema)) == 0

        indexes = inspector.get_indexes("raw01A", schema=schema)
        assert indexes[0]["name"] == "raw_start_date"
        assert indexes[0]["column_names"] == ["start_date"]
        assert indexes[1]["name"] == "raw_start_date_end_date"
        assert indexes[1]["column_names"] == ["end_date", "start_date"]

        with config_ctx.store.table_store.engine.connect() as conn:
            sql = f"SELECT string_col FROM [{schema}].[special_chars_join]"
            str_val = conn.execute(sa.text(sql)).fetchone()[0]
            assert str_val == "äöüßéç"
```

SQL scripts look as follows with placeholders like `{{out_schema}}` since pipedag manages schema names and schema swapping:
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

We run it with MSSQL database that we spin up with docker-compose. However, you still need to install the odbc driver:
Installing with
instructions [here](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)
worked.
But `odbcinst -j` revealed that it installed the configuration in `/etc/unixODBC/*`. But conda installed pyodbc brings
its own `odbcinst` executable and that shows odbc config files are expected in `/etc/*`. Symlinks were enough to fix the
problem. Try `python -c 'import pyodbc;print(pyodbc.drivers())'` and see whether you get more than an empty list.
Furthermore, make sure you use 127.0.0.1 instead of localhost. It seems that /etc/hosts is ignored.

The file [raw_sql.zip](raw_sql.zip) can be used as follows:

```bash
unzip raw_sql.zip
cd raw_sql
conda env create
conda activate raw_sql
docker-compose up
```

and in another terminal within the same directory:

```bash
cd raw_sql
conda activate raw_sql
python raw_sql.py
```
