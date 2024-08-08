# Imperative Materialization / materializing subqueries

This [example](../examples.md) shows how to easily materialize subqueries and use the result within the same task.

Subqueries are nice, but at some point they let the search space of query optimizers explode and suddenly increase 
query runtime by more than 10x. Thus for tables in the million row range, it is recommended to materialize all 
subqueries. This example shows that switching from subquery to materialized subquery can be quite easy with pipedag.

This task is actually quite close to many realistic queries:
```python
@materialize(lazy=True, input_type=sa.Table)
def lazy_task_4(input1: sa.sql.expression.Alias):
    # imperatively materialize a subquery
    subquery = f"""
        SELECT input1.a, sum(input1.x5) as x_sum FROM {ref(input1)} as input1
        GROUP BY a
    """
    query = f"""
        SELECT * FROM {ref(input1)} as input1 
        LEFT JOIN ({subquery}) as sub ON input1.a = sub.a
    """
    return Table(sa.text(query), name="enriched_aggregation").materialize()
```

Pipedag supports imperative materialization, which means you can call `Table(...).materialize()` within a task. 
This allows writing the subquery into a table called `_sub` with only one additional line:
```python
@materialize(lazy=True, input_type=sa.Table)
def lazy_task_4(input1: sa.sql.expression.Alias):
    # imperatively materialize a subquery
    subquery = f"""
        SELECT input1.a, sum(input1.x5) as x_sum FROM {ref(input1)} as input1
        GROUP BY a
    """
    sub_ref = Table(sa.text(subquery), name="_sub").materialize()
    query = f"""
        SELECT * FROM {ref(input1)} as input1 
        LEFT JOIN {ref(sub_ref)} as sub ON input1.a = sub.a
    """
    return Table(sa.text(query), name="enriched_aggregation").materialize()
```

Here is a complete example using imperative materialization everywhere. For a task returning a single table, 
`return Table(...)` or `return Table(...).materialize()` are identical. In case a task writes multiple tables, 
imperative materialization needs to assume that every previously materialized table is a dependency to subsequent 
materialized tables for automatic cache invalidation. 

```python
from __future__ import annotations

import tempfile

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.pipedag.context import StageLockContext
from pydiverse.pipedag.core.config import create_basic_pipedag_config
from pydiverse.pipedag.util.structlog import setup_logging


@materialize(lazy=True)
def lazy_task_1():
    return Table(sa.select(
        sa.literal(1).label("x"),
        sa.literal(2).label("y"),
    )).materialize()


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_2(input1: sa.sql.expression.Alias, input2: sa.sql.expression.Alias):
    query = sa.select(
        (input1.c.x * 5).label("x5"),
        input2.c.a,
    ).select_from(input1.outerjoin(input2, input2.c.x == input1.c.x))

    return Table(query, name="task_2_out", primary_key=["a"]).materialize()


def ref(tbl: sa.sql.expression.Alias):
    return f'"{tbl.original.schema}"."{tbl.original.name}"'


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_3(input1: sa.sql.expression.Alias):
    return Table(
        sa.text(f"SELECT * FROM {ref(input1)}")
    ).materialize()


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_4(input1: sa.sql.expression.Alias):
    # imperatively materialize a subquery
    subquery = f"""
        SELECT input1.a, sum(input1.x5) as x_sum FROM {ref(input1)} as input1
        GROUP BY a
    """
    sub_ref = Table(sa.text(subquery), name="_sub").materialize()
    query = f"""
        SELECT * FROM {ref(input1)} as input1 
        LEFT JOIN {ref(sub_ref)} as sub ON input1.a = sub.a
    """
    return Table(sa.text(query), name="enriched_aggregation").materialize()


@materialize(nout=2, version="1.0.0")
def eager_inputs():
    dfA = pd.DataFrame(
        {
            "a": [0, 1, 2, 4],
            "b": [9, 8, 7, 6],
        }
    )
    dfB = pd.DataFrame(
        {
            "a": [2, 1, 0, 1],
            "x": [1, 1, 2, 2],
        }
    )
    return Table(dfA, "dfA").materialize(), Table(dfB, "dfB_%%").materialize()


@materialize(version="1.0.0", input_type=pd.DataFrame)
def eager_task(tbl1: pd.DataFrame, tbl2: pd.DataFrame):
    return Table(tbl1.merge(tbl2, on="x")).materialize()


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        cfg = create_basic_pipedag_config(
            f"duckdb:///{temp_dir}/db.duckdb",
            disable_stage_locking=True,  # This is special for duckdb
            # Attention: If uncommented, stage and task names might be sent to the
            #   following URL. You can self-host kroki if you like:
            #   https://docs.kroki.io/kroki/setup/install/
            # kroki_url="https://kroki.io",
        ).get("default")
        with cfg:
            with Flow() as f:
                with Stage("stage_1"):
                    lazy_1 = lazy_task_1()
                    a, b = eager_inputs()

                with Stage("stage_2"):
                    lazy_2 = lazy_task_2(lazy_1, b)
                    lazy_3 = lazy_task_3(lazy_2)
                    eager = eager_task(lazy_1, b)

                with Stage("stage_3"):
                    lazy_4 = lazy_task_4(lazy_2)
                _ = lazy_3, lazy_4, eager  # unused terminal output tables

            # Run flow
            result = f.run()
            assert result.successful

            # Run in a different way for testing
            with StageLockContext():
                result = f.run()
                assert result.successful
                assert result.get(lazy_1, as_type=pd.DataFrame)["x"][0] == 1


if __name__ == "__main__":
    setup_logging()  # you can setup the logging and/or structlog libraries as you wish
    main()
```
For SQLAlchemy >= 2.0, you can use sa.Alias instead of sa.sql.expression.Alias.

Imperative materialization also has the advantage that the task stays in the stack trace in case exceptions happen 
during the materialization itself:

```python
from __future__ import annotations

import tempfile

import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.pipedag.core.config import create_basic_pipedag_config
from pydiverse.pipedag.util.structlog import setup_logging


@materialize(lazy=True)
def lazy_task_1():
    return Table(sa.text("<invalid query>")).materialize()

def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        cfg = create_basic_pipedag_config(
            f"duckdb:///{temp_dir}/db.duckdb",
            disable_stage_locking=True,  # This is special for duckdb
            # Attention: If uncommented, stage and task names might be sent to the
            #   following URL. You can self-host kroki if you like:
            #   https://docs.kroki.io/kroki/setup/install/
            # kroki_url="https://kroki.io",
        ).get("default")
        with cfg:
            with Flow() as f:
                with Stage("stage_1"):
                    lazy_1 = lazy_task_1()

            # Run flow
            result = f.run()

if __name__ == "__main__":
    setup_logging()  # you can setup the logging and/or structlog libraries as you wish
    main()
```

Stacktrace includes 
`File "/home/user/code/pydiverse.pipedag/example_imperative/failing_example.py", line 14, in lazy_task_1`:

```
Traceback (most recent call last):
  File "/home/user/code/pydiverse.pipedag/example_imperative/failing_example.py", line 36, in <module>
    main()
  File "/home/user/code/pydiverse.pipedag/example_imperative/failing_example.py", line 32, in main
    result = f.run()
  File "/home/user/code/pydiverse.pipedag/src/pydiverse/pipedag/core/flow.py", line 333, in run
    raise result.exception or Exception("Flow run failed")
...
  File "/home/user/code/pydiverse.pipedag/src/pydiverse/pipedag/materialize/core.py", line 717, in __call__
    result = self.fn(*args, **kwargs)
  File "/home/user/code/pydiverse.pipedag/example_imperative/failing_example.py", line 14, in lazy_task_1
    return Table(sa.text("<invalid query>")).materialize()
  File "/home/user/code/pydiverse.pipedag/src/pydiverse/pipedag/materialize/container.py", line 200, in materialize
    return task_context.imperative_materialize_callback(
...    
  File "/home/user/.cache/env/virtualenvs/pydiverse-pipedag-Hmb_rarN-py3.10/lib/python3.10/site-packages/duckdb_engine/__init__.py", line 162, in execute
    self.__c.execute(statement, parameters)
sqlalchemy.exc.ProgrammingError: (duckdb.duckdb.ParserException) Parser Error: syntax error at or near "<"
[SQL: CREATE TABLE stage_1__odd.lazy_task_1_ej2ik3i4op3osgqyqm3m_0000 AS
<invalid query>]
```