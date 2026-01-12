# Simple pipeline

This [example](../examples.md) (see also [example.zip](zip/example.zip)) shows a simple pipeline
with a few tasks and stages. It is the same example as used
in [Database Testing](../database_testing.md) but with a DuckDB connection that does not require `docker-compose` or
`pipedag.yaml`.

It also shows how to unit-test a pipeline by dematerializing tables after running the flow:
`result.get(lazy_1, as_type=pd.DataFrame)`


```python
import tempfile

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.pipedag.context import StageLockContext
from pydiverse.pipedag.core.config import create_basic_pipedag_config
from pydiverse.common.util.structlog import setup_logging


@materialize(lazy=True)
def lazy_task_1():
    return sa.select(
        sa.literal(1).label("x"),
        sa.literal(2).label("y"),
    )


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_2(input1: sa.Alias, input2: sa.Alias):
    query = sa.select(
        (input1.c.x * 5).label("x5"),
        input2.c.a,
    ).select_from(input1.outerjoin(input2, input2.c.x == input1.c.x))

    return Table(query, name="task_2_out", primary_key=["a"])


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_3(input1: sa.Alias):
    return sa.text(f"SELECT * FROM {input1.original.schema}.{input1.original.name}")


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_4(input1: sa.Alias):
    return sa.text(f"SELECT * FROM {input1.original.schema}.{input1.original.name}")


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
    return Table(dfA, "dfA"), Table(dfB, "dfB_%%")


@materialize(version="1.0.0", input_type=pd.DataFrame)
def eager_task(tbl1: pd.DataFrame, tbl2: pd.DataFrame):
    return tbl1.merge(tbl2, on="x")


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        cfg = create_basic_pipedag_config(
            f"duckdb:///{temp_dir}/db.duckdb",
            disable_stage_locking=True,  # This is special for duckdb
            # Attention: If uncommented, stage and task names might be sent to the
            #   following URL. You can self-host kroki if you like:
            #   https://docs.kroki.io/kroki/setup/install/
            #   You need to install optional dependency 'pydot' for any visualization
            #   URL to appear.
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

## What happens when `f.run()` is called

When you call `f.run()`, pipedag executes your pipeline stage by stage. Here's a step-by-step breakdown of what happens:

### 1. Stage-by-stage execution

Pipedag processes stages in order (`stage_1` → `stage_2` → `stage_3`). All tasks within a stage are executed before
moving to the next stage. Tasks within a stage may run in parallel if you use a parallel orchestration engine (like Dask
or Prefect).

If only a subset of stages is specified for execution (using `f.run(stage_2, stage_3)`), only those stages are run.

### 2. Using a temporary schema per stage

A stage corresponds to a **production schema** in the database (e.g., `stage_1`) and a **temporary schema** (e.g.,
`stage_1__tmp`).
When executing a stage, pipedag uses a **temporary schema** (e.g., `stage_1__tmp`) where all task outputs for
that stage are written. This keeps the work-in-progress isolated from any previously committed results.
If the schema already exists (e.g., from a previous run), all of its contents are dropped at the beginning of the
stage execution.

Note: Depending on the backend, schema renaming may not be supported. In such cases, schema names with `__odd` and
`__even` suffixes are used to alternate between two schemas for each stage.

### 3. Task execution, caching, and materialization

How and if a task runs depends on its type (lazy or eager) and cache validity.

- **Lazy tasks** (like `lazy_task_1`): The task function is always executed to produce its result. This is
  typically a lightweight operation e.g., a SQL query or small dataframes. The output of the
  task (e.g., the SQL query or the dataframe) is used as an input for determining the cache validity of the task. The
  task is only materialized (i.e. written to the database) if its cache is invalid.
- **Eager tasks** (like `eager_inputs`): The cache is checked *before execution*. If valid, the task is skipped entirely
  and the cached result is reused. Only cache-invalid eager tasks actually run their Python code.

During task execution, all outputs are written to the temporary schema of the current stage. If a task is cache-valid,
its output is either copied from the production schema or a view / synonym is created in the temporary schema.
To avoid a mixture of copies and views / synonyms within a stage, if at least one task in the stage is cache-invalid,
all cache-valid tasks are copied into the temporary schema.

### 4. Schema swapping

Once **all tasks in a stage complete successfully**, pipedag performs a swap of the temporary schema with the production
schema: In this example `stage_1` and `stage_1__tmp` are swapped. Hence, after the swap, `stage_1` contains the newly
computed
tables,
and `stage_1__tmp` contains the previous version (which will be dropped on the next run).

This ensures that the production schema always contains a consistent set of tables from a fully completed stage.

### 5. Final result

After all stages complete, `f.run()` returns a `Result` object. You can:

- Check `result.successful` to verify the pipeline completed
- Use `result.get(task_output, as_type=pd.DataFrame)` to retrieve any task's output as a specific type

![Simple pipeline visualization](simple_pipeline01.svg)
