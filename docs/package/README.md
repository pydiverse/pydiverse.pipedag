# pydiverse.pipedag

[![CI](https://github.com/pydiverse/pydiverse.pipedag/actions/workflows/tests.yml/badge.svg)](https://github.com/pydiverse/pydiverse.pipedag/actions/workflows/tests.yml)

A pipeline orchestration library executing tasks within one python session. It takes care of SQL table
(de)materialization, caching and cache invalidation. Blob storage is supported as well for example
for storing model files.

This is an early stage version 0.x, however, it is already used in real projects. We are happy to receive your 
feedback as [issues](https://github.com/pydiverse/pydiverse.pipedag/issues) on the GitHub repo. Feel free to also 
comment on existing issues to extend them to your needs or to add solution ideas.

## Usage

pydiverse.pipedag can either be installed via pypi with `pip install pydiverse-pipedag duckdb duckdb-engine` or via 
conda-forge with `conda install pydiverse-pipedag duckdb duckdb-engine -c conda-forge`. If you don't use duckdb for 
testing, you can obmit it here. However, it is needed to run the following example.

## Example

A flow can look like this (i.e. put this in a file named `run_pipeline.py`):

```python
import tempfile

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.pipedag.context import StageLockContext
from pydiverse.pipedag.core.config import create_basic_pipedag_config
from pydiverse.pipedag.util.structlog import setup_logging


@materialize(lazy=True)
def lazy_task_1():
    return sa.select(
        sa.literal(1).label("x"),
        sa.literal(2).label("y"),
    )


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_2(input1: sa.sql.expression.Alias, input2: sa.sql.expression.Alias):
    query = sa.select(
        (input1.c.x * 5).label("x5"),
        input2.c.a,
    ).select_from(input1.outerjoin(input2, input2.c.x == input1.c.x))

    return Table(query, name="task_2_out", primary_key=["a"])


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_3(input1: sa.sql.expression.Alias):
    return sa.text(f"SELECT * FROM {input1.original.schema}.{input1.original.name}")


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_4(input1: sa.sql.expression.Alias):
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
            # Attention: If uncommented, stage and task names might be sent to the following URL.
            #   You can self-host kroki if you like:
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

The `with tempfile.TemporaryDirectory()` is only needed to have an OS independent temporary directory available.
You can also get rid of it like this:

```python
def main():
    cfg = create_basic_pipedag_config(
        "duckdb:////tmp/pipedag/{instance_id}/db.duckdb",
        disable_stage_locking=True,  # This is special for duckdb
    ).get("default")
    ...
```

For a more sophisiticated setup with a `pipedag.yaml` configuration file and with a separate database 
(i.e. containerized Postgres), please look [here](https://pydiversepipedag.readthedocs.io/en/latest/database_testing.html).

## Troubleshooting

### Installing mssql odbc driver for linux

Installing with
instructions [here](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)
worked.
But `odbcinst -j` revealed that it installed the configuration in `/etc/unixODBC/*`. But conda installed pyodbc brings
its own `odbcinst` executable and that shows odbc config files are expected in `/etc/*`. Symlinks were enough to fix the
problem. Try `python -c 'import pyodbc;print(pyodbc.drivers())'` and see whether you get more than an empty list.
Furthermore, make sure you use 127.0.0.1 instead of localhost. It seems that /etc/hosts is ignored.
