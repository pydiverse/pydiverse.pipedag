# pydiverse.pipedag

[![CI](https://github.com/pydiverse/pydiverse.pipedag/actions/workflows/tests.yml/badge.svg)](https://github.com/pydiverse/pydiverse.pipedag/actions/workflows/tests.yml)

A pipeline orchestration library executing tasks within one python session. It takes care of SQL table
(de)materialization, caching and cache invalidation. Blob storage is supported as well for example
for storing model files.

This is an early stage version 0.x which lacks documentation. Please contact
https://github.com/orgs/pydiverse/teams/code-owners if you like to become an early adopter
or to contribute early stage usage examples.

## Usage

pydiverse.pipedag can either be installed via pypi with `pip install pydiverse-pipedag` or via conda-forge
with `conda install pydiverse-pipedag -c conda-forge`.

## Example

A flow can look like this (i.e. put this in a file named `run_pipeline.py`):

```python
import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.pipedag.context import StageLockContext


@materialize(lazy=True)
def lazy_task_1():
    return sa.select(
        sa.literal(1).label("x"),
        sa.literal(2).label("y"),
    )


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_2(input1: sa.Table, input2: sa.Table):
    query = sa.select(
        (input1.c.x * 5).label("x5"),
        input2.c.a,
    ).select_from(input1.outerjoin(input2, input2.c.x == input1.c.x))

    return Table(query, name="task_2_out", primary_key=["a"])


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_3(input1: sa.Table):
    return sa.text(f"SELECT * FROM {input1.original.schema}.{input1.name}")


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_4(input1: sa.Table):
    return sa.text(f"SELECT * FROM {input1.original.schema}.{input1.name}")


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
    main()
```

Create a file called `pipedag.yaml` in the same directory:

```yaml
instances:
  __any__:
    network_interface: "127.0.0.1"
    auto_table:
      - "pandas.DataFrame"
      - "sqlalchemy.sql.expression.TextClause"
      - "sqlalchemy.sql.expression.Selectable"

    fail_fast: true
    instance_id: pipedag_default
    table_store:
      class: "pydiverse.pipedag.backend.table.SQLTableStore"
      args:
        url: "postgresql://sa:Pydiverse23@127.0.0.1:6543/{instance_id}"
        create_database_if_not_exists: True

        print_materialize: true
        print_sql: true

      local_table_cache:
        store_input: true
        store_output: true
        use_stored_input_as_cache: true
        class: "pydiverse.pipedag.backend.table.cache.ParquetTableCache"
        args:
          base_path: "/tmp/pipedag/table_cache"

    blob_store:
      class: "pydiverse.pipedag.backend.blob.FileBlobStore"
      args:
        base_path: "/tmp/pipedag/blobs"

    lock_manager:
      class: "pydiverse.pipedag.backend.lock.DatabaseLockManager"

    orchestration:
      class: "pydiverse.pipedag.engine.SequentialEngine"
```

If you don't have a postgres database at hand, you can start a postgres database, with the following `docker-compose.yaml` file:

```yaml
version: "3.9"
services:
  postgres:
    image: postgres
    environment:
      POSTGRES_USER: sa
      POSTGRES_PASSWORD: Pydiverse23
    ports:
      - "6543:5432"
```

Run `docker-compose up` in the directory of your `docker-compose.yaml` and then execute
the flow script as follows with a shell like `bash` and a python environment that
includes `pydiverse-pipedag`, `pandas`, and `sqlalchemy`:

```bash
poetry run python run_pipeline.py
```

Finally, you may connect to your localhost postgres database `pipedag_default` and
look at tables in schemas `stage_1`..`stage_3`.

If you don't have a SQL UI at hand, you may use `psql` command line tool inside the docker container.
Check out the `NAMES` column in `docker ps` output. If the name of your postgres container is
`example_postgres_1`, then you can look at output tables like this:

```bash
docker exec example_postgres_1 psql --username=sa --dbname=pipedag_default -c 'select * from stage_1.dfa;'
```

Or more interactively:

```bash
docker exec -t -i example_postgres_1 bash
psql --username=sa --dbname=pipedag_default
\dt stage_*.*
select * from stage_2.task_2_out;
```

## Troubleshooting

### Installing mssql odbc driver for linux

Installing with
instructions [here](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver16#suse18)
worked.
But `odbcinst -j` revealed that it installed the configuration in `/etc/unixODBC/*`. But conda installed pyodbc brings
its own `odbcinst` executable and that shows odbc config files are expected in `/etc/*`. Symlinks were enough to fix the
problem. Try `python -c 'import pyodbc;print(pyodbc.drivers())'` and see whether you get more than an empty list.
Furthermore, make sure you use 127.0.0.1 instead of localhost. It seems that /etc/hosts is ignored.
