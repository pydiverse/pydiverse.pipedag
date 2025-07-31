# Database testing

In practice, most data pipelines will use a relational database server that cannot be launched together with a small test
script in a one-liner (Relational databases are still nice in the 1-100 Mio row range and do scale if you don't care
about concurrent read-/writes on a single table. They work beyond 1 Bio rows, but you need to be careful what you do then).
See [Table Backends](table_backends.md) for a list of currently supported databases.

The following example shows how to launch a postgres database in a container with docker-compose and how to work with it
using pipedag. Please make sure, you installed psycopg2 and adbc-driver-postgresql. When installing via pip, it might be
easier to install psycopg2-binary instead of psycopg.

```shell
pip install pydiverse-pipedag pydot psycopg2-binary adbc-driver-postgresql
```

```shell
conda install -c conda-forge pydiverse-pipedag pydot psycopg2 adbc-driver-postgresql
```

or much faster than conda after installing [pixi](https://pixi.sh/latest/installation/):
```shell
mkdir my_data_proj
cd my_data_proj
pixi init
pixi add pydiverse-pipedag pydot psycopg2 adbc-driver-postgresql
```

There are prepared example directories available for running similar example code with various database technologies:
{ref}`table_backends <section-examples>`

You can put the following example pipedag code in a file called `run_pipeline_simple.py`
(see [example_postgres](https://github.com/pydiverse/pydiverse.pipedag/tree/main/example_postgres)):

```python
import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.pipedag.context import StageLockContext
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

Create a file called `pipedag.yaml` in the same directory:

```yaml
instances:
  __any__:
    network_interface: "127.0.0.1"
    auto_table:
      - "pandas.DataFrame"
      - "sqlalchemy.sql.expression.TextClause"
      - "sqlalchemy.sql.expression.Selectable"

    # Attention: For disable_kroki: false, stage and task names might be sent to the kroki_url.
    #   You can self-host kroki if you like:
    #   https://docs.kroki.io/kroki/setup/install/
    #   You need to install optional dependency 'pydot' for any visualization
    #   URL to appear.
    disable_kroki: true
    kroki_url: "https://kroki.io"

    fail_fast: true
    instance_id: pipedag_default
    table_store:
      class: "pydiverse.pipedag.backend.table.SQLTableStore"
      args:
        url: "postgresql://sa:Pydiverse23@127.0.0.1:6543/{instance_id}"
        create_database_if_not_exists: True

        print_materialize: true
        print_sql: true

      # A local table cache is optional. It keeps a local copy of parquet files when working with dataframe input types.
      local_table_cache:
        store_input: true
        store_output: true
        use_stored_input_as_cache: true
        class: "pydiverse.pipedag.backend.table.cache.ParquetTableCache"
        args:
          base_path: "/tmp/pipedag/table_cache"

    blob_store:
      # Arbitrary objects can be stored in files when wrapped as a Blob. It uses pickle in the background.
      class: "pydiverse.pipedag.backend.blob.FileBlobStore"
      args:
        base_path: "/tmp/pipedag/blobs"

    lock_manager:
      # The DatabaseLockManager is most convenient for databases which are supported (postgres, mssql, ibm_db2).
      # It ensures that concurrent execution of the same pipeline instance are possible.
      class: "pydiverse.pipedag.backend.lock.DatabaseLockManager"

    orchestration:
      # Task parallelization is often not needed if the database parallelizes each query
      class: "pydiverse.pipedag.engine.SequentialEngine"
```

If you don't have a postgres database at hand, you can start a postgres database, with the following `docker-compose.yaml` file:

```yaml
services:
  postgres:
    image: postgres
    environment:
      POSTGRES_USER: sa
      POSTGRES_PASSWORD: Pydiverse23
    ports:
      - "6543:5432"
```

Run `docker-compose up` in the directory of your `docker-compose.yaml` in order to launch your postgres database.

To run the actual pipeline, call `python run_pipeline_simple.py`.

Finally, you may connect to your localhost postgres database `pipedag_default` and
look at tables in schemas `stage_1`..`stage_3`.

If you don't have a SQL UI at hand, you may use `psql` command line tool inside the docker container.
Check out the `NAMES` column in `docker ps` output. If the name of your postgres container is
`example_postgres-postgres-1`, then you can look at output tables like this:

```bash
docker exec example_postgres-postgres-1 psql --username=sa --dbname=pipedag_default -c 'select * from stage_1.dfa;'
```

Or more interactively:

```bash
docker exec -t -i example_postgres-postgres-1 bash
psql --username=sa --dbname=pipedag_default
\dt stage_*.*
select * from stage_2.task_2_out;
```
