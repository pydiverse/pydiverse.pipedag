# pydiverse.pipedag

[![CI](https://github.com/pydiverse/pydiverse.pipedag/actions/workflows/ci.yml/badge.svg)](https://github.com/pydiverse/pydiverse.pipedag/actions/workflows/ci.yml)

A pipeline orchestration library executing tasks within one python session. It takes care of SQL table
(de)materialization, caching and cache invalidation. Blob storage is supported as well for example
for storing model files.

This is an early stage version 0.x which lacks documentation. Please contact
https://github.com/orgs/pydiverse/teams/code-owners if you like to become an early adopter
or to contribute early stage usage examples.

## Preparing installation

To install the package locally in development mode, you first have to install
[Poetry](https://python-poetry.org/docs/#installation).

When installing poetry using conda(I know this sounds odd), it is recommended to install
also compilers, so source packages can be built on `poetry install`. Since we use psycopg2,
it also helps to install psycopg2 in conda to have pg_config available:

```bash
conda install -n poetry -c poetry conda-forge compilers cmake make psycopg2
conda activate poetry  # only needed for poetry install
```

On OSX, a way to install pg_config (needed for source building psycopg2 by `poetry install`) is

```bash
brew install postgresql
```

## Installation

> Currently, development on pipedag is not possible with Windows. The current setup of installing prefect and running
> tests with docker (to spin up Postgres and Zookeeper) fail in poetry dependency resolution. It would be a nice
> contribution to find drop-in replacements for both that run as simple python dependency without docker and moving
> docker based tests to github actions (multi-DB target tests will be moved to cloud anyways).

After that, install pydiverse pipedag like this:

```bash
git clone https://github.com/pydiverse/pydiverse.pipedag.git
cd pydiverse.pipedag

# Create the environment, activate it and install the pre-commit hooks
poetry install --all-extras
poetry shell
pre-commit install
```

## Pre-commit install with conda and python 3.8

We currently have some pre-commit hooks bound to python=3.8. So pre-commit install may fail when running with
python=3.10 python environment. However, the pre-commit environment does not need to be the same as the environment
used for testing pipedag code. When using conda, you may try:

```bash
conda install -n python38 -c python=3.8 pre_commit
conda activate python38
pre-commit install
```

## Testing

To facilitate easy testing, we provide a Docker Compose file to start all required servers.
Just run `docker compose up` in the root directory of the project to start everything, and then run `pytest` in a new
tab.

You can inspect the contents of the PipeDAT Postgres database at `postgresql://postgres:pipedag@127.0.0.1/pipedag`.
To reset the state of the docker containers you can run `docker compose down`.
This might be necessary if the database cache gets corrupted.

To run tests in parallel, pass the `--workers auto` flag to pytest.

## Testing db2 functionality

For running @pytest.mark.ibm_db2 tests, you need to spin up a docker container without `docker compose` since it needs
the `--priviledged` option which `docker compose` does not offer.

```bash
docker run -h db2server --name db2server --restart=always --detach --privileged=true -p 50000:50000 --env-file docker_db2.env_list -v /Docker:/database ibmcom/db2
```

Then check `docker logs db2server | grep -i completed` until you see `(*) Setup has completed.`.

Afterwards you can run `pytest --ibm_db2`.

## Example

A flow can look like this (see `example/run_pipeline.py`):

```python
from pydiverse.pipedag import materialize, Table, Flow, Stage
import sqlalchemy as sa
import pandas as pd

from pydiverse.pipedag.context import StageLockContext, RunContext
from pydiverse.pipedag.util import setup_structlog


@materialize(lazy=True)
def lazy_task_1():
    return sa.select([sa.literal(1).label("x"), sa.literal(2).label("y")])


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_2(input1: sa.Table, input2: sa.Table):
    query = sa.select([(input1.c.x * 5).label("x5"), input2.c.a]).select_from(
        input1.outerjoin(input2, input2.c.x == input1.c.x)
    )
    return Table(query, name="task_2_out", primary_key=["a"])


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_3(input: sa.Table, my_stage: Stage):
    return sa.text(f"SELECT * FROM {my_stage.transaction_name}.{input.name}")


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_4(input: sa.Table, prev_stage: Stage):
    return sa.text(f"SELECT * FROM {prev_stage.name}.{input.name}")


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

        with Stage("stage_2") as stage2:
            lazy_2 = lazy_task_2(lazy_1, b)
            lazy_3 = lazy_task_3(lazy_2, stage2)
            eager = eager_task(lazy_1, b)

        with Stage("stage_3"):
            lazy_4 = lazy_task_4(lazy_2, stage2)
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
    # initialize logging
    setup_structlog()
    main()
```

You also need a file called `pipedag.yaml` in the same directory (see `example/pipedag.yaml`):

```yaml
name: pipedag_tests
table_store_connections:
  postgres:
    # Postgres: this can be used after running `docker-compose up`
    url: "postgresql://{$POSTGRES_USERNAME}:{$POSTGRES_PASSWORD}@127.0.0.1:6543/{instance_id}"

instances:
  __any__:
    # listen-interface for pipedag context server which synchronizes some task state during DAG execution
    network_interface: "127.0.0.1"
    # classes to be materialized to table store even without pipedag Table wrapper (we have loose coupling between
    # pipedag and pydiverse.transform, so consider adding 'pydiverse.transform.Table' in your config)
    auto_table: ["pandas.DataFrame", "sqlalchemy.sql.elements.TextClause", "sqlalchemy.sql.selectable.Selectable"]
    fail_fast: true

    instance_id: pipedag_default
    table_store:
      class: "pydiverse.pipedag.backend.table.SQLTableStore"

      # Postgres: this can be used after running `docker-compose up`
      table_store_connection: postgres
      create_database_if_not_exists: True

      # print select statements before being encapsualted in materialize expressions and tables before writing to
      # database
      print_materialize: true
      # print final sql statements
      print_sql: true

    blob_store:
      class: "pydiverse.pipedag.backend.blob.FileBlobStore"
      base_path: "/tmp/pipedag/blobs"

    lock_manager:
      class: "pydiverse.pipedag.backend.lock.ZooKeeperLockManager"
      hosts: "localhost:2181"

    orchestration:
      class: "pydiverse.pipedag.engine.SequentialEngine"
```

If you don't have a postgres, Microsoft SQL Server, or IBM DB2 database at hand, you can start a postgres database, you can use a file like `example/docker-compose.yaml`:

```yaml
version: "3.9"
services:
  postgres:
    image: postgres
    environment:
      POSTGRES_USER: sa
      POSTGRES_PASSWORD: Pydiverse23
      POSTGRES_PORT: 6543
    ports:
      - 6543:5432
  zoo:
    image: zookeeper
    environment:
      ZOO_4LW_COMMANDS_WHITELIST: ruok
    ports:
      - 2181:2181
```

You can run the example with `bash` as follows:

```bash
cd example
docker-compose up
```

and in another terminal

```bash
cd example
export POSTGRES_USERNAME=sa
export POSTGRES_PASSWORD=Pydiverse23
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

## Packaging

For publishing with poetry to pypi, see:
https://www.digitalocean.com/community/tutorials/how-to-publish-python-packages-to-pypi-using-poetry-on-ubuntu-22-04

Packages are first released on test.pypi.org:

- see https://stackoverflow.com/questions/68882603/using-python-poetry-to-publish-to-test-pypi-org
- `poetry version prerelease` or `poetry version patch`
- `poetry build`
- `poetry publish -r test-pypi`
- verify with https://test.pypi.org/search/?q=pydiverse.pipedag

Finally, they are published via:

- `git tag `\<version>
- `git push --tags`
- `poetry publish`

Conda-forge packages are updated via:

- https://github.com/conda-forge/pydiverse-pipedag-feedstock#updating-pydiverse-pipedag-feedstock
