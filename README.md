# pydiverse.pipedag

[![Tests](https://github.com/pydiverse/pydiverse.pipedag/actions/workflows/tests.yml/badge.svg)](https://github.com/pydiverse/pydiverse.pipedag/actions/workflows/tests.yml)
[![Docs](https://readthedocs.org/projects/pydiversepipedag/badge/?version=latest&style=flat)](https://readthedocs.org/projects/pydiversepipedag/builds/)

A pipeline orchestration library executing tasks within one python session. It takes care of SQL table
(de)materialization, caching and cache invalidation. Blob storage is supported as well for example
for storing model files.

This is an early stage version 0.x, however, it is already used in real projects. Please contact
https://github.com/orgs/pydiverse/teams/code-owners if you like to become an early adopter. We will 
help you getting started and are curious about how it fits your needs.

## Preparing installation

To install the package locally in development mode, you first have to install
[Poetry](https://python-poetry.org/docs/#installation).

When installing poetry using conda (I know this sounds odd), it is recommended to install
also compilers, so source packages can be built on `poetry install`. Since we use psycopg2,
it also helps to install psycopg2 in conda to have pg_config available:

```bash
conda create -n poetry -c conda-forge poetry compilers cmake make psycopg2 docker-compose
conda activate poetry  # only needed for poetry install
```

On OSX, a way to install pg_config (needed for source building psycopg2 by `poetry install`) is

```bash
brew install postgresql
```

On OS X with `arm64` architecture, an `x86_64` toolchain is required for DB2 development:
- Ensure that Rosetta 2 is installed:
```bash
softwareupdate --install-rosetta
```
- Create the conda environment in `x86_64` mode:
```bash
conda create -n poetry
conda activate poetry
conda config --env --set subdir osx-64 
conda install -c conda-forge poetry compilers cmake make psycopg2 docker-compose python=3.11
```
- Install homebrew for  `x86_64`  and use it to install gcc. We need this because ibm_db depends on `libstdc++.6.dylib`:
```bash
arch -x86_64 /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
arch -x86_64 /usr/local/bin/brew install gcc
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

## Testing

After installation, you should be able to run:

```bash
poetry run pytest --workers 4
```

To be able to run all tests (for different databases or table types), you have to install the test dependency group:

```bash
poetry install --with=tests
```

## Pre-commit install with conda and python 3.9

We currently have some pre-commit hooks bound to python=3.9. So pre-commit install may fail when running with
python=3.10 python environment. However, the pre-commit environment does not need to be the same as the environment
used for testing pipedag code. When using conda, you may try:

```bash
conda create -n python39 -c conda-forge python=3.9 pre-commit
conda activate python39
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
docker run -h db2server --name db2server --restart=always --detach --privileged=true -p 50000:50000 --env-file docker_db2.env_list -v /Docker:/database icr.io/db2_community/db2
```

On OS X we need to use
```bash
docker run -h db2server  --platform linux/amd64 --name db2server --restart=always --detach --privileged=true -p 50000:50000 --env-file docker_db2.env_list --env IS_OSXFS=true --env PERSISTENT_HOME=false -v /Users/`whoami`/Docker:/database icr.io/db2_community/db2
```
instead.

Then check `docker logs db2server | grep -i completed` until you see `(*) Setup has completed.`.

Afterwards you can run `pytest --ibm_db2`.

## Example

A flow can look like this (see [`example/run_pipeline.py`](`example/run_pipeline.py`)):

```python
import tempfile

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.pipedag.context import StageLockContext
from pydiverse.pipedag.core.config import get_basic_pipedag_config
from pydiverse.pipedag.util.structlog import setup_logging


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
    with tempfile.TemporaryDirectory() as temp_dir:
        cfg = get_basic_pipedag_config(
            f"duckdb:///{temp_dir}/db.duckdb",
            disable_stage_locking=True,  # This is special for duckdb
        ).get("default")
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
        result = f.run(config=cfg)
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

The `with tempfile.TemporaryDirectory()` is only needed to have an OS independent temporary directory available.
You can also get rid of it like this:

```python
def main():
    cfg = get_basic_pipedag_config(
        "duckdb:////tmp/pipedag/{instance_id}/db.duckdb",
        disable_stage_locking=True,  # This is special for duckdb
    ).get("default")
    ...
```

## Example with separate database server and configuration file (i.e. Postgres in docker container)

A more realistic example can be found in [`example_postgres/run_pipeline.py`](example_postgres/run_pipeline.py).
Please note that there are `pipedag.yaml` and `docker-compose.yaml` files in the example directory.
This is also described on 
[pydiversepipedag.readthedocs.io](https://pydiversepipedag.readthedocs.io/en/latest/database_testing.html).

You can run this example with `bash` as follows:

```bash
cd example_postgres
docker-compose up
```

and in another terminal

```bash
cd example_postgres
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

On `arm64` OS X with an `x86_64` environment it is necessary to compile `pyodbc` using
```bash
arch -x86_64 /usr/local/bin/brew install unixodbc
LDFLAGS="$LDFLAGS -L/usr/local/lib"
CPPFLAGS="$CPPFLAGS -I/usr/local/include"
pip uninstall pyodbc
pip install --no-cache --pre --no-binary :all: pyodbc
```

## Packaging and publishing to pypi and conda-forge using github actions

- `poetry version prerelease` or `poetry version patch`
- set correct release date in changelog.md
- push increased version number to `main` branch
- tag commit with `git tag <version>`, e.g. `git tag 0.7.0`
- `git push --tags`

## Packaging and publishing to Pypi manually

For publishing with poetry to pypi, see:
https://www.digitalocean.com/community/tutorials/how-to-publish-python-packages-to-pypi-using-poetry-on-ubuntu-22-04

Packages are first released on test.pypi.org:

- see https://stackoverflow.com/questions/68882603/using-python-poetry-to-publish-to-test-pypi-org
- `poetry version prerelease` or `poetry version patch`
- push increased version number to `main` branch
- `poetry build`
- `poetry publish -r test-pypi`
- verify with https://test.pypi.org/search/?q=pydiverse.pipedag

Finally, they are published via:

- `git tag `\<version>
- `git push --tags`
- `poetry publish`

## Publishing package on conda-forge manually

Conda-forge packages are updated via:

- https://github.com/conda-forge/pydiverse-pipedag-feedstock#updating-pydiverse-pipedag-feedstock
- update `recipe/meta.yaml`
- test meta.yaml in pipedag repo: `conda-build build ../pydiverse-pipedag-feedstock/recipe/meta.yaml`
- commit `recipe/meta.yaml` to branch of fork and submit PR
