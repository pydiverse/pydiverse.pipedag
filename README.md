# pydiverse.pipedag

[![CI](https://img.shields.io/github/actions/workflow/status/pydiverse/pydiverse.pipedag/tests.yml?style=flat-square&branch=main&label=tests)](https://github.com/pydiverse/pydiverse.pipedag/actions/workflows/tests.yml)
[![Docs](https://readthedocs.org/projects/pydiversepipedag/badge/?version=latest&style=flat-square)](https://pydiversepipedag.readthedocs.io/en/latest)
[![pypi-version](https://img.shields.io/pypi/v/pydiverse-pipedag.svg?logo=pypi&logoColor=white&style=flat-square)](https://pypi.org/project/pydiverse-pipedag)
[![conda-forge](https://img.shields.io/conda/pn/conda-forge/pydiverse-pipedag?logoColor=white&logo=conda-forge&style=flat-square)](https://prefix.dev/channels/conda-forge/packages/pydiverse-pipedag)

A pipeline orchestration library executing tasks within one python session. It takes care of SQL table
(de)materialization, caching and cache invalidation. Blob storage is supported as well for example
for storing model files.

This is an early stage version 0.x, however, it is already used in real projects. We are happy to receive your
feedback as [issues](https://github.com/pydiverse/pydiverse.pipedag/issues) on the GitHub repo. Feel free to also
comment on existing issues to extend them to your needs or to add solution ideas.

## Preparing installation

To install the package locally in development mode, you will need to install
[pixi](https://pixi.sh/latest/). For those who haven't used pixi before, it is a
poetry style dependency management tool based on conda/micromamba/conda-forge package
ecosystem. The conda-forge repository has well maintained packages for Linux, macOS,
and Windows supporting both ARM and X86 architectures. Especially, installing
psycopg2 in a portable way got much easier with pixi. In addition, pixi is really
strong in creating lock files for reproducible environments (including system libraries)
with many essential features missing in alternative tools like poetry (see [pixi.toml](pixi.toml)).

Docker is used to test against Postgres, MS SQL Server, and DB2 database targets. The bulk of
unit tests requires a Postgres test database to be up and running which can be started with
docker-compose. Pixi can also help you install docker-compose if it is not already part of your
docker (or alternative container runtime) installation:

```bash
pixi global install docker-compose
```

## Installation

> Currently, development on pipedag is not tested with Windows. Installing packages with pixi
> should work. If you are interested in contributing with Windows, please submit an
> [issue](https://github.com/pydiverse/pydiverse.pipedag/issues)
> and we will try to help you with initial setup problems.

To install pydiverse pipedag try this:

```bash
git clone https://github.com/pydiverse/pydiverse.pipedag.git
cd pydiverse.pipedag

# Create the environment, activate it and install the pre-commit hooks
pixi install  # see pixi.toml for more environments
pixi run pre-commit install
```

You can also use alternative environments as you find them in [pixi.toml](pixi.toml):

```bash
pixi install -e py312
pixi run -e py312 pre-commit install
```

Please, bear in mind, that we currently still want to be python 3.9 compatible while
always supporting the newest python version available on conda-forge.

When using Pycharm, you might find it useful that we install a `conda` executable stub you can
use for creating conda interpreters: `<pydiverse.pipedag checkout>/.pixi/envs/default/libexec/conda`
For more information, see [here](https://pixi.sh/latest/ide_integration/pycharm/).

## Testing

Most tests are based on a Postgres container running. You can launch it with a working docker-compose setup via:

```bash
docker-compose down; docker-compose up
```

The down command helps ensure a clean state within the databases launched.

After installation and launching docker container in the background, you should be able to run:

```bash
pixi run pytest --workers 4
```

You can peak in [pytest.ini](pytest.ini) and [github actions](.github/workflows/tests.yml)
to see different parameters to launch more tests.

```bash
pixi run pytest --workers=auto --mssql --duckdb --snowflake --pdtransform --ibis --polars --dask --prefect
```

for `--ibm_db2`, see the [IBM DB2 development](#ibm-db2-development) section.

## Example

A flow can look like this (see [`example/run_pipeline.py`](`example/run_pipeline.py`)):

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

Attention: sa.Alias only exists for SQLAlchemy >= 2.0. Use sa.Table or sa.sql.expression.Alias for older versions.

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
pixi run python run_pipeline.py
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

### IBM DB2 development

The `ibm_db` package is only available on the following platforms: linux-64, osx-arm64, win-64.

> [!NOTE]
> Because of this, the IBM DB2 drivers are only available in the `py312ibm` and `py310ibm`
> environments.
> You can run tests using `pixi run -e py312ibm pytest --ibm_db2`.

## Troubleshooting

### Installing mssql odbc driver for macOS and Linux

Install via Microsoft's
instructions for [Linux](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)
or [macOS](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos).

In one Linux installation case, `odbcinst -j` revealed that it installed the configuration in `/etc/unixODBC/*`.
But conda installed pyodbc brings its own `odbcinst` executable and that shows odbc config files are expected in
`/etc/*`. Symlinks were enough to fix the problem. Try `pixi run python -c 'import pyodbc;print(pyodbc.drivers())'`
and see whether you get more than an empty list.

Same happened for MacOS. The driver was installed in `/opt/homebrew/etc/odbcinst.ini` but pyodbc expected it in
`/etc/odbcinst.ini`. This can also be solved by `sudo ln -s /opt/homebrew/etc/odbcinst.ini /etc/odbcinst.ini`.

Furthermore, make sure you use 127.0.0.1 instead of localhost. It seems that /etc/hosts is ignored.

### Incompatibility with pydiverse.transform 0.2.0 - 0.2.2

Pydiverse.pipedag doesn't support pydiverse.transform 0.2.0 - 0.2.2, please consider using a newer Version,
or downgrading to pydiverse.transform 0.1.6.

### Incompatibility of ibm-db2 with pydiverse.transform > 0.2.2

The current versions of pydiverse.transform do not support ibm-db2 backend.

## Packaging and publishing to pypi and conda-forge using github actions

- bump version number in [pyproject.toml](pyproject.toml)
- set correct release date in [changelog.md](docs/source/changelog.md)
- push increased version number to `main` branch
- tag commit with `git tag <version>`, e.g. `git tag 0.7.0`
- `git push --tags`

The package should appear on https://pypi.org/project/pydiverse-pipedag/ in a timely manner. It is normal that it takes
a few hours until the new package version is available on https://conda-forge.org/packages/.

### Packaging and publishing to Pypi manually

Packages are first released on test.pypi.org:

- bump version number in [pyproject.toml](pyproject.toml) (check consistency with [changelog.md](docs/source/changelog.md))
- push increased version number to `main` branch
- `pixi run -e release hatch build`
- `pixi run -e release twine upload --repository testpypi dist/*`
- verify with https://test.pypi.org/search/?q=pydiverse.pipedag

Finally, they are published via:

- `git tag <version>`
- `git push --tags`
- Attention: Please, only continue here, if automatic publishing fails for some reason!
- `pixi run -e release hatch build`
- `pixi run -e release twine upload --repository pypi dist/*`

### Publishing package on conda-forge manually

Conda-forge packages are updated via:

- Attention: Please, only continue here, if automatic conda-forge publishing fails for longer than 24h!
- https://github.com/conda-forge/pydiverse-pipedag-feedstock#updating-pydiverse-pipedag-feedstock
- update `recipe/meta.yaml`
- test meta.yaml in pipedag repo: `conda-build build ../pydiverse-pipedag-feedstock/recipe/meta.yaml`
- commit `recipe/meta.yaml` to branch of fork and submit PR
