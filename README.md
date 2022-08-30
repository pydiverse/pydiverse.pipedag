# pydiverse.pipedag

[![CI](https://github.com/pydiverse/pydiverse.pipedag/actions/workflows/ci.yml/badge.svg)](https://github.com/pydiverse/pydiverse.pipedag/actions/workflows/ci.yml)

A pipeline orchestration layer built on top of prefect for caching and cache invalidation to SQL and blob store targets.

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
poetry install
poetry shell
pre-commit install
```

## Testing

To facilitate easy testing, we provide a Docker Compose file to start all required servers.
Just run `docker compose up` in the root directory of the project to start everything, and then run `pytest` in a new
tab.

You can inspect the contents of the PipeDAT Postgres database at `postgresql://postgres:pipedag@127.0.0.1/pipedag`.
To reset the state of the docker containers you can run `docker compose down`.
This might be necessary if the database cache gets corrupted.
