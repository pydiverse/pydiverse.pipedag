# pydiverse.pipedag

[![CI](https://github.com/pydiverse/pydiverse.pipedag/actions/workflows/ci.yml/badge.svg)](https://github.com/pydiverse/pydiverse.pipedag/actions/workflows/ci.yml)

A pipeline orchestration layer built on top of prefect for caching and cache invalidation to SQL and blob store targets.

## Installation

To install the package locally in development mode, you first have to install
[Poetry](https://python-poetry.org/docs/#installation).
After that, install pydiverse pipedag like this:

```bash
git clone https://github.com/Quantco/pdpipedag.git
cd pdpipedag

# Create the environment, activate it and install the pre-commit hooks
poetry install
poetry shell
pre-commit install
```

## Testing

To facilitate easy testing, we provide a Docker Compose file to start all required servers.
Just run `docker compose up` in the root directory of the project to start everything, and then run `pytest` in a new tab.

You can inspect the contents of the PipeDAT Postgres database at `postgresql://postgres:pipedag@127.0.0.1/pipedag`.
To reset the state of the docker containers you can run `docker compose down`.
This might be necessary if the database cache gets corrupted.
