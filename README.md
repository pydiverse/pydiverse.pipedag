# pydiverse.pipedag

[![CI](https://github.com/pydiverse/pydiverse.pipedag/actions/workflows/ci.yml/badge.svg)](https://github.com/pydiverse/pydiverse.pipedag/actions/workflows/ci.yml)

A pipeline orchestration layer built on top of prefect for caching and cache invalidation to SQL and blob store targets.

## Installation

You can install the package in development mode using:

```bash
git clone https://github.com/pydiverse/pydiverse.pipedag.git
cd pydiverse.pipedag

# create and activate a fresh environment named pydiverse.pipedag
# see environment.yml for details
mamba env create
conda activate pydiverse.pipedag

pre-commit install
pip install --no-build-isolation -e .
```

## Testing

To facilitate easy testing, we provide a Docker Compose file to start all required servers.
Just run `docker compose up` in the root directory of the project to start everything, and then run `pytest` in a new tab.

You can inspect the contents of the PipeDAT Postgres database at `postgresql://postgres:pipedag@127.0.0.1/pipedag`.
To reset the state of the docker containers you can run `docker compose down`.
This might be necessary if the database cache gets corrupted.
