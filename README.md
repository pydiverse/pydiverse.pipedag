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
