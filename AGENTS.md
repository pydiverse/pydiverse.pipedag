# Agent Guidelines for pydiverse.pipedag

## Project Overview

pydiverse.pipedag is a pipeline orchestration framework that materializes task outputs into various table stores (PostgreSQL, MSSQL, DuckDB, IBM DB2, Snowflake, Parquet+DuckDB). It provides transactional schema-swapping, caching, and multi-user synchronization.

## Repository Structure

- `src/pydiverse/pipedag/` — main package
  - `backend/table/sql/dialects/` — per-database table store implementations
  - `backend/table/cache/` — local table cache (parquet-based)
  - `backend/lock/` — lock manager implementations (ZooKeeper, database, file)
  - `materialize/` — materialization/dematerialization logic
  - `context/` — runtime context (RunContext, ConfigContext, StageLockContext)
  - `container/` — Table, View, ExternalTableReference
- `tests/` — pytest test suite
  - `tests/fixtures/instances.py` — defines all test database instances and markers
  - `tests/conftest.py` — custom pytest options (--s3, --postgres, etc.)
- `example_parquet_s3/` — example pipeline with S3/MinIO + PostgreSQL metadata store
- `tmp/` — gitignored scratch directory for temporary files, cloned repos, and build artifacts. Use this instead of `/tmp` for anything related to this project

## Running Tests

Tests use pixi for environment management. Instance selection is via pytest flags:

```shell
# Local DuckDB/Parquet tests (default-enabled)
pixi run pytest tests/ -k "parquet_backend"
pixi run pytest tests/ -k "duckdb"

# S3 Parquet tests (needs MinIO running)
pixi run pytest tests/ --s3 -k "parquet_s3_backend"

# PostgreSQL tests (needs PostgreSQL running)
pixi run pytest tests/ -k "postgres"

# Disable defaults with --no-<flag>
pixi run pytest tests/ --no-postgres --no-duckdb --s3

# Run all enabled backends in CI style
pixi run pytest tests/ --s3 --ibis --pdtransform
```

Default-enabled backends: `postgres`, `duckdb`, `polars`, `lock_tests`.

Docker services for local testing:
```shell
docker compose up  # starts postgres, minio, zookeeper, mssql, ibm_db2, prefect
```
In a typical workflow, services are already running while you iterate on code and tests.
In rare cases a restart of containers might be needed to clear corrupted caches or to switch from a different
docker-compose.yaml to working on this repo.

## Release & Dependency Update Procedure

Releases and dependency updates are separate concerns and go through distinct steps:

1. **Bug fix / feature release** — change only version, sha256, and build number in the
   conda-forge feedstock (`pydiverse-pipedag-feedstock`). Do not touch dependency bounds.
   This keeps the fix available to users regardless of their pinned dependency versions.

2. **Extend upper bounds** — after the pixi-update branch is tested and merged, update
   upper bounds in both `pyproject.toml` and the feedstock recipe to match what was
   validated. This is a separate feedstock PR.

3. **Raise lower bounds** — done as a separate release. Some dependencies (e.g. pyarrow)
   are intentionally kept with old lower bounds because downstream projects often pin them.

4. **Update examples** — after the feedstock is merged and the package appears on
   conda-forge, bump the lower bound in each `example_*/pixi.toml` (e.g.
   `pydiverse-pipedag = ">=0.12.13,<0.13"`), then run `pixi run zip-examples` from the
   project root. This updates lockfiles and regenerates the zipped examples under
   `docs/source/examples/zip/`. If the package is not yet available, retry
   `pixi update pydiverse-pipedag` inside the example directory until it resolves.

The feedstock repo can be cloned into `tmp/pydiverse-pipedag-feedstock` for local work.

## Skills

Read the relevant `SKILL.md` when working in a matching area.

| Skill | Directory | When to load |
|---|---|---|
| parquet-backend-metastore | `.claude/skills/parquet-backend-metastore/` | ParquetTableStore, sync_views, metadata_store, duckdb_parquet, schema_prefix, S3, GCS, MinIO |
| schema-swapping | `.claude/skills/schema-swapping/` | init_stage, commit_stage, transaction, __odd, __even, Schema, Stage |
| table-hooks | `.claude/skills/table-hooks/` | register_table, materialize, retrieve, PandasTableHook, PolarsTableHook, SQLAlchemyTableHook, PyArrow, View |
| test-instances | `.claude/skills/test-instances/` | pytest, with_instances, skip_instances, --s3, --postgres, CI, conftest, INSTANCE_MARKS |
