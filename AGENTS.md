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

MSSQL on macOS: msodbcsql18 hardcodes `/opt/homebrew/opt/openssl/lib`, which Homebrew now points to the unsupported
openssl@4 ([upstream issue](https://github.com/microsoft/homebrew-mssql-release/issues/113)). Install openssl@3 and
prefix the command: `DYLD_LIBRARY_PATH=/opt/homebrew/opt/openssl@3/lib pytest ... --mssql`. Exporting it in `.envrc`
doesn't reach the agent's shell, because macOS SIP strips `DYLD_*` variables.

### Working without network access (agent sandbox)

Plain `pixi run` (even with `--frozen`) needs network access, because the pixi-build source package re-initializes
its build backend on every run. Offline:
- Default env: run tools directly (`pytest ...`, `python ...`) when it's activated, e.g. via `eval "$(pixi shell-hook)"` in `.envrc`.
- Other envs: `pixi run --no-install -e <env> ...`. Tasks: `pixi run --as-is <task>`.
- Run `pixi install` outside the sandbox when the env needs updating (after lockfile/dependency changes).
- Commit with `SKIP=pixi-install git commit ...`. All other pre-commit hooks use `pixi run --as-is` and work offline.

## Skills

Read the relevant `SKILL.md` when working in a matching area.

| Skill | Directory | When to load |
|---|---|---|
| parquet-backend-metastore | `.claude/skills/parquet-backend-metastore/` | ParquetTableStore, sync_views, metadata_store, duckdb_parquet, schema_prefix, S3, GCS, MinIO |
| schema-swapping | `.claude/skills/schema-swapping/` | init_stage, commit_stage, transaction, __odd, __even, Schema, Stage |
| table-hooks | `.claude/skills/table-hooks/` | register_table, materialize, retrieve, PandasTableHook, PolarsTableHook, SQLAlchemyTableHook, PyArrow, View |
| test-instances | `.claude/skills/test-instances/` | pytest, with_instances, skip_instances, --s3, --postgres, CI, conftest, INSTANCE_MARKS |
