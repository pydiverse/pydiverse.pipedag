---
name: parquet-backend-metastore
description: >
  Rules for working on ParquetTableStore and its metadata synchronization layer.
  Use when touching duckdb_parquet.py, sync_views, metadata_store, schema_prefix,
  or S3/GCS/MinIO integration.
---

# ParquetTableStore & Metadata Sync

The `ParquetTableStore` (in `backend/table/sql/dialects/duckdb_parquet.py`) materializes tables as parquet files (local or S3/GCS) and maintains a DuckDB file with views pointing to them.

## Key design rules

- `stage.name` and `stage.transaction_name` are **bare names** (e.g. `"stage_1"`, `"stage_1__odd"`)
- `self.get_schema(name).get()` applies `schema_prefix`/`schema_suffix` to produce the **full schema name** used in DuckDB and in the `sync_views` metadata table
- The `sync_views` table in the relational metadata store tracks views with **full (prefixed) schema names** — always use `self.get_schema(...).get()` before querying or writing to `sync_views`
- `_get_read_view_transaction_name()` and `_get_read_view_original_transaction_name()` return **bare** names — prefix them before metadata operations
- `metadata_sync_views()`, `metadata_track_view()`, `metadata_track_view_flush()`, `metadata_track_view_drop()` all expect **full schema names**
- DuckDB operations (CREATE/DROP VIEW, duckdb_views() queries) use **full schema names**
- The metadata store connection (`self.metadata_store.engine_connect()`) connects to the relational DB (e.g. PostgreSQL); `self.engine_connect()` connects to DuckDB — never execute DuckDB DDL on the metadata connection

## Multi-user sync model

The `sync_views` table (in the relational metadata store) tracks which user created which view:
- Columns: `schema` (PK), `view_name` (PK), `user_id` (PK), `target`, `target_type`, `obsolete`
- Each DuckDB file has a `user_id` (UUID) stored in a local `user_id` table
- View creation marks other users' entries as obsolete; `metadata_sync_views()` reconciles
