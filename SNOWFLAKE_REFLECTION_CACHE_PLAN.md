# Snowflake Reflection Cache With Transaction-To-Final Transfer

## Summary

- Keep the feature in `src/pydiverse/pipedag/backend/table/sql/dialects/snowflake.py`.
- Do not change `src/pydiverse/pipedag/backend/table/sql/hooks.py`; all callers continue to use `store.reflect_table(...)`.
- Key the cache by concrete schema strings from `self.get_schema(...).get()`.
- Use the transaction schema cache while a stage is running.
- On `commit_stage`, replace the final-schema cache with the transaction-schema cache.

## Stage Semantics And Cache Keys

- `stage.name`
  - Logical committed stage name.
  - Final schema key is `self.get_schema(stage.name).get()`.
- `stage.transaction_name`
  - Physical writable schema for the current run.
  - Transaction schema key is `self.get_schema(stage.transaction_name).get()`.
- `stage.current_name`
  - Before commit: equals `stage.transaction_name`.
  - After commit: equals `stage.name`.
- Cache rule
  - Never key by `stage.current_name`, because it changes across the lifecycle.
  - Always compute explicit keys:
    - `final_schema_key = self.get_schema(stage.name).get()`
    - `transaction_schema_key = self.get_schema(stage.transaction_name).get()`

## Functions To Change

- In `src/pydiverse/pipedag/backend/table/sql/dialects/snowflake.py`
  - Change `SnowflakeTableStore.reflect_table()`
    - Replace `sa.Table(..., autoload_with=...)`.
    - Read or warm a schema-level cache.
    - Build a fresh `sa.Table` from cached column specs.
    - If a warm cache does not contain the requested table, reload that schema once and retry.
  - Add `SnowflakeTableStore.init_stage()`
    - Call `super().init_stage(stage)` first.
    - Clear only the transaction-schema cache entry for the newly initialized transaction schema.
    - Do not touch the final-schema cache.
  - Add `SnowflakeTableStore.commit_stage()`
    - Compute final and transaction schema keys before calling `super().commit_stage(stage)`.
    - Call `super().commit_stage(stage)`.
    - Clear the final-schema cache entry.
    - Transfer the transaction-schema cache payload to the final-schema key.
  - Add private helpers:
    - `_reflection_cache_key(schema: str) -> tuple`
    - `_clear_reflection_cache_for_schema(schema: str)`
    - `_replace_reflection_cache(from_schema: str, to_schema: str, *, keep_source: bool)`
    - `_load_schema_reflection(schema: str)`
    - `_build_reflected_table(schema: str, table_name: str, column_specs)`
    - `_sa_type_from_info_schema_row(row)`

## Precise Lifecycle Behavior

- `init_stage(stage)`
  - Base behavior may change `stage.transaction_name` for `READ_VIEWS`, so the Snowflake override must call `super().init_stage(stage)` first.
  - After that, compute:
    - `transaction_schema_key = self.get_schema(stage.transaction_name).get()`
  - Clear only that key.
  - Reason: the transaction schema is freshly recreated for the new stage run, while the final schema remains valid until commit.

- During stage execution
  - Materialization writes to `stage.transaction_name`.
  - Retrieval before commit resolves through `stage.current_name`, which still points to `stage.transaction_name`.
  - Therefore `reflect_table()` naturally uses and warms the transaction-schema cache.

- `commit_stage(stage)`
  - Before commit, compute:
    - `transaction_schema_key = self.get_schema(stage.transaction_name).get()`
    - `final_schema_key = self.get_schema(stage.name).get()`
  - Call `super().commit_stage(stage)`.
  - After successful commit:
    - Clear the final-schema cache entry first.
    - Then transfer the transaction-schema cache payload to the final-schema key.
  - Transfer mode by commit technique:
    - `SCHEMA_SWAP`
      - Move payload from transaction key to final key and delete the transaction key.
    - `READ_VIEWS`
      - Copy payload from transaction key to final key and keep the transaction key.
  - Result:
    - After commit, retrieval via `stage.current_name == stage.name` uses the final-schema cache.
    - For `READ_VIEWS`, the transaction cache remains available because the physical tables still live there.

## How `autoload_with` Is Replaced

- Old path
  - `sa.Table(table_name, sa.MetaData(), schema=..., autoload_with=conn_or_engine)`
- New path
  - Query `INFORMATION_SCHEMA.COLUMNS` once for the whole schema.
  - Cache normalized metadata as:
    - `{table_name: [column_spec, ...]}`
  - Build fresh tables on demand:
    - `sa.Table(table_name, sa.MetaData(), *(sa.Column(...) ...), schema=schema)`
- Important detail
  - Cache only metadata, never `sa.Table` objects.

## Test Plan

- Add tests in `tests/test_sql_dialect/test_snowflake.py`.
- Test `init_stage()` behavior
  - Seed cache for both final and transaction schema keys.
  - Run `init_stage()`.
  - Assert only the transaction-schema key is cleared.
- Test `commit_stage()` for `SCHEMA_SWAP`
  - Seed final and transaction cache entries with different payloads.
  - Run `commit_stage()`.
  - Assert final-schema cache was replaced by transaction payload and transaction key was removed.
- Test `commit_stage()` for `READ_VIEWS`
  - Seed final and transaction cache entries with different payloads.
  - Run `commit_stage()`.
  - Assert final-schema cache was replaced by transaction payload and transaction key was preserved.
- Test schema cache reload-on-miss
  - Warm a schema cache.
  - Add a new table later in the same transaction schema.
  - `reflect_table()` should reload once when that table is missing.
- Test `database.schema` handling
  - Ensure cache keys and returned `tbl.schema` preserve the fully qualified schema string.

## Assumptions

- The final schema cache remains valid for the whole stage until `commit_stage()`.
- On commit, the final-schema cache should always be replaced, never merged.
- Snowflake reflected tables only need query-building fidelity, column order, names, nullability, and type information used by current hooks.
- Constraint/index/default reflection is intentionally out of scope.
