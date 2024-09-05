# Changelog

## 0.9.7 (2024-09-05)
- Add support for passing `inputs` for tasks returning multiple Tables and for `RawSql` tasks.

## 0.9.6 (2024-08-29)
- Support ExternalTableReference creation at flow wiring time. A pipedag `Table(ExternalTableReference(...))` object can
    be passed as a parameter into any task instead of any other pipedag table reference.
- Fixed bug that caused a crash when retrieving a polars dataframe from SQL using polars >= 1
- Fix warning about `ignore_position_hashes` being printed even if the flag was not set.
- Added support for `inputs` argument for `flow.run()` allowing to pass `ExternalTableReference`
    objects to the flow that override the outputs of selected tasks.

## 0.9.5 (2024-07-22)
- Fixed a bug in primary key generation when materializing pandas dataframe to postgres database

## 0.9.4 (2024-07-18)
- Primary key and index identifiers are now automatically truncated to 63 characters to avoid issues with some database systems.
- Added `ignore_position_hashes` option to `flow.run()` and `get_output_from_store()`. 
    If `True`, the position hashes of tasks are not checked when retrieving the inputs of a task from the cache.
    This can prevent caching errors when evaluating subgraphs. 
    For this to work a task may never be used more than once per stage.
- Fixed a bug related to imperative materialization

## 0.9.3 (2024-06-11)
- Added `upload_table()` and `download_table()` functions to the PandasTableHook to allow for easy 
    customization of up and download behavior of pandas and polars tables from/to the table store.
- More robust way of looking up hooks independent of import order. Subclasses of table stores don't 
    copy registered hooks in the moment of declaration. When registering a hook it is possible now, to 
    specify the hooks that are replaced by a new registration.

## 0.9.2 (2024-05-07)
- @input_stage_versions decorator allows specifying tasks which compare tables within the current stage transaction 
    schema and another version of that stage. This can be the currently active stage schema of the same pipeline 
    instance or from another instance. See: https://pydiversepipedag.readthedocs.io/en/latest/examples.html

## 0.9.1 (2024-04-26)
- Support Snowflake as a backend for `SQLTableStore`.
- For mssql backend, moved primary key adding after filling complete table.
- Make polars dematerialization robust against missing connectorx. Fall back to pandas if connectorx is not available.
- Fix some bugs with pandas < 2 and sqlalchemy < 2 compatibility as well as pyarrow handling.
- Use pd.StringDtype("pyarrow") instead of pd.ArrowDtype(pa.string()) for dtype "string[pyarrow]"

## 0.9.0 (2024-04-17)
- Support imperative materialization with `tbl_ref = dag.Table(...).materialize()`. This is particularly useful for 
    materializing subqueries within a task. It also helps see task in stack trace when materialization fails. There is
    one downside of using it: when a task returns multiple tables, it is assumed that all tables depend on previously 
    imperatively materialized tables.
- Support group nodes with or without barrier effect on task ordering. They either be added by `with GroupNode():` 
    blocks around or within `with Stage():` blocks. Or they can be added in configuration via
    `visualization: default: group_nodes: group_name: {label: "some_str", tasks: ["task_name"], stages: ["stage_name"]}`.
    Visualization of group nodes can be controlled very flexibly with hide_box, hide_content, box_color_always, ...
- ExternalTableReference moved module and is now also a member of pydiverse.pipedag module. This is a breaking 
    interface change for pipedag.
- PrefectEngine moved to module pydiverse.pipedag.engine.prefect.PrefectEngine because it would otherwise import prefect
    whenever it is installed in environment which messes with logging library initialization. This is a breaking 
    interface change.
- Fixed an edgecase for mssql backend causing queries with columns named "from" to crash. The code to insert an INTO into
    mssql SELECT statements is still hacky but supports open quote detection. Comments may still confuse the logic.

## 0.8.0 (2024-04-02)
- Significant refactoring of materialization is included. It splits creation of table from filling a table in many cases.
    This may lead to unexpected changes in log output. For now, the `INSERT INTO SELECT` statement is only printed in 
    shortened version, because the creation of the table already includes the same statement in full. In the future, this
    might be made configurable, so your feedback is highly welcome.
- pipedag.Table() now supports new parameters `nullable` and `non_nullable`. This allows specifying which columns are 
    nullable both as a positive and negative list. If both are specified, they must mention each column in the table and
    have no overlap. For most dialects, non-nullable statements are issued after creating the empty table. For dialects 
    `mssql` and `ibm_db2`, both nullable and non-nullable column alterations are issued because constant literals create
    non-nullable columns by default. If neither nullable nor non_nullable are specified, the default `CREATE TABLE as SELECT`
    is kept unmodified except for primary key columns where some dialects require explicit `NOT NULL` statements.
- Refactored configuration for cache validation options. Now, there is a separate section called cache_validation configurable
    per instance which includes the following options:
  * mode: NORMAL, ASSERT_NO_FRESH_INPUT (protect a stable pipeline / fail if tasks with cache function are executed), 
        IGNORE_FRESH_INPUT (same as ignore_cache_function=True before), 
        FORCE_FRESH_INPUT (invalidates all tasks with cache function), FORCE_CACHE_INVALID (rerun all tasks)
  * disable_cache_function: True disables the call of cache functions. Downside: next mode=NORMAL run will be cache invalid.
  * ignore_task_version: Option existed before but a level higher
  * REMOVED option ignore_cache_function: Use `cache_validation: mode: IGNORE_FRESH_INPUT` in pipedag.yaml or 
    `flow.run(cache_validation_mode=CacheValidationMode.IGNORE_FRESH_INPUT)` instead.
- Set transaction isolation level to READ UNCOMMITTED via SQLAlchemy functionality
- Fix that unlogged tables were created as logged tables when they were copied as cache valid
- Materialize lazy tasks, when they are executed without stage context.

## 0.7.2 (2024-03-25)
- Disable Kroki links by default. New setting disable_kroki=True allows to still default kroki_url to https://kroki.io.
    Function create_basic_pipedag_config() just has a kroki_url parameter which defaults to None.
- Added max_query_print_length parameter to MSSqlTableStore to limit the length of the printed SQL queries.
    Default is max_query_print_length=500000 characters.
- Fix bug when creating a table with the same name as a `Table` given by `ExternalTableReference` in the same stage  
- New config options for `SQLTableStore`:
  * `max_concurrent_copy_operations` to limit the number of concurrent copy operations when copying tables between schemas.
  * `sqlalchemy_pool_size` and `sqlalchemy_pool_timeout` to configure the pool size and timeout for the SQLAlchemy connection pool.
  * The defaults fix a bug by setting sqlalchemy options to not time out when the first cache invalid task in a stage triggers
    copying of cache valid tables between schemas and copying takes longer than 30s.

## 0.7.1 (2024-03-11)
- Fix bug when Reading DECIMAL(precision, scale) columns to pandas task (precision was interpreted like for Float where 
precision <= 24 leads to float32). Beware that ``isinstance(sa.Float(), sa.Numeric) == True``.

## 0.7.0 (2024-03-10)
- Rework `TableReference` support:
  * Rename `TableReference` to `ExternalTableReference`
  * Add support for `ExternalTableReference` to point to tables in external (i.e. not managed by `pipedag`) schemas. 
  * Remove support for `ExternalTableReference` that points to table in schema of current stage. I.e. `ExternalTableReference` can only point to tables in external schemas.
- Support code based configuration (see create_basic_pipedag_config() in README.md example without config file and without docker-compose)
- Added NoBlobStore in case you don't want to provide a directory that is created or needs to exist
- Fix polars import in `pyproject.toml` when using OS X with rosetta2
- Bug fix ibm_db2 backend:
  * input tables for SQL queries were not locked

## 0.6.10 (2024-02-29)
- Fix bug where a `Task` that was declared lazy but provided a `Table` without a query string would always be cache valid.
- Improved documentation

## 0.6.9 (2024-01-24)
- Update dependencies and remove some upper boundaries
- Polars dependency moved to >= 0.18.12 due to incompatible interface change
- Workaround for duckdb issue: https://github.com/duckdb/duckdb/issues/10322
- Workaround for prefect needing pytz dependency without declaring it on pypi

## 0.6.8 (2023-12-15)
- Bug fix ibm_db2 backend:
  * unspecified materialization_details was failing to load configuration 
- Bug fixes for mssql backend:
  * SELECT-INTO was invalid for keyword suffix labels: i.e. `SELECT 1 as prefix_FROM`
  * Raw SQL statements changing database link of connection via `USE` was causing pipedag generated commands to fail

## 0.6.7 (2023-12-05)
- increased metadata_version to 0.3.2 => please delete metadata with pipedag-manage when upgrading from <= 0.6.6 to >= 0.6.7
- Make separator customizable when splitting RawSql into statements.
- Add `DropNickname` for DB2 and drop nicknames when dropping schemas.
- Add debug function `materialize_table`.
- Update install instructions and dependencies to enable DB2 and mssql development on OS X with an `arm64` architecture.
- Update PR template
- Run `RUNSTATS` on every DB2 table after creation
- Add `materialization_details` as an option to `IBMDB2TableStore`. For now DB2 compression, DB2 table spaces are supported and Postgres `unlogged` tables are supported.
  - For Postgres `unlogged` tables this is a breaking change. The `unlogged_tables` option does not exist anymore. Instead, use `materialization_details: __any__: unlogged: true`.

Workaround for known Problems:
  - add materialization_details in configuration when using ibm_db2 database connection 

## 0.6.6 (2023-08-17)
- Implement support for loading polars dataframes from DuckDB.
- Accelerate storing of dataframes (pandas and polars) to DuckDB (10-100x speedup).
- Fix `TypeError` being raised when using pydiverse transform SQLTableImpl together with a local table cache. 

## 0.6.5 (2023-08-16)
- Implemented automatic versioning of tasks by setting task version to [](#AUTO_VERSION).
  This feature is currently only supported by Polars [`LazyFrame`](inv:pl#reference/lazyframe/index) and by Pandas.
- Added [](#kroki_url) config option.

## 0.6.4 (2023-08-07)
- Allow invocation of undecorated task functions when calling task object outside of flow definition context.
- Rename `ignore_fresh_input` to `ignore_cache_function`.
- Fix race condition leading to `JSONDecodeError` in [](#ParquetTableCache) when setting `store_input: true` together with the `DaskEngine`.
- Fix running subset of tasks not working due to tables and blobs being retrieved from wrong schema.

## 0.6.3 (2023-07-25)
- Fix crash during config initialization when using [](#DatabaseLockManager) together with `PostgreSQL`.

## 0.6.2 (2023-07-23)
- Switch back to using numpy nullable dtypes for Pandas as default.
- Ensure that indices get created in same schema as corresponding table (IBM Db2).
- Fix private method `SQLTableStore.get_stage_hash`.

## 0.6.1 (2023-07-19)
- Create initial documentation for pipedag.
- Remove stage argument from [](#RawSql) initializer.
- Add [](#RawSql) to public API.
- Fix [](#PrefectTwoEngine) failing on retrieval of results.
- Added [](#Flow.get_stage()), and [](#Stage.get_task()) methods.
- Added [](#MaterializingTask.get_output_from_store()) method to allow retrieval of task output without running the Flow.
- Created [TableReference](#ExternalTableReference) to simplify complex table loading operations.
- Allow for easy retrieval of tables generated by [](#RawSql). 
  Passing a RawSql object into a task results in all tables that were generated by the RawSql to be dematerialized.
  The tables can then be accessed using `raw_sql["table_name"]`.
  Alternatively, the same syntax can also be used during flow definition to only pass in a specific table. 
- Fix private method `SQLTableStore.get_stage_hash` not working for IBM DB2.

## 0.6.0 (2023-07-07)
- Added [`delete-schemas`](#reference/cli:delete-schemas) command to `pipedag-manage` to help with cleaning up database
- Remove all support for mssql database swapping. 
  Instead, we now properly support schema swapping.
- Fix UNLOGGED tables not working with Postgres.
- Added `hook_args` section to `table_store` part of config file to support passing config arguments to table hooks.
- Added `dtype_backend` hook argument for `PandasTableHook` to overriding the default pandas dtype backend to use.
- Update raw sql metadata table (`SQLTableStore`).
- Remove `engine_dispatch` and replace with SQLTableStore subclasses.
- Moved local table cache from `pydiverse.pipedag.backend.table_cache` to `pydiverse.pipedag.backend.table.cache` namespace.
- Changed order in which flow / instance config gets resolved.

## 0.5.0 (2023-06-28)
- add support for DuckDB
- add support for pyarrow backed pandas dataframes
- support execution of subflow
- store final state of task in flow result object
- tasks now have a `position_hash` associated with them to identify them purely based on their position (e.g. stage, name and input wiring) inside a flow.
- breaking change to metadata: added position_hash to `tasks` metadata table and change type of hash columns from String(32) to String(20).
- `Flow`, `Subflow`, and `Result` objects now provide additional options for visualizing them
- added `unlogged_tables` flag to SQLTableStore for creating UNLOGGED tables with Postgres.
- created [`pipedag-manage`](#reference/cli) command line utility with [`clear-metadata`](#reference/cli:clear-metadata) command to help with migrating between different pipedag metadata versions.

## 0.4.1 (2023-06-17)
- implement [](#DaskEngine): orchestration engine for running multiple tasks in parallel
- implement [](#DatabaseLockManager): lock manager based on locking mechanism provided by database

## 0.4.0 (2023-06-08)
- update public interface
- encrypt IPC communication
- remove preemptive `os.makedirs` from ParquetTableCache
- improve logging and provide structlog utilities

## 0.3.0 (2023-05-25)
- breaking change to pipedag.yaml:
  introduced `args` subsections for arguments that are passed to backend classes
- fix ibm_db_sa bug when copying dataframes from cache: uppercase table names by default
- nicer readable SQL queries: use automatic aliases for inputs of SQLAlchemy tasks
- implement option ignore_task_version: disable eager task caching for some instances to reduce overhead from task version bumping
- implement local table cache: store input/output of dataframe tasks in parquet files and allow using it as cache to avoid rereading from database

## 0.2.4 (2023-05-05)
- fix errors by increasing output_json length in metadata table
- fix cache invalidation: query normalization before checking for changes
- add rudimentary support for ibis tasks (postgres + mssql)
- add rudimentary support for polars + tidypolars tasks
- implemented pandas type mapping to avoid row wise type checks of object columns
- support pandas 2.0 (no arrow features used that)
- support sqlalchemy 2.0 (except for with polars)

## 0.2.3 (2023-04-17)
- fixed python 3.9 compatibility (`traceback.format_exception` syntax changed)
- fixed deferred table copy when task is invalid (introduced with 0.2.2)
- fixed mssql to not reflect full schema while renamings happen
- fixed clearing of metadata tables for lazy tables and raw sql tables
- fixed mssql synonym resolution when reading input table for pandas task
- initial implementation of issue #62: make query canonical before hashing
- retry some DB calls in case they are aborted as deadlock victim

## 0.2.2 (2023-03-31)
- added option `avoid_drop_create_schema` to table store configuration
- improve performance when working with IBM DB2 dialect (i.e. table locking)
- prevent table copying and schema swapping for 100% cache valid stages

## 0.2.1 (2023-01-15)
- removed contextvars dependency (not needed for python >= 3.7 and broke conda-forge build)

## 0.2.0 (2023-01-14)
- `SQLTableStore`: support for Microsoft SQL Server and IBM DB2 (Linux) database connection strings
- Support primary keys and indexes (can be configured with Table object and used in custom RawSql code)
- `RawSql`: support additional return type for `@materialize` tasks which allows to emit raw SQL string including multiple create statements (currently, views/functions/procedures are only supported for dialect mssql). 
   This feature should only be used for getting legacy code running in pipedag before converting it to programatically generated or manual SELECT statements.
- Support pytsql library for executing raw SQL scripts with `dialect=mssql` (i.e. supports PRINT capture)
- Manual Cache Invalidation for source nodes:
  `@materialize(cache=f)` parameter can take an arbitrary function that gets the same arguments as the task function and returns a hash. 
  If the hash is different from for the previous run, the task is considered cache invalid.
- New configuration file format pipedag.yaml can be used to configure multiple pipedag instances: see docs/reference/config.rst

## 0.1.0 (2022-09-01)
Initial release.

- `@materialize` annotations
- flow definition with nestable stages
- zookeeper synchronization
- postgres database backend
- Prefect 1.x and 2.x support
- multi-processing/multi-node support for state exchange between `@materialize` tasks
- support materialization for: pandas, sqlalchemy, raw sql text, pydiverse.transform
